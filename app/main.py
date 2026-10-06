import asyncio
import hashlib
import json
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import SessionLocal, engine, get_session
from app.messaging import create_broker, declare_topology
from app.models import OutboxEvent, Payment
from app.outbox import run_relay
from app.schemas import PaymentAccepted, PaymentCreate, PaymentOut, PaymentStatus
from app.security import verify_api_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

settings = get_settings()
broker = create_broker(settings.rabbitmq_url)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await broker.start()
    await declare_topology(broker, settings)
    stop = asyncio.Event()
    relay = asyncio.create_task(run_relay(broker, SessionLocal, settings, stop))
    try:
        yield
    finally:
        stop.set()
        await relay
        await broker.stop()
        await engine.dispose()


app = FastAPI(title="Payment processing service", lifespan=lifespan)
router = APIRouter(prefix="/api/v1", dependencies=[Depends(verify_api_key)])

SessionDep = Annotated[AsyncSession, Depends(get_session)]


def _request_hash(body: PaymentCreate) -> str:
    canonical = json.dumps(
        {
            "amount": f"{body.amount:.2f}",  # 100.5 и 100.50 — один и тот же запрос
            "currency": body.currency.value,
            "description": body.description,
            "metadata": body.metadata,
            "webhook_url": str(body.webhook_url),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _replay(existing: Payment, request_hash: str) -> Payment:
    """Тот же ключ и то же тело -> прежний результат; иначе 409."""
    if existing.request_hash != request_hash:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "Idempotency-Key was already used with a different request body",
        )
    return existing


async def _get_by_idempotency_key(session: AsyncSession, key: str) -> Payment | None:
    return (
        await session.execute(select(Payment).where(Payment.idempotency_key == key))
    ).scalar_one_or_none()


@router.post(
    "/payments",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
)
async def create_payment(
    body: PaymentCreate,
    session: SessionDep,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ],
) -> Payment:
    request_hash = _request_hash(body)
    # Повторный запрос с тем же ключом возвращает уже созданный платеж
    if existing := await _get_by_idempotency_key(session, idempotency_key):
        return _replay(existing, request_hash)

    payment = Payment(
        id=uuid.uuid4(),
        amount=body.amount,
        currency=body.currency.value,
        description=body.description,
        meta=body.metadata,
        status=PaymentStatus.PENDING.value,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        webhook_url=str(body.webhook_url),
    )
    # Платеж и событие пишутся в ОДНОЙ транзакции (Outbox pattern).
    # Нарушение уникальности может сработать на любом шаге INSERT/COMMIT,
    # поэтому вся запись находится внутри try.
    try:
        session.add(payment)
        session.add(
            OutboxEvent(event_type="payment.new", payload={"payment_id": str(payment.id)})
        )
        await session.commit()
    except IntegrityError:
        # Гонка: параллельный запрос с тем же ключом успел вставить запись раньше
        await session.rollback()
        if existing := await _get_by_idempotency_key(session, idempotency_key):
            return _replay(existing, request_hash)
        raise
    return payment


@router.get("/payments/{payment_id}", response_model=PaymentOut)
async def get_payment(payment_id: UUID, session: SessionDep) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return payment


app.include_router(router)
