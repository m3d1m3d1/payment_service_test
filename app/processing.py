"""Бизнес-логика consumer'а: эмуляция шлюза, обновление статуса, webhook."""

import asyncio
import logging
import random
import uuid
from uuid import UUID

import httpx
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import Settings
from app.models import Payment, utcnow
from app.schemas import PaymentStatus

logger = logging.getLogger("processing")


class PaymentNotFound(Exception):
    """Событие ссылается на несуществующий платеж (повтор бессмысленен)."""


async def _emulate_gateway(settings: Settings) -> PaymentStatus:
    await asyncio.sleep(
        random.uniform(settings.processing_min_seconds, settings.processing_max_seconds)
    )
    ok = random.random() < settings.processing_success_rate
    return PaymentStatus.SUCCEEDED if ok else PaymentStatus.FAILED


def _event_id(payment_id: UUID) -> str:
    """Стабильный id события: одинаков при всех повторных отправках."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"payment.processed:{payment_id}"))


def _webhook_payload(p: Payment) -> dict:
    return {
        "event": "payment.processed",
        "event_id": _event_id(p.id),
        "payment_id": str(p.id),
        "status": p.status,
        "amount": str(p.amount),
        "currency": p.currency,
        "description": p.description,
        "metadata": p.meta,
        "created_at": p.created_at.isoformat(),
        "processed_at": p.processed_at.isoformat() if p.processed_at else None,
    }


async def process_payment(
    payment_id: UUID,
    settings: Settings,
    session_factory: async_sessionmaker,
    http: httpx.AsyncClient,
) -> None:
    """Идемпотентная обработка: безопасна при повторной доставке сообщения.

    1. Если платеж ещё pending — эмулируем шлюз и фиксируем итоговый статус
       (UPDATE ... WHERE status='pending' защищает от двойной обработки).
    2. Если webhook ещё не доставлен — отправляем (at-least-once: возможен повтор,
       если процесс упал после ответа получателя, поэтому есть event_id). Любая ошибка отправки
       пробрасывается наверх, и сообщение уходит на повтор.
    """
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    if payment is None:
        raise PaymentNotFound(str(payment_id))

    if payment.status == PaymentStatus.PENDING:
        new_status = await _emulate_gateway(settings)
        async with session_factory() as session, session.begin():
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                .values(status=new_status.value, processed_at=utcnow())
            )
        logger.info("Payment %s -> %s", payment_id, new_status)

    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    if payment.webhook_sent_at is not None:
        logger.info("Webhook for %s already delivered, skipping", payment_id)
        return

    # Доставка at-least-once: получатель дедуплицирует по event_id / X-Event-Id
    payload = _webhook_payload(payment)
    response = await http.post(
        payment.webhook_url, json=payload, headers={"X-Event-Id": payload["event_id"]}
    )
    response.raise_for_status()  # не-2xx считается ошибкой доставки

    async with session_factory() as session, session.begin():
        await session.execute(
            update(Payment).where(Payment.id == payment_id).values(webhook_sent_at=utcnow())
        )
    logger.info("Webhook for %s delivered", payment_id)
