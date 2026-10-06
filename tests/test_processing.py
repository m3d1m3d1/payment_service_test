from uuid import UUID, uuid4

import httpx
import pytest

from app.config import Settings
from app.db import SessionLocal
from app.models import Payment
from app.processing import PaymentNotFound, process_payment
from tests.conftest import BODY


def make_settings(success_rate: float) -> Settings:
    return Settings(
        processing_min_seconds=0, processing_max_seconds=0, processing_success_rate=success_rate
    )


class FakeHttp:
    def __init__(self, status: int = 200):
        self.status = status
        self.calls: list[dict] = []

    async def post(self, url, json=None, headers=None):
        self.calls.append({"url": url, "json": json, "headers": headers})
        return httpx.Response(self.status, request=httpx.Request("POST", url))


async def _create(client) -> str:
    r = await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k"})
    return r.json()["payment_id"]


async def _load(payment_id: str) -> Payment:
    async with SessionLocal() as s:
        return await s.get(Payment, UUID(payment_id))


async def test_success_updates_status_and_sends_webhook(client):
    pid = await _create(client)
    http = FakeHttp()
    await process_payment(UUID(pid), make_settings(1.0), SessionLocal, http)

    p = await _load(pid)
    assert p.status == "succeeded" and p.processed_at and p.webhook_sent_at
    assert len(http.calls) == 1
    call = http.calls[0]
    assert call["url"] == BODY["webhook_url"]
    assert call["json"]["status"] == "succeeded" and call["json"]["payment_id"] == pid
    assert call["headers"]["X-Event-Id"] == call["json"]["event_id"]


async def test_gateway_failure_is_a_final_status_with_webhook(client):
    pid = await _create(client)
    http = FakeHttp()
    await process_payment(UUID(pid), make_settings(0.0), SessionLocal, http)

    assert (await _load(pid)).status == "failed"
    assert http.calls[0]["json"]["status"] == "failed"


async def test_webhook_error_raises_and_redelivery_only_resends_webhook(client):
    pid = await _create(client)

    with pytest.raises(httpx.HTTPStatusError):
        await process_payment(UUID(pid), make_settings(1.0), SessionLocal, FakeHttp(500))
    p = await _load(pid)
    assert p.status == "succeeded" and p.webhook_sent_at is None

    # Повторная доставка: шлюз НЕ эмулируется заново (даже при success_rate=0)
    http = FakeHttp()
    await process_payment(UUID(pid), make_settings(0.0), SessionLocal, http)
    p = await _load(pid)
    assert p.status == "succeeded" and p.webhook_sent_at is not None
    assert len(http.calls) == 1


async def test_already_delivered_is_skipped(client):
    pid = await _create(client)
    await process_payment(UUID(pid), make_settings(1.0), SessionLocal, FakeHttp())

    http = FakeHttp()
    await process_payment(UUID(pid), make_settings(1.0), SessionLocal, http)
    assert http.calls == []


async def test_unknown_payment_raises_not_found():
    http = FakeHttp()
    with pytest.raises(PaymentNotFound):
        await process_payment(uuid4(), make_settings(1.0), SessionLocal, http)
    assert http.calls == []
