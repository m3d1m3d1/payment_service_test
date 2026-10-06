import pytest
from sqlalchemy import select

from app.db import SessionLocal
from app.messaging import EXCHANGE, NEW_ROUTING_KEY, create_broker
from app.models import OutboxEvent
from app.outbox import publish_pending
from tests.conftest import BODY


class FakeBroker:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.published: list[tuple[dict, dict]] = []

    async def publish(self, payload, **kwargs):
        if self.fail:
            raise ConnectionError("rabbit is down")
        self.published.append((payload, kwargs))


async def _unpublished() -> int:
    async with SessionLocal() as s:
        rows = (await s.execute(select(OutboxEvent))).scalars().all()
    return sum(1 for r in rows if r.published_at is None)


def test_broker_raises_for_unroutable_publications(monkeypatch):
    from app import messaging

    captured: dict = {}

    def broker_factory(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(messaging, "RabbitBroker", broker_factory)
    create_broker("amqp://localhost")

    assert captured["url"] == "amqp://localhost"
    assert captured["default_channel"].on_return_raises is True


async def test_publish_marks_events_and_is_not_repeated(client):
    r = await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k"})
    broker = FakeBroker()

    assert await publish_pending(broker, SessionLocal, 10) == 1
    payload, kwargs = broker.published[0]
    assert payload == {"payment_id": r.json()["payment_id"]}
    assert kwargs["exchange"] == EXCHANGE and kwargs["routing_key"] == NEW_ROUTING_KEY
    assert kwargs["persist"] is True and kwargs["headers"] == {"x-attempt": 1}

    assert await publish_pending(broker, SessionLocal, 10) == 0
    assert len(broker.published) == 1


async def test_failed_publish_keeps_event_for_next_attempt(client):
    await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k"})

    with pytest.raises(ConnectionError):
        await publish_pending(FakeBroker(fail=True), SessionLocal, 10)
    assert await _unpublished() == 1  # событие не потеряно

    healthy = FakeBroker()
    assert await publish_pending(healthy, SessionLocal, 10) == 1  # брокер вернулся
    assert await _unpublished() == 0
