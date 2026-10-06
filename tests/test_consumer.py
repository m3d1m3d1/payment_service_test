"""Логика retry/DLQ в обработчике сообщений (без реального брокера)."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from app import consumer
from app.processing import PaymentNotFound
from app.messaging import EXCHANGE, retry_routing_key


@pytest.fixture
def env(monkeypatch):
    publish = AsyncMock()
    monkeypatch.setattr(consumer.broker, "publish", publish)
    monkeypatch.setattr(consumer, "http_client", MagicMock())
    return publish


def make_message(headers=None):
    msg = MagicMock()
    msg.headers = headers or {}
    msg.ack, msg.nack, msg.reject = AsyncMock(), AsyncMock(), AsyncMock()
    return msg


def handler():
    # FastStream оборачивает функцию; достаем исходную
    h = consumer.handle_new_payment
    return getattr(h, "_original_call", h)


async def test_success_acks(env, monkeypatch):
    monkeypatch.setattr(consumer, "process_payment", AsyncMock())
    msg = make_message()
    await handler()({"payment_id": str(uuid4())}, msg)
    msg.ack.assert_awaited_once()
    env.assert_not_awaited()


@pytest.mark.parametrize("attempt", [1, 2])
async def test_failure_schedules_delayed_retry(env, monkeypatch, attempt):
    monkeypatch.setattr(consumer, "process_payment", AsyncMock(side_effect=RuntimeError))
    msg = make_message({"x-attempt": attempt})
    body = {"payment_id": str(uuid4())}
    await handler()(body, msg)

    kwargs = env.await_args.kwargs
    assert kwargs["exchange"] == EXCHANGE
    assert kwargs["routing_key"] == retry_routing_key(attempt)
    assert kwargs["headers"] == {"x-attempt": attempt + 1}
    msg.ack.assert_awaited_once()
    msg.reject.assert_not_awaited()


async def test_third_failure_goes_to_dlq(env, monkeypatch):
    monkeypatch.setattr(consumer, "process_payment", AsyncMock(side_effect=RuntimeError))
    msg = make_message({"x-attempt": 3})
    await handler()({"payment_id": str(uuid4())}, msg)
    msg.reject.assert_awaited_once()
    msg.ack.assert_not_awaited()
    env.assert_not_awaited()


@pytest.mark.parametrize("body", [{}, {"payment_id": "not-a-uuid"}, {"payment_id": None}])
async def test_malformed_message_goes_to_dlq(env, body):
    msg = make_message()
    await handler()(body, msg)
    msg.reject.assert_awaited_once()


async def test_garbage_attempt_header_is_treated_as_first_attempt(env, monkeypatch):
    monkeypatch.setattr(consumer, "process_payment", AsyncMock(side_effect=RuntimeError))
    msg = make_message({"x-attempt": "garbage"})
    await handler()({"payment_id": str(uuid4())}, msg)
    assert env.await_args.kwargs["headers"] == {"x-attempt": 2}


async def test_missing_payment_goes_straight_to_dlq_without_retry(env, monkeypatch):
    monkeypatch.setattr(consumer, "process_payment", AsyncMock(side_effect=PaymentNotFound("x")))
    msg = make_message()
    await handler()({"payment_id": str(uuid4())}, msg)
    msg.reject.assert_awaited_once()
    env.assert_not_awaited()
