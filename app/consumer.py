"""Consumer платежей. Запуск: faststream run app.consumer:app"""

import logging
from uuid import UUID

import httpx
from faststream import FastStream
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.rabbit import RabbitMessage

from app.config import get_settings
from app.db import SessionLocal, engine
from app.messaging import (
    EXCHANGE,
    MAIN_QUEUE,
    create_broker,
    declare_topology,
    retry_routing_key,
)
from app.processing import PaymentNotFound, process_payment

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("consumer")

settings = get_settings()
broker = create_broker(settings.rabbitmq_url)
app = FastStream(broker)

http_client: httpx.AsyncClient | None = None


@app.on_startup
async def setup() -> None:
    global http_client
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
    await broker.connect()
    await declare_topology(broker, settings)


@app.after_shutdown
async def teardown() -> None:
    if http_client:
        await http_client.aclose()
    await engine.dispose()


def _attempt(message: RabbitMessage) -> int:
    try:
        return max(1, int((message.headers or {}).get("x-attempt", 1)))
    except (TypeError, ValueError):
        return 1


@broker.subscriber(MAIN_QUEUE, EXCHANGE, ack_policy=AckPolicy.MANUAL)
async def handle_new_payment(body: dict, message: RabbitMessage) -> None:
    attempt = _attempt(message)

    try:
        payment_id = UUID(str(body["payment_id"]))
    except (KeyError, ValueError, TypeError):
        logger.error("Malformed message %r -> DLQ", body)
        await message.reject()  # requeue=False -> DLX -> payments.dlq
        return

    try:
        assert http_client is not None
        await process_payment(payment_id, settings, SessionLocal, http_client)
    except PaymentNotFound:
        logger.error("Payment %s does not exist -> DLQ (no retry)", payment_id)
        await message.reject()
        return
    except Exception as exc:
        logger.warning(
            "Attempt %d/%d failed for payment %s: %r",
            attempt, settings.max_attempts, payment_id, exc,
        )
        if attempt >= settings.max_attempts:
            logger.error("Payment %s exhausted retries -> DLQ", payment_id)
            await message.reject()
            return
        try:
            # Очередь задержки: TTL растёт экспоненциально (2с, 4с, ...)
            await broker.publish(
                body,
                exchange=EXCHANGE,
                routing_key=retry_routing_key(attempt),
                headers={"x-attempt": attempt + 1},
                persist=True,
            )
        except Exception:
            logger.exception("Could not schedule retry, requeueing message")
            await message.nack()
            return

    await message.ack()
