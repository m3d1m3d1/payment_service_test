"""Outbox relay: читает неопубликованные события из БД и публикует в RabbitMQ.

Событие помечается опубликованным только после подтверждения брокера
(publisher confirms), в одной транзакции с блокировкой FOR UPDATE SKIP LOCKED,
поэтому несколько экземпляров relay не публикуют одно событие дважды
одновременно. Гарантия доставки — at-least-once; consumer идемпотентен.
"""

import asyncio
import logging

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import Settings
from app.messaging import EXCHANGE, NEW_ROUTING_KEY
from app.models import OutboxEvent, utcnow

logger = logging.getLogger("outbox")


async def publish_pending(
    broker: RabbitBroker, session_factory: async_sessionmaker, batch_size: int
) -> int:
    async with session_factory() as session, session.begin():
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        events = (await session.execute(stmt)).scalars().all()
        for event in events:
            await broker.publish(
                event.payload,
                exchange=EXCHANGE,
                routing_key=NEW_ROUTING_KEY,
                persist=True,
                message_id=str(event.id),
                headers={"x-attempt": 1},
            )
            event.published_at = utcnow()
        return len(events)


async def run_relay(
    broker: RabbitBroker,
    session_factory: async_sessionmaker,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    logger.info("Outbox relay started")
    while not stop.is_set():
        try:
            published = await publish_pending(
                broker, session_factory, settings.outbox_batch_size
            )
            if published:
                logger.info("Published %d outbox event(s)", published)
            if published >= settings.outbox_batch_size:
                continue  # возможно, есть ещё
        except Exception:
            logger.exception("Outbox publish failed, will retry")
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval)
        except TimeoutError:
            pass
    logger.info("Outbox relay stopped")
