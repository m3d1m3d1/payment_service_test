"""Топология RabbitMQ: общая для API (outbox relay) и consumer.

payments (direct) --payments.new--> payments.new
                                       |  (после 3-й неудачи reject)
                                       v
payments.dlx (direct) --payments.dlq--> payments.dlq

Повторные попытки: payments.new.retry.N (TTL = base * 2^(N-1)),
по истечении TTL сообщение dead-letter'ится обратно в payments.new.
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import Channel

from app.config import Settings

NEW_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.dlq"

EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

MAIN_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX.name,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)
DLQ = RabbitQueue("payments.dlq", durable=True, routing_key=DLQ_ROUTING_KEY)


def create_broker(url: str) -> RabbitBroker:
    return RabbitBroker(url, default_channel=Channel(on_return_raises=True))


def retry_routing_key(attempt: int) -> str:
    """Routing key очереди задержки после неудачной попытки №attempt."""
    return f"payments.new.retry.{attempt}"


def retry_queue(attempt: int, settings: Settings) -> RabbitQueue:
    delay_ms = settings.retry_base_delay_ms * 2 ** (attempt - 1)
    return RabbitQueue(
        retry_routing_key(attempt),
        durable=True,
        routing_key=retry_routing_key(attempt),
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": EXCHANGE.name,
            "x-dead-letter-routing-key": NEW_ROUTING_KEY,
        },
    )


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    """Идемпотентно объявляет обменники, очереди и биндинги."""
    exchange = await broker.declare_exchange(EXCHANGE)
    dlx = await broker.declare_exchange(DLX)

    main = await broker.declare_queue(MAIN_QUEUE)
    await main.bind(exchange, routing_key=NEW_ROUTING_KEY)

    dlq = await broker.declare_queue(DLQ)
    await dlq.bind(dlx, routing_key=DLQ_ROUTING_KEY)

    for attempt in range(1, settings.max_attempts):
        queue = retry_queue(attempt, settings)
        declared = await broker.declare_queue(queue)
        await declared.bind(exchange, routing_key=retry_routing_key(attempt))
