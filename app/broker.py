import json
from typing import Any

from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitMessage, RabbitQueue

from app.config import get_settings


def decode_event(message: RabbitMessage) -> Any:
    try:
        return json.loads(message.body)
    except (ValueError, UnicodeError, RecursionError):
        return None


exchange = RabbitExchange("payments", durable=True)
dead_exchange = RabbitExchange("payments.dead", durable=True)
queue = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "payments.dead",
        "x-dead-letter-routing-key": "payments.dlq",
    },
)
dead_queue = RabbitQueue("payments.dlq", durable=True)
broker = RabbitBroker(
    get_settings().rabbitmq_url,
    graceful_timeout=15,
    decoder=decode_event,
    default_channel=Channel(publisher_confirms=True, on_return_raises=True, prefetch_count=1),
)


async def declare_topology():
    main = await broker.declare_exchange(exchange)
    dead = await broker.declare_exchange(dead_exchange)
    q = await broker.declare_queue(queue)
    dlq = await broker.declare_queue(dead_queue)
    await q.bind(main, routing_key="payments.new")
    await dlq.bind(dead, routing_key="payments.dlq")
