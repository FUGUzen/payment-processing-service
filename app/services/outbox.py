import asyncio
import logging
from datetime import UTC, datetime

from app.broker import broker, dead_exchange, exchange
from app.config import get_settings
from app.db import Session
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)
OUTBOX_BATCH_SIZE = 50


async def publish_batch() -> int:
    async with Session() as session, session.begin():
        repository = OutboxRepository(session)
        events = await repository.get_ready_for_update(limit=OUTBOX_BATCH_SIZE)
        for event in events:
            await broker.publish(
                event.payload,
                exchange=dead_exchange if event.routing_key == "payments.dlq" else exchange,
                routing_key=event.routing_key,
                persist=True,
                mandatory=True,
                message_id=str(event.id),
                timeout=10,
            )
            repository.mark_published(event, datetime.now(UTC))
        return len(events)


async def run_outbox():
    while True:
        try:
            count = await publish_batch()
        except Exception:
            logger.exception("outbox_publish_failed")
            count = 0
        await asyncio.sleep(
            0 if count == OUTBOX_BATCH_SIZE else get_settings().outbox_poll_interval
        )
