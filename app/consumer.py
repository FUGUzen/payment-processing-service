import asyncio
import logging
from typing import Any

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as DatabaseTimeout

from app.broker import broker, declare_topology, exchange, queue
from app.db import engine
from app.schemas import PaymentEvent
from app.services.outbox import run_outbox
from app.services.processing import InvalidPaymentEvent, process_payment, schedule_failure

logger = logging.getLogger(__name__)
app = FastStream(broker)
client = httpx.AsyncClient(trust_env=False)
outbox_task: asyncio.Task | None = None


@broker.subscriber(queue, exchange, no_ack=True)
async def handle_payment(body: Any, message: RabbitMessage):
    try:
        event = PaymentEvent.model_validate(body)
    except ValidationError:
        logger.warning("invalid_payment_event")
        await message.reject(requeue=False)
        return
    try:
        await process_payment(event, client)
    except InvalidPaymentEvent:
        logger.warning("invalid_payment_event")
        await message.reject(requeue=False)
    except (OperationalError, InterfaceError, DatabaseTimeout, ConnectionError):
        # Database outages cannot persist a retry. Requeue without consuming the retry budget.
        logger.exception("payment_processing_unavailable")
        await asyncio.sleep(1)
        await message.nack(requeue=True)
    except Exception as exc:
        logger.exception("payment_processing_failed")
        try:
            await schedule_failure(event, exc)
        except Exception:
            logger.exception("retry_schedule_unavailable")
            await asyncio.sleep(1)
            await message.nack(requeue=True)
        else:
            await message.ack()
    else:
        await message.ack()


@app.on_startup
async def setup_broker():
    await broker.connect()
    await declare_topology()


@app.after_startup
async def start_outbox():
    global outbox_task
    outbox_task = asyncio.create_task(run_outbox())


@app.on_shutdown
async def shutdown():
    if outbox_task:
        outbox_task.cancel()
        try:
            await outbox_task
        except asyncio.CancelledError:
            pass


@app.after_shutdown
async def close_resources():
    await client.aclose()
    await engine.dispose()
