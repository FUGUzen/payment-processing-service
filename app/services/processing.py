import logging
from datetime import UTC, datetime, timedelta

import httpx

from app.config import Settings, get_settings
from app.db import Session
from app.integrations.gateway import charge
from app.integrations.webhooks import send_webhook
from app.models import Outbox, Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)


class InvalidPaymentEvent(Exception):
    pass


async def process_payment(event: PaymentEvent, client: httpx.AsyncClient):
    settings = get_settings()
    # Lock only this payment; a duplicate delivery must not repeat a completed charge.
    async with Session() as session, session.begin():
        payment = await PaymentRepository(session).get_for_update(event.payment_id)
        if payment is None:
            raise InvalidPaymentEvent("Unknown payment")
        if payment.webhook_delivered_at or event.attempt <= payment.delivery_attempts:
            return
        if event.attempt != payment.delivery_attempts + 1:
            raise InvalidPaymentEvent("Unexpected attempt number")
        if payment.status == "pending":
            payment.status = await charge()
            payment.processed_at = datetime.now(UTC)

    async with Session() as session, session.begin():
        payment = await PaymentRepository(session).get_for_update(event.payment_id)
        if payment.webhook_delivered_at or event.attempt <= payment.delivery_attempts:
            return
        try:
            await send_webhook(
                payment,
                client,
                settings.webhook_timeout,
                settings.webhook_signing_secret.get_secret_value()
                if settings.webhook_signing_secret
                else None,
            )
        except (httpx.HTTPError, TimeoutError) as exc:
            record_failure(payment, event, exc, OutboxRepository(session), settings)
        else:
            payment.delivery_attempts = event.attempt
            payment.webhook_delivered_at = datetime.now(UTC)
            payment.last_error = None
            logger.info("webhook_delivered payment_id=%s attempt=%s", payment.id, event.attempt)


def record_failure(
    payment: Payment,
    event: PaymentEvent,
    error: Exception,
    outbox: OutboxRepository,
    settings: Settings,
) -> None:
    payment.delivery_attempts = event.attempt
    payment.last_error = type(error).__name__
    terminal = event.attempt == 3
    outbox.add(
        Outbox(
            routing_key="payments.dlq" if terminal else "payments.new",
            payload={
                "payment_id": str(payment.id),
                "attempt": event.attempt if terminal else event.attempt + 1,
            },
            available_at=datetime.now(UTC)
            + timedelta(
                seconds=0 if terminal else settings.retry_base_seconds * 2 ** (event.attempt - 1)
            ),
        )
    )
    logger.warning(
        "processing_failed payment_id=%s attempt=%s terminal=%s error=%s",
        payment.id,
        event.attempt,
        terminal,
        type(error).__name__,
    )


async def schedule_failure(event: PaymentEvent, error: Exception) -> None:
    async with Session() as session, session.begin():
        payment = await PaymentRepository(session).get_for_update(event.payment_id)
        if payment is None:
            raise InvalidPaymentEvent("Unknown payment")
        if payment.webhook_delivered_at or event.attempt <= payment.delivery_attempts:
            return
        record_failure(payment, event, error, OutboxRepository(session), get_settings())
