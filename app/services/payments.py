from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import Outbox, Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    pass


class PaymentNotFound(Exception):
    pass


class InvalidPaymentRequest(Exception):
    pass


class PaymentService:
    def __init__(self, session: AsyncSession, settings: Settings):
        self.session = session
        self.settings = settings
        self.payments = PaymentRepository(session)
        self.outbox = OutboxRepository(session)

    async def create(self, data: PaymentCreate, idempotency_key: str) -> Payment:
        if (
            data.webhook_url.host not in self.settings.allowed_hosts
            or data.webhook_url.username is not None
            or data.webhook_url.password is not None
        ):
            raise InvalidPaymentRequest("Webhook host is not allowed")
        if not idempotency_key.strip() or "\x00" in idempotency_key:
            raise InvalidPaymentRequest("Invalid Idempotency-Key")
        fingerprint = data.fingerprint()
        candidate = Payment(
            id=uuid4(),
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            status="pending",
            idempotency_key=idempotency_key,
            request_hash=fingerprint,
            webhook_url=str(data.webhook_url),
            delivery_attempts=0,
        )
        async with self.session.begin():
            if await self.payments.insert_if_absent(candidate):
                self.outbox.add(
                    Outbox(
                        routing_key="payments.new",
                        payload={"payment_id": str(candidate.id), "attempt": 1},
                    )
                )
            payment = await self.payments.get_by_idempotency_key(idempotency_key)
            if payment is None:
                raise RuntimeError("Payment disappeared after insert")
            if payment.request_hash != fingerprint:
                raise IdempotencyConflict(
                    "Idempotency-Key was already used with a different request"
                )
        return payment

    async def get(self, payment_id: UUID) -> Payment:
        payment = await self.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFound("Payment not found")
        return payment
