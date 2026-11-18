from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def insert_if_absent(self, payment: Payment) -> bool:
        payment_id = await self.session.scalar(
            insert(Payment)
            .values(
                id=payment.id,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                metadata_=payment.metadata_,
                status=payment.status,
                idempotency_key=payment.idempotency_key,
                request_hash=payment.request_hash,
                webhook_url=payment.webhook_url,
                delivery_attempts=payment.delivery_attempts,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        return payment_id is not None

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        return await self.session.scalar(select(Payment).where(Payment.idempotency_key == key))

    async def get_for_update(self, payment_id: UUID) -> Payment | None:
        return await self.session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
