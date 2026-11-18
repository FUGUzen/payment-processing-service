from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox


class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    def add(self, event: Outbox) -> None:
        self.session.add(event)

    async def get_ready_for_update(self, limit: int) -> list[Outbox]:
        events = await self.session.scalars(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
            .order_by(Outbox.available_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(events)

    def mark_published(self, event: Outbox, published_at: datetime) -> None:
        event.published_at = published_at
