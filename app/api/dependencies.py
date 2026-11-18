import secrets
from typing import Annotated

from fastapi import Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session
from app.services.payments import PaymentService


async def authenticate(x_api_key: Annotated[str | None, Header()] = None):
    if not x_api_key or not secrets.compare_digest(
        x_api_key.encode(), get_settings().api_key.get_secret_value().encode()
    ):
        raise HTTPException(401, "Invalid API key")


def get_payment_service(session: Annotated[AsyncSession, Depends(get_session)]) -> PaymentService:
    return PaymentService(session, get_settings())
