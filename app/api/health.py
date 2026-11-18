import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session

router = APIRouter(prefix="/health")


@router.get("/ready")
async def ready(session: Annotated[AsyncSession, Depends(get_session)]):
    try:
        async with asyncio.timeout(2):
            await session.execute(text("SELECT 1"))
    except (SQLAlchemyError, OSError) as exc:
        raise HTTPException(503, "Database unavailable") from exc
    return {"status": "ready"}
