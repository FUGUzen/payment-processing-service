import os
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("API_KEY", "test-api-key-at-least-16")
os.environ.setdefault("WEBHOOK_ALLOWED_HOSTS", "webhook-receiver")

from app.db import get_session
from app.main import app
from app.models import Base


@pytest_asyncio.fixture
async def database(monkeypatch):
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL required for PostgreSQL integration tests")
    schema = "test_" + uuid4().hex
    admin = create_async_engine(url)
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr("app.services.processing.Session", sessions)
    monkeypatch.setattr("app.services.outbox.Session", sessions)

    async def override():
        async with sessions() as session:
            yield session

    app.dependency_overrides[get_session] = override
    yield sessions
    app.dependency_overrides.clear()
    await engine.dispose()
    async with admin.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await admin.dispose()


@pytest_asyncio.fixture
async def client(database):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": os.environ["API_KEY"]},
    ) as client:
        yield client


@pytest.fixture
def payload():
    return {
        "amount": "100.00",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": "42"},
        "webhook_url": "http://webhook-receiver/hook",
    }
