import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from sqlalchemy import func, select

from app.models import Outbox, Payment
from app.schemas import PaymentEvent
from app.services.outbox import publish_batch
from app.services.processing import process_payment


async def create(client, payload, key="key-1"):
    response = await client.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": key})
    assert response.status_code == 202, response.text
    return UUID(response.json()["payment_id"])


async def test_atomic_create_and_read(client, database, payload):
    payment_id = await create(client, payload)
    response = await client.get(f"/api/v1/payments/{payment_id}")
    assert response.json()["amount"] == "100.00"
    assert response.json()["metadata"] == payload["metadata"]
    async with database() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_concurrent_idempotency(client, database, payload):
    ids = await asyncio.gather(*(create(client, payload) for _ in range(10)))
    assert len(set(ids)) == 1
    async with database() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
    response = await client.post(
        "/api/v1/payments", json={**payload, "amount": "101"}, headers={"Idempotency-Key": "key-1"}
    )
    assert response.status_code == 409


async def test_auth_and_validation(client, payload):
    assert (
        await client.get(
            "/api/v1/payments/00000000-0000-0000-0000-000000000000", headers={"X-API-Key": "wrong"}
        )
    ).status_code == 401
    assert (await client.post("/api/v1/payments", json=payload)).status_code == 422
    assert (
        await client.post(
            "/api/v1/payments",
            json={**payload, "webhook_url": "http://169.254.169.254/"},
            headers={"Idempotency-Key": "x"},
        )
    ).status_code == 422


@pytest.mark.parametrize("status", ["succeeded", "failed"])
async def test_delivery_and_duplicate(client, database, payload, monkeypatch, status):
    payment_id = await create(client, payload)
    gateway = AsyncMock(return_value=status)
    monkeypatch.setattr("app.services.processing.charge", gateway)
    calls = []

    def receive(request):
        calls.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as webhook:
        event = PaymentEvent(payment_id=payment_id)
        await asyncio.gather(process_payment(event, webhook), process_payment(event, webhook))
    assert gateway.await_count == 1
    assert len(calls) == 1
    assert calls[0].headers["X-Event-ID"] == str(payment_id)
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == status
        assert payment.webhook_delivered_at is not None


async def test_retries_to_dlq(client, database, payload, monkeypatch):
    payment_id = await create(client, payload)
    gateway = AsyncMock(return_value="succeeded")
    monkeypatch.setattr("app.services.processing.charge", gateway)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(503))
    ) as webhook:
        for attempt in (1, 2, 3):
            before = datetime.now(UTC)
            await process_payment(PaymentEvent(payment_id=payment_id, attempt=attempt), webhook)
            async with database() as session:
                events = (await session.scalars(select(Outbox).order_by(Outbox.created_at))).all()
                assert len(events) == attempt + 1
                if attempt < 3:
                    assert (events[-1].available_at - before).total_seconds() >= 2 ** (attempt - 1)
        await process_payment(PaymentEvent(payment_id=payment_id, attempt=3), webhook)
    async with database() as session:
        events = (await session.scalars(select(Outbox))).all()
        assert len(events) == 4
        assert sum(event.routing_key == "payments.dlq" for event in events) == 1
        payment = await session.get(Payment, payment_id)
        assert payment.status == "succeeded"
        assert payment.delivery_attempts == 3
        assert payment.webhook_delivered_at is None
    assert gateway.await_count == 1


async def test_outbox_failure_keeps_event(client, database, payload, monkeypatch):
    await create(client, payload)
    publish = AsyncMock(side_effect=ConnectionError)
    monkeypatch.setattr("app.services.outbox.broker.publish", publish)
    with pytest.raises(ConnectionError):
        await publish_batch()
    async with database() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is None
    publish.side_effect = None
    assert await publish_batch() == 1
    assert publish.call_args.kwargs["persist"] is True
    assert publish.call_args.kwargs["mandatory"] is True
    assert await publish_batch() == 0


async def test_creation_rolls_back_when_outbox_write_fails(client, database, payload, monkeypatch):
    def fail_add(self, event):
        raise RuntimeError("Outbox unavailable")

    monkeypatch.setattr("app.repositories.outbox.OutboxRepository.add", fail_add)
    with pytest.raises(RuntimeError, match="Outbox unavailable"):
        await client.post(
            "/api/v1/payments", json=payload, headers={"Idempotency-Key": "atomic-key"}
        )
    async with database() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_get_missing_payment(client):
    response = await client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404


async def test_webhook_recovers_without_repeating_gateway(client, database, payload, monkeypatch):
    payment_id = await create(client, payload)
    gateway = AsyncMock(return_value="succeeded")
    monkeypatch.setattr("app.services.processing.charge", gateway)
    statuses = iter([503, 200])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(next(statuses)))
    ) as webhook:
        await process_payment(PaymentEvent(payment_id=payment_id), webhook)
        await process_payment(PaymentEvent(payment_id=payment_id, attempt=2), webhook)
        await process_payment(PaymentEvent(payment_id=payment_id), webhook)
    gateway.assert_awaited_once()
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.delivery_attempts == 2
        assert payment.webhook_delivered_at is not None
        assert payment.last_error is None


@pytest.mark.parametrize("userinfo", ["user:pass", ":pass"])
async def test_webhook_userinfo_rejected(client, payload, userinfo):
    response = await client.post(
        "/api/v1/payments",
        json={**payload, "webhook_url": f"http://{userinfo}@webhook-receiver/hook"},
        headers={"Idempotency-Key": "userinfo"},
    )
    assert response.status_code == 422


async def test_unexpected_processing_failures_reach_dlq(client, database, payload, monkeypatch):
    from app.consumer import handle_payment

    payment_id = await create(client, payload)
    monkeypatch.setattr("app.services.processing.charge", AsyncMock(side_effect=RuntimeError))
    from types import SimpleNamespace

    for attempt in (1, 2, 3):
        message = SimpleNamespace(ack=AsyncMock(), nack=AsyncMock(), reject=AsyncMock())
        await handle_payment._original_call(
            {"payment_id": str(payment_id), "attempt": attempt}, message
        )
        message.ack.assert_awaited_once()
        message.nack.assert_not_awaited()
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.delivery_attempts == 3
        assert payment.status == "pending"
        dlq = (
            await session.scalars(select(Outbox).where(Outbox.routing_key == "payments.dlq"))
        ).all()
        assert len(dlq) == 1


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
async def test_nonfinite_metadata_returns_validation_error(client, payload, value):
    import json

    response = await client.post(
        "/api/v1/payments",
        content=json.dumps({**payload, "metadata": {"nested": [value]}}),
        headers={"Idempotency-Key": "nonfinite", "Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert response.json()["detail"]
    assert "input" not in response.json()["detail"][0]


@pytest.mark.parametrize(
    "fields",
    [
        {"description": "a\x00b"},
        {"description": "\ud800"},
        {"metadata": {"nested": ["a\x00b"]}},
        {"metadata": {"nested": ["\ud800"]}},
        {"metadata": {"\x00": "value"}},
    ],
)
async def test_unsupported_postgres_text_returns_422(client, payload, fields):
    import json

    response = await client.post(
        "/api/v1/payments",
        content=json.dumps({**payload, **fields}),
        headers={"Idempotency-Key": "invalid-text", "Content-Type": "application/json"},
    )
    assert response.status_code == 422


async def test_get_preserves_idempotency_key(client, payload):
    key = "merchant-order-2026-001"
    payment_id = await create(client, payload, key=key)
    response = await client.get(f"/api/v1/payments/{payment_id}")
    assert response.status_code == 200
    assert response.json()["idempotency_key"] == key
    assert await create(client, payload, key=key) == payment_id


async def test_readiness_requires_auth_and_checks_database(client):
    assert (await client.get("/health/ready")).status_code == 200
    response = await client.get("/health/ready", headers={"X-API-Key": "incorrect"})
    assert response.status_code == 401


@pytest.mark.parametrize("failure", ["sqlalchemy", "connection", "dns"])
async def test_readiness_database_failure_returns_503(client, failure):
    import socket

    from sqlalchemy.exc import OperationalError

    from app.db import get_session
    from app.main import app

    async def unavailable():
        from types import SimpleNamespace

        yield SimpleNamespace(
            execute=AsyncMock(
                side_effect={
                    "sqlalchemy": OperationalError(None, None, Exception()),
                    "connection": ConnectionRefusedError(),
                    "dns": socket.gaierror(),
                }[failure]
            )
        )

    app.dependency_overrides[get_session] = unavailable
    assert (await client.get("/health/ready")).status_code == 503


async def test_webhook_total_timeout_schedules_retry(client, database, payload, monkeypatch):
    payment_id = await create(client, payload)
    monkeypatch.setattr("app.services.processing.charge", AsyncMock(return_value="succeeded"))
    monkeypatch.setattr("app.services.processing.send_webhook", AsyncMock(side_effect=TimeoutError))
    async with httpx.AsyncClient() as webhook:
        await process_payment(PaymentEvent(payment_id=payment_id), webhook)
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "succeeded"
        assert payment.delivery_attempts == 1
        assert payment.last_error == "TimeoutError"
        events = (await session.scalars(select(Outbox))).all()
        assert any(event.payload["attempt"] == 2 for event in events)


async def test_outbox_respects_batch_size(client, database, payload, monkeypatch):
    await create(client, payload, key="batch-1")
    await create(client, payload, key="batch-2")
    publish = AsyncMock()
    monkeypatch.setattr("app.services.outbox.OUTBOX_BATCH_SIZE", 1)
    monkeypatch.setattr("app.services.outbox.broker.publish", publish)
    assert await publish_batch() == 1
    publish.assert_awaited_once()
    async with database() as session:
        pending = await session.scalar(
            select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None))
        )
        assert pending == 1
