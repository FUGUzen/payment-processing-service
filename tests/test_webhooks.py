import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from app.integrations.webhooks import send_webhook


@pytest.fixture
def payment():
    return SimpleNamespace(
        id=uuid4(),
        status="succeeded",
        amount=Decimal("10.00"),
        currency="RUB",
        metadata_={"order": "42"},
        processed_at=datetime.now(UTC),
        webhook_url="http://webhook-receiver/hook",
    )


async def test_signature_covers_exact_body_and_timestamp(payment, monkeypatch):
    monkeypatch.setattr("app.integrations.webhooks.time.time", lambda: 1800000000)
    secret = "independent-signing-secret-at-least-32"

    async def handler(request):
        timestamp = request.headers["X-Webhook-Timestamp"]
        expected = hmac.new(
            secret.encode(), timestamp.encode() + b"." + request.content, hashlib.sha256
        ).hexdigest()
        assert request.headers["X-Webhook-Signature"] == "v1=" + expected
        assert json.loads(request.content)["event_id"] == str(payment.id)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await send_webhook(payment, client, 1, secret)


async def test_webhook_total_deadline(payment):
    async def slow_handler(request):
        await asyncio.sleep(1)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_handler)) as client:
        with pytest.raises(TimeoutError):
            await send_webhook(payment, client, 0.01)


async def test_webhook_does_not_buffer_response_body(payment):
    class UnreadBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise AssertionError("Webhook response body must not be read")
            yield b""

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=UnreadBody()))
    ) as client:
        await send_webhook(payment, client, 1)
