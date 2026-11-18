from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.broker import decode_event
from app.consumer import handle_payment


@pytest.mark.parametrize(
    "body", [b"{broken", b"\xff", b"null", b"[]", b'"string"', b"[" * 10000 + b"0" + b"]" * 10000]
)
async def test_malformed_event_rejected(body):
    message = SimpleNamespace(body=body, reject=AsyncMock(), ack=AsyncMock(), nack=AsyncMock())
    await handle_payment._original_call(decode_event(message), message)
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()
    message.nack.assert_not_awaited()


async def test_unknown_error_scheduled_before_ack(monkeypatch):
    calls = []

    async def schedule(*args):
        calls.append("schedule")

    async def ack():
        calls.append("ack")

    monkeypatch.setattr("app.consumer.process_payment", AsyncMock(side_effect=RuntimeError))
    monkeypatch.setattr("app.consumer.schedule_failure", schedule)
    message = SimpleNamespace(reject=AsyncMock(), ack=ack, nack=AsyncMock())
    await handle_payment._original_call({"payment_id": str(uuid4()), "attempt": 1}, message)
    assert calls == ["schedule", "ack"]
