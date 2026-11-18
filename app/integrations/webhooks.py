import asyncio
import hashlib
import hmac
import json
import time

import httpx

from app.models import Payment


def webhook_payload(payment: Payment) -> dict:
    return {
        "event_id": str(payment.id),
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": str(payment.amount),
        "currency": payment.currency,
        "metadata": payment.metadata_,
        "processed_at": payment.processed_at.isoformat(),
    }


async def send_webhook(
    payment: Payment,
    client: httpx.AsyncClient,
    timeout: float,
    signing_secret: str | None = None,
) -> None:
    body = json.dumps(webhook_payload(payment), separators=(",", ":"), allow_nan=False).encode()
    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": str(payment.id),
        "X-Event-ID": str(payment.id),
    }
    if signing_secret:
        timestamp = str(int(time.time()))
        signature = hmac.new(
            signing_secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256
        ).hexdigest()
        headers["X-Webhook-Timestamp"] = timestamp
        headers["X-Webhook-Signature"] = "v1=" + signature
    async with asyncio.timeout(timeout):
        async with client.stream(
            "POST",
            payment.webhook_url,
            content=body,
            headers=headers,
            timeout=timeout,
            follow_redirects=False,
        ) as response:
            if not 200 <= response.status_code < 300:
                raise httpx.HTTPStatusError(
                    "Webhook rejected", request=response.request, response=response
                )
