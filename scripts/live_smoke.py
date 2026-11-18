"""Optional live smoke test. Requires PostgreSQL CREATEDB and a dedicated RabbitMQ vhost."""

import asyncio
import json
import os
import socket
import subprocess
import sys
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import asyncpg
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


async def main():
    database = "payment_smoke_" + uuid4().hex
    admin_url = os.environ["TEST_DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    admin = await asyncpg.connect(admin_url)
    await admin.execute(f'CREATE DATABASE "{database}"')
    os.environ["DATABASE_URL"] = (
        admin_url.rsplit("/", 1)[0].replace("postgresql://", "postgresql+asyncpg://")
        + "/"
        + database
    )
    os.environ["WEBHOOK_ALLOWED_HOSTS"] = "127.0.0.1"
    os.environ.setdefault("API_KEY", "live-smoke-key-not-production")
    os.environ.setdefault("OUTBOX_POLL_INTERVAL", "0.05")
    status = 200
    deliveries = []

    async def webhook(reader, writer):
        raw_headers = await reader.readuntil(b"\r\n\r\n")
        headers = {}
        for line in raw_headers.decode().split("\r\n")[1:]:
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.lower()] = value.strip()
        body = await reader.readexactly(int(headers.get("content-length", 0)))
        deliveries.append(json.loads(body))
        writer.write(
            f"HTTP/1.1 {status} Result\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode()
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(webhook, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    started = False
    broker = engine = None
    processes = []
    process_mode = "--processes" in sys.argv
    try:
        subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], check=True)
        from sqlalchemy import select

        from app.broker import broker, dead_queue, exchange
        from app.consumer import app as worker
        from app.db import Session, engine
        from app.main import app
        from app.models import Payment

        if process_mode:
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                api_port = listener.getsockname()[1]
            processes.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        "app.main:app",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(api_port),
                    ]
                )
            )
            processes.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "faststream",
                        "run",
                        "app.consumer:app",
                    ]
                )
            )
            async with httpx.AsyncClient(trust_env=False) as readiness:
                for _ in range(100):
                    assert all(p.poll() is None for p in processes), "Service process exited"
                    try:
                        response = await readiness.get(
                            f"http://127.0.0.1:{api_port}/api/v1/payments/{uuid4()}",
                            headers={"X-API-Key": os.environ["API_KEY"]},
                        )
                        if response.status_code == 404:
                            break
                    except httpx.ConnectError:
                        pass
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError("API did not become ready")
            await broker.connect()
        else:
            await worker.start()
            started = True
        raw_dlq = await broker.declare_queue(dead_queue)

        async def wait_payment(payment_id, predicate):
            for _ in range(300):
                async with Session() as session:
                    payment = await session.scalar(select(Payment).where(Payment.id == payment_id))
                    if predicate(payment):
                        return payment
                await asyncio.sleep(0.1)
            raise AssertionError("Payment did not reach expected state")

        async def take_dlq():
            for _ in range(100):
                message = await raw_dlq.get(fail=False)
                if message:
                    await message.ack()
                    return message
                await asyncio.sleep(0.1)
            raise AssertionError("DLQ message missing")

        async with httpx.AsyncClient(
            trust_env=False,
            transport=None if process_mode else httpx.ASGITransport(app=app),
            base_url=f"http://127.0.0.1:{api_port}" if process_mode else "http://api",
            headers={"X-API-Key": os.environ["API_KEY"]},
        ) as api:

            async def create(key):
                response = await api.post(
                    "/api/v1/payments",
                    headers={"Idempotency-Key": key},
                    json={
                        "amount": "42.50",
                        "currency": "RUB",
                        "description": "live smoke",
                        "metadata": {"order": key},
                        "webhook_url": f"http://127.0.0.1:{port}/hook",
                    },
                )
                assert response.status_code == 202, response.text
                return response.json()["payment_id"]

            success_id = await create("live-success")
            payment = await wait_payment(success_id, lambda p: p.webhook_delivered_at is not None)
            assert payment.delivery_attempts == 1
            assert payment.status in {"succeeded", "failed"}
            assert len(deliveries) == 1
            assert deliveries[0]["event_id"] == success_id
            assert await create("live-success") == success_id
            await broker.publish(
                {"payment_id": success_id, "attempt": 1},
                exchange=exchange,
                routing_key="payments.new",
                persist=True,
            )
            await asyncio.sleep(0.3)
            assert len(deliveries) == 1

            status = 503
            failed_id = await create("live-retry")
            payment = await wait_payment(failed_id, lambda p: p.delivery_attempts == 3)
            assert payment.webhook_delivered_at is None
            message = await take_dlq()
            assert json.loads(message.body)["payment_id"] == failed_id
            assert len([d for d in deliveries if d["payment_id"] == failed_id]) == 3

            await broker.publish(
                b"{malformed",
                exchange=exchange,
                routing_key="payments.new",
                content_type="application/json",
                persist=True,
            )
            message = await take_dlq()
            assert message.body == b"{malformed"

            # A poison message must not block the sole consumer.
            status = 200
            await wait_payment(
                await create("after-poison"), lambda p: p.webhook_delivered_at is not None
            )
            print(
                f"LIVE PASS ({'subprocess TCP' if process_mode else 'in-process'}): "
                "API/outbox/Rabbit/consumer/webhook, duplicates, "
                "3 retries, DLQ, malformed JSON"
            )
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                await asyncio.to_thread(process.wait, timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait)
        if process_mode and broker is not None:
            await broker.stop()
            await engine.dispose()
        if started:
            await worker.stop()
            await engine.dispose()
        server.close()
        await server.wait_closed()
        with suppress(Exception):
            await admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
