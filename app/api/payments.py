from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException

from app.api.dependencies import get_payment_service
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetails
from app.services.payments import (
    IdempotencyConflict,
    InvalidPaymentRequest,
    PaymentNotFound,
    PaymentService,
)

router = APIRouter(prefix="/api/v1/payments")
Service = Annotated[PaymentService, Depends(get_payment_service)]


@router.post("", status_code=202, response_model=PaymentAccepted)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    service: Service,
):
    try:
        payment = await service.create(body, idempotency_key)
    except InvalidPaymentRequest as exc:
        raise HTTPException(422, str(exc)) from exc
    except IdempotencyConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/{payment_id}", response_model=PaymentDetails)
async def get_payment(payment_id: UUID, service: Service):
    try:
        payment = await service.get(payment_id)
    except PaymentNotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    return PaymentDetails(
        idempotency_key=payment.idempotency_key,
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
    )
