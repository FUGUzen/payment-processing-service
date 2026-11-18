import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


def validate_text(value):
    if isinstance(value, str):
        if "\x00" in value:
            raise ValueError("NUL characters are not supported")
        value.encode("utf-8")
    elif isinstance(value, dict):
        for key, item in value.items():
            validate_text(key)
            validate_text(item)
    elif isinstance(value, list):
        for item in value:
            validate_text(item)
    return value


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(default="", max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("description", "metadata")
    @classmethod
    def valid_text(cls, value):
        return validate_text(value)

    @field_validator("metadata")
    @classmethod
    def limit_metadata(cls, value):
        if len(json.dumps(value, allow_nan=False).encode()) > 16384:
            raise ValueError("metadata exceeds 16 KiB")
        return value

    def fingerprint(self) -> str:
        data = self.model_dump(mode="json")
        data["amount"] = format(self.amount.quantize(Decimal("0.01")), "f")
        return hashlib.sha256(
            json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: str
    created_at: datetime


class PaymentDetails(PaymentAccepted):
    idempotency_key: str
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    processed_at: datetime | None


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=3)
