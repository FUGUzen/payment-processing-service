import pytest
from pydantic import ValidationError

from app.schemas import PaymentCreate, PaymentEvent


def test_fingerprint_normalizes_amount_and_key_order(payload):
    first = PaymentCreate(**payload)
    second = PaymentCreate(**{**payload, "amount": "100", "metadata": {"order_id": "42"}})
    assert first.fingerprint() == second.fingerprint()


@pytest.mark.parametrize("amount", ["0", "-1", "1.001", "NaN", "Infinity", "10000000000000000"])
def test_invalid_amount(payload, amount):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**payload, "amount": amount})


def test_changed_payload_changes_fingerprint(payload):
    assert (
        PaymentCreate(**payload).fingerprint()
        != PaymentCreate(**{**payload, "amount": "101"}).fingerprint()
    )


def test_retry_limit():
    with pytest.raises(ValidationError):
        PaymentEvent(payment_id="f4a80f26-ae34-4f23-b28b-135a6b585966", attempt=4)


@pytest.mark.parametrize("number", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_metadata_rejected(payload, number):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**payload, "metadata": {"value": number}})
