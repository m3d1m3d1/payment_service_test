import json
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator


MAX_METADATA_BYTES = 16 * 1024
MAX_WEBHOOK_URL_LENGTH = 2048


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class PaymentCreate(BaseModel):
    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: str = Field(min_length=1, max_length=1024)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl

    @field_validator("metadata")
    @classmethod
    def _limit_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value, ensure_ascii=False).encode("utf-8")) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata is too large (max {MAX_METADATA_BYTES} bytes)")
        return value

    @field_validator("webhook_url")
    @classmethod
    def _limit_url(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if len(str(value)) > MAX_WEBHOOK_URL_LENGTH:  # длина колонки в БД
            raise ValueError(f"webhook_url is too long (max {MAX_WEBHOOK_URL_LENGTH})")
        return value


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="meta")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
