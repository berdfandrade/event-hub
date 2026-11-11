from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(UTC)


class EventStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRYING = "retrying"
    COMPLETED = "completed"
    FAILED = "failed"


class EventCreate(BaseModel):
    """Corpo aceito pelo webhook."""

    name: str = Field(min_length=1, examples=["user.created"])
    payload: dict[str, Any] = Field(default_factory=dict)
    source: str = Field(min_length=1, examples=["auth-service"])


class Event(EventCreate):
    id: UUID = Field(default_factory=uuid4)
    status: EventStatus = EventStatus.PENDING
    attempts: int = 0
    last_error: str | None = None
    idempotency_key: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    processed_at: datetime | None = None


class EventStats(BaseModel):
    by_status: dict[str, int]
    queued: int
    scheduled_retries: int
    dead_letter: int
