"""Pydantic models for each pipeline stage boundary."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RawMetadata(BaseModel):
    model_config = ConfigDict(extra="allow")

    device: str | None = None
    location: str | None = None


class RawLog(BaseModel):
    """Permissive model for incoming records. Nulls and extra fields are allowed."""

    model_config = ConfigDict(extra="allow")

    user_id: str | None = None
    timestamp: str
    action_type: str | None = None
    metadata: RawMetadata | None = None


def _parse_to_utc(value: Any) -> datetime:
    """Parse str or datetime to UTC. Naive datetimes are assumed UTC."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    if isinstance(value, str):
        normalised = value.replace("Z", "+00:00") if value.endswith("Z") else value
        dt = datetime.fromisoformat(normalised)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    raise ValueError(f"timestamp must be str or datetime, got {type(value).__name__}")


class CleanLog(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    event_id: str
    user_id: str
    action_type: str
    timestamp: datetime
    device: str | None = None
    location: str | None = None
    raw_payload: str

    @field_validator("timestamp", mode="before")
    @classmethod
    def _parse_timestamp(cls, v: Any) -> datetime:
        return _parse_to_utc(v)

    @field_validator("raw_payload", mode="before")
    @classmethod
    def _serialize_payload(cls, v: Any) -> str:
        if isinstance(v, str):
            return v
        return json.dumps(v or {}, default=str, sort_keys=True)


class DQFailure(BaseModel):
    event_id: str | None = None
    reason: str
    raw_record: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("failed_at", mode="before")
    @classmethod
    def _parse_failed_at(cls, v: Any) -> datetime:
        if v is None:
            return datetime.now(UTC)
        return _parse_to_utc(v)


def generate_event_id(user_id: str, timestamp: str, action_type: str) -> str:
    """sha256(user_id|timestamp|action_type)[:16] - deterministic, idempotent across re-runs."""
    key = f"{user_id}|{timestamp}|{action_type}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
