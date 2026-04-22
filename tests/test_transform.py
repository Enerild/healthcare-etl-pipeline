"""Unit tests for the transform layer."""

from __future__ import annotations

from datetime import UTC

from src.models import RawLog, RawMetadata
from src.transform import transform


def make_raw(**overrides) -> RawLog:
    defaults = {
        "user_id": "user_1",
        "timestamp": "2025-05-13T23:30:47.448076",
        "action_type": "login",
        "metadata": RawMetadata(device="iOS", location="Munich"),
    }
    defaults.update(overrides)
    return RawLog(**defaults)


class TestFiltering:
    def test_valid_record_passes(self):
        clean, failures = transform([make_raw()])
        assert len(clean) == 1
        assert len(failures) == 0

    def test_drops_missing_user_id(self):
        clean, failures = transform([make_raw(user_id=None)])
        assert len(clean) == 0
        assert len(failures) == 1
        assert "user_id" in failures[0].reason

    def test_drops_missing_action_type(self):
        clean, failures = transform([make_raw(action_type=None)])
        assert len(clean) == 0
        assert len(failures) == 1
        assert "action_type" in failures[0].reason

    def test_deduplicates_by_business_key(self):
        # Same user_id + timestamp + action_type -> same hash -> duplicate
        records = [make_raw(), make_raw()]
        clean, failures = transform(records)
        assert len(clean) == 1
        assert len(failures) == 1
        assert "Duplicate" in failures[0].reason


class TestFieldMapping:
    def test_flattens_metadata_fields(self):
        clean, _ = transform([make_raw()])
        assert clean[0].device == "iOS"
        assert clean[0].location == "Munich"

    def test_handles_missing_metadata(self):
        clean, _ = transform([make_raw(metadata=None)])
        assert clean[0].device is None
        assert clean[0].location is None

    def test_generates_stable_event_id(self):
        clean1, _ = transform([make_raw()])
        clean2, _ = transform([make_raw()])
        # Same business key -> same event_id across runs (idempotency)
        assert clean1[0].event_id == clean2[0].event_id


class TestTimestampHandling:
    def test_timestamp_becomes_utc_aware(self):
        clean, _ = transform([make_raw(timestamp="2025-05-13T23:30:47")])
        assert clean[0].timestamp.tzinfo == UTC

    def test_invalid_timestamp_becomes_failure(self):
        clean, failures = transform([make_raw(timestamp="not-a-date")])
        assert len(clean) == 0
        assert len(failures) == 1


class TestMixedBatch:
    def test_mixed_batch_splits_correctly(self):
        records = [
            make_raw(user_id="user_1"),
            make_raw(user_id=None),
            make_raw(user_id="user_2"),
            make_raw(action_type=None),
        ]
        clean, failures = transform(records)
        assert len(clean) == 2
        assert len(failures) == 2

    def test_empty_input(self):
        clean, failures = transform([])
        assert clean == []
        assert failures == []
