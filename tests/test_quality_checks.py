"""Unit tests for the quality_checks layer."""

from __future__ import annotations

from datetime import UTC, datetime

from src.models import CleanLog
from src.quality_checks import run_quality_checks


def make_clean(**overrides) -> CleanLog:
    defaults = {
        "event_id": "abcdef0123456789",
        "user_id": "user_1",
        "action_type": "login",
        "timestamp": datetime(2025, 5, 13, 23, 30, 47, tzinfo=UTC),
        "device": "iOS",
        "location": "Munich",
        "raw_payload": "{}",
    }
    defaults.update(overrides)
    return CleanLog(**defaults)


class TestQualityChecks:
    def test_all_pass_on_clean_data(self):
        records = [make_clean(event_id=f"evt_{i:016x}") for i in range(5)]
        report = run_quality_checks(records)
        assert report.failed == 0

    def test_empty_input_returns_empty_report(self):
        report = run_quality_checks([])
        assert report.total_records == 0
        assert report.checks == []

    def test_success_rate_calculation(self):
        records = [make_clean(event_id=f"evt_{i:016x}") for i in range(3)]
        report = run_quality_checks(records)
        assert report.success_rate == 100.0

    def test_unknown_action_type_flagged(self):
        records = [make_clean(action_type="unknown_action")]
        report = run_quality_checks(records)
        failed = [c for c in report.checks if c.status == "FAIL"]
        assert any(c.name == "known_action_types" for c in failed)

    def test_unknown_device_flagged(self):
        records = [make_clean(device="Smartwatch")]
        report = run_quality_checks(records)
        failed = [c for c in report.checks if c.status == "FAIL"]
        assert any(c.name == "known_devices" for c in failed)

    def test_tz_aware_timestamps_pass(self):
        records = [make_clean()]
        report = run_quality_checks(records)
        ts_check = next(c for c in report.checks if c.name == "timestamps_are_tz_aware")
        assert ts_check.status == "PASS"

    def test_duplicate_event_ids_flagged(self):
        records = [make_clean(event_id="dup"), make_clean(event_id="dup")]
        report = run_quality_checks(records)
        dup_check = next(c for c in report.checks if c.name == "no_duplicate_event_ids")
        assert dup_check.status == "FAIL"
