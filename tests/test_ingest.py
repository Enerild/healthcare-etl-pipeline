"""Unit tests for the ingest layer."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from src.ingest import ingest


def _write_json(data) -> Path:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump(data, f)
        return Path(f.name)


class TestIngest:
    def test_reads_valid_records(self):
        data = [
            {
                "user_id": "user_1",
                "timestamp": "2025-05-13T23:30:47.448076",
                "action_type": "login",
                "metadata": {"device": "iOS", "location": "Munich"},
            }
        ]
        path = _write_json(data)
        records, failures = ingest(path)
        assert len(records) == 1
        assert records[0].user_id == "user_1"
        assert records[0].metadata.device == "iOS"

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            ingest("/nonexistent/path/logs.json")

    def test_raises_on_invalid_json(self):
        import json as _json

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            f.write("not valid json {{")
            path = Path(f.name)
        with pytest.raises(_json.JSONDecodeError):
            ingest(path)

    def test_raises_on_non_array_json(self):
        path = _write_json({"key": "value"})  # object, not array
        with pytest.raises(ValueError, match="Expected a JSON array"):
            ingest(path)

    def test_handles_empty_array(self):
        path = _write_json([])
        records, failures = ingest(path)
        assert records == []

    def test_partial_schema_errors_are_skipped(self):
        data = [
            {
                "user_id": "user_1",
                "timestamp": "2025-05-13T23:30:47",
                "action_type": "login",
                "metadata": {"device": "iOS", "location": "Munich"},
            },
            "this is not a dict",  # invalid - should be skipped with warning
        ]
        path = _write_json(data)
        records, failures = ingest(path)
        assert len(records) == 1
        assert len(failures) == 1  # the invalid record is audited, not silently dropped
        assert "Schema validation error" in failures[0].reason
