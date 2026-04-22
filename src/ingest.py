"""Read raw_logs.json into RawLog models. Business-rule violations pass through to transform."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from pydantic import ValidationError

from src.models import DQFailure, RawLog

logger = logging.getLogger(__name__)


def ingest(path: str | Path) -> tuple[list[RawLog], list[DQFailure]]:
    """
    Parse the raw JSON file into RawLog instances.
    Records that fail schema validation are returned as DQFailure rather than silently dropped.
    Raises FileNotFoundError, ValueError, or JSONDecodeError for fatal input problems.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Log file not found: {path}")

    logger.info("Reading logs from %s", path)
    raw = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(raw, list):
        raise ValueError(f"Expected a JSON array, got {type(raw).__name__}")

    records: list[RawLog] = []
    schema_failures: list[DQFailure] = []

    for i, item in enumerate(raw):
        try:
            records.append(RawLog(**item))
        except (ValidationError, TypeError) as e:
            raw_str = (
                json.dumps(item, default=str) if isinstance(item, dict) else json.dumps(str(item))
            )
            schema_failures.append(
                DQFailure(
                    reason=f"Schema validation error at record [{i}]: {e}", raw_record=raw_str
                )
            )
            logger.warning("Record [%d] failed schema validation: %s", i, e)

    logger.info("Ingested %d records (%d schema errors)", len(records), len(schema_failures))
    return records, schema_failures
