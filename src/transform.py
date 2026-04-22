"""Apply business rules to raw logs and return clean records + failures."""

from __future__ import annotations

import json
import logging

from src.models import CleanLog, DQFailure, RawLog, _parse_to_utc, generate_event_id

logger = logging.getLogger(__name__)


def transform(records: list[RawLog]) -> tuple[list[CleanLog], list[DQFailure]]:
    clean: list[CleanLog] = []
    failures: list[DQFailure] = []
    seen_event_ids: set[str] = set()

    for raw in records:
        raw_dump = raw.model_dump()
        raw_str = json.dumps(raw_dump, default=str, sort_keys=True)

        if not raw.user_id:
            failures.append(
                DQFailure(
                    reason="Missing user_id",
                    raw_record=raw_str,
                )
            )
            logger.debug("Dropped record: missing user_id")
            continue

        if not raw.action_type:
            failures.append(
                DQFailure(
                    reason="Missing action_type",
                    raw_record=raw_str,
                )
            )
            logger.debug("Dropped record: missing action_type for user %s", raw.user_id)
            continue

        try:
            normalized_ts = _parse_to_utc(raw.timestamp).isoformat()
        except (ValueError, TypeError) as e:
            failures.append(
                DQFailure(
                    reason=f"Unparseable timestamp: {e}",
                    raw_record=raw_str,
                )
            )
            logger.debug("Dropped record: unparseable timestamp for user %s", raw.user_id)
            continue

        event_id = generate_event_id(raw.user_id, normalized_ts, raw.action_type)

        if event_id in seen_event_ids:
            failures.append(
                DQFailure(
                    event_id=event_id,
                    reason="Duplicate event (same user_id, timestamp, action_type)",
                    raw_record=raw_str,
                )
            )
            logger.debug("Dropped duplicate event_id: %s", event_id)
            continue

        device = raw.metadata.device if raw.metadata else None
        location = raw.metadata.location if raw.metadata else None

        record = CleanLog(
            event_id=event_id,
            user_id=raw.user_id,
            action_type=raw.action_type,
            timestamp=normalized_ts,
            device=device,
            location=location,
            raw_payload=raw_dump,
        )
        clean.append(record)
        seen_event_ids.add(event_id)

    logger.info("Transform complete: %d clean, %d failures", len(clean), len(failures))
    return clean, failures
