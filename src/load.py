"""Upsert clean records into the star schema and log audit data."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime

from psycopg2.extras import execute_values

from src.db import get_connection
from src.models import CleanLog, DQFailure

logger = logging.getLogger(__name__)


def load(
    clean_records: list[CleanLog],
    dq_failures: list[DQFailure],
    run_id: str | None = None,
    dq_summary: dict | None = None,
) -> dict:
    run_id = run_id or str(uuid.uuid4())
    started_at = datetime.now(UTC)
    rows_inserted = 0
    rows_failed = 0

    conn = get_connection()

    try:
        with conn, conn.cursor() as cur:
            rows_inserted = _load_fact_and_dims(cur, clean_records, run_id)
            rows_failed = _load_dq_failures(cur, dq_failures, run_id)
            _record_pipeline_run(
                cur,
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                rows_ingested=rows_inserted + rows_failed,
                rows_clean=rows_inserted,
                rows_failed=rows_failed,
                status="success",
                dq_summary=dq_summary,
            )

        return {
            "run_id": run_id,
            "rows_inserted": rows_inserted,
            "rows_failed": rows_failed,
            "status": "success",
        }

    except Exception as e:
        logger.error("Load failed: %s", e)
        _record_failed_run(run_id, started_at, str(e))
        raise
    finally:
        conn.close()


def _load_fact_and_dims(cur, records: list[CleanLog], run_id: str) -> int:
    """Upsert dimensions and fact rows. Returns count of fact rows inserted."""
    if not records:
        return 0

    # dims first so FK constraints are satisfied
    user_rows = list({(r.user_id,) for r in records})
    execute_values(
        cur,
        """
        INSERT INTO dim_users (user_id)
        VALUES %s
        ON CONFLICT (user_id) DO NOTHING
        """,
        user_rows,
    )
    logger.debug("Upserted %d dim_users rows", len(user_rows))

    action_rows = list({(r.action_type,) for r in records})
    execute_values(
        cur,
        """
        INSERT INTO dim_actions (action_type)
        VALUES %s
        ON CONFLICT (action_type) DO NOTHING
        """,
        action_rows,
    )
    logger.debug("Upserted %d dim_actions rows", len(action_rows))

    fact_rows = [
        (
            r.event_id,
            r.user_id,
            r.action_type,
            r.timestamp.isoformat(),
            r.device,
            r.location,
            r.raw_payload,
            run_id,
        )
        for r in records
    ]
    execute_values(
        cur,
        """
        INSERT INTO fact_user_actions
            (event_id, user_id, action_type, ts, device, location, raw_payload, run_id)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
        """,
        fact_rows,
    )
    cur.execute("SELECT COUNT(*) FROM fact_user_actions WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    inserted = row[0] if row else 0
    logger.info("Inserted %d/%d fact rows (run_id=%s)", inserted, len(fact_rows), run_id)
    return inserted


def _load_dq_failures(cur, failures: list[DQFailure], run_id: str) -> int:
    """Log DQ failures to the audit table. Returns count written."""
    if not failures:
        return 0

    failure_rows = [
        (
            f.event_id,
            f.reason,
            f.raw_record,
            f.failed_at.isoformat(),
            run_id,
        )
        for f in failures
    ]
    execute_values(
        cur,
        """
        INSERT INTO dq_failures
            (event_id, reason, raw_record, failed_at, run_id)
        VALUES %s
        """,
        failure_rows,
    )
    logger.info("Logged %d DQ failures (run_id=%s)", len(failure_rows), run_id)
    return len(failure_rows)


def _record_pipeline_run(
    cur,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    rows_ingested: int,
    rows_clean: int,
    rows_failed: int,
    status: str,
    dq_summary: dict | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO pipeline_runs
            (run_id, started_at, finished_at, rows_ingested,
             rows_clean, rows_failed, status, dq_summary)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            started_at.isoformat(),
            finished_at.isoformat(),
            rows_ingested,
            rows_clean,
            rows_failed,
            status,
            json.dumps(dq_summary) if dq_summary else None,
        ),
    )
    logger.info("Pipeline run %s recorded (status=%s)", run_id, status)


def _record_failed_run(run_id: str, started_at: datetime, error: str) -> None:
    """Best-effort recording of a failed run. Swallows any secondary errors."""
    try:
        conn = get_connection()
        with conn, conn.cursor() as cur:
            _record_pipeline_run(
                cur,
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                rows_ingested=0,
                rows_clean=0,
                rows_failed=0,
                status=f"failed: {error}",
            )
        conn.close()
    except Exception as secondary:
        logger.error("Could not record failed run: %s", secondary)
