"""Prefect flow for the healthcare ETL pipeline. Run directly: python flows/etl_flow.py"""

from __future__ import annotations

import logging
import uuid

from prefect import flow, get_run_logger, task

from src.ingest import ingest
from src.load import load
from src.quality_checks import run_quality_checks
from src.transform import transform


@task(name="ingest-raw-logs", retries=2, retry_delay_seconds=5)
def ingest_task(path: str):
    logger = get_run_logger()
    logger.info("Ingesting from %s", path)
    records, schema_failures = ingest(path)
    logger.info("Ingested %d raw records (%d schema errors)", len(records), len(schema_failures))
    return records, schema_failures


@task(name="transform-records")
def transform_task(records):
    logger = get_run_logger()
    clean, failures = transform(records)
    logger.info("Transform: %d clean, %d failures", len(clean), len(failures))
    return clean, failures


@task(name="quality-checks")
def quality_task(clean_records):
    logger = get_run_logger()
    report = run_quality_checks(clean_records)
    logger.info("DQ report: %d/%d checks passed", report.passed, len(report.checks))
    for check in report.checks:
        level = logging.INFO if check.status == "PASS" else logging.WARNING
        logger.log(level, "  [%s] %s: %s", check.status, check.name, check.detail)
    return report


@task(name="load-to-database", retries=1, retry_delay_seconds=10)
def load_task(clean_records, dq_failures, dq_report, run_id: str):
    logger = get_run_logger()
    dq_summary = {
        "passed": dq_report.passed,
        "failed": dq_report.failed,
        "checks": [
            {"name": c.name, "status": c.status, "detail": c.detail} for c in dq_report.checks
        ],
    }
    result = load(clean_records, dq_failures, run_id=run_id, dq_summary=dq_summary)
    logger.info(
        "Loaded: %d inserted, %d failures logged",
        result["rows_inserted"],
        result["rows_failed"],
    )
    return result


@flow(
    name="healthcare-etl",
    description="Ingests mobile app JSON logs into a star schema.",
)
def etl_pipeline(log_path: str = "data/raw_logs.json") -> dict:
    run_id = str(uuid.uuid4())
    logger = get_run_logger()
    logger.info("Starting ETL run %s", run_id)

    raw_records, ingest_failures = ingest_task(log_path)
    clean_records, transform_failures = transform_task(raw_records)
    dq_failures = ingest_failures + transform_failures
    report = quality_task(clean_records)
    result = load_task(clean_records, dq_failures, report, run_id)

    logger.info("ETL complete. Run ID: %s", run_id)
    return result


if __name__ == "__main__":
    etl_pipeline()
