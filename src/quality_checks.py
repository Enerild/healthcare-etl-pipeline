"""Informational DQ checks on the clean dataset. Failures don't block the load."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

from src.models import CleanLog

logger = logging.getLogger(__name__)

CheckStatus = Literal["PASS", "FAIL"]


@dataclass
class CheckResult:
    name: str
    status: CheckStatus
    detail: str


@dataclass
class DQReport:
    total_records: int = 0
    passed: int = 0
    failed: int = 0
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        total = len(self.checks)
        if total == 0:
            return 0.0
        return round(self.passed / total * 100, 2)

    def add_check(self, name: str, passed: bool, detail: str = "") -> None:
        status: CheckStatus = "PASS" if passed else "FAIL"
        self.checks.append(CheckResult(name=name, status=status, detail=detail))
        if passed:
            self.passed += 1
        else:
            self.failed += 1
        log_level = logging.INFO if passed else logging.WARNING
        logger.log(log_level, "DQ [%s] %s - %s", status, name, detail)


KNOWN_ACTION_TYPES: frozenset[str] = frozenset(
    {
        "login",
        "logout",
        "view_item",
        "add_to_cart",
        "purchase",
    }
)

KNOWN_DEVICES: frozenset[str] = frozenset({"iOS", "Android", "Web"})


def run_quality_checks(records: list[CleanLog]) -> DQReport:
    report = DQReport(total_records=len(records))

    if not records:
        logger.warning("No records to quality-check.")
        return report

    # Check 1: No null user_ids (model invariant, but verify defensively)
    null_users = [r for r in records if not r.user_id]
    report.add_check(
        name="no_null_user_ids",
        passed=len(null_users) == 0,
        detail=f"{len(null_users)} null user_ids found",
    )

    # Check 2: No null action_types
    null_actions = [r for r in records if not r.action_type]
    report.add_check(
        name="no_null_action_types",
        passed=len(null_actions) == 0,
        detail=f"{len(null_actions)} null action_types found",
    )

    # Check 3: No duplicate event_ids within the batch
    event_ids = [r.event_id for r in records]
    duplicates = len(event_ids) - len(set(event_ids))
    report.add_check(
        name="no_duplicate_event_ids",
        passed=duplicates == 0,
        detail=f"{duplicates} duplicate event_ids found",
    )

    # Check 4: All timestamps are timezone-aware (UTC)
    tz_naive = [r for r in records if r.timestamp.tzinfo is None]
    report.add_check(
        name="timestamps_are_tz_aware",
        passed=len(tz_naive) == 0,
        detail=f"{len(tz_naive)} timezone-naive timestamps found",
    )

    # Check 5: action_types are from the known catalogue
    unknown_actions = {r.action_type for r in records if r.action_type not in KNOWN_ACTION_TYPES}
    report.add_check(
        name="known_action_types",
        passed=len(unknown_actions) == 0,
        detail=(
            f"{len(unknown_actions)} unknown action_type(s): {sorted(unknown_actions)}"
            if unknown_actions
            else "all action_types in known catalogue"
        ),
    )

    # Check 6: devices are from the known catalogue (non-null only)
    unknown_devices = {r.device for r in records if r.device and r.device not in KNOWN_DEVICES}
    report.add_check(
        name="known_devices",
        passed=len(unknown_devices) == 0,
        detail=(
            f"{len(unknown_devices)} unknown device(s): {sorted(unknown_devices)}"
            if unknown_devices
            else "all devices in known catalogue"
        ),
    )

    logger.info(
        "DQ complete: %d/%d checks passed (%.2f%% record quality)",
        report.passed,
        len(report.checks),
        report.success_rate,
    )
    return report
