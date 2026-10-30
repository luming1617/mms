"""Lifecycle JSONL case loading and validation.

Schema (docs/4 §四, extended with the §五 annotation dimensions):
setup_events / actions / assertions, plus case-level category, policy_id,
priority (hard_gate|regular), dataset_version and review_status.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

CASES_DIR = Path(__file__).resolve().parent / "cases"

CATEGORIES = {"memory_write", "conflict_update", "expiry", "deletion_isolation"}
PRIORITIES = {"hard_gate", "regular"}
ACTION_TYPES = {
    "ingest",
    "wait_seconds",
    "run_lifecycle",
    "request_deletion",
    "approve_review",
    "assert_deletion_status",
}
ASSERTION_TYPES = {
    "decision",
    "current_fact",
    "historical_fact",
    "write_quality",
    "leak_rate",
    "barrier",
    "isolation",
    "deletion_status",
}
RISK_BY_PRIORITY = {"hard_gate": "critical", "regular": "high"}


class CaseValidationError(ValueError):
    pass


def _validate_event(event: dict[str, Any], where: str) -> None:
    missing = [field for field in ("event_id", "text", "fact_key") if not event.get(field)]
    if missing:
        raise CaseValidationError(f"{where}: event missing {missing}")


def _validate_action(action: dict[str, Any], where: str) -> None:
    if action.get("type") not in ACTION_TYPES:
        raise CaseValidationError(f"{where}: unknown action type {action.get('type')!r}")
    if action["type"] == "ingest":
        _validate_event(action, where)
        if action.get("expected_decision") and action["expected_decision"] not in {
            "ADD",
            "NOOP",
            "REINFORCE",
            "SUPERSEDE",
            "CONFLICT",
            "ADD_TEMPORAL_VERSION",
            "REJECTED",
            "IGNORE",
            "NEEDS_VERIFICATION",
            "RESOLVE",
            "EXPIRE",
            "DELETE_REQUESTED",
        }:
            raise CaseValidationError(f"{where}: unknown expected_decision {action['expected_decision']!r}")
    if action["type"] == "wait_seconds" and not isinstance(action.get("seconds"), (int, float)):
        raise CaseValidationError(f"{where}: wait_seconds needs a numeric 'seconds'")
    if action["type"] == "assert_deletion_status" and not action.get("expected_status"):
        raise CaseValidationError(f"{where}: assert_deletion_status needs expected_status")


def _validate_assertion(assertion: dict[str, Any], where: str) -> None:
    if assertion.get("type") not in ASSERTION_TYPES:
        raise CaseValidationError(f"{where}: unknown assertion type {assertion.get('type')!r}")
    if not assertion.get("id"):
        raise CaseValidationError(f"{where}: assertion needs an id")
    kind = assertion["type"]
    if kind in {"current_fact", "historical_fact", "isolation", "barrier"} and not assertion.get("query"):
        raise CaseValidationError(f"{where}: {kind} assertion needs a query")
    if kind == "leak_rate" and not (assertion.get("query") or assertion.get("queries")):
        raise CaseValidationError(f"{where}: leak_rate assertion needs query or queries")
    if kind in {"current_fact", "historical_fact"} and not any(
        key in assertion for key in ("must_contain", "must_not_contain", "requires_confirmation", "memory_count_max")
    ):
        raise CaseValidationError(
            f"{where}: {kind} assertion needs must_contain/must_not_contain/requires_confirmation/memory_count_max"
        )
    if kind == "decision" and not assertion.get("expected_outcome"):
        raise CaseValidationError(f"{where}: decision assertion needs expected_outcome")
    if kind == "write_quality" and not (assertion.get("expected_facts") or assertion.get("forbidden_facts")):
        raise CaseValidationError(f"{where}: write_quality needs expected_facts/forbidden_facts")


def validate_case(case: dict[str, Any], where: str) -> None:
    missing = [
        field
        for field in ("case_id", "category", "policy_id", "priority", "dataset_version", "review_status")
        if not case.get(field)
    ]
    if missing:
        raise CaseValidationError(f"{where}: case missing {missing}")
    if case["category"] not in CATEGORIES:
        raise CaseValidationError(f"{where}: unknown category {case['category']!r}")
    if case["priority"] not in PRIORITIES:
        raise CaseValidationError(f"{where}: unknown priority {case['priority']!r}")
    if case["review_status"] != "approved":
        raise CaseValidationError(f"{where}: only approved cases may run in the gate")
    for index, event in enumerate(case.get("setup_events") or []):
        _validate_event(event, f"{where}: setup_events[{index}]")
    for index, action in enumerate(case.get("actions") or []):
        _validate_action(action, f"{where}: actions[{index}]")
    for index, event in enumerate(case.get("other_customer_events") or []):
        _validate_event(event, f"{where}: other_customer_events[{index}]")
    if not case.get("assertions"):
        raise CaseValidationError(f"{where}: case has no assertions")
    for index, assertion in enumerate(case.get("assertions") or []):
        _validate_assertion(assertion, f"{where}: assertions[{index}]")


def load_cases(category: str, path: Path | None = None) -> list[dict[str, Any]]:
    source = path or CASES_DIR / f"{category}.jsonl"
    cases: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for line_number, line in enumerate(source.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip() or line.strip().startswith("#"):
            continue
        case = json.loads(line)
        where = f"{source.name}:{line_number}"
        validate_case(case, where)
        if case["case_id"] in seen_ids:
            raise CaseValidationError(f"{where}: duplicate case_id {case['case_id']}")
        seen_ids.add(case["case_id"])
        cases.append(case)
    if not cases:
        raise CaseValidationError(f"{source} contains no cases")
    return cases
