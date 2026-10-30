"""Deterministic contract metric.

Checks the structured retrieval/decision contract declared by each golden's
``expectations`` block against the actual response captured by the harness.
This metric never calls an LLM: it is the always-on PR gate floor
(docs/1-模型质量评测体系设计.md §3.1 - prefer deterministic checks before
LLM Judge).

The harness serializes the executed scenario result into
``test_case.metadata["actual"]``; expectations come from
``test_case.metadata["contract"]``.
"""

from __future__ import annotations

import json
from itertools import pairwise
from typing import Any

from deepeval.metrics import BaseMetric
from deepeval.test_case import LLMTestCase

REQUIRED_MEMORY_FIELDS = (
    "memory_id",
    "memory_type",
    "content",
    "score",
    "matched_view_kind",
    "confidence_label",
    "use_policy",
)


class MemoryContextContractMetric(BaseMetric):
    def __init__(self, threshold: float = 1.0):
        super().__init__()
        self.threshold = threshold
        self.async_mode = False

    def measure(self, test_case: LLMTestCase, *args, **kwargs) -> float:
        metadata = test_case.metadata or {}
        contract = metadata.get("contract") or {}
        actual_raw = metadata.get("actual")
        actual = json.loads(actual_raw) if isinstance(actual_raw, str) else (actual_raw or {})
        checks = self._run_checks(contract, actual)
        self.score_breakdown = {name: bool(ok) for name, ok in checks.items()}
        self.score = sum(1 for ok in checks.values() if ok) / len(checks) if checks else 1.0
        failed = [name for name, ok in checks.items() if not ok]
        self.reason = (
            "all deterministic contract checks passed" if not failed else f"failed contract checks: {', '.join(failed)}"
        )
        return self.score

    async def a_measure(self, test_case: LLMTestCase, *args, **kwargs) -> float:
        return self.measure(test_case, *args, **kwargs)

    def _run_checks(self, contract: dict[str, Any], actual: dict[str, Any]) -> dict[str, bool]:
        memories = actual.get("memories") or []
        contents = [str(memory.get("content", "")) for memory in memories]
        checks: dict[str, bool] = {}
        if "http_status" in contract:
            checks["http_status"] = actual.get("http_status") == contract["http_status"]
        if "top1_content" in contract:
            expected = str(contract["top1_content"])
            checks["top1_content"] = bool(contents) and contents[0] == expected
        if "top1_contains" in contract:
            expected = str(contract["top1_contains"])
            checks["top1_contains"] = bool(contents) and expected in contents[0]
        if "memory_count_min" in contract:
            checks["memory_count_min"] = len(memories) >= int(contract["memory_count_min"])
        if "memory_count_max" in contract:
            checks["memory_count_max"] = len(memories) <= int(contract["memory_count_max"])
        if "contents_subset" in contract:
            allowed = {str(item) for item in contract["contents_subset"]}
            checks["contents_subset"] = set(contents) <= allowed
        if "absent_terms" in contract:
            joined = "\n".join(contents)
            checks["absent_terms"] = not any(str(term) in joined for term in contract["absent_terms"])
        if contract.get("scores_non_increasing"):
            scores = [float(memory.get("score") or 0.0) for memory in memories]
            checks["scores_non_increasing"] = all(a >= b for a, b in pairwise(scores))
        if contract.get("required_memory_fields"):
            checks["required_memory_fields"] = all(
                all(field in memory for field in REQUIRED_MEMORY_FIELDS) for memory in memories
            )
        if contract.get("trust_untrusted"):
            checks["trust_untrusted"] = actual.get("trust") == "untrusted_data"
        if contract.get("lexical_used"):
            checks["lexical_used"] = bool(actual.get("lexical_used"))
        if contract.get("token_budget_filtered"):
            filtered = (actual.get("filtered_reasons") or {}).get("token_budget", 0)
            checks["token_budget_filtered"] = bool(filtered)
        if "decision_outcomes" in contract:
            expected_outcomes = [str(item) for item in contract["decision_outcomes"]]
            outcomes = [str(decision.get("outcome")) for decision in actual.get("decisions") or []]
            checks["decision_outcomes"] = outcomes == expected_outcomes
        if "decision_reason_codes" in contract:
            expected_reasons = {str(item) for item in contract["decision_reason_codes"]}
            reason_codes = {
                str(code) for decision in actual.get("decisions") or [] for code in decision.get("reason_codes") or []
            }
            checks["decision_reason_codes"] = expected_reasons <= reason_codes
        if contract.get("review_approved"):
            checks["review_approved"] = actual.get("review_status") == "APPROVED"
        if "deletion_status" in contract:
            checks["deletion_status"] = actual.get("deletion_status") == contract["deletion_status"]
        if "barrier_code" in contract:
            checks["barrier_code"] = actual.get("error_code") == contract["barrier_code"]
        if not checks:
            checks["contract_declared"] = False
        return checks

    @property
    def __name__(self):
        return "Memory Context Contract"
