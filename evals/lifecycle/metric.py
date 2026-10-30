"""DeepEval integration for lifecycle cases and benchmark QA cases.

Both build an LLMTestCase, evaluate it via assert_test and record per-metric
results in the shared EvalRecorder, mirroring evals/runner.run_golden.
Lifecycle cases are hard-gate material (threshold 1.0); benchmark cases are
measurement-only (threshold 0.0, docs/4 §六 priority "regular").
"""

from __future__ import annotations

import time
from typing import Any

from deepeval import assert_test
from deepeval.metrics import BaseMetric
from deepeval.test_case import LLMTestCase

from evals.harness import EvalTarget
from evals.judge import JudgeConfig
from evals.lifecycle.cases import RISK_BY_PRIORITY
from evals.lifecycle.runner import execute_case
from evals.recorder import EvalRecorder

MAX_REASON_LENGTH = 2000


class LifecycleAssertionMetric(BaseMetric):
    """Aggregates the deterministic assertion results of one lifecycle case.

    score = passed assertions / total; threshold 1.0 (every assertion must
    hold - deletion/isolation leak assertions are hard-gate samples).
    """

    def __init__(self, threshold: float = 1.0):
        super().__init__()
        self.threshold = threshold
        self.async_mode = False

    def measure(self, test_case: LLMTestCase, *args, **kwargs) -> float:
        assertions: dict[str, Any] = (test_case.metadata or {}).get("assertions") or {}
        if not assertions:
            self.score = 0.0
            self.score_breakdown = {}
            self.reason = "case produced no assertion results"
            return self.score
        self.score_breakdown = {name: bool(result.get("ok")) for name, result in assertions.items()}
        self.score = sum(1 for ok in self.score_breakdown.values() if ok) / len(self.score_breakdown)
        failed = [name for name, ok in self.score_breakdown.items() if not ok]
        self.reason = (
            "all lifecycle assertions passed"
            if not failed
            else f"failed assertions: {', '.join(failed)} ({self._details(assertions, failed)})"
        )
        return self.score

    def _details(self, assertions: dict[str, Any], failed: list[str]) -> str:
        parts = []
        for name in failed[:5]:
            result = assertions.get(name) or {}
            detail = result.get("detail") or {
                k: result[k] for k in ("http_status", "error_code", "contents") if k in result
            }
            parts.append(f"{name}: {detail}")
        return " | ".join(str(part)[:200] for part in parts)

    async def a_measure(self, test_case: LLMTestCase, *args, **kwargs) -> float:
        return self.measure(test_case, *args, **kwargs)

    @property
    def __name__(self):
        return "Lifecycle Assertions"


def run_lifecycle_case(target: EvalTarget, case: dict[str, Any], recorder: EvalRecorder, judge: JudgeConfig) -> None:
    judge.ensure_usable(purpose=f"lifecycle case {case['case_id']}")
    actual = execute_case(target, case)
    assertions = actual["assertions"]
    descriptions = []
    for assertion in case.get("assertions") or []:
        result = assertions.get(assertion["id"]) or {}
        if assertion["type"] in {"current_fact", "historical_fact"}:
            descriptions.append(
                f"{assertion['id']}: {assertion.get('query')} -> {result.get('contents') or result.get('detail')}"
            )
    test_case = LLMTestCase(
        name=str(case["case_id"]),
        input=str(case.get("description") or case["case_id"]),
        actual_output="\n".join(descriptions) or "(no query assertions)",
        expected_output=str(case.get("policy_id")),
        metadata={"assertions": assertions, "case_id": case["case_id"]},
    )
    metrics: list[BaseMetric] = [LifecycleAssertionMetric()]
    started = time.perf_counter()
    failure: str | None = None
    try:
        assert_test(test_case=test_case, metrics=metrics, run_async=False)
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"[:MAX_REASON_LENGTH]
        raise
    finally:
        latency_ms = (time.perf_counter() - started) * 1000
        for metric in metrics:
            score = metric.score if isinstance(metric.score, (int, float)) else None
            recorder.record(
                case_id=str(case["case_id"]),
                category=f"lifecycle.{case['category']}",
                risk_level=RISK_BY_PRIORITY[case["priority"]],
                dataset_version=str(case.get("dataset_version", "unknown")),
                metric_name=getattr(metric, "__name__", None) or type(metric).__name__,
                threshold=float(metric.threshold) if metric.threshold is not None else None,
                score=round(score, 6) if score is not None else None,
                passed=metric.is_successful(),
                reason=str(metric.reason)[:MAX_REASON_LENGTH] if metric.reason else None,
                latency_ms=latency_ms,
                error=failure,
                judge=False,
            )
