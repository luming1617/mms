"""Scenario execution and DeepEval evaluation runner.

Each golden declares a scenario (retrieval / ingestion_gate / deletion_barrier).
The runner executes it against the real service via the harness, assembles an
LLMTestCase (actual output = memory context, retrieval_context = retrieved
contents), evaluates it with the deterministic contract metric plus optional
LLM-judge metrics through ``assert_test``, and records every metric result.
"""

from __future__ import annotations

import time
from typing import Any

from deepeval import assert_test
from deepeval.test_case import LLMTestCase

from evals.harness import EvalTarget
from evals.judge import JudgeConfig
from evals.metrics import MemoryContextContractMetric, build_judge_metrics
from evals.recorder import EvalRecorder

MAX_REASON_LENGTH = 2000


def execute_scenario(target: EvalTarget, golden_metadata: dict[str, Any], query_text: str) -> dict[str, Any]:
    scenario = golden_metadata["scenario"]
    customer = target.new_customer()
    target.grant_consent(customer)
    if golden_metadata.get("seed_fact_key_registry"):
        # explicit registry row first: goldens that need conflict semantics
        # declare a mutual-exclusion group that the generic row must not override
        target.seed_fact_key_registry(str(golden_metadata["seed_fact_key_registry"]))
    for seed in golden_metadata.get("seed_candidates") or []:
        target.seed_fact_key_registry(str(seed["fact_key"]))
    decisions = [
        target.ingest(customer, seed, index) for index, seed in enumerate(golden_metadata.get("seed_candidates") or [])
    ]
    target.drain_embeddings()
    if golden_metadata.get("run_lifecycle"):
        target.run_lifecycle()

    actual: dict[str, Any] = {
        "scenario": scenario,
        "decisions": decisions,
        "review_status": None,
        "deletion_status": None,
        "error_code": None,
        "http_status": None,
        "memories": [],
        "memory_contents": [],
        "trust": None,
        "lexical_used": None,
        "filtered_reasons": {},
        "degraded": None,
    }

    if golden_metadata.get("review_flow") == "approve_first":
        review = target.approve_first_review(customer)
        actual["review_status"] = (review or {}).get("status")

    if scenario == "deletion_barrier":
        target.request_deletion(customer)
        review = target.approve_first_review(customer) or {}
        actual["review_status"] = review.get("status")
        deletion = review.get("deletion") or {}
        if deletion.get("request_id"):
            status = target.deletion_status(customer, str(deletion["request_id"]))
            actual["deletion_status"] = status.get("status")

    query_customer = customer
    if golden_metadata.get("query", {}).get("as_other_customer"):
        query_customer = target.new_customer()
        target.grant_consent(query_customer)

    query = dict(golden_metadata.get("query") or {})
    query["query_text"] = query_text
    started = time.perf_counter()
    http_status, payload, error_code = target.query(query_customer, query)
    actual["target_latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
    actual["http_status"] = http_status
    actual["error_code"] = error_code
    if payload is not None:
        if "memory_context" in payload:
            # Doc 11.4 agent envelope: flat memory_context for the agent, full
            # retrieval result (memories/metrics) under `retrieval`.
            context = payload.get("retrieval") or {}
            memories = context.get("memories") or payload.get("memory_context") or []
            actual["trust"] = payload.get("trust")
        else:
            context = payload
            memories = context.get("memories") or []
            actual["trust"] = None
        actual["memories"] = memories
        actual["memory_contents"] = [str(memory.get("content", "")) for memory in memories]
        actual["lexical_used"] = context.get("lexical_used")
        actual["filtered_reasons"] = context.get("filtered_reasons") or {}
        actual["degraded"] = context.get("degraded")
    return actual


def assemble_memory_context(actual: dict[str, Any]) -> str:
    contents = actual.get("memory_contents") or []
    if not contents:
        error = actual.get("error_code")
        return f"(no memories returned; error={error})" if error else "(no memories returned)"
    return "\n".join(f"{index}. {content}" for index, content in enumerate(contents, start=1))


def build_test_case(golden: Any, actual: dict[str, Any]) -> LLMTestCase:
    metadata = golden.additional_metadata or {}
    return LLMTestCase(
        name=str(metadata.get("case_id", "case")),
        input=golden.input,
        actual_output=assemble_memory_context(actual),
        expected_output=golden.expected_output,
        context=list(golden.context or []),
        retrieval_context=list(actual.get("memory_contents") or []),
        metadata={
            "contract": metadata.get("expectations") or {},
            "actual": actual,
            "golden": {"case_id": metadata.get("case_id"), "category": metadata.get("category")},
        },
    )


def metric_label(metric: Any) -> str:
    name = getattr(metric, "name", None)
    if isinstance(name, str) and name:
        return name
    dunder = getattr(metric, "__name__", None)
    if isinstance(dunder, str) and dunder:
        return dunder
    return type(metric).__name__


def run_golden(target: EvalTarget, golden: Any, recorder: EvalRecorder, judge: JudgeConfig) -> None:
    metadata = golden.additional_metadata or {}
    applicable = list(metadata.get("applicable_metrics") or [])
    judge.ensure_usable(purpose=f"case {metadata.get('case_id')}")
    actual = execute_scenario(target, metadata, golden.input)
    test_case = build_test_case(golden, actual)
    metrics: list[Any] = [MemoryContextContractMetric()]
    metrics.extend(build_judge_metrics(applicable, metadata, judge))
    failure: Exception | None = None
    for metric in metrics:
        # measure one metric per assert_test call so latency_ms is this
        # metric's own runtime instead of a per-case average
        is_judge = not isinstance(metric, MemoryContextContractMetric)
        started = time.perf_counter()
        try:
            assert_test(test_case=test_case, metrics=[metric], run_async=False)
        except Exception as exc:  # record all metrics first, then fail loudly
            failure = failure or exc
        finally:
            latency_ms = (time.perf_counter() - started) * 1000
            error = None
            if getattr(metric, "error", None):
                error = str(metric.error)[:MAX_REASON_LENGTH]
            elif failure is not None and not metric.is_successful():
                error = f"{type(failure).__name__}: {failure}"[:MAX_REASON_LENGTH]
            score = metric.score if isinstance(metric.score, (int, float)) else None
            recorder.record(
                case_id=str(metadata.get("case_id", "case")),
                category=str(metadata.get("category", "uncategorized")),
                risk_level=str(metadata.get("risk_level", "unknown")),
                dataset_version=str(metadata.get("dataset_version", "unknown")),
                metric_name=metric_label(metric),
                threshold=float(metric.threshold) if metric.threshold is not None else None,
                score=round(score, 6) if score is not None else None,
                passed=metric.is_successful(),
                reason=str(metric.reason)[:MAX_REASON_LENGTH] if metric.reason else None,
                latency_ms=latency_ms,
                error=error,
                judge=is_judge,
            )
    if failure is not None:
        raise failure
