"""DeepEval runner for benchmark QA cases (docs/4 §二.4/§三.3).

Replays the case history through the real write pipeline, queries with the
question only (no answer leakage), generates the hypothesis with the
configured reader, evaluates with measurement-only metrics and appends the
official-shape result to the benchmark JSONL.
"""

from __future__ import annotations

import time
from typing import Any

from deepeval import assert_test
from deepeval.metrics import BaseMetric
from deepeval.test_case import LLMTestCase

from evals.benchmarks import reader as benchmark_reader
from evals.benchmarks import replay as benchmark_replay
from evals.benchmarks.metrics import answer_in_context, build_benchmark_metrics
from evals.benchmarks.scoring import BenchmarkResultWriter
from evals.harness import EvalTarget
from evals.judge import JudgeConfig
from evals.recorder import EvalRecorder

MAX_REASON_LENGTH = 2000


def _dataset_version(case: dict[str, Any]) -> str:
    source = case.get("source") or {}
    benchmark = str(source.get("benchmark") or "benchmark")
    version = str(source.get("benchmark_version") or source.get("dataset") or "").strip()
    return f"{benchmark} {version}".strip()


def run_benchmark_case(
    target: EvalTarget,
    case: dict[str, Any],
    recorder: EvalRecorder,
    judge: JudgeConfig,
    *,
    result_writer: BenchmarkResultWriter | None = None,
    replayed: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute one converted LongMemEval question (or LoCoMo QA with a
    pre-replayed conversation space) end to end."""
    judge.ensure_usable(purpose=f"benchmark case {case['question_id']}")
    if replayed is None:
        customer = target.new_customer()
        target.grant_consent(customer)
        replayed = benchmark_replay.replay_sessions(
            target,
            customer,
            case["sessions"],
            fact_prefix=f"lme.{case['question_id']}",
        )
        replayed["customer"] = customer
    customer = replayed["customer"]
    started = time.perf_counter()
    query_result = benchmark_replay.benchmark_query(target, customer, case["question"])
    query_result["latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
    hypothesis = benchmark_reader.generate_hypothesis(case["question"], query_result["retrieved"])

    metadata = {
        "gold_answer": case["answer"],
        "retrieved": query_result["retrieved"],
        "question_type": case["question_type"],
    }
    if case.get("answer_session_ids") and replayed.get("session_map"):
        # LongMemEval: evidence recall at session granularity
        metadata["source_map"] = replayed["session_map"]
        metadata["evidence_ids"] = list(case["answer_session_ids"])
    elif case.get("evidence_dia_ids") is not None and replayed.get("source_map"):
        # LoCoMo: evidence recall at dia_id granularity
        metadata["source_map"] = replayed["source_map"]
        metadata["evidence_ids"] = list(case["evidence_dia_ids"])
    test_case = LLMTestCase(
        name=str(case["question_id"]),
        input=str(case["question"]),
        actual_output=hypothesis["hypothesis"],
        expected_output=str(case["answer"]),
        retrieval_context=list(query_result["retrieved"]),
        metadata=metadata,
    )
    metrics: list[BaseMetric] = build_benchmark_metrics(metadata, judge)
    dataset_version = _dataset_version(case)
    failure: Exception | None = None
    for metric in metrics:
        # one metric per assert_test call: latency_ms stays per metric
        is_judge = metric.__class__.__name__ == "GEval"
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
                case_id=str(case["question_id"]),
                category=f"benchmark.{case['question_type']}",
                risk_level="medium",
                dataset_version=dataset_version,
                metric_name=getattr(metric, "__name__", None) or type(metric).__name__,
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
    extra: dict[str, Any] = {
        "gold_in_context": answer_in_context(case["answer"], query_result["retrieved"]) if case.get("answer") else None,
        "retrieved_count": query_result["retrieved_count"],
        "query_latency_ms": query_result["latency_ms"],
        "evidence_dia_ids": case.get("evidence_dia_ids") or [],
        "benchmark": case.get("source", {}).get("benchmark"),
    }
    if result_writer is not None:
        result_writer.add(
            question_id=str(case["question_id"]),
            question_type=str(case["question_type"]),
            question=str(case["question"]),
            gold_answer=str(case.get("answer") or ""),
            adversarial_answer=str(case.get("adversarial_answer") or ""),
            hypothesis=hypothesis["hypothesis"],
            reader=hypothesis["reader"],
            retrieved=list(query_result["retrieved"]),
            extra=extra,
        )
    return {**query_result, "hypothesis": hypothesis}
