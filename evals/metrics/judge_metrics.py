"""LLM-as-a-judge metric builders.

Metrics are built per golden from ``additional_metadata.applicable_metrics`` and
the optional ``judge`` block (per-case criteria / thresholds). Thresholds here
are starting points, not universal standards: calibrate them against
human-reviewed samples (docs/1-模型质量评测体系设计.md §9.1, see
evals/calibration/).
"""

from __future__ import annotations

from typing import Any

from deepeval.metrics import (
    AnswerRelevancyMetric,
    ContextualPrecisionMetric,
    ContextualRecallMetric,
    ContextualRelevancyMetric,
    GEval,
)
from deepeval.test_case import SingleTurnParams

from evals.judge import JudgeConfig

DEFAULT_RELEVANCY_THRESHOLD = 0.75
DEFAULT_CORRECTNESS_THRESHOLD = 0.80
DEFAULT_CONTEXTUAL_THRESHOLD = 0.75

DEFAULT_CORRECTNESS_CRITERIA = (
    "Evaluate whether the actual memory context is consistent with the expected answer and the "
    "provided reference facts. Penalize factual contradictions, missing essential information, "
    "unsupported claims and privacy violations."
)

PARAMS = SingleTurnParams


def _judge_overrides(metadata: dict[str, Any]) -> dict[str, Any]:
    return metadata.get("judge") or {}


def build_judge_metrics(applicable: list[str], metadata: dict[str, Any], judge: JudgeConfig) -> list[Any]:
    """Build the LLM-judge metrics declared by a golden (empty when no judge is configured)."""
    if not judge.available:
        return []
    overrides = _judge_overrides(metadata)
    model = judge.model
    metrics: list[Any] = []
    if "AnswerRelevancy" in applicable:
        metrics.append(
            AnswerRelevancyMetric(
                threshold=float(overrides.get("answer_relevancy_threshold", DEFAULT_RELEVANCY_THRESHOLD)),
                model=model,
                include_reason=True,
            )
        )
    if "AnswerCorrectness" in applicable:
        metrics.append(
            GEval(
                name="Answer Correctness",
                criteria=str(overrides.get("criteria") or DEFAULT_CORRECTNESS_CRITERIA),
                evaluation_params=[
                    PARAMS.INPUT,
                    PARAMS.ACTUAL_OUTPUT,
                    PARAMS.EXPECTED_OUTPUT,
                ],
                threshold=float(overrides.get("threshold", DEFAULT_CORRECTNESS_THRESHOLD)),
                model=model,
                strict_mode=False,
            )
        )
    if "ContextualRelevancy" in applicable:
        metrics.append(
            ContextualRelevancyMetric(
                threshold=float(overrides.get("contextual_relevancy_threshold", DEFAULT_CONTEXTUAL_THRESHOLD)),
                model=model,
                include_reason=True,
            )
        )
    if "ContextualRecall" in applicable:
        metrics.append(
            ContextualRecallMetric(
                threshold=float(overrides.get("contextual_recall_threshold", DEFAULT_CONTEXTUAL_THRESHOLD)),
                model=model,
                include_reason=True,
            )
        )
    if "ContextualPrecision" in applicable:
        metrics.append(
            ContextualPrecisionMetric(
                threshold=float(overrides.get("contextual_precision_threshold", DEFAULT_CONTEXTUAL_THRESHOLD)),
                model=model,
                include_reason=True,
            )
        )
    return metrics
