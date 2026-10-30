"""DeepEval metrics for public benchmark QA cases.

Public benchmarks are measurement sets (docs/4 §六 priority "regular"), not
merge gates: their metrics use threshold 0.0 so scores are recorded and
trend-compared against the eval baseline without blocking on any single
question. Deterministic metrics always run; the LLM-judge layer (answer
correctness GEval) only runs when a judge is configured.
"""

from __future__ import annotations

import re
from typing import Any

from deepeval.metrics import BaseMetric
from deepeval.test_case import LLMTestCase

from evals.judge import JudgeConfig
from evals.metrics.judge_metrics import DEFAULT_CORRECTNESS_CRITERIA

TOKEN_RE = re.compile(r"[a-z0-9]+")
PUNCT = re.compile(r"[^\w\s]")


def normalize(text: str) -> str:
    return " ".join(PUNCT.sub(" ", str(text).lower()).split())


def answer_in_context(gold_answer: str, retrieved: list[str]) -> bool:
    """Normalized containment: does the retrieved memory context surface the
    gold answer (evidence sufficiency, the offline retrieval-grounded proxy)?"""
    gold = normalize(gold_answer)
    if not gold:
        return False
    joined = normalize(" ".join(retrieved))
    if gold in joined:
        return True
    gold_tokens = set(TOKEN_RE.findall(gold))
    if not gold_tokens:
        return False
    return all(token in joined for token in gold_tokens)


class GoldAnswerRecallMetric(BaseMetric):
    """Retrieval-grounded evidence sufficiency: the retrieved memory context
    contains the gold answer. Measures the MMS pipeline, not the reader."""

    def __init__(self, threshold: float = 0.0):
        super().__init__()
        self.threshold = threshold
        self.async_mode = False

    def measure(self, test_case: LLMTestCase, *args, **kwargs) -> float:
        metadata = test_case.metadata or {}
        gold = str(metadata.get("gold_answer", ""))
        retrieved = list(metadata.get("retrieved") or [])
        hit = answer_in_context(gold, retrieved)
        self.score = 1.0 if hit else 0.0
        self.score_breakdown = {"gold_in_context": hit}
        self.reason = (
            "gold answer is present in the retrieved memory context"
            if hit
            else "gold answer is not present in the retrieved memory context"
        )
        return self.score

    async def a_measure(self, test_case: LLMTestCase, *args, **kwargs) -> float:
        return self.measure(test_case, *args, **kwargs)

    @property
    def __name__(self):
        return "Gold Answer Recall"


class EvidenceRecallMetric(BaseMetric):
    """EvidenceRecall@K = |E ∩ R_K| / |E| (docs/4 §三.3).

    E is the gold evidence id set (sidecar); R_K maps retrieved memory content
    back to source ids through the replay write log (exact text match). Only
    results with a real source mapping may use this metric - memory ids are
    never substituted for source ids."""

    def __init__(self, threshold: float = 0.0):
        super().__init__()
        self.threshold = threshold
        self.async_mode = False

    def measure(self, test_case: LLMTestCase, *args, **kwargs) -> float:
        metadata = test_case.metadata or {}
        evidence = {str(item) for item in metadata.get("evidence_ids") or []}
        source_map: dict[str, str] = metadata.get("source_map") or {}
        retrieved = list(metadata.get("retrieved") or [])
        covered = {source_map[content] for content in retrieved if content in source_map}
        self.score = (len(evidence & covered) / len(evidence)) if evidence else 1.0
        self.score_breakdown = {
            "evidence_count": len(evidence),
            "covered": len(evidence & covered),
            "unmapped_retrieved": sum(1 for content in retrieved if content not in source_map),
        }
        self.reason = f"evidence recall {self.score}: covered {sorted(evidence & covered)} of {sorted(evidence)}"
        return self.score

    async def a_measure(self, test_case: LLMTestCase, *args, **kwargs) -> float:
        return self.measure(test_case, *args, **kwargs)

    @property
    def __name__(self):
        return "Evidence Recall@K"


def build_benchmark_metrics(metadata: dict[str, Any], judge: JudgeConfig) -> list[Any]:
    metrics: list[Any] = []
    if metadata.get("gold_answer"):
        metrics.append(GoldAnswerRecallMetric())
    if metadata.get("evidence_ids") is not None:
        metrics.append(EvidenceRecallMetric())
    if judge.available:
        from deepeval.metrics import GEval
        from deepeval.test_case import SingleTurnParams

        metrics.append(
            GEval(
                name="Answer Correctness (Judge)",
                criteria=str(metadata.get("judge_criteria") or DEFAULT_CORRECTNESS_CRITERIA),
                evaluation_params=[
                    SingleTurnParams.INPUT,
                    SingleTurnParams.ACTUAL_OUTPUT,
                    SingleTurnParams.EXPECTED_OUTPUT,
                ],
                threshold=float(metadata.get("judge_threshold", 0.80)),
                model=judge.model,
                strict_mode=False,
            )
        )
    return metrics
