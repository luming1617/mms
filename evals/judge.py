"""Judge model configuration.

The evaluated application (target) and the model that judges it must stay
separate (docs/1-模型质量评测体系设计.md §5.3):

- target model: the MMS retrieval pipeline (embedding provider, decision rules)
- judge model: the LLM backing DeepEval metrics, selected via MMS_EVAL_JUDGE_MODEL

Judge metrics are only built when a judge model is configured. Deterministic
contract metrics always run so the PR gate stays offline and reproducible.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass

JUDGE_MODEL_ENV = "MMS_EVAL_JUDGE_MODEL"
REQUIRE_JUDGE_ENV = "MMS_EVAL_REQUIRE_JUDGE"


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


JUDGE_PROMPT_VERSION = "evals-judge-criteria-v1"


def _package_version(name: str) -> str | None:
    try:
        from importlib.metadata import PackageNotFoundError, version
    except ImportError:  # pragma: no cover - stdlib always provides it on 3.10+
        return None
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def judge_fingerprint(model: str | None) -> str | None:
    """Stable id for the judge configuration recorded in every eval report.

    Covers the judge model id, the DeepEval prompt/implementation version and
    the default criteria/thresholds. Per-case judge overrides live in the
    golden datasets and are covered by the recorded dataset hashes.
    """
    if not model:
        return None
    from evals.metrics.judge_metrics import (
        DEFAULT_CONTEXTUAL_THRESHOLD,
        DEFAULT_CORRECTNESS_CRITERIA,
        DEFAULT_CORRECTNESS_THRESHOLD,
        DEFAULT_RELEVANCY_THRESHOLD,
    )

    payload = {
        "model": model,
        "deepeval_version": _package_version("deepeval"),
        "prompt_version": JUDGE_PROMPT_VERSION,
        "criteria": DEFAULT_CORRECTNESS_CRITERIA,
        "thresholds": {
            "answer_relevancy": DEFAULT_RELEVANCY_THRESHOLD,
            "answer_correctness": DEFAULT_CORRECTNESS_THRESHOLD,
            "contextual": DEFAULT_CONTEXTUAL_THRESHOLD,
        },
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


@dataclass(frozen=True)
class JudgeConfig:
    model: str | None
    required: bool

    @classmethod
    def from_env(cls) -> JudgeConfig:
        model = os.environ.get(JUDGE_MODEL_ENV) or None
        return cls(model=model, required=_env_flag(REQUIRE_JUDGE_ENV))

    @property
    def available(self) -> bool:
        return bool(self.model)

    @property
    def label(self) -> str:
        return self.model or "not-configured"

    def ensure_usable(self, *, purpose: str) -> None:
        """Fail loudly when a judge is required but missing (never silently pass)."""
        if self.required and not self.available:
            raise RuntimeError(
                f"{REQUIRE_JUDGE_ENV}=1 but {JUDGE_MODEL_ENV} is not set; refusing to run {purpose} "
                "without an LLM judge (docs/1-模型质量评测体系设计.md §7.3)"
            )
