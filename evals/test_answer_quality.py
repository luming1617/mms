"""Core answer-quality evaluation (docs/1-模型质量评测体系设计.md §6.1).

Deterministic memory-context contract plus optional AnswerRelevancy /
AnswerCorrectness LLM-judge metrics over the versioned core golden set.
"""

from pathlib import Path

import pytest

from evals.conftest import golden_ids, requires_db
from evals.loader import load_dataset
from evals.runner import run_golden

pytestmark = [pytest.mark.evaluation, pytest.mark.integration, requires_db]

dataset = load_dataset(Path(__file__).resolve().parent / "datasets" / "core_goldens.json")


@pytest.mark.parametrize("golden", dataset.goldens, ids=golden_ids(dataset))
def test_answer_quality(golden, target, recorder, judge):
    run_golden(target, golden, recorder, judge)
