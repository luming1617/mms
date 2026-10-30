"""RAG-quality evaluation (docs/1-模型质量评测体系设计.md §3.3, §6.2).

The retrieval_context handed to the judge metrics always comes from the real
query response, never from the ideal answer. Faithfulness is intentionally not
used: the service under test is a retrieval pipeline whose "answer" is the
memory context itself, so contextual relevancy / recall / precision and a
no-fabrication correctness metric are the meaningful judge dimensions.
"""

from pathlib import Path

import pytest

from evals.conftest import golden_ids, requires_db
from evals.loader import load_dataset
from evals.runner import run_golden

pytestmark = [pytest.mark.evaluation, pytest.mark.integration, requires_db]

dataset = load_dataset(Path(__file__).resolve().parent / "datasets" / "rag_cases.json")


@pytest.mark.parametrize("golden", dataset.goldens, ids=golden_ids(dataset))
def test_rag_quality(golden, target, recorder, judge):
    run_golden(target, golden, recorder, judge)
