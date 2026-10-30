"""Regression evaluation suite (docs/1-模型质量评测体系设计.md §4.2).

Every confirmed quality issue should grow this dataset: duplicate-ingest
idempotency, conflict review supersession, lifecycle expiry and the deletion
barrier run through the same golden-driven harness as the core set.
"""

from pathlib import Path

import pytest

from evals.conftest import golden_ids, requires_db
from evals.loader import load_dataset
from evals.runner import run_golden

pytestmark = [pytest.mark.evaluation, pytest.mark.integration, requires_db]

dataset = load_dataset(Path(__file__).resolve().parent / "datasets" / "regression_cases.json")


@pytest.mark.parametrize("golden", dataset.goldens, ids=golden_ids(dataset))
def test_regression(golden, target, recorder, judge):
    run_golden(target, golden, recorder, judge)
