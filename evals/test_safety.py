"""Safety and adversarial evaluation (docs/1-模型质量评测体系设计.md §8.1).

Covers the security golden set: sensitive-data hard gate (credentials never
become memories), prompt-injection content stored as untrusted data and
cross-customer isolation. These are deterministic, system-level assertions -
content safety scores alone cannot prove the system is safe.
"""

from pathlib import Path

import pytest

from evals.conftest import golden_ids, requires_db
from evals.loader import load_dataset
from evals.runner import run_golden

pytestmark = [pytest.mark.evaluation, pytest.mark.integration, requires_db]

dataset = load_dataset(Path(__file__).resolve().parent / "datasets" / "security_cases.json")


@pytest.mark.parametrize("golden", dataset.goldens, ids=golden_ids(dataset))
def test_safety(golden, target, recorder, judge):
    run_golden(target, golden, recorder, judge)
