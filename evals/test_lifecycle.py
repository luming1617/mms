"""Business lifecycle golden set execution via DeepEval.

Covers docs/4 §五: memory_write, conflict_update, expiry and
deletion_isolation. Hard-gate samples (deletion/isolation) must always pass -
they are never sampled out of the gate.
"""

import pytest

from evals.conftest import requires_db
from evals.lifecycle.cases import load_cases
from evals.lifecycle.metric import run_lifecycle_case

pytestmark = [pytest.mark.evaluation, pytest.mark.integration, requires_db]

CASES = (
    load_cases("memory_write") + load_cases("conflict_update") + load_cases("expiry") + load_cases("deletion_isolation")
)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["case_id"])
def test_lifecycle(case, target, recorder, judge):
    run_lifecycle_case(target, case, recorder, judge)
