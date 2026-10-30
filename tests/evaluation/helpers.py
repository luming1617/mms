import json
import os

import pytest
from deepeval import assert_test
from deepeval.test_case import LLMTestCase
from tests.evaluation.metrics import ServiceResultMetric

DSN = os.environ.get("MMS_TEST_DATABASE_URL")
requires_db = pytest.mark.skipif(not DSN, reason="set MMS_TEST_DATABASE_URL for DeepEval service tests")


def evaluate(name: str, input_text: str, actual: dict, expected: dict) -> None:
    case = LLMTestCase(
        name=name,
        input=input_text,
        actual_output=json.dumps(actual, ensure_ascii=False, default=str, sort_keys=True),
        expected_output=json.dumps(expected, ensure_ascii=False, default=str, sort_keys=True),
        metadata={"executor": "real-mms-service", "llm_judge": False},
    )
    assert_test(case, [ServiceResultMetric()], run_async=False)
