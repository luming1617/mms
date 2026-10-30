import json
from typing import Any

from deepeval.metrics import BaseMetric
from deepeval.test_case import LLMTestCase


class ServiceResultMetric(BaseMetric):
    def __init__(self):
        super().__init__()
        self.threshold = 1.0
        self.async_mode = False

    def measure(self, test_case: LLMTestCase, *args, **kwargs) -> float:
        actual = json.loads(test_case.actual_output)
        expected = json.loads(test_case.expected_output)
        checks = self._checks(expected, actual)
        matches = sum(checks.values())
        self.score = matches / len(checks) if checks else 1.0
        self.score_breakdown = checks
        self.reason = "all service fields matched" if all(checks.values()) else "one or more service fields mismatched"
        return self.score

    async def a_measure(self, test_case: LLMTestCase, *args, **kwargs) -> float:
        return self.measure(test_case, *args, **kwargs)

    def _checks(self, expected: Any, actual: Any) -> dict[str, bool]:
        if isinstance(expected, dict) and isinstance(actual, dict):
            checks = {}
            for key, value in expected.items():
                nested = self._checks(value, actual.get(key))
                checks.update({f"{key}.{name}": result for name, result in nested.items()})
            return checks
        return {"value": actual == expected}

    @property
    def __name__(self):
        return "Service Result Contract"
