import importlib.util
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "metrics_contract.py"
SPEC = importlib.util.spec_from_file_location("metrics_contract", MODULE_PATH)
assert SPEC and SPEC.loader
metrics_contract = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = metrics_contract
SPEC.loader.exec_module(metrics_contract)


def test_every_alert_and_slo_metric_is_registered():
    # Docs 15.3 metric/alert contract: an unregistered name makes the rule evaluate to
    # empty series, so the alert can never fire.
    assert metrics_contract.check_contract() == []


def test_unregistered_reference_is_reported(tmp_path: Path):
    rules = tmp_path / "alerts.yml"
    rules.write_text(
        """groups:
  - name: test
    rules:
      - alert: TestAlert
        expr: increase(mms_does_not_exist_total[1m]) > 0
""",
        encoding="utf-8",
    )
    registry = tmp_path / "metrics.py"
    registry.write_text(
        """from prometheus_client import Counter

OTHER = Counter("mms_registered_total", "registered", ["reason"])
""",
        encoding="utf-8",
    )
    violations = metrics_contract.check_contract((rules,), registry)
    assert violations == [{"rule": "TestAlert", "metric": "mms_does_not_exist_total"}]


def test_histogram_series_suffixes_resolve_to_base_name(tmp_path: Path):
    rules = tmp_path / "slo-rules.yml"
    rules.write_text(
        """groups:
  - name: test
    rules:
      - record: mms:test:p95
        expr: histogram_quantile(0.95, sum by (le) (rate(mms_registered_seconds_bucket[5m])))
""",
        encoding="utf-8",
    )
    registry = tmp_path / "metrics.py"
    registry.write_text(
        """from prometheus_client import Histogram

OTHER = Histogram("mms_registered_seconds", "registered")
""",
        encoding="utf-8",
    )
    assert metrics_contract.check_contract((rules,), registry) == []
