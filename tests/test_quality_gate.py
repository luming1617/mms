import argparse
import importlib.util
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "quality_gate.py"
SPEC = importlib.util.spec_from_file_location("quality_gate", MODULE_PATH)
assert SPEC and SPEC.loader
quality_gate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = quality_gate
SPEC.loader.exec_module(quality_gate)


def test_fingerprint_diff_reports_new_and_resolved_items():
    baseline = {"fingerprints": {"ruff": ["E501|a.py|x", "F401|b.py|y"]}}
    new, resolved = quality_gate.fingerprint_diff({"E501|a.py|x", "B008|c.py|z"}, baseline, "ruff", {"B008|c.py|z"})
    assert new == []
    assert resolved == ["F401|b.py|y"]


def test_expired_exemptions_are_governance_failures():
    stage = quality_gate.StageResult("bandit", quality_gate.PASS)
    expired = [
        {
            "tool": "bandit",
            "fingerprint": "B608|src/mms/x.py|sql",
            "reason": "reviewed",
            "owner": "security",
            "expires": "2000-01-01",
        }
    ]
    allowed, problems = quality_gate.active_exemptions(expired, "bandit", stage)
    assert allowed == set()
    assert problems and "expired exemption" in problems[0]


def test_exemptions_require_reason_owner_and_expiry():
    stage = quality_gate.StageResult("mypy", quality_gate.PASS)
    allowed, problems = quality_gate.active_exemptions([{"tool": "mypy", "fingerprint": "x"}], "mypy", stage)
    assert allowed == set()
    assert any("missing" in problem for problem in problems)


def test_coverage_metrics_accepts_src_layout(tmp_path):
    payload = {
        "totals": {
            "num_statements": 10,
            "covered_lines": 9,
            "num_branches": 4,
            "covered_branches": 3,
            "excluded_lines": 0,
        },
        "files": {"src/mms/api/main.py": {"summary": {"percent_covered": 90.0}}},
    }
    path = tmp_path / "coverage.json"
    path.write_text(json.dumps(payload))
    metrics, problems = quality_gate._coverage_metrics(path)
    assert problems == []
    assert metrics["line"] == 90.0
    assert metrics["branch"] == 75.0
    assert metrics["files_measured"] == 1


def test_coverage_metrics_rejects_empty_or_foreign_reports(tmp_path):
    payload = {
        "totals": {"num_statements": 10, "covered_lines": 10, "num_branches": 0, "covered_branches": 0},
        "files": {"other/package.py": {}},
    }
    path = tmp_path / "coverage.json"
    path.write_text(json.dumps(payload))
    _, problems = quality_gate._coverage_metrics(path)
    assert problems


def test_overall_status_semantics():
    assert quality_gate.overall_status([quality_gate.StageResult("a", quality_gate.PASS)]) == quality_gate.PASS
    assert quality_gate.overall_status([quality_gate.StageResult("a", quality_gate.WARN)]) == quality_gate.WARN
    assert quality_gate.overall_status([quality_gate.StageResult("a", quality_gate.FAIL)]) == quality_gate.FAIL
    assert quality_gate.overall_status([quality_gate.StageResult("a", quality_gate.SKIP)]) == quality_gate.PASS


def test_baseline_stage_allows_cold_start_in_update_mode(tmp_path):
    args = argparse.Namespace(baseline=tmp_path / "baseline.json", update_baseline=True)
    stage = quality_gate.stage_baseline(args, quality_gate.load_baseline(args.baseline), [], tmp_path)
    assert stage.status == quality_gate.PASS
    assert any("will create" in detail for detail in stage.details)


def test_baseline_stage_requires_file_without_update_mode(tmp_path):
    args = argparse.Namespace(baseline=tmp_path / "baseline.json", update_baseline=False)
    stage = quality_gate.stage_baseline(args, quality_gate.load_baseline(args.baseline), [], tmp_path)
    assert stage.status == quality_gate.FAIL
    assert any("baseline missing" in detail for detail in stage.details)


def _patch_ruff(monkeypatch, findings):
    monkeypatch.setattr(quality_gate.shutil, "which", lambda name: f"/usr/bin/{name}")

    def fake_run_command(command, **kwargs):
        return quality_gate.CommandResult(1, json.dumps(findings), "", 0.01)

    monkeypatch.setattr(quality_gate, "run_command", fake_run_command)


def _patch_mypy(monkeypatch, stdout):
    monkeypatch.setattr(quality_gate.shutil, "which", lambda name: f"/usr/bin/{name}")

    def fake_run_command(command, **kwargs):
        return quality_gate.CommandResult(1, stdout, "", 0.01)

    monkeypatch.setattr(quality_gate, "run_command", fake_run_command)


def test_stage_ruff_applies_approved_exemptions(tmp_path, monkeypatch):
    _patch_ruff(monkeypatch, [{"code": "F401", "filename": "src/mms/services/rules.py", "message": "unused import"}])
    exemptions = [
        {
            "tool": "ruff",
            "fingerprint": "F401|src/mms/services/rules.py|unused import",
            "reason": "tracked in ticket-123",
            "owner": "platform",
            "expires": "2999-01-01",
        }
    ]
    args = argparse.Namespace(update_baseline=False)
    stage = quality_gate.stage_ruff(args, {"fingerprints": {"ruff": []}}, exemptions, tmp_path)
    assert stage.status == quality_gate.WARN
    assert stage.metrics["new_violations"] == 0


def test_stage_ruff_flags_new_violations_without_exemption(tmp_path, monkeypatch):
    _patch_ruff(monkeypatch, [{"code": "F401", "filename": "src/mms/services/rules.py", "message": "unused import"}])
    args = argparse.Namespace(update_baseline=False)
    stage = quality_gate.stage_ruff(args, {"fingerprints": {"ruff": []}}, [], tmp_path)
    assert stage.status == quality_gate.FAIL
    assert stage.metrics["new_violations"] == 1


def test_stage_mypy_applies_approved_exemptions(tmp_path, monkeypatch):
    _patch_mypy(monkeypatch, "src/mms/services/service.py:84: error: incompatible types [arg-type]\n")
    exemptions = [
        {
            "tool": "mypy",
            "fingerprint": "arg-type|src/mms/services/service.py|incompatible types",
            "reason": "tracked in ticket-123",
            "owner": "platform",
            "expires": "2999-01-01",
        }
    ]
    args = argparse.Namespace(update_baseline=False)
    stage = quality_gate.stage_mypy(args, {"fingerprints": {"mypy": []}}, exemptions, tmp_path)
    assert stage.status == quality_gate.WARN
    assert stage.metrics["new_errors"] == 0


def test_stage_mypy_flags_new_errors_without_exemption(tmp_path, monkeypatch):
    _patch_mypy(monkeypatch, "src/mms/services/service.py:84: error: incompatible types [arg-type]\n")
    args = argparse.Namespace(update_baseline=False)
    stage = quality_gate.stage_mypy(args, {"fingerprints": {"mypy": []}}, [], tmp_path)
    assert stage.status == quality_gate.FAIL
    assert stage.metrics["new_errors"] == 1


def test_benchmark_environments_must_match_exactly_for_enforcement():
    baseline_env = {
        "machine": "x86_64",
        "system": "Linux",
        "python_series": "3.13",
        "cpu_count": 4,
        "platform": "Linux-7.0.0-38-generic-x86_64-with-glibc2.41",
        "ci": False,
    }
    assert quality_gate._environments_comparable(baseline_env, dict(baseline_env))
    for key in ("machine", "system", "python_series", "cpu_count", "platform", "ci"):
        drifted = dict(baseline_env)
        drifted[key] = "mismatch"
        assert not quality_gate._environments_comparable(baseline_env, drifted)


def test_partial_baseline_update_preserves_unrun_tool_debt(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "counts": {
                    "ruff_violations": 2,
                    "mypy_errors": 3,
                    "coverage_line": 90.0,
                    "coverage_branch": 70.0,
                    "bandit_high": 1,
                    "bandit_medium": 2,
                    "pip_audit_vulnerabilities": 1,
                    "mutation_score": 50.0,
                },
                "fingerprints": {
                    "ruff": ["F401|a.py|1"],
                    "mypy": ["mypy|b.py|2"],
                    "bandit": ["B608|c.py|3"],
                },
            }
        )
    )
    report = {
        "profile": "local",
        "config_sha256": "sha",
        "git": {},
        "tools": {},
        "thresholds": {},
        "metrics": {"ruff_violations": 0},
        "stages": [
            {
                "name": "ruff",
                "status": quality_gate.PASS,
                "summary": "",
                "metrics": {"fingerprints": []},
                "details": [],
                "artifacts": {},
                "duration_seconds": 0.1,
            }
        ],
    }
    args = argparse.Namespace(baseline=baseline_path, accept_new=False, accept_coverage_drop=False)
    assert quality_gate.update_baseline_file(args, report) == 0
    updated = json.loads(baseline_path.read_text(encoding="utf-8"))
    assert updated["counts"] == {
        "ruff_violations": 0,
        "mypy_errors": 3,
        "coverage_line": 90.0,
        "coverage_branch": 70.0,
        "bandit_high": 1,
        "bandit_medium": 2,
        "pip_audit_vulnerabilities": 1,
        "mutation_score": 50.0,
    }
    assert updated["fingerprints"] == {
        "ruff": [],
        "mypy": ["mypy|b.py|2"],
        "bandit": ["B608|c.py|3"],
    }
