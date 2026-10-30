#!/usr/bin/env python3
"""Unified code quality gate for the Memory Management Service.

The gate implements the six evaluation dimensions described in
docs/0-代码质量评测体系设计.md:

1. static analysis      Ruff lint + format, Bandit, complexity advisory
2. types and contracts  mypy + OpenAPI baseline comparison
3. automated tests      pytest unit/integration suites
4. test sufficiency     line/branch coverage + new-code coverage
5. security and deps    Bandit, pip-audit, Gitleaks (when installed)
6. performance          pytest-benchmark with a recorded baseline
7. model quality        DeepEval offline evaluation over evals/ golden datasets
                          (docs/1-模型质量评测体系设计.md): deterministic contracts
                          on every run, LLM-judge metrics when a judge is configured,
                          score baselines and per-case result persistence.

Two execution profiles are provided for local and CI use, plus a nightly
profile for heavy checks. Existing debt is recorded in quality/baseline.json
and every run is archived under quality/reports/ for trend analysis.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
DEFAULT_REPORT_DIR = ROOT / "quality" / "reports"
DEFAULT_BASELINE = ROOT / "quality" / "baseline.json"
DEFAULT_EXEMPTIONS = ROOT / "quality" / "exemptions.json"
DEFAULT_PERFORMANCE_BASELINE = ROOT / "quality" / "performance-baseline.json"
DEFAULT_EVAL_BASELINE = ROOT / "quality" / "eval-baseline.json"
OPENAPI_SCRIPT = ROOT / "scripts" / "openapi_contract.py"
METRICS_CONTRACT_SCRIPT = ROOT / "scripts" / "metrics_contract.py"
CONFIG_FILES = ("pyproject.toml", ".gitleaks.toml")
DEFAULT_THRESHOLDS: dict[str, Any] = {
    "coverage_line": 85.0,
    "coverage_branch": 75.0,
    "coverage_regression_slack": 1.0,
    "new_code_coverage": 90.0,
    "complexity_review_cc": 10,
    "long_function_lines": 50,
    "benchmark_warn_regression_pct": 20.0,
    "benchmark_fail_regression_pct": 50.0,
    "eval_score_regression_slack": 0.05,
}
PASS = "PASS"  # noqa: S105 - status label, not a credential
FAIL = "FAIL"
WARN = "WARN"
SKIP = "SKIP"
ERROR = "ERROR"
PROFILES: dict[str, list[str]] = {
    "local": ["baseline", "ruff", "format", "mypy", "pytest-unit", "openapi", "metrics-contract"],
    "ci": [
        "baseline",
        "ruff",
        "format",
        "mypy",
        "bandit",
        "pip-audit",
        "pytest",
        "new-code-coverage",
        "llm-evaluation",
        "openapi",
        "metrics-contract",
        "complexity",
        "gitleaks",
    ],
    "nightly": [
        "baseline",
        "ruff",
        "format",
        "mypy",
        "bandit",
        "pip-audit",
        "pytest",
        "new-code-coverage",
        "llm-evaluation",
        "openapi",
        "complexity",
        "gitleaks",
        "benchmark",
        "mutation",
    ],
}
ALL_STAGES = [
    "baseline",
    "ruff",
    "format",
    "mypy",
    "bandit",
    "pip-audit",
    "pytest",
    "pytest-unit",
    "new-code-coverage",
    "llm-evaluation",
    "openapi",
    "metrics-contract",
    "complexity",
    "gitleaks",
    "benchmark",
    "mutation",
]


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT.resolve()))
    except ValueError:
        return str(path)


def pkg_version(name: str) -> str | None:
    try:
        import importlib.metadata as metadata

        return metadata.version(name)
    except Exception:
        return None


def command_version(command: str, *args: str) -> str | None:
    executable = shutil.which(command)
    if not executable:
        return None
    try:
        result = subprocess.run([executable, *args], capture_output=True, text=True, timeout=20, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    output = (result.stdout or result.stderr).strip().splitlines()
    return output[0].strip() if output else None


def git_metadata() -> dict[str, Any]:
    if not (ROOT / ".git").exists():
        return {"available": False, "commit": None, "branch": None, "dirty": None}

    def git(*args: str) -> str | None:
        try:
            result = subprocess.run(
                ["git", "-C", str(ROOT), *args], capture_output=True, text=True, timeout=20, check=False
            )
        except (OSError, subprocess.SubprocessError):
            return None
        return result.stdout.strip() if result.returncode == 0 else None

    dirty = git("status", "--porcelain")
    return {
        "available": True,
        "commit": git("rev-parse", "HEAD"),
        "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "dirty": bool(dirty),
    }


def config_sha256() -> str:
    digest = hashlib.sha256()
    for name in CONFIG_FILES:
        path = ROOT / name
        digest.update(name.encode())
        if path.is_file():
            digest.update(path.read_bytes())
    return digest.hexdigest()


def environment_metadata() -> dict[str, Any]:
    return {
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "ci": bool(os.environ.get("CI")),
    }


def tool_versions() -> dict[str, str | None]:
    return {
        "ruff": command_version("ruff", "--version"),
        "mypy": command_version("mypy", "--version"),
        "pytest": command_version("pytest", "--version"),
        "pytest-cov": pkg_version("pytest-cov"),
        "bandit": command_version("bandit", "--version"),
        "pip-audit": command_version("pip-audit", "--version"),
        "gitleaks": command_version("gitleaks", "version"),
        "radon": command_version("radon", "--version"),
        "diff-cover": command_version("diff-cover", "--version"),
        "pytest-benchmark": pkg_version("pytest-benchmark"),
        "mutmut": command_version("mutmut", "--version"),
        "deepeval": pkg_version("deepeval"),
    }


@dataclass
class CommandResult:
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float


@dataclass
class StageResult:
    name: str
    status: str
    summary: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)
    details: list[str] = field(default_factory=list)
    artifacts: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "summary": self.summary,
            "duration_seconds": round(self.duration_seconds, 3),
            "metrics": self.metrics,
            "details": self.details[:40],
            "artifacts": self.artifacts,
        }


def run_command(
    command: Sequence[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int = 1800,
    cwd: Path = ROOT,
) -> CommandResult:
    started = time.monotonic()
    try:
        result = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            cwd=str(cwd),
            env=env,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        return CommandResult(124, stdout, stderr + f"\ntimeout after {timeout}s", time.monotonic() - started)
    except OSError as exc:
        return CommandResult(127, "", str(exc), time.monotonic() - started)
    return CommandResult(result.returncode, result.stdout, result.stderr, time.monotonic() - started)


def write_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def write_json(path: Path, payload: Any) -> Path:
    return write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def load_json(path: Path, default: Any = None) -> Any:
    if not path.is_file():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def stage_env(report_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(SRC) + (os.pathsep + pythonpath if pythonpath else "")
    env.setdefault("COVERAGE_FILE", str(report_dir / ".coverage"))
    env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    return env


def load_baseline(path: Path) -> dict[str, Any]:
    payload = load_json(path, default=None)
    if payload is None:
        return {"schema_version": 1, "fingerprints": {}, "counts": {}, "thresholds": {}}
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError(f"unsupported baseline format: {path}")
    payload.setdefault("fingerprints", {})
    payload.setdefault("counts", {})
    payload.setdefault("thresholds", {})
    return payload


def load_exemptions(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path, default={"schema_version": 1, "exemptions": []})
    if not isinstance(payload, dict) or not isinstance(payload.get("exemptions"), list):
        raise ValueError(f"invalid exemptions file: {path}")
    return payload["exemptions"]


def active_exemptions(exemptions: list[dict[str, Any]], tool: str, result: StageResult) -> tuple[set[str], list[str]]:
    allowed: set[str] = set()
    problems: list[str] = []
    today = date.today()
    for item in exemptions:
        if item.get("tool") != tool:
            continue
        missing = [key for key in ("fingerprint", "reason", "owner", "expires") if not item.get(key)]
        if missing:
            problems.append(f"exemption missing {', '.join(missing)}: {item}")
            continue
        try:
            expires = date.fromisoformat(str(item["expires"]))
        except ValueError:
            problems.append(f"exemption has invalid expires date: {item}")
            continue
        if expires < today:
            problems.append(f"expired exemption for {item['fingerprint']} (owner {item['owner']}, expired {expires})")
            continue
        allowed.add(str(item["fingerprint"]))
    if problems:
        result.details.extend(problems)
    return allowed, problems


def fingerprint_diff(
    current: set[str], baseline: dict[str, Any], tool: str, exempted: set[str]
) -> tuple[list[str], list[str]]:
    previous = set(str(item) for item in baseline.get("fingerprints", {}).get(tool, []))
    new = sorted(current - previous - exempted)
    resolved = sorted(previous - current)
    return new, resolved


def stage_baseline(
    args: argparse.Namespace, baseline: dict[str, Any], exemptions: list[dict[str, Any]], report_dir: Path
) -> StageResult:
    stage = StageResult("baseline", PASS)
    problems: list[str] = []
    if not args.baseline.is_file():
        if args.update_baseline:
            stage.details.append(f"baseline missing; this run will create {rel(args.baseline)}")
        else:
            problems.append(
                f"baseline missing: {rel(args.baseline)}; run "
                "`python scripts/quality_gate.py --update-baseline --accept-new` to record the initial debt"
            )
    else:
        recorded = baseline.get("config_sha256")
        current = config_sha256()
        if recorded and recorded != current and not args.update_baseline:
            problems.append(
                "quality configuration changed since the baseline was recorded; review and re-baseline intentionally"
            )
    for tool in ("ruff", "mypy", "bandit", "pip-audit"):
        allowed, exemption_problems = active_exemptions(exemptions, tool, stage)
        problems.extend(exemption_problems)
        if allowed:
            stage.metrics.setdefault("active_exemptions", {})[tool] = sorted(allowed)
    if problems:
        stage.status = FAIL
        stage.summary = "baseline or exemption governance failed"
        stage.details.extend(problems)
    else:
        stage.summary = "baseline and exemptions are valid"
    stage.metrics["config_sha256"] = config_sha256()
    stage.metrics["baseline_updated_at"] = baseline.get("updated_at")
    stage.metrics["baseline_commit"] = baseline.get("commit")
    write_json(report_dir / "baseline-check.json", stage.as_dict())
    return stage


def stage_ruff(
    args: argparse.Namespace, baseline: dict[str, Any], exemptions: list[dict[str, Any]], report_dir: Path
) -> StageResult:
    stage = StageResult("ruff", PASS)
    if not shutil.which("ruff"):
        stage.status = FAIL
        stage.summary = "ruff is not installed; install the dev extra"
        return stage
    command = ["ruff", "check", "src", "tests", "evals", "scripts", "loadtests", "--output-format=json"]
    result = run_command(command, env=stage_env(report_dir))
    write_text(report_dir / "ruff.json", result.stdout or "[]")
    if result.returncode not in (0, 1):
        stage.status = ERROR
        stage.summary = f"ruff failed with exit code {result.returncode}"
        stage.details.extend([result.stderr.strip()[:2000]])
        return stage
    try:
        findings = json.loads(result.stdout or "[]")
    except json.JSONDecodeError:
        stage.status = ERROR
        stage.summary = "ruff produced invalid JSON"
        stage.details.append((result.stdout or result.stderr)[:2000])
        return stage
    fingerprints: set[str] = set()
    by_code: dict[str, int] = {}
    by_file: dict[str, int] = {}
    for finding in findings:
        code = str(finding.get("code") or "UNKNOWN")
        filename = rel(Path(str(finding.get("filename", ""))))
        message = str(finding.get("message", ""))
        fingerprints.add(f"{code}|{filename}|{message}")
        by_code[code] = by_code.get(code, 0) + 1
        by_file[filename] = by_file.get(filename, 0) + 1
    exempted, _ = active_exemptions(exemptions, "ruff", stage)
    new, resolved = fingerprint_diff(fingerprints, baseline, "ruff", exempted) if not args.update_baseline else ([], [])
    stage.metrics.update(
        {
            "violations": len(findings),
            "fixable": sum(1 for finding in findings if finding.get("fix")),
            "by_code": dict(sorted(by_code.items(), key=lambda item: (-item[1], item[0]))),
            "by_file": dict(sorted(by_file.items(), key=lambda item: (-item[1], item[0]))),
            "fingerprints": sorted(fingerprints),
            "new_violations": len(new),
            "resolved_violations": len(resolved),
        }
    )
    if new:
        stage.status = FAIL
        stage.summary = f"{len(findings)} existing, {len(new)} new violation(s) exceed the baseline"
        stage.details.extend(new[:30])
    elif findings:
        stage.status = WARN
        stage.summary = f"{len(findings)} known violation(s) match the baseline; reduce debt with --update-baseline"
    else:
        stage.summary = "no lint violations"
    stage.artifacts.append(rel(report_dir / "ruff.json"))
    return stage


def stage_format(report_dir: Path) -> StageResult:
    stage = StageResult("format", PASS)
    if not shutil.which("ruff"):
        stage.status = FAIL
        stage.summary = "ruff is not installed; install the dev extra"
        return stage
    result = run_command(
        ["ruff", "format", "--check", "src", "tests", "evals", "scripts", "loadtests"],
        env=stage_env(report_dir),
    )
    write_text(report_dir / "ruff-format.txt", result.stdout + result.stderr)
    files = sorted(
        {
            line.split("Would reformat:", 1)[1].strip()
            for line in result.stdout.splitlines()
            if line.startswith("Would reformat:")
        }
        | {
            line.split("-->", 1)[1].split(":", 1)[0].strip()
            for line in result.stdout.splitlines()
            if line.strip().startswith("-->")
        }
    )
    stage.metrics["files_to_format"] = len(files)
    stage.metrics["files"] = files[:30]
    if result.returncode != 0:
        stage.status = FAIL
        stage.summary = f"{len(files)} file(s) are not formatted"
        stage.details.extend(files[:30])
    else:
        stage.summary = "formatting is clean"
    stage.artifacts.append(rel(report_dir / "ruff-format.txt"))
    return stage


MYPY_RE = re.compile(
    r"^(?P<file>.+?):(?P<line>\d+):(?:(?P<column>\d+):)? error: (?P<message>.*?)(?:\s+\[(?P<code>[a-z0-9-]+)\])?$"
)


def stage_mypy(
    args: argparse.Namespace, baseline: dict[str, Any], exemptions: list[dict[str, Any]], report_dir: Path
) -> StageResult:
    stage = StageResult("mypy", PASS)
    if not shutil.which("mypy"):
        stage.status = FAIL
        stage.summary = "mypy is not installed; install the dev extra"
        return stage
    result = run_command(
        ["mypy", "--show-error-codes", "--no-error-summary", "--no-pretty", "src/mms"], env=stage_env(report_dir)
    )
    write_text(report_dir / "mypy.txt", result.stdout + result.stderr)
    if result.returncode not in (0, 1):
        stage.status = ERROR
        stage.summary = f"mypy failed with exit code {result.returncode}"
        stage.details.append((result.stderr or result.stdout).strip()[:2000])
        return stage
    fingerprints: set[str] = set()
    by_code: dict[str, int] = {}
    by_file: dict[str, int] = {}
    errors: list[str] = []
    for line in result.stdout.splitlines():
        match = MYPY_RE.match(line)
        if not match:
            continue
        filename = rel(Path(match.group("file")))
        message = match.group("message").strip()
        code = match.group("code") or "misc"
        fingerprints.add(f"{code}|{filename}|{message}")
        by_code[code] = by_code.get(code, 0) + 1
        by_file[filename] = by_file.get(filename, 0) + 1
        errors.append(f"{filename}:{match.group('line')}: {message} [{code}]")
    exempted, _ = active_exemptions(exemptions, "mypy", stage)
    new, resolved = fingerprint_diff(fingerprints, baseline, "mypy", exempted) if not args.update_baseline else ([], [])
    stage.metrics.update(
        {
            "errors": len(errors),
            "by_code": dict(sorted(by_code.items(), key=lambda item: (-item[1], item[0]))),
            "by_file": dict(sorted(by_file.items(), key=lambda item: (-item[1], item[0]))),
            "fingerprints": sorted(fingerprints),
            "new_errors": len(new),
            "resolved_errors": len(resolved),
            "examples": errors[:20],
        }
    )
    if new:
        stage.status = FAIL
        stage.summary = f"{len(errors)} existing, {len(new)} new type error(s) exceed the baseline"
        stage.details.extend(new[:30])
    elif errors:
        stage.status = WARN
        stage.summary = f"{len(errors)} known type error(s) match the baseline; reduce debt with --update-baseline"
    else:
        stage.summary = "no type errors"
    stage.artifacts.append(rel(report_dir / "mypy.txt"))
    return stage


def stage_bandit(
    args: argparse.Namespace, baseline: dict[str, Any], exemptions: list[dict[str, Any]], report_dir: Path
) -> StageResult:
    stage = StageResult("bandit", PASS)
    if not shutil.which("bandit"):
        stage.status = FAIL
        stage.summary = "bandit is not installed; install the dev extra"
        return stage
    result = run_command(
        ["bandit", "-c", "pyproject.toml", "-r", "src/mms", "-f", "json", "-q"], env=stage_env(report_dir)
    )
    write_text(report_dir / "bandit.json", result.stdout or "{}")
    if result.returncode not in (0, 1):
        stage.status = ERROR
        stage.summary = f"bandit failed with exit code {result.returncode}"
        stage.details.append((result.stderr or result.stdout).strip()[:2000])
        return stage
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        stage.status = ERROR
        stage.summary = "bandit produced invalid JSON"
        stage.details.append((result.stdout or result.stderr)[:2000])
        return stage
    severity_by_fp: dict[str, str] = {}
    findings: list[str] = []
    counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for item in payload.get("results", []) or []:
        severity = str(item.get("issue_severity", "LOW")).upper()
        counts[severity] = counts.get(severity, 0) + 1
        filename = rel(Path(str(item.get("filename", ""))))
        fingerprint = f"{item.get('test_id')}|{filename}|{item.get('issue_text')}"
        severity_by_fp[fingerprint] = severity
        findings.append(
            f"{severity} {item.get('test_id')} {filename}:{item.get('line_number')} {item.get('issue_text')}"
        )
    exempted, _ = active_exemptions(exemptions, "bandit", stage)
    new, resolved = (
        fingerprint_diff(set(severity_by_fp), baseline, "bandit", exempted) if not args.update_baseline else ([], [])
    )
    blocking = [fp for fp in new if severity_by_fp.get(fp) in {"HIGH", "MEDIUM"}]
    warnings = [fp for fp in new if severity_by_fp.get(fp) == "LOW"]
    stage.metrics.update(
        {
            "high": counts["HIGH"],
            "medium": counts["MEDIUM"],
            "low": counts["LOW"],
            "new": len(new),
            "new_high_or_medium": len(blocking),
            "new_low": len(warnings),
            "resolved": len(resolved),
            "fingerprints": sorted(severity_by_fp),
        }
    )
    if blocking:
        stage.status = FAIL
        stage.summary = f"{len(blocking)} new high/medium severity issue(s)"
        stage.details.extend(blocking[:30])
    elif new or counts["HIGH"] or counts["MEDIUM"]:
        stage.status = WARN
        stage.summary = (
            f"known issues: high={counts['HIGH']} medium={counts['MEDIUM']} low={counts['LOW']}; "
            f"new low={len(warnings)}; fix high/medium and ratchet the baseline"
        )
        stage.details.extend(findings[:20])
    else:
        stage.summary = "no security findings"
    stage.artifacts.append(rel(report_dir / "bandit.json"))
    return stage


def stage_pip_audit(
    args: argparse.Namespace, baseline: dict[str, Any], exemptions: list[dict[str, Any]], report_dir: Path
) -> StageResult:
    stage = StageResult("pip-audit", PASS)
    if not shutil.which("pip-audit"):
        stage.status = FAIL
        stage.summary = "pip-audit is not installed; install the dev extra"
        return stage
    result = run_command(
        [
            "pip-audit",
            "-f",
            "json",
            "--progress-spinner",
            "off",
            "--skip-editable",
            "--vulnerability-service",
            "osv",
        ],
        env=stage_env(report_dir),
    )
    write_text(report_dir / "pip-audit.json", result.stdout or "{}")
    if result.returncode not in (0, 1):
        stage.status = ERROR
        stage.summary = f"pip-audit failed with exit code {result.returncode}"
        stage.details.append((result.stderr or result.stdout).strip()[:2000])
        return stage
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        stage.status = ERROR
        stage.summary = "pip-audit produced invalid JSON"
        stage.details.append((result.stdout or result.stderr)[:2000])
        return stage
    vulnerabilities: dict[str, str] = {}
    for dependency in payload.get("dependencies", []) or []:
        name = str(dependency.get("name", ""))
        version = str(dependency.get("version", ""))
        for vuln in dependency.get("vulns", []) or []:
            vuln_id = str(vuln.get("id", "UNKNOWN"))
            fingerprint = f"{vuln_id}|{name}"
            vulnerabilities[fingerprint] = (
                f"{name}=={version} {vuln_id} fix={','.join(vuln.get('fix_versions', []) or []) or 'none'}"
            )
    exempted, _ = active_exemptions(exemptions, "pip-audit", stage)
    new, resolved = (
        fingerprint_diff(set(vulnerabilities), baseline, "pip-audit", exempted)
        if not args.update_baseline
        else ([], [])
    )
    stage.metrics.update(
        {
            "vulnerabilities": len(vulnerabilities),
            "new": len(new),
            "resolved": len(resolved),
            "fingerprints": sorted(vulnerabilities),
            "scope": sys.prefix,
        }
    )
    if new:
        stage.status = FAIL
        stage.summary = f"{len(new)} new dependency vulnerability/vulnerabilities"
        stage.details.extend(vulnerabilities[fp] for fp in new[:30])
    elif vulnerabilities:
        stage.status = WARN
        stage.summary = f"{len(vulnerabilities)} known vulnerability/vulnerabilities; no new findings"
        stage.details.extend(list(vulnerabilities.values())[:20])
    else:
        stage.summary = "no known dependency vulnerabilities"
    stage.artifacts.append(rel(report_dir / "pip-audit.json"))
    return stage


def _parse_junit(path: Path) -> dict[str, int]:
    if not path.is_file():
        return {}
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return {}
    suites = root.findall("testsuite") if root.tag == "testsuites" else [root]
    if not suites:
        return {}
    return {
        "tests": sum(int(suite.attrib.get("tests", 0)) for suite in suites),
        "failures": sum(int(suite.attrib.get("failures", 0)) for suite in suites),
        "errors": sum(int(suite.attrib.get("errors", 0)) for suite in suites),
        "skipped": sum(int(suite.attrib.get("skipped", 0)) for suite in suites),
        "time": round(sum(float(suite.attrib.get("time", 0.0)) for suite in suites), 3),
    }


def _coverage_metrics(path: Path) -> tuple[dict[str, Any], list[str]]:
    problems: list[str] = []
    if not path.is_file():
        return {}, ["coverage report was not generated"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    totals = payload.get("totals", {})
    statements = int(totals.get("num_statements", 0))
    covered = int(totals.get("covered_lines", 0))
    branches = int(totals.get("num_branches", 0))
    covered_branches = int(totals.get("covered_branches", 0))
    files = payload.get("files", {}) or {}
    measured = [name for name in files if name.startswith(("mms/", "src/mms/"))]
    if statements <= 0 or not measured:
        problems.append("coverage did not include any src/mms modules")
    excluded = int(totals.get("excluded_lines", 0))
    metrics = {
        "line": round(100.0 * covered / statements, 2) if statements else 0.0,
        "branch": round(100.0 * covered_branches / branches, 2) if branches else 0.0,
        "statements": statements,
        "covered_lines": covered,
        "branches": branches,
        "covered_branches": covered_branches,
        "excluded_lines": excluded,
        "files_measured": len(measured),
        "lowest_files": sorted(
            (
                {"file": name, "percent": round(float(data.get("summary", {}).get("percent_covered", 0.0)), 2)}
                for name, data in files.items()
            ),
            key=lambda item: item["percent"],
        )[:10],
    }
    return metrics, problems


def stage_pytest(
    args: argparse.Namespace,
    baseline: dict[str, Any],
    report_dir: Path,
    *,
    unit_only: bool,
) -> StageResult:
    stage = StageResult("pytest-unit" if unit_only else "pytest", PASS)
    if not shutil.which("pytest"):
        stage.status = FAIL
        stage.summary = "pytest is not installed; install the dev extra"
        return stage
    if not unit_only and not os.environ.get("MMS_TEST_DATABASE_URL"):
        stage.status = FAIL
        stage.summary = "MMS_TEST_DATABASE_URL is required for integration coverage"
        stage.details.append(
            "start PostgreSQL, run scripts/init_db.py against a test database, and export MMS_TEST_DATABASE_URL"
        )
        return stage
    thresholds = {**DEFAULT_THRESHOLDS, **baseline.get("thresholds", {}), **args.threshold_overrides()}
    junit = report_dir / ("junit-unit.xml" if unit_only else "junit.xml")
    command = [
        "pytest",
        "tests",
        "-m",
        "unit" if unit_only else "not slow",
        "-q",
        "-p",
        "no:cacheprovider",
        f"--junitxml={junit}",
    ]
    coverage_json = report_dir / "coverage.json"
    coverage_xml = report_dir / "coverage.xml"
    if not unit_only:
        command.extend(
            [
                "--cov=mms",
                "--cov-branch",
                "--cov-report=term-missing",
                f"--cov-report=json:{coverage_json}",
                f"--cov-report=xml:{coverage_xml}",
            ]
        )
    result = run_command(command, env=stage_env(report_dir), timeout=3600)
    write_text(report_dir / ("pytest-unit.txt" if unit_only else "pytest.txt"), result.stdout + result.stderr)
    counts = _parse_junit(junit)
    stage.metrics["tests"] = counts
    stage.metrics["failed"] = counts.get("failures", 0) + counts.get("errors", 0)
    stage.artifacts.extend([rel(junit), rel(report_dir / ("pytest-unit.txt" if unit_only else "pytest.txt"))])
    if unit_only:
        if result.returncode != 0 or counts.get("failed", 0):
            stage.status = FAIL
            stage.summary = f"unit tests failed (exit {result.returncode})"
            stage.details.append((result.stdout or result.stderr)[-3000:])
        elif counts.get("tests", 0) == 0:
            stage.status = FAIL
            stage.summary = "no unit tests were collected"
        else:
            stage.summary = f"{counts.get('tests', 0)} unit test(s) passed"
        return stage
    coverage, problems = _coverage_metrics(coverage_json)
    stage.metrics["coverage"] = coverage
    stage.artifacts.extend([rel(coverage_json), rel(coverage_xml)])
    failures: list[str] = []
    if result.returncode != 0 or counts.get("failed", 0):
        failures.append(f"pytest exit code {result.returncode}; failed={counts.get('failed', 0)}")
    failures.extend(problems)
    if coverage:
        if coverage["line"] < float(thresholds["coverage_line"]):
            failures.append(f"line coverage {coverage['line']}% < {thresholds['coverage_line']}%")
        if coverage["branches"] and coverage["branch"] < float(thresholds["coverage_branch"]):
            failures.append(f"branch coverage {coverage['branch']}% < {thresholds['coverage_branch']}%")
        previous_line = baseline.get("counts", {}).get("coverage_line") if not args.update_baseline else None
        if isinstance(previous_line, int | float) and coverage["line"] < float(previous_line) - float(
            thresholds["coverage_regression_slack"]
        ):
            failures.append(f"line coverage regressed from {previous_line}% to {coverage['line']}%")
        if not coverage["branches"]:
            failures.append("branch coverage was not measured; keep branch = true in coverage config")
    if failures:
        stage.status = FAIL
        stage.summary = "test or coverage gate failed"
        stage.details.extend(failures)
    else:
        stage.summary = (
            f"{counts.get('tests', 0)} test(s) passed; line {coverage.get('line')}%, branch {coverage.get('branch')}%"
        )
    return stage


def stage_new_code_coverage(args: argparse.Namespace, report_dir: Path) -> StageResult:
    stage = StageResult("new-code-coverage", PASS)
    if not shutil.which("diff-cover"):
        stage.status = FAIL
        stage.summary = "diff-cover is not installed; install the dev extra"
        return stage
    if not (ROOT / ".git").exists():
        if args.require_new_code_coverage:
            stage.status = FAIL
            stage.summary = "new-code coverage requires a git worktree"
        else:
            stage.status = SKIP
            stage.summary = "skipped: not a git worktree (CI enforces this gate)"
        return stage
    check = run_command(["git", "-C", str(ROOT), "rev-parse", "--verify", args.diff_base], env=stage_env(report_dir))
    if check.returncode != 0:
        if args.require_new_code_coverage:
            stage.status = FAIL
            stage.summary = f"diff base not available: {args.diff_base}"
            stage.details.append(check.stderr.strip())
        else:
            stage.status = SKIP
            stage.summary = f"skipped: diff base {args.diff_base} is not available"
        return stage
    coverage_xml = report_dir / "coverage.xml"
    if not coverage_xml.is_file():
        stage.status = FAIL
        stage.summary = "coverage.xml is missing; run the pytest stage first"
        return stage
    thresholds = {**DEFAULT_THRESHOLDS, **args.threshold_overrides()}
    result = run_command(
        [
            "diff-cover",
            str(coverage_xml),
            f"--compare-branch={args.diff_base}",
            f"--fail-under={thresholds['new_code_coverage']}",
            "--total-percent-float",
            "--branch-coverage",
        ],
        env=stage_env(report_dir),
    )
    write_text(report_dir / "diff-cover.txt", result.stdout + result.stderr)
    match = re.search(r"Total:\s+([0-9.]+)%", result.stdout)
    percent = float(match.group(1)) if match else (100.0 if result.returncode == 0 else None)
    stage.metrics["report"] = {"total_percent_covered": percent}
    stage.metrics["threshold"] = thresholds["new_code_coverage"]
    stage.artifacts.append(rel(report_dir / "diff-cover.txt"))
    if result.returncode != 0:
        stage.status = FAIL
        stage.summary = f"new/changed line coverage is below {thresholds['new_code_coverage']}%"
        stage.details.append((result.stdout or result.stderr)[-3000:])
    else:
        stage.summary = f"changed-line coverage {percent}% (threshold {thresholds['new_code_coverage']}%)"
    return stage


def stage_openapi(args: argparse.Namespace, report_dir: Path) -> StageResult:
    stage = StageResult("openapi", PASS)
    baseline = args.openapi_baseline
    if not baseline.is_file():
        stage.status = FAIL
        stage.summary = f"OpenAPI baseline missing: {rel(baseline)}"
        stage.details.append("run `python scripts/openapi_contract.py --write` after an intentional contract release")
        return stage
    json_report = report_dir / "openapi-contract.json"
    result = run_command(
        [
            sys.executable,
            str(OPENAPI_SCRIPT),
            "--check",
            "--baseline",
            str(baseline),
            "--json-report",
            str(json_report),
        ],
        env=stage_env(report_dir),
    )
    write_text(report_dir / "openapi-contract.txt", result.stdout + result.stderr)
    payload = load_json(json_report, default={})
    stage.metrics.update(payload.get("counts", {}))
    stage.metrics["changes"] = payload.get("changes", [])[:50]
    stage.artifacts.extend([rel(json_report), rel(report_dir / "openapi-contract.txt")])
    if result.returncode != 0:
        stage.status = FAIL
        stage.summary = "breaking OpenAPI contract change detected"
        stage.details.extend(
            f"{item.get('kind')} {item.get('location')}: {item.get('message')}"
            for item in payload.get("changes", [])[:30]
        )
    else:
        stage.summary = (
            f"no breaking contract change ({stage.metrics.get('warning', 0)} warning, "
            f"{stage.metrics.get('info', 0)} info)"
        )
    return stage


def stage_metrics_contract(report_dir: Path) -> StageResult:
    stage = StageResult("metrics-contract", PASS)
    json_report = report_dir / "metrics-contract.json"
    result = run_command(
        [sys.executable, str(METRICS_CONTRACT_SCRIPT), "--json-report", str(json_report)],
        env=stage_env(report_dir),
    )
    write_text(report_dir / "metrics-contract.txt", result.stdout + result.stderr)
    stage.artifacts.extend([rel(json_report), rel(report_dir / "metrics-contract.txt")])
    payload = load_json(json_report, default={})
    violations = payload.get("violations", [])
    stage.metrics["violations"] = len(violations)
    if result.returncode != 0:
        stage.status = FAIL
        stage.summary = "alert/SLO rules reference unregistered metrics (docs 15.3)"
        stage.details.extend(f"{item.get('rule')}: {item.get('metric')}" for item in violations[:30])
    else:
        stage.summary = "every metric referenced by alert/SLO rules is registered"
    return stage


def stage_gitleaks(args: argparse.Namespace, report_dir: Path) -> StageResult:
    stage = StageResult("gitleaks", PASS)
    if not shutil.which("gitleaks"):
        if os.environ.get("CI") or args.require_gitleaks:
            stage.status = FAIL
            stage.summary = "gitleaks is not installed in CI"
        else:
            stage.status = SKIP
            stage.summary = "skipped: gitleaks is not installed (CI runs the secret scan)"
        return stage
    report = report_dir / "gitleaks.json"
    result = run_command(
        [
            "gitleaks",
            "detect",
            "--source",
            str(ROOT),
            "--no-git",
            "--redact",
            "--report-format",
            "json",
            "--report-path",
            str(report),
            "--config",
            str(ROOT / ".gitleaks.toml"),
        ],
        env=stage_env(report_dir),
    )
    findings = load_json(report, default=[]) or []
    stage.metrics["findings"] = len(findings)
    stage.metrics["rules"] = sorted({str(item.get("RuleID")) for item in findings})
    stage.artifacts.append(rel(report))
    if findings:
        stage.status = FAIL
        stage.summary = f"{len(findings)} potential secret(s) detected"
        stage.details.extend(
            f"{item.get('RuleID')} {item.get('File')}:{item.get('StartLine')}" for item in findings[:30]
        )
    elif result.returncode not in (0, 1):
        stage.status = ERROR
        stage.summary = f"gitleaks failed with exit code {result.returncode}"
        stage.details.append(result.stderr.strip()[:2000])
    else:
        stage.summary = "no leaked secrets detected"
    return stage


def _long_functions(threshold: int) -> list[str]:
    import ast

    violations: list[str] = []
    for path in sorted((SRC / "mms").rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.end_lineno:
                length = node.end_lineno - node.lineno + 1
                if length > threshold:
                    violations.append(f"{rel(path)}:{node.lineno} {node.name} ({length} lines)")
    return violations


def stage_complexity(args: argparse.Namespace, baseline: dict[str, Any], report_dir: Path) -> StageResult:
    stage = StageResult("complexity", PASS)
    thresholds = {**DEFAULT_THRESHOLDS, **baseline.get("thresholds", {}), **args.threshold_overrides()}
    details: list[str] = []
    metrics: dict[str, Any] = {}
    if shutil.which("ruff"):
        result = run_command(
            ["ruff", "check", "--select", "C90", "src/mms", "--output-format=json"], env=stage_env(report_dir)
        )
        write_text(report_dir / "complexity-ruff.json", result.stdout or "[]")
        try:
            findings = json.loads(result.stdout or "[]")
        except json.JSONDecodeError:
            findings = []
        metrics["ruff_complexity_findings"] = len(findings)
        details.extend(f"{rel(Path(item.get('filename', '')))}: {item.get('message')}" for item in findings[:20])
        stage.artifacts.append(rel(report_dir / "complexity-ruff.json"))
    long_functions = _long_functions(int(thresholds["long_function_lines"]))
    metrics["long_functions"] = len(long_functions)
    details.extend(long_functions[:20])
    if shutil.which("radon"):
        cc = run_command(["radon", "cc", "-s", "-j", "src/mms"], env=stage_env(report_dir))
        mi = run_command(["radon", "mi", "-j", "src/mms"], env=stage_env(report_dir))
        write_text(report_dir / "radon-cc.json", cc.stdout or "{}")
        write_text(report_dir / "radon-mi.json", mi.stdout or "{}")
        try:
            cc_payload = json.loads(cc.stdout or "{}")
            blocks = [
                block
                for file_blocks in cc_payload.values()
                for block in file_blocks
                if isinstance(block, dict) and block.get("complexity", 0) > thresholds["complexity_review_cc"]
            ]
            metrics["radon_functions_over_review"] = len(blocks)
            metrics["radon_max_complexity"] = max((block.get("complexity", 0) for block in blocks), default=0)
            details.extend(f"{block.get('name')} complexity {block.get('complexity')}" for block in blocks[:20])
        except (json.JSONDecodeError, AttributeError):
            pass
        try:
            mi_payload = json.loads(mi.stdout or "{}")
            values = [float(item.get("mi", 0.0)) for item in mi_payload.values() if isinstance(item, dict)]
            metrics["mi_average"] = round(sum(values) / len(values), 2) if values else None
            metrics["mi_low_files"] = sum(1 for value in values if value < 20)
        except (json.JSONDecodeError, AttributeError, TypeError, ValueError):
            pass
        stage.artifacts.extend([rel(report_dir / "radon-cc.json"), rel(report_dir / "radon-mi.json")])
    stage.metrics = metrics
    if details:
        stage.status = WARN
        stage.summary = (
            f"advisory complexity review: {metrics.get('ruff_complexity_findings', 0)} C90 finding(s), "
            f"{metrics.get('long_functions', 0)} long function(s)"
        )
        stage.details.extend(details[:30])
    else:
        stage.summary = "no complexity advisory findings"
    return stage


def _benchmark_environment() -> dict[str, Any]:
    return {
        **environment_metadata(),
        "system": platform.system(),
        "processor": platform.processor(),
        "python": sys.version,
        "python_series": f"{sys.version_info.major}.{sys.version_info.minor}",
    }


_BENCHMARK_COMPARABILITY_KEYS = ("machine", "system", "python_series", "cpu_count", "platform", "ci")


def _environments_comparable(old: dict[str, Any], current: dict[str, Any]) -> bool:
    """Regressions are enforced only when the recording environment matches exactly.

    Cross-runner comparisons (different cpu_count, platform string, or CI flag)
    stay advisory so heterogeneous hardware cannot produce false failures.
    """
    return all(old.get(key) == current.get(key) for key in _BENCHMARK_COMPARABILITY_KEYS)


def stage_benchmark(args: argparse.Namespace, baseline: dict[str, Any], report_dir: Path) -> StageResult:
    stage = StageResult("benchmark", PASS)
    if not shutil.which("pytest"):
        stage.status = FAIL
        stage.summary = "pytest-benchmark requires pytest"
        return stage
    thresholds = {**DEFAULT_THRESHOLDS, **baseline.get("thresholds", {}), **args.threshold_overrides()}
    json_report = report_dir / "benchmark.json"
    command = [
        "pytest",
        "tests/benchmarks",
        "-m",
        "benchmark",
        "-q",
        "-p",
        "no:cacheprovider",
        f"--benchmark-json={json_report}",
        f"--benchmark-storage={report_dir / 'benchmark-storage'}",
    ]
    result = run_command(command, env=stage_env(report_dir), timeout=1800)
    write_text(report_dir / "benchmark.txt", result.stdout + result.stderr)
    stage.artifacts.extend([rel(json_report), rel(report_dir / "benchmark.txt")])
    if result.returncode != 0 or not json_report.is_file():
        stage.status = FAIL
        stage.summary = "benchmark run failed"
        stage.details.append((result.stdout or result.stderr)[-3000:])
        return stage
    payload = json.loads(json_report.read_text(encoding="utf-8"))
    current = {
        item["fullname"]: {"median": float(item.get("stats", {}).get("median", 0.0)), "unit": "seconds"}
        for item in payload.get("benchmarks", [])
    }
    stage.metrics["benchmarks"] = current
    stage.metrics["environment"] = _benchmark_environment()
    performance_baseline = load_json(args.performance_baseline, default=None)
    if args.update_performance_baseline:
        write_json(
            args.performance_baseline,
            {
                "schema_version": 1,
                "generated_at": utc_now(),
                "environment": _benchmark_environment(),
                "benchmarks": current,
            },
        )
        stage.summary = f"performance baseline written to {rel(args.performance_baseline)}"
        stage.artifacts.append(rel(args.performance_baseline))
        return stage
    if not performance_baseline:
        stage.status = WARN
        stage.summary = "no performance baseline; run with --update-performance-baseline on a stable runner"
        return stage
    old_benchmarks = performance_baseline.get("benchmarks", {})
    old_environment = performance_baseline.get("environment", {})
    current_environment = _benchmark_environment()
    comparable = _environments_comparable(old_environment, current_environment)
    regressions: list[str] = []
    warnings: list[str] = []
    compared = 0
    for name, data in current.items():
        old = old_benchmarks.get(name, {}).get("median")
        if not old:
            continue
        compared += 1
        change = (data["median"] - float(old)) / float(old) * 100.0
        data["regression_pct"] = round(change, 2)
        if change > float(thresholds["benchmark_fail_regression_pct"]):
            regressions.append(f"{name}: +{change:.1f}% (median {old:.6f}s -> {data['median']:.6f}s)")
        elif change > float(thresholds["benchmark_warn_regression_pct"]):
            warnings.append(f"{name}: +{change:.1f}%")
    stage.metrics["compared"] = compared
    stage.metrics["environment_match"] = comparable
    stage.metrics["enforced"] = bool(args.enforce_performance)
    stage.metrics["regressions"] = len(regressions)
    stage.metrics["warnings"] = len(warnings)
    if regressions and comparable and args.enforce_performance:
        stage.status = FAIL
        stage.summary = (
            f"{len(regressions)} benchmark regression(s) exceed {thresholds['benchmark_fail_regression_pct']}%"
        )
        stage.details.extend(regressions[:20])
    elif regressions:
        stage.status = WARN
        stage.summary = (
            f"{len(regressions)} apparent regression(s); performance is advisory until "
            "the baseline is recorded on the same runner and --enforce-performance is set"
        )
        stage.details.extend(regressions[:20])
    elif warnings:
        stage.status = WARN
        stage.summary = (
            f"{len(warnings)} benchmark(s) exceed the {thresholds['benchmark_warn_regression_pct']}% warning threshold"
        )
        stage.details.extend(warnings[:20])
    else:
        stage.summary = f"{compared} benchmark(s) within tolerance"
    return stage


def stage_mutation(args: argparse.Namespace, report_dir: Path) -> StageResult:
    stage = StageResult("mutation", PASS)
    if not shutil.which("mutmut"):
        stage.status = WARN
        stage.summary = "mutmut is not installed; nightly mutation testing skipped"
        return stage
    script = ROOT / "scripts" / "mutation_check.py"
    if not script.is_file():
        stage.status = WARN
        stage.summary = "mutation_check.py is missing"
        return stage
    json_report = report_dir / "mutation.json"
    result = run_command(
        [sys.executable, str(script), "--json-report", str(json_report)],
        env=stage_env(report_dir),
        timeout=7200,
    )
    write_text(report_dir / "mutation.txt", result.stdout + result.stderr)
    stage.artifacts.extend([rel(json_report), rel(report_dir / "mutation.txt")])
    payload = load_json(json_report, default={}) or {}
    stage.metrics.update(payload.get("metrics", {}))
    if result.returncode != 0:
        stage.status = WARN
        stage.summary = f"mutation score {payload.get('metrics', {}).get('score')} (advisory in this phase)"
        stage.details.append((result.stdout or result.stderr)[-3000:])
    else:
        stage.summary = f"mutation score {payload.get('metrics', {}).get('score')}"
    return stage


def stage_llm_evaluation(args: argparse.Namespace, baseline: dict[str, Any], report_dir: Path) -> StageResult:
    """Run the DeepEval offline evaluation suite over evals/ golden datasets.

    Gate rules (docs/1-模型质量评测体系设计.md §7.3):
    - any failing golden blocks the run (critical/high risk cases included)
    - category mean-score regression beyond the slack blocks the run
    - judge-model errors surface as test failures, never silent passes
    - a missing judge downgrades the stage to WARN: deterministic contracts
      passed, but semantic quality was not LLM-judged in this environment
    """
    stage = StageResult("llm-evaluation", PASS)
    if not shutil.which("pytest"):
        stage.status = FAIL
        stage.summary = "pytest is not installed; install the dev extra"
        return stage
    if not os.environ.get("MMS_TEST_DATABASE_URL"):
        stage.status = FAIL
        stage.summary = "MMS_TEST_DATABASE_URL is required: evaluation drives the real service"
        return stage
    if not (ROOT / "evals").is_dir():
        stage.status = FAIL
        stage.summary = "evals/ module is missing"
        return stage
    thresholds = {**DEFAULT_THRESHOLDS, **baseline.get("thresholds", {}), **args.threshold_overrides()}
    junit = report_dir / "junit-evaluation.xml"
    env = stage_env(report_dir)
    env["MMS_EVAL_REPORT_DIR"] = str(report_dir)
    env.setdefault("MMS_EVAL_HISTORY_FILE", str(DEFAULT_REPORT_DIR / "eval-history.ndjson"))
    # public-benchmark replays are heavy (thousands of writes per question);
    # the PR gate runs the deterministic core + lifecycle goldens, the nightly
    # workflow opts in via MMS_EVAL_INCLUDE_PUBLIC_BENCHMARK=1 (docs/4 §六)
    marker = (
        "evaluation"
        if os.environ.get("MMS_EVAL_INCLUDE_PUBLIC_BENCHMARK", "").strip().lower() in {"1", "true", "yes", "on"}
        else "evaluation and not public_benchmark"
    )
    command = [
        "pytest",
        "evals",
        "-m",
        marker,
        "-q",
        "-p",
        "no:cacheprovider",
        f"--junitxml={junit}",
    ]
    result = run_command(command, env=env, timeout=1800)
    write_text(report_dir / "llm-evaluation.txt", result.stdout + result.stderr)
    counts = _parse_junit(junit)
    eval_report = load_json(report_dir / "eval-results.json", default=None) or {}
    summary = eval_report.get("summary", {})
    stage.metrics.update(
        {
            "tests": counts,
            "failed": counts.get("failures", 0) + counts.get("errors", 0),
            "run_id": summary.get("run_id"),
            "cases": summary.get("cases"),
            "metric_runs": summary.get("metric_runs"),
            "pass_rate": summary.get("pass_rate"),
            "mean_score": summary.get("mean_score"),
            "by_category": summary.get("by_category"),
            "judge_model": summary.get("judge_model"),
            "judge_configured": summary.get("judge_configured"),
        }
    )
    stage.artifacts.extend([rel(junit), rel(report_dir / "llm-evaluation.txt"), rel(report_dir / "eval-results.json")])
    failures: list[str] = []
    if result.returncode != 0 or counts.get("failed", 0):
        failures.append(f"evaluation pytest exit {result.returncode}; failed={counts.get('failed', 0)}")
    if not summary:
        failures.append("eval recorder produced no summary (recorder fixture crashed?)")
    failures.extend(_eval_baseline_regressions(args, baseline, thresholds, report_dir, summary, stage))
    if failures:
        stage.status = FAIL
        stage.summary = "model-quality evaluation failed"
        stage.details.extend(failures)
        stage.details.append((result.stdout or result.stderr)[-2000:])
    else:
        judge = summary.get("judge_model") or "not-configured"
        stage.summary = (
            f"{summary.get('cases', 0)} golden case(s), {summary.get('metric_runs', 0)} metric run(s): "
            f"{summary.get('pass_rate', 0):.0%} pass; judge={judge}"
        )
        if not summary.get("judge_configured"):
            stage.status = WARN
            stage.details.append(
                "LLM-judge metrics skipped: set MMS_EVAL_JUDGE_MODEL (and provider credentials) to "
                "enable semantic scoring; deterministic contracts all passed"
            )
    return stage


def _eval_baseline_regressions(
    args: argparse.Namespace,
    baseline: dict[str, Any],
    thresholds: dict[str, Any],
    report_dir: Path,
    summary: dict[str, Any],
    stage: StageResult,
) -> list[str]:
    """Compare category mean scores with quality/eval-baseline.json."""
    if not summary:
        return []
    if args.update_eval_baseline:
        payload = {
            "schema_version": 1,
            "generated_at": utc_now(),
            "run_id": summary.get("run_id"),
            "thresholds": {"score_regression_slack": thresholds["eval_score_regression_slack"]},
            "categories": {
                category: {
                    "mean_score": stats.get("mean_score"),
                    "pass_rate": stats.get("passed", 0) / stats["entries"] if stats.get("entries") else None,
                    "entries": stats.get("entries"),
                }
                for category, stats in (summary.get("by_category") or {}).items()
            },
        }
        write_json(args.eval_baseline, payload)
        stage.details.append(f"evaluation baseline written to {rel(args.eval_baseline)}")
        stage.artifacts.append(rel(args.eval_baseline))
        return []
    recorded = load_json(args.eval_baseline, default=None)
    if not recorded:
        stage.details.append("no evaluation baseline; run with --update-eval-baseline to record category mean scores")
        return []
    slack = float(
        recorded.get("thresholds", {}).get("score_regression_slack", thresholds["eval_score_regression_slack"])
    )
    regressions: list[str] = []
    for category, stats in (recorded.get("categories") or {}).items():
        current = (summary.get("by_category") or {}).get(category, {}).get("mean_score")
        previous = stats.get("mean_score")
        if isinstance(current, (int, float)) and isinstance(previous, (int, float)) and current < previous - slack:
            regressions.append(f"{category}: mean score regressed {previous} -> {current} (slack {slack})")
    new_categories = sorted(set(summary.get("by_category") or {}) - set(recorded.get("categories") or {}))
    if new_categories:
        stage.details.append(f"new evaluation categories not in baseline: {new_categories}")
    return regressions


STAGE_FUNCTIONS = {
    "ruff": lambda args, baseline, exemptions, report_dir: stage_ruff(args, baseline, exemptions, report_dir),
    "format": lambda args, baseline, exemptions, report_dir: stage_format(report_dir),
    "mypy": lambda args, baseline, exemptions, report_dir: stage_mypy(args, baseline, exemptions, report_dir),
    "bandit": lambda args, baseline, exemptions, report_dir: stage_bandit(args, baseline, exemptions, report_dir),
    "pip-audit": lambda args, baseline, exemptions, report_dir: stage_pip_audit(args, baseline, exemptions, report_dir),
    "pytest": lambda args, baseline, exemptions, report_dir: stage_pytest(args, baseline, report_dir, unit_only=False),
    "pytest-unit": lambda args, baseline, exemptions, report_dir: stage_pytest(
        args, baseline, report_dir, unit_only=True
    ),
    "new-code-coverage": lambda args, baseline, exemptions, report_dir: stage_new_code_coverage(args, report_dir),
    "openapi": lambda args, baseline, exemptions, report_dir: stage_openapi(args, report_dir),
    "metrics-contract": lambda args, baseline, exemptions, report_dir: stage_metrics_contract(report_dir),
    "complexity": lambda args, baseline, exemptions, report_dir: stage_complexity(args, baseline, report_dir),
    "gitleaks": lambda args, baseline, exemptions, report_dir: stage_gitleaks(args, report_dir),
    "benchmark": lambda args, baseline, exemptions, report_dir: stage_benchmark(args, baseline, report_dir),
    "mutation": lambda args, baseline, exemptions, report_dir: stage_mutation(args, report_dir),
    "llm-evaluation": lambda args, baseline, exemptions, report_dir: stage_llm_evaluation(args, baseline, report_dir),
}


def overall_status(stages: list[StageResult]) -> str:
    if any(stage.status in {FAIL, ERROR} for stage in stages):
        return FAIL
    if any(stage.status == WARN for stage in stages):
        return WARN
    return PASS


def aggregate_metrics(stages: list[StageResult]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for stage in stages:
        if stage.status == SKIP:
            continue
        key = stage.name
        if key == "ruff":
            metrics["ruff_violations"] = stage.metrics.get("violations")
        elif key == "mypy":
            metrics["mypy_errors"] = stage.metrics.get("errors")
        elif key == "pytest":
            metrics["tests"] = stage.metrics.get("tests")
            metrics["coverage"] = stage.metrics.get("coverage")
        elif key == "pytest-unit":
            metrics["unit_tests"] = stage.metrics.get("tests")
        elif key == "bandit":
            metrics["bandit"] = {name: stage.metrics.get(name) for name in ("high", "medium", "low", "new")}
        elif key == "pip-audit":
            metrics["dependencies"] = {name: stage.metrics.get(name) for name in ("vulnerabilities", "new")}
        elif key == "new-code-coverage":
            metrics["new_code_coverage"] = stage.metrics.get("report")
        elif key == "openapi":
            metrics["openapi"] = {name: stage.metrics.get(name) for name in ("breaking", "warning", "info")}
        elif key == "complexity":
            metrics["complexity"] = stage.metrics
        elif key == "benchmark":
            metrics["benchmark"] = {name: stage.metrics.get(name) for name in ("compared", "regressions", "warnings")}
        elif key == "mutation":
            metrics["mutation"] = stage.metrics
        elif key == "llm-evaluation":
            metrics["evaluation"] = {
                name: stage.metrics.get(name)
                for name in ("cases", "metric_runs", "pass_rate", "mean_score", "judge_model", "judge_configured")
            }
        elif key == "gitleaks":
            metrics["gitleaks"] = {"findings": stage.metrics.get("findings")}
    coverage = metrics.get("coverage") or {}
    if coverage:
        metrics["coverage_line"] = coverage.get("line")
        metrics["coverage_branch"] = coverage.get("branch")
    return metrics


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"## Code quality: {report['status']}",
        "",
        f"- profile: `{report['profile']}`",
        f"- commit: `{report.get('git', {}).get('commit') or 'n/a'}`",
        f"- config: `{report.get('config_sha256', '')[:12]}`",
        "",
        "| Stage | Status | Summary |",
        "|---|---|---|",
    ]
    for stage in report["stages"]:
        summary = str(stage.get("summary", "")).replace("|", "\\|")
        lines.append(f"| {stage['name']} | {stage['status']} | {summary} |")
    metrics = report.get("metrics", {})
    if metrics.get("coverage_line") is not None:
        lines.extend(
            [
                "",
                f"Coverage: line **{metrics['coverage_line']}%**, branch **{metrics.get('coverage_branch')}%**.",
            ]
        )
    evaluation = metrics.get("evaluation") or {}
    if evaluation.get("cases") is not None:
        judge = evaluation.get("judge_model") or "not-configured"
        lines.extend(
            [
                "",
                f"Evaluation: **{evaluation.get('cases')}** golden case(s), "
                f"pass rate **{evaluation.get('pass_rate')}**, judge `{judge}`.",
            ]
        )
    lines.append("")
    lines.append(f"Report: `{report.get('report_path', '')}`")
    return "\n".join(lines) + "\n"


def write_reports(
    args: argparse.Namespace,
    report_dir: Path,
    profile: str,
    stages: list[StageResult],
    started_at: str,
    duration: float,
) -> dict[str, Any]:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + f"-{profile}"
    report: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "profile": profile,
        "status": overall_status(stages),
        "started_at": started_at,
        "finished_at": utc_now(),
        "duration_seconds": round(duration, 3),
        "git": git_metadata(),
        "environment": environment_metadata(),
        "tools": tool_versions(),
        "config_sha256": config_sha256(),
        "thresholds": {
            **DEFAULT_THRESHOLDS,
            **(load_baseline(args.baseline).get("thresholds") if args.baseline.is_file() else {}),
            **args.threshold_overrides(),
        },
        "metrics": aggregate_metrics(stages),
        "stages": [stage.as_dict() for stage in stages],
        "report_path": rel(report_dir / f"{run_id}.json"),
    }
    write_json(report_dir / f"{report['run_id']}.json", report)
    write_json(report_dir / "latest.json", report)
    markdown = render_markdown(report)
    write_text(report_dir / "latest.md", markdown)
    history = report_dir / "history.ndjson"
    entry = {
        "run_id": report["run_id"],
        "finished_at": report["finished_at"],
        "profile": report["profile"],
        "status": report["status"],
        "git": report["git"],
        "metrics": report["metrics"],
        "stages": {stage["name"]: stage["status"] for stage in report["stages"]},
    }
    with history.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as handle:
            handle.write(markdown)
    return report


def collect_current_fingerprints(stages: list[StageResult]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for stage in stages:
        fingerprints = stage.metrics.get("fingerprints")
        if isinstance(fingerprints, list) and stage.name in {"ruff", "mypy", "bandit", "pip-audit"}:
            result[stage.name] = sorted(str(item) for item in fingerprints)
    return result


def update_baseline_file(args: argparse.Namespace, report: dict[str, Any]) -> int:
    old = load_baseline(args.baseline)
    current = collect_current_fingerprints(
        [
            StageResult(
                **{
                    key: stage[key]
                    for key in ("name", "status", "summary", "metrics", "details", "artifacts", "duration_seconds")
                }
            )
            for stage in report["stages"]
        ]
    )
    for tool, values in old.get("fingerprints", {}).items():
        current.setdefault(tool, [str(item) for item in values])
    old_counts = old.get("counts", {})

    def preserved(key: str, value: Any) -> Any:
        return value if value is not None else old_counts.get(key)

    new_findings = {
        tool: sorted(set(values) - set(str(item) for item in old.get("fingerprints", {}).get(tool, [])))
        for tool, values in current.items()
    }
    new_findings = {tool: values for tool, values in new_findings.items() if values}
    if new_findings and not args.accept_new:
        print("refusing to update baseline: new findings were not approved", file=sys.stderr)
        for tool, values in new_findings.items():
            for value in values[:20]:
                print(f"  {tool}: {value}", file=sys.stderr)
        print("re-run with --accept-new only after reviewing each new finding", file=sys.stderr)
        return 2
    previous_line = old.get("counts", {}).get("coverage_line")
    current_line = report.get("metrics", {}).get("coverage_line")
    if (
        isinstance(previous_line, int | float)
        and isinstance(current_line, int | float)
        and current_line < float(previous_line) - float(DEFAULT_THRESHOLDS["coverage_regression_slack"])
        and not args.accept_coverage_drop
    ):
        print(
            f"refusing to lower coverage baseline from {previous_line}% to {current_line}%; "
            "use --accept-coverage-drop only for an approved scope change",
            file=sys.stderr,
        )
        return 2
    baseline = {
        "schema_version": 1,
        "updated_at": utc_now(),
        "commit": report.get("git", {}).get("commit"),
        "profile": report["profile"],
        "config_sha256": report["config_sha256"],
        "tools": report.get("tools"),
        "thresholds": report.get("thresholds"),
        "counts": {
            "ruff_violations": preserved("ruff_violations", report.get("metrics", {}).get("ruff_violations")),
            "mypy_errors": preserved("mypy_errors", report.get("metrics", {}).get("mypy_errors")),
            "coverage_line": preserved("coverage_line", report.get("metrics", {}).get("coverage_line")),
            "coverage_branch": preserved("coverage_branch", report.get("metrics", {}).get("coverage_branch")),
            "bandit_high": preserved("bandit_high", (report.get("metrics", {}).get("bandit") or {}).get("high")),
            "bandit_medium": preserved("bandit_medium", (report.get("metrics", {}).get("bandit") or {}).get("medium")),
            "pip_audit_vulnerabilities": preserved(
                "pip_audit_vulnerabilities",
                (report.get("metrics", {}).get("dependencies") or {}).get("vulnerabilities"),
            ),
            "mutation_score": preserved(
                "mutation_score",
                (report.get("metrics", {}).get("mutation") or {}).get("score")
                if isinstance(report.get("metrics", {}).get("mutation"), dict)
                else None,
            ),
        },
        "fingerprints": current,
        "notes": "Fingerprints are line-number independent. Re-baseline only after reviewing a reduction in debt.",
    }
    write_json(args.baseline, baseline)
    print(f"baseline updated: {rel(args.baseline)}")
    for tool, values in sorted(current.items()):
        print(f"  {tool}: {len(values)} fingerprint(s)")
    return 0


def parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def select_stages(args: argparse.Namespace) -> list[str]:
    selected = parse_csv(args.only) if args.only else list(PROFILES[args.profile])
    unknown = [name for name in selected if name not in ALL_STAGES]
    if unknown:
        raise SystemExit(f"unknown stage(s): {', '.join(unknown)}")
    skipped = set(parse_csv(args.skip))
    return [name for name in selected if name not in skipped]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the MMS unified code quality gate")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="local")
    parser.add_argument("--only", help="comma-separated stages to run instead of the profile")
    parser.add_argument("--skip", help="comma-separated stages to skip")
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--exemptions", type=Path, default=DEFAULT_EXEMPTIONS)
    parser.add_argument("--performance-baseline", type=Path, default=DEFAULT_PERFORMANCE_BASELINE)
    parser.add_argument("--eval-baseline", type=Path, default=DEFAULT_EVAL_BASELINE)
    parser.add_argument("--openapi-baseline", type=Path, default=ROOT / "contracts" / "openapi-baseline.json")
    parser.add_argument("--diff-base", default="origin/main")
    parser.add_argument("--require-new-code-coverage", action="store_true")
    parser.add_argument("--require-gitleaks", action="store_true")
    parser.add_argument("--update-baseline", action="store_true")
    parser.add_argument("--accept-new", action="store_true", help="allow new findings when writing the baseline")
    parser.add_argument("--accept-coverage-drop", action="store_true")
    parser.add_argument("--update-performance-baseline", action="store_true")
    parser.add_argument("--update-eval-baseline", action="store_true", help="record evaluation category means")
    parser.add_argument("--enforce-performance", action="store_true")
    parser.add_argument("--coverage-line", type=float, help="override the line coverage threshold")
    parser.add_argument("--coverage-branch", type=float, help="override the branch coverage threshold")
    parser.add_argument("--new-code-coverage", type=float, help="override the changed-line coverage threshold")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    return parser


def threshold_overrides(args: argparse.Namespace) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    if args.coverage_line is not None:
        overrides["coverage_line"] = args.coverage_line
    if args.coverage_branch is not None:
        overrides["coverage_branch"] = args.coverage_branch
    if args.new_code_coverage is not None:
        overrides["new_code_coverage"] = args.new_code_coverage
    return overrides


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.threshold_overrides = lambda: threshold_overrides(args)  # type: ignore[attr-defined]
    report_dir = args.report_dir.resolve()
    report_dir.mkdir(parents=True, exist_ok=True)
    try:
        baseline = load_baseline(args.baseline)
        exemptions = load_exemptions(args.exemptions)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"invalid quality baseline/exemptions: {exc}", file=sys.stderr)
        return 2
    stages_to_run = select_stages(args)
    started_at = utc_now()
    started = time.monotonic()
    stages: list[StageResult] = []
    if "baseline" in stages_to_run:
        stages.append(stage_baseline(args, baseline, exemptions, report_dir))
        if args.fail_fast and stages[-1].status in {FAIL, ERROR}:
            stages_to_run = []
    for name in stages_to_run:
        if name == "baseline":
            continue
        if not args.quiet:
            print(f"==> {name}", flush=True)
        stage_started = time.monotonic()
        try:
            stage = STAGE_FUNCTIONS[name](args, baseline, exemptions, report_dir)
        except Exception as exc:  # gate orchestration must never hide a broken tool
            stage = StageResult(name, ERROR, f"stage crashed: {exc}")
        stage.duration_seconds = time.monotonic() - stage_started
        stages.append(stage)
        if not args.quiet:
            print(f"    {stage.status}: {stage.summary}", flush=True)
        if args.fail_fast and stage.status in {FAIL, ERROR}:
            break
    report = write_reports(args, report_dir, args.profile, stages, started_at, time.monotonic() - started)
    if not args.quiet:
        print()
        for stage in report["stages"]:
            print(f"{stage['status']:5} {stage['name']:20} {stage['summary']}")
        print(f"\noverall: {report['status']} ({report['duration_seconds']:.1f}s)")
        print(f"report: {rel(report_dir / 'latest.json')}")
    if args.update_baseline:
        blockers = [
            stage["name"]
            for stage in report["stages"]
            if stage["name"] in {"format", "pytest", "openapi", "gitleaks"} and stage["status"] in {FAIL, ERROR}
        ]
        if blockers:
            print(
                f"refusing to update the baseline while required stage(s) fail: {', '.join(blockers)}",
                file=sys.stderr,
            )
            return 2
        code = update_baseline_file(args, report)
        if code != 0:
            return code
    return 0 if report["status"] in {PASS, WARN} else 1


if __name__ == "__main__":
    raise SystemExit(main())
