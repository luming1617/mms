#!/usr/bin/env python3
"""Metric/alert contract check (docs/记忆管理-技术方案.md 15.3).

Every metric referenced by deploy/prometheus/alerts.yml and
deploy/prometheus/slo-rules.yml must be registered in src/mms/utils/metrics.py.
An unregistered name makes the PromQL expression evaluate to empty series, which
means the alert or SLO silently never fires. New metrics must be registered first,
then referenced by rules; this check fails the build on the reverse order.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RULE_FILES = (
    ROOT / "deploy" / "prometheus" / "alerts.yml",
    ROOT / "deploy" / "prometheus" / "slo-rules.yml",
)
DEFAULT_METRICS_MODULE = ROOT / "src" / "mms" / "utils" / "metrics.py"

_SERIES_SUFFIXES = ("_bucket", "_count", "_sum")
_DECLARATION_RE = re.compile(r'\b(?:Counter|Gauge|Histogram)\(\s*"([^"]+)"')
_REFERENCE_RE = re.compile(r"\b(mms_[a-z0-9_]+)\b")
_RULE_RE = re.compile(r"^\s*-\s+(?:record|alert):\s*(\S+)", re.MULTILINE)


def registered_metrics(metrics_module: Path = DEFAULT_METRICS_MODULE) -> set[str]:
    source = metrics_module.read_text(encoding="utf-8")
    return set(_DECLARATION_RE.findall(source))


def _base_name(metric: str) -> str:
    for suffix in _SERIES_SUFFIXES:
        if metric.endswith(suffix):
            return metric[: -len(suffix)]
    return metric


def referenced_metrics(rule_files: tuple[Path, ...]) -> dict[str, set[str]]:
    """Map each rule name to the metric names its expression references."""
    references: dict[str, set[str]] = {}
    for path in rule_files:
        text = path.read_text(encoding="utf-8")
        rules = list(_RULE_RE.finditer(text))
        for index, match in enumerate(rules):
            end = rules[index + 1].start() if index + 1 < len(rules) else len(text)
            block = text[match.start() : end]
            names = {_base_name(name) for name in _REFERENCE_RE.findall(block)}
            references.setdefault(match.group(1), set()).update(names)
    return references


def check_contract(
    rule_files: tuple[Path, ...] = DEFAULT_RULE_FILES, metrics_module: Path = DEFAULT_METRICS_MODULE
) -> list[dict[str, str]]:
    registered = registered_metrics(metrics_module)
    violations: list[dict[str, str]] = []
    for rule, names in referenced_metrics(rule_files).items():
        for name in sorted(names):
            if name not in registered:
                violations.append({"rule": rule, "metric": name})
    return violations


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json-report", type=Path, default=None, help="write violations as JSON to this path")
    args = parser.parse_args()

    violations = check_contract()
    if args.json_report:
        args.json_report.parent.mkdir(parents=True, exist_ok=True)
        args.json_report.write_text(json.dumps({"violations": violations}, indent=2), encoding="utf-8")

    if violations:
        print(f"metric/alert contract violations: {len(violations)}")
        for item in violations:
            print(f"  rule {item['rule']} references unregistered metric {item['metric']}")
        print("register the metric in src/mms/utils/metrics.py before shipping the rule (docs 15.3)")
        return 1
    print("metric/alert contract OK: every referenced metric is registered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
