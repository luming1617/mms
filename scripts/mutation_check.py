#!/usr/bin/env python3
"""Run mutmut for the curated critical modules and export a JSON summary.

Mutation testing is advisory in the current phase: the nightly profile records
the score and trend but does not block merges until per-module thresholds have
been agreed. The selected modules cover decision rules, normalization and
canonical hashing; their tests do not require PostgreSQL.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "quality" / "reports" / "mutation.json"
STATS_PATH = ROOT / "mutants" / "mutmut-cicd-stats.json"


def run(command: list[str], timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run mutation testing for critical modules")
    parser.add_argument("--json-report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--max-children", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--fail-under", type=float, default=None, help="optional mutation score threshold")
    args = parser.parse_args()

    if not shutil.which("mutmut"):
        print("mutmut is not installed; install the mutation extra", file=sys.stderr)
        return 2
    run_result = run(["mutmut", "run", "--max-children", str(args.max_children)], timeout=args.timeout)
    if run_result.returncode != 0:
        print(run_result.stdout[-4000:], file=sys.stderr)
        print(run_result.stderr[-2000:], file=sys.stderr)
    exported = run(["mutmut", "export-cicd-stats"], timeout=120)
    if not STATS_PATH.is_file():
        print(exported.stdout[-2000:], file=sys.stderr)
        print(exported.stderr[-2000:], file=sys.stderr)
        return 1
    stats = json.loads(STATS_PATH.read_text(encoding="utf-8"))
    killed = int(stats.get("killed", 0))
    survived = int(stats.get("survived", 0))
    timeout_count = int(stats.get("timeout", 0))
    suspicious = int(stats.get("suspicious", 0))
    denominator = killed + survived + timeout_count + suspicious
    score = round(100.0 * killed / denominator, 2) if denominator else 0.0
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "tool": "mutmut",
        "modules": [
            "src/mms/services/rules.py",
            "src/mms/services/normalizer.py",
            "src/mms/utils/hashing.py",
        ],
        "metrics": {
            "score": score,
            "killed": killed,
            "survived": survived,
            "timeout": timeout_count,
            "suspicious": suspicious,
            "no_tests": int(stats.get("no_tests", 0)),
            "skipped": int(stats.get("skipped", 0)),
            "total": int(stats.get("total", 0)),
            "raw": stats,
        },
    }
    args.json_report.parent.mkdir(parents=True, exist_ok=True)
    args.json_report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"mutation score: {score}% (killed={killed}, survived={survived}, timeout={timeout_count})")
    if args.fail_under is not None and score < args.fail_under:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
