#!/usr/bin/env python3
"""Judge-model calibration (docs/1-模型质量评测体系设计.md §9.1).

Compares the LLM judge's verdicts against human labels on a fixed sample set
and reports the two error directions that matter for gate safety:

- 误通过率 (false-pass rate): human says fail, judge says pass. High-risk
  scenarios must drive this toward zero before the judge gates merges.
- 误拒绝率 (false-fail rate): human says pass, judge says fail. Too high and
  the gate produces useless manual review queues.

Usage:
    MMS_EVAL_JUDGE_MODEL=gpt-4o python evals/calibration/run_calibration.py \
        [--max-false-pass-rate 0.05] [--max-false-fail-rate 0.20]

Writes quality/reports/judge-calibration.json and exits non-zero when the
error budgets are exceeded.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from deepeval.metrics import GEval
from deepeval.test_case import LLMTestCase, SingleTurnParams

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from evals.judge import JudgeConfig
from evals.recorder import default_report_dir, git_commit, utc_now

CALIBRATION_FILE = Path(__file__).resolve().parent / "judge_calibration.json"

CALIBRATION_CRITERIA = (
    "Evaluate whether the actual memory context satisfies the expected behavior for the given "
    "query. Judge strictly: penalize fabricated information, privacy leaks (credentials, secrets) "
    "and rankings that put irrelevant memories above the answer. Genuinely stored but weakly "
    "related memories are acceptable when the expected behavior does not forbid them."
)


def load_samples() -> list[dict[str, Any]]:
    samples = json.loads(CALIBRATION_FILE.read_text(encoding="utf-8"))
    for sample in samples:
        missing = [
            key for key in ("case_id", "input", "expected_output", "actual_output", "human_pass") if key not in sample
        ]
        if missing:
            raise ValueError(f"calibration sample {sample.get('case_id')} missing fields: {missing}")
    return samples


def run_calibration(judge: JudgeConfig, samples: list[dict[str, Any]]) -> dict[str, Any]:
    metric = GEval(
        name="Judge Calibration",
        criteria=CALIBRATION_CRITERIA,
        evaluation_params=[
            SingleTurnParams.INPUT,
            SingleTurnParams.ACTUAL_OUTPUT,
            SingleTurnParams.EXPECTED_OUTPUT,
        ],
        threshold=0.80,
        model=judge.model,
        strict_mode=False,
    )
    entries: list[dict[str, Any]] = []
    for sample in samples:
        test_case = LLMTestCase(
            name=str(sample["case_id"]),
            input=str(sample["input"]),
            actual_output=str(sample["actual_output"]),
            expected_output=str(sample["expected_output"]),
        )
        metric.measure(test_case)
        judge_pass = bool(metric.is_successful())
        human_pass = bool(sample["human_pass"])
        entries.append(
            {
                "case_id": sample["case_id"],
                "human_pass": human_pass,
                "judge_pass": judge_pass,
                "judge_score": round(metric.score, 6) if isinstance(metric.score, (int, float)) else None,
                "judge_reason": str(metric.reason) if metric.reason else None,
                "classification": (
                    "agree_pass"
                    if human_pass and judge_pass
                    else "agree_fail"
                    if not human_pass and not judge_pass
                    else "false_pass"
                    if judge_pass
                    else "false_fail"
                ),
                "human_note": sample.get("human_note"),
            }
        )
    total = len(entries)
    false_pass = sum(1 for entry in entries if entry["classification"] == "false_pass")
    false_fail = sum(1 for entry in entries if entry["classification"] == "false_fail")
    return {
        "schema_version": 1,
        "kind": "judge-calibration",
        "generated_at": utc_now(),
        "commit_sha": git_commit(Path(__file__).resolve().parents[2]),
        "judge_model": judge.model,
        "criteria": CALIBRATION_CRITERIA,
        "sample_count": total,
        "false_pass_rate": round(false_pass / total, 4) if total else None,
        "false_fail_rate": round(false_fail / total, 4) if total else None,
        "agreement_rate": round((total - false_pass - false_fail) / total, 4) if total else None,
        "entries": entries,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Calibrate the DeepEval judge model against human labels")
    parser.add_argument("--max-false-pass-rate", type=float, default=0.05)
    parser.add_argument("--max-false-fail-rate", type=float, default=0.20)
    args = parser.parse_args(argv)

    judge = JudgeConfig.from_env()
    judge.ensure_usable(purpose="judge calibration")
    if not judge.available:
        print(
            "judge calibration requires an LLM judge: set MMS_EVAL_JUDGE_MODEL "
            "(and the provider credentials, e.g. OPENAI_API_KEY)",
            file=sys.stderr,
        )
        return 1
    report = run_calibration(judge, load_samples())

    report_dir = default_report_dir()
    report_dir.mkdir(parents=True, exist_ok=True)
    output = report_dir / "judge-calibration.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"judge model: {report['judge_model']}")
    print(f"samples: {report['sample_count']}")
    print(f"false-pass rate (误通过率): {report['false_pass_rate']}")
    print(f"false-fail rate (误拒绝率): {report['false_fail_rate']}")
    print(f"agreement rate: {report['agreement_rate']}")
    print(f"report: {output}")
    for entry in report["entries"]:
        if entry["classification"] in {"false_pass", "false_fail"}:
            print(f"  {entry['classification']}: {entry['case_id']} (human_pass={entry['human_pass']})")

    false_pass_rate = report["false_pass_rate"] or 0.0
    false_fail_rate = report["false_fail_rate"] or 0.0
    if false_pass_rate > args.max_false_pass_rate:
        print(
            f"false-pass rate {false_pass_rate} exceeds budget {args.max_false_pass_rate}: "
            "the judge is not safe for merge gating yet",
            file=sys.stderr,
        )
        return 1
    if false_fail_rate > args.max_false_fail_rate:
        print(
            f"false-fail rate {false_fail_rate} exceeds budget {args.max_false_fail_rate}: "
            "the judge would flood manual review",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
