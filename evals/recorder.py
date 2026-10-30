"""Evaluation result persistence (docs/1-模型质量评测体系设计.md §9.2, §10).

Every executed case/metric is recorded with the fields required for
run-over-run comparison: run_id, commit, target model info, judge model,
dataset version, metric name/threshold, score, reason, passed flag and
latency. Reports land in quality/reports/ next to the code-quality reports;
summaries append to eval-history.ndjson for trend analysis.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

REPORT_SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def git_commit(root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def target_model_info() -> dict[str, Any]:
    from mms.config import get_settings
    from mms.services.extractor import RuleBasedExtractor
    from mms.services.rules import DECISION_RULE_SET_VERSION

    settings = get_settings()
    return {
        "service": "mms",
        "embedding_provider": settings.embedding_provider,
        "embedding_model": settings.embedding_model,
        "embedding_version": settings.embedding_version,
        "decision_rule_set": DECISION_RULE_SET_VERSION,
        "extractor_version": RuleBasedExtractor().extractor_version,
    }


def default_report_dir() -> Path:
    env = os.environ.get("MMS_EVAL_REPORT_DIR")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[1] / "quality" / "reports"


def default_history_file() -> Path:
    env = os.environ.get("MMS_EVAL_HISTORY_FILE")
    if env:
        return Path(env)
    return default_report_dir() / "eval-history.ndjson"


class EvalRecorder:
    def __init__(
        self,
        *,
        report_dir: Path | None = None,
        history_file: Path | None = None,
        judge_model: str | None = None,
        judge_model_fingerprint: str | None = None,
        reader_model_fingerprint: str | None = None,
        dataset_versions: dict[str, str] | None = None,
        dataset_hashes: dict[str, str] | None = None,
        root: Path | None = None,
    ) -> None:
        self.root = root or Path(__file__).resolve().parents[1]
        self.report_dir = report_dir or default_report_dir()
        self.history_file = history_file or default_history_file()
        self.run_id = (
            os.environ.get("MMS_EVAL_RUN_ID") or f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:6]}"
        )
        self.started_at = utc_now()
        self.judge_model = judge_model
        self.judge_model_fingerprint = judge_model_fingerprint
        self.reader_model_fingerprint = reader_model_fingerprint
        self.dataset_versions = dataset_versions or {}
        self.dataset_hashes = dataset_hashes or {}
        self.target = target_model_info()
        self.commit = git_commit(self.root)
        self.entries: list[dict[str, Any]] = []
        self._started_monotonic = time.monotonic()

    def record(
        self,
        *,
        case_id: str,
        category: str,
        risk_level: str,
        dataset_version: str,
        metric_name: str,
        threshold: float | None,
        score: float | None,
        passed: bool | None,
        reason: str | None,
        latency_ms: float,
        error: str | None = None,
        judge: bool = False,
    ) -> None:
        self.entries.append(
            {
                "run_id": self.run_id,
                "case_id": case_id,
                "category": category,
                "risk_level": risk_level,
                "dataset_version": dataset_version,
                "metric_name": metric_name,
                "threshold": threshold,
                "score": score,
                "passed": passed,
                "reason": reason,
                "latency_ms": round(latency_ms, 3),
                "error": error,
                "llm_judge": judge,
                "commit_sha": self.commit,
                "target_model": self.target,
                "judge_model": self.judge_model,
            }
        )

    def summary(self) -> dict[str, Any]:
        total = len(self.entries)
        passed = sum(1 for entry in self.entries if entry["passed"] is True)
        failed = sum(1 for entry in self.entries if entry["passed"] is False)
        errored = sum(1 for entry in self.entries if entry["error"])
        scored = [entry["score"] for entry in self.entries if isinstance(entry["score"], (int, float))]
        by_category: dict[str, dict[str, Any]] = {}
        by_risk: dict[str, dict[str, Any]] = {}
        for entry in self.entries:
            for bucket, key in ((by_category, entry["category"]), (by_risk, entry["risk_level"])):
                stats = bucket.setdefault(key, {"entries": 0, "passed": 0, "failed": 0, "score_sum": 0.0, "scored": 0})
                stats["entries"] += 1
                if entry["passed"] is True:
                    stats["passed"] += 1
                if entry["passed"] is False:
                    stats["failed"] += 1
                if isinstance(entry["score"], (int, float)):
                    stats["score_sum"] += entry["score"]
                    stats["scored"] += 1
        for bucket in (by_category, by_risk):
            for stats in bucket.values():
                stats["mean_score"] = round(stats["score_sum"] / stats["scored"], 4) if stats["scored"] else None
                del stats["score_sum"]
                del stats["scored"]
        high_risk_failures = [
            entry["case_id"]
            for entry in self.entries
            if entry["risk_level"] in {"critical", "high"} and (entry["passed"] is False or entry["error"])
        ]
        return {
            "run_id": self.run_id,
            "cases": len({entry["case_id"] for entry in self.entries}),
            "metric_runs": total,
            "passed": passed,
            "failed": failed,
            "errored": errored,
            "pass_rate": round(passed / total, 4) if total else None,
            "mean_score": round(sum(scored) / len(scored), 4) if scored else None,
            "by_category": dict(sorted(by_category.items())),
            "by_risk": dict(sorted(by_risk.items())),
            "high_risk_failures": sorted(set(high_risk_failures)),
            "judge_model": self.judge_model,
            "judge_configured": bool(self.judge_model),
        }

    def finish(self) -> dict[str, Any]:
        report = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "run_id": self.run_id,
            "kind": "llm-evaluation",
            "started_at": self.started_at,
            "finished_at": utc_now(),
            "duration_seconds": round(time.monotonic() - self._started_monotonic, 3),
            "commit_sha": self.commit,
            "target_model": self.target,
            "judge_model": self.judge_model,
            "judge_model_fingerprint": self.judge_model_fingerprint,
            "reader_model_fingerprint": self.reader_model_fingerprint,
            "dataset_versions": self.dataset_versions,
            "dataset_hashes": self.dataset_hashes,
            "summary": self.summary(),
            "entries": self.entries,
        }
        self.report_dir.mkdir(parents=True, exist_ok=True)
        (self.report_dir / "eval-results.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.history_file.parent.mkdir(parents=True, exist_ok=True)
        with self.history_file.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "run_id": report["run_id"],
                        "finished_at": report["finished_at"],
                        "commit_sha": report["commit_sha"],
                        "dataset_hashes": report["dataset_hashes"],
                        "judge_model_fingerprint": report["judge_model_fingerprint"],
                        "reader_model_fingerprint": report["reader_model_fingerprint"],
                        "summary": report["summary"],
                        "target_model": report["target_model"],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
                + "\n"
            )
        return report
