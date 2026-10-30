"""LongMemEval raw -> converted case adapter (docs/4 §二).

Every question becomes one independent case: the full haystack is replayed
session-by-session through the MMS write pipeline, timestamps are preserved
(rule three) and answer/evidence fields stay sidecar-only (rule two).
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Any

from evals.benchmarks.case import validate_qa_case

DATE_NOISE = re.compile(r"\s*\([^)]*\)")
BENCHMARK_NAME = "LongMemEval"
BENCHMARK_VERSION = "cleaned (2025-09 release)"


def parse_date(value: str | None) -> str | None:
    """'2023/05/20 (Sat) 02:21' -> ISO 8601 UTC; the weekday hint is stripped
    because its locale-dependent parsing adds nothing."""
    if not value:
        return None
    cleaned = DATE_NOISE.sub("", str(value)).strip()
    parsed = datetime.strptime(cleaned, "%Y/%m/%d %H:%M").replace(tzinfo=UTC)
    return parsed.isoformat()


def turn_source_id(question_id: str, session_index: int, turn_index: int) -> str:
    return f"{question_id}:{session_index}:{turn_index}"


def convert_question(question: dict[str, Any]) -> dict[str, Any]:
    sessions: list[dict[str, Any]] = []
    for session_index, session in enumerate(question["haystack_sessions"]):
        session_id = str(question["haystack_session_ids"][session_index])
        occurred_at = parse_date(question["haystack_dates"][session_index])
        turns = []
        for turn_index, turn in enumerate(session):
            content = str(turn.get("content") or "").strip()
            if not content:
                continue
            turns.append(
                {
                    "role": "user" if turn.get("role") == "user" else "assistant",
                    "content": content[:4000],
                    "source_id": turn_source_id(question["question_id"], session_index, turn_index),
                    "session_id": session_id,
                }
            )
        if turns:
            sessions.append({"session_id": session_id, "occurred_at": occurred_at, "turns": turns})
    case = {
        "question_id": str(question["question_id"]),
        "question_type": str(question["question_type"]),
        "question": str(question["question"]),
        "question_date": str(question.get("question_date") or ""),
        "answer": str(question["answer"]),
        "answer_session_ids": list(question.get("answer_session_ids") or []),
        "sessions": sessions,
        "turn_count": sum(len(session["turns"]) for session in sessions),
        "priority": "regular",
        "source": {
            "benchmark": BENCHMARK_NAME,
            "benchmark_version": BENCHMARK_VERSION,
            "dataset": "longmemeval_s_cleaned.json",
        },
    }
    validate_qa_case(case, f"longmemeval:{case['question_id']}")
    return case


def select_sample(cases: list[dict[str, Any]], per_type: int = 1) -> list[dict[str, Any]]:
    """Deterministic stratified selection: for each question_type take the
    questions whose turn count is closest to that type's median (ties broken
    by question_id) - representative history sizes, reproducible samples."""
    selected: list[dict[str, Any]] = []
    types = sorted({case["question_type"] for case in cases})
    for question_type in types:
        bucket = sorted(
            (case for case in cases if case["question_type"] == question_type), key=lambda c: c["question_id"]
        )
        sizes = sorted(case["turn_count"] for case in bucket)
        median = sizes[len(sizes) // 2]
        ranked = sorted(
            bucket,
            key=lambda case: (abs(case["turn_count"] - median), case["question_id"]),
        )
        selected.extend(ranked[:per_type])
    return sorted(selected, key=lambda case: (case["question_type"], case["question_id"]))


def sample_metadata(
    source_file: str, cases: list[dict[str, Any]], *, dataset_hash: str | None = None
) -> dict[str, Any]:
    return {
        "dataset_name": f"longmemeval-{source_file}",
        "dataset_version": "1.0",
        "dataset_hash": dataset_hash,
        "benchmark_name": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "source_commit_or_release": "huggingface xiaowu0162/longmemeval-cleaned",
        "language_coverage": ["en-US"],
        "modality_limitations": ["text-only"],
        "case_count": len(cases),
        "selection": "per-question-type median turn count",
        "question_types": sorted({case["question_type"] for case in cases}),
        "conversion_script": "evals/benchmarks/convert.py",
    }


def stable_run_id() -> str:
    """Customer ids must be unique per run (rule: case isolation) but the
    question suffix keeps them traceable in the database."""
    return uuid.uuid4().hex[:8]
