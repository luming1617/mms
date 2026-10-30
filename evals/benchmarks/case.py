"""Converted benchmark case loading and validation.

A converted LongMemEval case (one per question) follows the field table in
docs/4 §二.2; answer/evidence fields are sidecar data used only by the
evaluator (rule two: no answer leakage into the system under test). A LoCoMo
sample file holds one conversation plus the selected QA items, sharing a
single replayed memory space across its questions.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SAMPLES_DIR = Path(__file__).resolve().parent / "samples"

REQUIRED_QA_FIELDS = (
    "question_id",
    "question_type",
    "question",
    "answer",
    "sessions",
    "priority",
    "source",
)
REQUIRED_SESSION_FIELDS = ("session_id", "occurred_at", "turns")
REQUIRED_TURN_FIELDS = ("role", "content", "source_id")


class CaseValidationError(ValueError):
    pass


def _validate_turn(turn: dict[str, Any], where: str) -> None:
    missing = [field for field in REQUIRED_TURN_FIELDS if not turn.get(field)]
    if missing:
        raise CaseValidationError(f"{where}: turn missing {missing}")


def validate_qa_case(case: dict[str, Any], where: str) -> None:
    missing = [field for field in REQUIRED_QA_FIELDS if not case.get(field)]
    if missing:
        raise CaseValidationError(f"{where}: case missing {missing}")
    if not case["sessions"]:
        raise CaseValidationError(f"{where}: case has no sessions to replay")
    for index, session in enumerate(case["sessions"]):
        missing_session = [field for field in REQUIRED_SESSION_FIELDS if not session.get(field)]
        if missing_session:
            raise CaseValidationError(f"{where}: session {index} missing {missing_session}")
        for turn_index, turn in enumerate(session["turns"]):
            _validate_turn(turn, f"{where}: session {index} turn {turn_index}")
    if case["priority"] not in {"hard_gate", "regular", "stress"}:
        raise CaseValidationError(f"{where}: unknown priority {case['priority']!r}")


def load_qa_cases(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    cases = payload["cases"] if isinstance(payload, dict) else payload
    if not isinstance(cases, list) or not cases:
        raise CaseValidationError(f"{path} must contain a non-empty list of cases")
    for case in cases:
        validate_qa_case(case, str(path))
    return cases


def load_locomo_sample(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "sample" in payload:
        payload = payload["sample"]
    for field in ("sample_id", "speaker_a", "speaker_b", "sessions", "qas", "priority", "source"):
        if not payload.get(field):
            raise CaseValidationError(f"{path}: locomo sample missing {field}")
    for index, session in enumerate(payload["sessions"]):
        missing = [field for field in ("session_id", "occurred_at", "turns") if not session.get(field)]
        if missing:
            raise CaseValidationError(f"{path}: session {index} missing {missing}")
        for turn in session["turns"]:
            missing_turn = [field for field in ("dia_id", "speaker", "text") if not turn.get(field)]
            if missing_turn:
                raise CaseValidationError(f"{path}: turn missing {missing_turn}")
            if turn["dia_id"] in {"", None}:
                raise CaseValidationError(f"{path}: empty dia_id")
    for index, qa in enumerate(payload["qas"]):
        missing = [
            field for field in ("question_id", "question", "answer", "category", "evidence_dia_ids") if field not in qa
        ]
        if missing:
            raise CaseValidationError(f"{path}: qa {index} missing {missing}")
    return payload
