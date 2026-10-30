"""LoCoMo raw -> converted sample adapter (docs/4 §三).

One conversation is converted into a single sample file: the sessions (with
timestamps) plus the selected QA items. All QAs of a conversation share the
replayed memory space; speaker identity is preserved in source metadata
(never blindly mapped to user/assistant); dia_id evidence stays sidecar-only.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

BENCHMARK_NAME = "LoCoMo"
BENCHMARK_VERSION = "locomo10 (ACL 2024 release)"

LOCOMO_DATE = "%I:%M %p on %d %B, %Y"
# QA category numbers come straight from the dataset; names follow the
# categories used in the LoCoMo paper's evaluation breakdown.
CATEGORY_NAMES = {
    1: "temporal",
    2: "local",
    3: "global",
    4: "adversarial",
    5: "relational",
}


def parse_session_date(value: str | None) -> str | None:
    if not value:
        return None
    parsed = datetime.strptime(str(value).strip(), LOCOMO_DATE).replace(tzinfo=UTC)
    return parsed.isoformat()


def _parse_evidence(raw: Any) -> list[str]:
    if isinstance(raw, list):
        items = [str(item) for item in raw]
    else:
        items = [
            item.strip().strip("'\"") for item in str(raw or "").strip("[]").split(",") if item.strip().strip("'\"")
        ]
    return [item for item in items if item and item.lower() != "none"]


def convert_conversation(conversation: dict[str, Any], *, qas_per_category: int = 2) -> dict[str, Any]:
    raw_conv = conversation["conversation"]
    speaker_a = str(raw_conv["speaker_a"])
    speaker_b = str(raw_conv["speaker_b"])
    sessions: list[dict[str, Any]] = []
    session_index = 1
    while f"session_{session_index}" in raw_conv:
        turns = []
        for turn in raw_conv[f"session_{session_index}"]:
            text = str(turn.get("text") or "").strip()
            dia_id = str(turn.get("dia_id") or "").strip()
            if text and dia_id:
                turns.append({"dia_id": dia_id, "speaker": str(turn["speaker"]), "text": text[:4000]})
        if turns:
            sessions.append(
                {
                    "session_id": f"session_{session_index}",
                    "occurred_at": parse_session_date(raw_conv.get(f"session_{session_index}_date_time")),
                    "turns": turns,
                }
            )
        session_index += 1

    by_category: dict[int, list[dict[str, Any]]] = {}
    for index, qa in enumerate(conversation.get("qa") or []):
        category = int(qa["category"])
        by_category.setdefault(category, []).append((index, qa))
    qas = []
    for category in sorted(by_category):
        for index, qa in sorted(by_category[category])[:qas_per_category]:
            # LoCoMo adversarial QAs carry only adversarial_answer - the
            # plausible-but-wrong answer the system must NOT assert; correct
            # behavior is judged at the reader level (no gold answer exists)
            adversarial = str(qa.get("adversarial_answer") or "")
            qas.append(
                {
                    "question_id": f"{conversation['sample_id']}-qa{index}",
                    "question": str(qa["question"]),
                    "answer": str(qa.get("answer") or ""),
                    "adversarial_answer": adversarial,
                    "category": category,
                    "category_name": CATEGORY_NAMES.get(category, f"category_{category}"),
                    "evidence_dia_ids": _parse_evidence(qa.get("evidence")),
                }
            )
    return {
        "sample_id": str(conversation["sample_id"]),
        "speaker_a": speaker_a,
        "speaker_b": speaker_b,
        "sessions": sessions,
        "turn_count": sum(len(session["turns"]) for session in sessions),
        "qas": qas,
        "priority": "regular",
        "source": {
            "benchmark": BENCHMARK_NAME,
            "benchmark_version": BENCHMARK_VERSION,
            "dataset": "locomo10.json",
        },
    }


def sample_metadata(sample: dict[str, Any], *, dataset_hash: str | None = None) -> dict[str, Any]:
    return {
        "dataset_name": f"locomo-{sample['sample_id']}",
        "dataset_version": "1.0",
        "dataset_hash": dataset_hash,
        "benchmark_name": BENCHMARK_NAME,
        "benchmark_version": BENCHMARK_VERSION,
        "source_commit_or_release": "github snap-research/LoCoMo data/locomo10.json",
        "language_coverage": ["en-US"],
        "modality_limitations": [
            "text-only: LoCoMo image URLs/descriptions are not replayed (docs/4 §三.2)",
        ],
        "session_count": len(sample["sessions"]),
        "turn_count": sample["turn_count"],
        "qa_count": len(sample["qas"]),
        "qa_categories": sorted({qa["category"] for qa in sample["qas"]}),
        "speakers": [sample["speaker_a"], sample["speaker_b"]],
    }
