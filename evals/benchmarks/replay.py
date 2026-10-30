"""Benchmark replay: write history through the real MMS pipeline (rule one).

Every conversation turn is submitted as a structured candidate through the
public API, in session order, with the session timestamp as observed_at
(rule three). Fact keys are unique per turn so the decision engine stores the
full history (SERVICE_HISTORY semantics); speaker identity is preserved in
source_refs. Write visibility waits for the async embedding pipeline with a
declared timeout so async lag is never misread as memory loss.
"""

from __future__ import annotations

import time
from typing import Any

from evals.harness import EvalTarget

ROLE_SOURCE_KIND = {"user": "USER_EXPLICIT", "assistant": "AGENT_CONFIRMED"}
READINESS_TIMEOUT_SECONDS = 300.0
QUERY_MAX_ITEMS = 10
QUERY_MAX_TOKENS = 8000


def fact_key_for(source_id: str, prefix: str) -> str:
    key = source_id.lower().replace(":", ".")
    safe = "".join(char if char in "abcdefghijklmnopqrstuvwxyz0123456789._-" else "." for char in key)
    return f"{prefix}.{safe}"


def replay_sessions(
    target: EvalTarget,
    customer: str,
    sessions: list[dict[str, Any]],
    *,
    fact_prefix: str,
    turn_field: str = "content",
    text_field: str = "content",
) -> dict[str, Any]:
    """Replay all sessions of one case and wait for write visibility.

    Returns the sidecar source map (written text -> source_id) used by the
    evaluator for evidence-recall diagnostics; it never feeds the query.
    """
    source_map: dict[str, str] = {}
    session_map: dict[str, str] = {}
    fact_keys: list[str] = []
    writes: list[dict[str, Any]] = []
    for session in sessions:
        for turn in session["turns"]:
            text = str(turn[text_field])
            fact_keys.append(fact_key_for(str(turn[turn_field]), fact_prefix))
            source_map[text] = str(turn[turn_field])
            session_map[text] = str(session.get("session_id") or "")
    target.seed_fact_keys(fact_keys)
    started = time.perf_counter()
    for session in sessions:
        occurred_at = session.get("occurred_at") or _now_iso()
        for turn in session["turns"]:
            role = str(turn.get("role") or turn.get("speaker") or "user")
            writes.append(
                target.ingest(
                    customer,
                    {
                        "memory_type": "SERVICE_HISTORY",
                        "fact_key": fact_key_for(str(turn[turn_field]), fact_prefix),
                        "object": {"value": str(turn[turn_field])},
                        "content": str(turn[text_field]),
                        "content_language": "en-US",
                        "source_kind": ROLE_SOURCE_KIND.get(role, "USER_EXPLICIT"),
                        "observed_at": occurred_at,
                        "source_refs": [
                            {
                                "source_id": str(turn[turn_field]),
                                "session_id": str(session.get("session_id") or ""),
                                "role": role,
                            }
                        ],
                    },
                    len(writes),
                )
            )
    target.drain_embeddings()
    _await_write_visibility(target, customer, timeout_seconds=READINESS_TIMEOUT_SECONDS)
    return {
        "source_map": source_map,
        "session_map": session_map,
        "customer": None,
        "write_count": len(writes),
        "outcomes": sorted({write["outcome"] for write in writes}),
        "replay_seconds": round(time.perf_counter() - started, 3),
    }


def _now_iso() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


def _await_write_visibility(target: EvalTarget, customer: str, *, timeout_seconds: float) -> None:
    """Docs/4 rule: declare the readiness condition and timeout explicitly.

    Draining the embedding worker is the primary wait; the pending check below
    guards against worker regressions silently eating writes."""
    from mms.utils.db import get_db

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with get_db().connection() as conn:
            pending = conn.execute(
                "SELECT count(*) AS n FROM memory_embedding_job WHERE customer_id=%s AND status IN ('PENDING','RUNNING','RETRY')",
                (customer,),
            ).fetchone()["n"]
        if not pending:
            return
        target.drain_embeddings()
        time.sleep(0.2)
    raise TimeoutError(f"write visibility not reached within {timeout_seconds}s for {customer}")


def benchmark_query(target: EvalTarget, customer: str, question: str) -> dict[str, Any]:
    status, payload, error_code = target.query(
        customer,
        {
            "query_text": question,
            "language": "en-US",
            "purpose": "CURRENT_ANSWER",
            "max_items": QUERY_MAX_ITEMS,
            "max_tokens": QUERY_MAX_TOKENS,
        },
    )
    memories = (payload or {}).get("memories") or []
    return {
        "http_status": status,
        "error_code": error_code,
        "retrieved": [str(memory.get("content", "")) for memory in memories],
        "retrieved_count": len(memories),
        "latency_ms": None,
    }


def replay_locomo_conversation(target: EvalTarget, customer: str, sample: dict[str, Any]) -> dict[str, Any]:
    return replay_sessions(
        target,
        customer,
        sample["sessions"],
        fact_prefix=f"locomo.{sample['sample_id']}",
        turn_field="dia_id",
        text_field="text",
    )
