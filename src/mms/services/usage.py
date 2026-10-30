from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from ..models import CandidateInput
from ..observability.llm import publish_feedback_score
from ..utils.db import DbConnection, get_db
from ..utils.errors import MmsError
from ..utils.metrics import CORRECTIONS, FEEDBACK, RETRIEVAL_TOKENS
from .normalizer import SECRET_PATTERNS


class FeedbackService:
    def __init__(self, memory_service):
        self.memory_service = memory_service

    def submit(
        self,
        conn: DbConnection,
        customer_id: str,
        memory_id: UUID,
        *,
        idempotency_key: str,
        feedback_type: str,
        request_id: str | None,
        comment: str | None,
        correction: CandidateInput | None,
    ) -> dict[str, Any]:
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (f"feedback:{customer_id}:{idempotency_key}",)
        )
        existing = conn.execute(
            "SELECT id,feedback_type,created_at FROM memory_feedback WHERE customer_id=%s AND idempotency_key=%s",
            (customer_id, idempotency_key),
        ).fetchone()
        if existing:
            return {**dict(existing), "duplicate": True}
        memory = conn.execute(
            "SELECT 1 FROM customer_memory WHERE id=%s AND customer_id=%s",
            (memory_id, customer_id),
        ).fetchone()
        if not memory:
            raise MmsError("memory not found", code="NOT_FOUND", status_code=404)
        if feedback_type == "INCORRECT" and correction is None:
            raise MmsError(
                "INCORRECT feedback requires a correction candidate", code="VALIDATION_FAILED", status_code=422
            )
        if correction and correction.customer_id != customer_id:
            raise MmsError("correction customer mismatch", code="CUSTOMER_MISMATCH", status_code=403)
        prepared = self.memory_service.prepare_candidate(correction) if correction else None
        redacted = comment
        if redacted:
            for pattern in SECRET_PATTERNS:
                redacted = pattern.sub("[REDACTED]", redacted)
        feedback_id = uuid4()
        conn.execute(
            """INSERT INTO memory_feedback
                 (id,customer_id,idempotency_key,request_id,memory_id,feedback_type,actor_type,actor_id,comment_redacted)
               VALUES (%s,%s,%s,%s,%s,%s,'CUSTOMER',%s,%s)""",
            (feedback_id, customer_id, idempotency_key, request_id, memory_id, feedback_type, customer_id, redacted),
        )
        FEEDBACK.labels(feedback_type=feedback_type).inc()
        # User feedback closes the quality loop: aggregated gauge (docs/3 §9.2) plus a
        # Langfuse score on the originating retrieval trace (docs/3 §7.2/§8).
        publish_feedback_score(request_id, feedback_type, redacted)
        decision = None
        if correction:
            decision = self.memory_service.process_candidate(conn, correction, prepared=prepared)
            CORRECTIONS.labels(outcome=decision.outcome).inc()
        return {"id": feedback_id, "feedback_type": feedback_type, "duplicate": False, "correction_decision": decision}


class UsageService:
    def record_retrieval(
        self,
        customer_id: str,
        request_id: str,
        purpose: str,
        memories: list[dict[str, Any]],
        *,
        degraded: bool,
        token_count: int,
    ) -> None:
        memory_ids = [item["memory_id"] for item in memories]
        rank_map = {item["memory_id"]: rank for rank, item in enumerate(memories, start=1)}
        matched_views = {
            item["memory_id"]: {
                "rank": rank,
                "view_kind": item.get("matched_view_kind"),
                "view_language": item.get("matched_view_language"),
            }
            for rank, item in enumerate(memories, start=1)
        }
        with get_db().transaction() as conn:
            conn.execute(
                """INSERT INTO memory_usage_event
                     (customer_id,request_id,purpose,retrieved_memory_ids,used_memory_ids,rank_map,matched_views,used,degraded)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,false,%s)
                   ON CONFLICT (customer_id,request_id) DO NOTHING""",
                (customer_id, request_id, purpose, memory_ids, [], Jsonb(rank_map), Jsonb(matched_views), degraded),
            )
        RETRIEVAL_TOKENS.labels(direction="output").inc(token_count)

    def record_use(
        self,
        conn: DbConnection,
        customer_id: str,
        request_id: str,
        used_memory_ids: list[UUID],
        *,
        answer_hash: str | None,
        model_version: str | None,
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT retrieved_memory_ids FROM memory_usage_event WHERE customer_id=%s AND request_id=%s FOR UPDATE",
            (customer_id, request_id),
        ).fetchone()
        if not row:
            raise MmsError("retrieval usage event not found", code="NOT_FOUND", status_code=404)
        retrieved = set(row["retrieved_memory_ids"])
        if not set(used_memory_ids).issubset(retrieved):
            raise MmsError(
                "used_memory_ids must be a subset of retrieved_memory_ids", code="VALIDATION_FAILED", status_code=422
            )
        conn.execute(
            """UPDATE memory_usage_event SET used_memory_ids=%s,used=%s,answer_hash=%s,model_version=%s
               WHERE customer_id=%s AND request_id=%s""",
            (used_memory_ids, bool(used_memory_ids), answer_hash, model_version, customer_id, request_id),
        )
        return {"request_id": request_id, "used": bool(used_memory_ids), "used_memory_ids": used_memory_ids}
