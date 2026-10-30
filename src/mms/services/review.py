from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from ..config import get_settings
from ..models import CandidateInput, Decision, NormalizedCandidate
from ..utils.db import DbConnection, require_row
from ..utils.embedding import provider_from_settings
from ..utils.errors import MmsError
from .normalizer import redact_text
from .repository import MemoryRepository


class ReviewService:
    def __init__(self, repository: MemoryRepository, memory_service=None):
        self.repository = repository
        self._memory_service = memory_service

    @property
    def memory_service(self):
        if self._memory_service is None:
            from .service import MemoryService

            settings = get_settings()
            provider = provider_from_settings(settings)
            self._memory_service = MemoryService(self.repository, provider)
        return self._memory_service

    def ensure_candidate_task(
        self,
        conn: DbConnection,
        candidate: NormalizedCandidate,
        decision: Decision,
        *,
        review_sla_days: int = 30,
    ) -> None:
        existing = conn.execute(
            "SELECT id FROM memory_review_task WHERE candidate_id=%s AND status IN ('PENDING','IN_REVIEW')",
            (candidate.candidate_id,),
        ).fetchone()
        if existing:
            return
        conn.execute(
            """INSERT INTO memory_review_task
                 (id,customer_id,task_type,candidate_id,status,risk_level,due_at,suggested_action,created_by)
               VALUES (%s,%s,'CANDIDATE',%s,'PENDING',%s,now()+make_interval(days => %s),%s,'mms-engine')""",
            (
                uuid4(),
                candidate.customer_id,
                candidate.candidate_id,
                candidate.sensitivity,
                review_sla_days,
                "SUPERSEDE" if decision.target_memory_id else "ADD",
            ),
        )

    def create_conflict(
        self,
        conn: DbConnection,
        candidate: NormalizedCandidate,
        decision: Decision,
        *,
        review_sla_days: int = 30,
    ) -> None:
        if not decision.target_memory_id:
            raise ValueError("CONFLICT requires a target memory")
        target = require_row(
            conn.execute(
                "SELECT * FROM customer_memory WHERE id=%s AND customer_id=%s FOR UPDATE",
                (decision.target_memory_id, candidate.customer_id),
            ).fetchone(),
            "conflict target memory",
        )
        conn.execute(
            "UPDATE customer_memory SET lifecycle_status='CONFLICTED',updated_at=now() WHERE id=%s", (target["id"],)
        )
        conflict = conn.execute(
            """SELECT * FROM memory_conflict WHERE customer_id=%s AND fact_key=%s AND entity_type=%s
                 AND entity_ref=%s AND value_slot=%s AND status IN ('OPEN','IN_REVIEW') FOR UPDATE""",
            (
                candidate.customer_id,
                candidate.fact_key,
                candidate.entity_type,
                candidate.entity_ref,
                candidate.value_slot,
            ),
        ).fetchone()
        if conflict is None:
            conflict = require_row(
                conn.execute(
                    """INSERT INTO memory_conflict
                         (id,customer_id,fact_key,entity_type,entity_ref,value_slot,status,risk_level,detected_reason_codes,current_memory_id)
                       VALUES (%s,%s,%s,%s,%s,%s,'OPEN',%s,%s,%s) RETURNING *""",
                    (
                        uuid4(),
                        candidate.customer_id,
                        candidate.fact_key,
                        candidate.entity_type,
                        candidate.entity_ref,
                        candidate.value_slot,
                        candidate.sensitivity,
                        Jsonb(decision.reason_codes),
                        target["id"],
                    ),
                ).fetchone(),
                "memory conflict",
            )
            conn.execute(
                """INSERT INTO memory_conflict_item
                     (id,customer_id,conflict_id,memory_id,value_hash,object_json,content_redacted,source_kind,observed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    uuid4(),
                    candidate.customer_id,
                    conflict["id"],
                    target["id"],
                    target["object_hash"],
                    Jsonb(target["object_json"]),
                    redact_text(target["content"]),
                    target["source_kind"],
                    target["observed_at"],
                ),
            )
        conflict = require_row(conflict, "memory conflict")
        conn.execute(
            """INSERT INTO memory_conflict_item
                 (id,customer_id,conflict_id,candidate_id,value_hash,object_json,content_redacted,source_kind,observed_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                uuid4(),
                candidate.customer_id,
                conflict["id"],
                candidate.candidate_id,
                candidate.object_hash,
                Jsonb(candidate.object_json),
                redact_text(candidate.content),
                candidate.source_kind,
                candidate.observed_at,
            ),
        )
        task = conn.execute(
            "SELECT id FROM memory_review_task WHERE conflict_id=%s AND status IN ('PENDING','IN_REVIEW')",
            (conflict["id"],),
        ).fetchone()
        if not task:
            conn.execute(
                """INSERT INTO memory_review_task
                     (id,customer_id,task_type,conflict_id,status,risk_level,due_at,suggested_action,created_by)
                   VALUES (%s,%s,'CONFLICT',%s,'PENDING',%s,now()+make_interval(days => %s),'SUPERSEDE','mms-engine')""",
                (uuid4(), candidate.customer_id, conflict["id"], candidate.sensitivity, review_sla_days),
            )

    def queue(
        self,
        conn: DbConnection,
        customer_id: str,
        status: str = "PENDING",
        limit: int = 50,
        cursor: str | None = None,
    ) -> list[dict[str, Any]]:
        tasks = conn.execute(
            """SELECT * FROM memory_review_task WHERE customer_id=%s AND status=%s
               AND (%s::uuid IS NULL OR
                    (priority, COALESCE(due_at,'infinity'::timestamptz), created_at, id) >
                    (SELECT priority, COALESCE(due_at,'infinity'::timestamptz), created_at, id
                       FROM memory_review_task WHERE id=%s))
               ORDER BY priority, COALESCE(due_at,'infinity'::timestamptz), created_at, id LIMIT %s""",
            (customer_id, status, cursor, cursor, limit),
        ).fetchall()
        result = []
        for task in tasks:
            item = dict(task)
            if task["conflict_id"]:
                conflict = conn.execute(
                    """SELECT fact_key,entity_type,entity_ref,value_slot,risk_level,detected_reason_codes,current_memory_id
                       FROM memory_conflict WHERE id=%s""",
                    (task["conflict_id"],),
                ).fetchone()
                item["conflict"] = dict(conflict) if conflict else None
                if conflict:
                    # Doc 5.4: reviewers need the field authority rule alongside the values.
                    registry = conn.execute(
                        """SELECT authority_source,allowed_source_kinds,requires_authority,cardinality
                           FROM fact_key_registry WHERE fact_key=%s AND status='ACTIVE'
                           ORDER BY version DESC LIMIT 1""",
                        (conflict["fact_key"],),
                    ).fetchone()
                    item["authority_rule"] = dict(registry) if registry else None
                item["items"] = [
                    dict(row)
                    for row in conn.execute(
                        """SELECT i.memory_id,i.candidate_id,i.evidence_id,i.value_hash,i.object_json,
                                  i.content_redacted,i.source_kind,i.observed_at,i.created_at,
                                  c.source_refs,c.source_system
                           FROM memory_conflict_item i
                           LEFT JOIN memory_candidate c ON c.candidate_id=i.candidate_id
                           WHERE i.conflict_id=%s ORDER BY i.created_at""",
                        (task["conflict_id"],),
                    ).fetchall()
                ]
            result.append(item)
        return result

    def decide(
        self,
        conn: DbConnection,
        review_id: UUID,
        *,
        customer_id: str,
        reviewer_id: str,
        decision_version: int,
        approve: bool,
        reason_code: str,
        note: str | None,
        co_reviewer_id: str | None = None,
    ) -> dict[str, Any]:
        task = conn.execute(
            "SELECT * FROM memory_review_task WHERE id=%s AND customer_id=%s", (review_id, customer_id)
        ).fetchone()
        if not task:
            raise MmsError("review task not found", code="NOT_FOUND", status_code=404)
        if task["decision_version"] != decision_version:
            raise MmsError("review decision version is stale", code="CONCURRENT_MODIFICATION", status_code=409)
        if task["status"] in {"APPROVED", "REJECTED"}:
            return dict(task)
        if task["status"] not in {"PENDING", "IN_REVIEW"}:
            raise MmsError("review task is not actionable", code="REVIEW_CLOSED", status_code=409)
        if reviewer_id == task["created_by"]:
            raise MmsError("reviewer must differ from creator", code="FOUR_EYES_REQUIRED", status_code=403)
        # Doc 5.4/12.6: high-risk resolutions need four eyes, and erasure is the highest-risk
        # resolution of all — the DELETION task is created with risk_level=HIGH, so the same
        # rule covers it (docs 12.6 explicitly lists deletion as double-signed).
        if approve and task["risk_level"] == "HIGH":
            if not co_reviewer_id or co_reviewer_id == reviewer_id or co_reviewer_id == task["created_by"]:
                raise MmsError(
                    "high-risk review requires a co-reviewer distinct from reviewer and creator",
                    code="FOUR_EYES_REQUIRED",
                    status_code=403,
                )
            if task["task_type"] == "DELETION":
                requester = conn.execute(
                    "SELECT requested_by FROM memory_deletion_request WHERE id=%s AND customer_id=%s",
                    (task["deletion_request_id"], customer_id),
                ).fetchone()
                if requester is not None and co_reviewer_id == requester["requested_by"]:
                    raise MmsError(
                        "deletion co-reviewer must differ from the requester",
                        code="FOUR_EYES_REQUIRED",
                        status_code=403,
                    )
        candidate_id = task["candidate_id"]
        if approve and task["task_type"] == "CONFLICT":
            candidate_id = require_row(
                conn.execute(
                    "SELECT candidate_id FROM memory_conflict_item WHERE conflict_id=%s AND candidate_id IS NOT NULL "
                    "ORDER BY created_at DESC LIMIT 1",
                    (task["conflict_id"],),
                ).fetchone(),
                "conflict candidate",
            )["candidate_id"]
        candidate = self._load_candidate(conn, candidate_id) if approve and task["task_type"] != "DELETION" else None
        # Candidate discovery (embedding provider I/O) runs before the review row is locked and
        # before any fact lock is taken inside process_candidate (doc 4.12).
        prepared = self.memory_service.prepare_candidate(candidate) if candidate else None
        task = require_row(
            conn.execute(
                "SELECT * FROM memory_review_task WHERE id=%s AND customer_id=%s FOR UPDATE",
                (review_id, customer_id),
            ).fetchone(),
            "review task",
        )
        if task["decision_version"] != decision_version:
            raise MmsError("review decision version is stale", code="CONCURRENT_MODIFICATION", status_code=409)
        if task["status"] in {"APPROVED", "REJECTED"}:
            return dict(task)
        if task["status"] not in {"PENDING", "IN_REVIEW"}:
            raise MmsError("review task is not actionable", code="REVIEW_CLOSED", status_code=409)
        if task["task_type"] == "DELETION":
            from .deletion import DeletionService

            status = "APPROVED" if approve else "REJECTED"
            conn.execute(
                """UPDATE memory_review_task SET status=%s,reviewed_by=%s,co_reviewer_id=%s,resolution_reason_code=%s,
                     resolution_note_redacted=%s,resolved_at=now(),updated_at=now() WHERE id=%s""",
                (status, reviewer_id, co_reviewer_id, reason_code, note, review_id),
            )
            if approve:
                deletion_result = DeletionService(self.repository).execute(
                    conn, customer_id, task["deletion_request_id"], reviewer_id
                )
            else:
                conn.execute(
                    "UPDATE memory_deletion_request SET status='REJECTED',updated_at=now() WHERE id=%s",
                    (task["deletion_request_id"],),
                )
                # Doc 6.7.1: cancelling a barrier bumps its generation so carriers that
                # captured the pre-cancel generation stay rejected, and downstream caches
                # reload the transition through the outbox event.
                flipped = conn.execute(
                    """UPDATE customer_deletion_barrier SET status='CANCELLED',generation=generation+1,updated_at=now()
                       WHERE customer_id=%s RETURNING request_id,generation""",
                    (customer_id,),
                ).fetchone()
                if flipped is not None:
                    self.repository.insert_outbox(
                        conn,
                        flipped["request_id"],
                        "customer.deletion.barrier.changed.v1",
                        {
                            "customer_ref": self.repository.ensure_identity(conn, customer_id),
                            "generation": int(flipped["generation"]),
                            "status": "CANCELLED",
                        },
                        f"barrier:{customer_id}:{flipped['generation']}:cancelled",
                        int(flipped["generation"]),
                    )
                    self.repository.invalidate_barrier_cache(customer_id)
                    self.repository.notify_barrier_changed(conn, customer_id)
                deletion_result = {"request_id": task["deletion_request_id"], "status": "REJECTED"}
            return {**dict(task), "status": status, "deletion": deletion_result}

        outcome = None
        if approve:
            outcome = self.memory_service.process_candidate(conn, candidate, review_override=True, prepared=prepared)
            if outcome.outcome in {"NEEDS_VERIFICATION", "CONFLICT", "REJECTED", "IGNORE"}:
                raise MmsError(
                    "review approval did not produce a write decision", code="REVIEW_NOT_ACTIONABLE", status_code=409
                )
        status = "APPROVED" if approve else "REJECTED"
        conn.execute(
            """UPDATE memory_review_task SET status=%s,reviewed_by=%s,co_reviewer_id=%s,resolution_reason_code=%s,
                 resolution_note_redacted=%s,resolved_at=now(),updated_at=now() WHERE id=%s""",
            (status, reviewer_id, co_reviewer_id, reason_code, note, review_id),
        )
        if task["task_type"] == "CONFLICT":
            conn.execute(
                """UPDATE memory_conflict SET status=%s,winning_memory_id=%s,resolution_reason_code=%s,
                     resolved_at=now(),updated_at=now() WHERE id=%s""",
                (
                    "RESOLVED" if approve else "DISMISSED",
                    outcome.new_memory_id if outcome else None,
                    reason_code,
                    task["conflict_id"],
                ),
            )
            if not approve:
                conn.execute(
                    "UPDATE customer_memory SET lifecycle_status='ACTIVE',updated_at=now() "
                    "WHERE id=(SELECT current_memory_id FROM memory_conflict WHERE id=%s) AND lifecycle_status='CONFLICTED'",
                    (task["conflict_id"],),
                )
        if not approve and task["candidate_id"]:
            self.repository.set_candidate_result(conn, task["candidate_id"], "REJECTED", [reason_code])
        row = conn.execute("SELECT * FROM memory_review_task WHERE id=%s", (review_id,)).fetchone()
        return dict(require_row(row, "review task"))

    def _load_candidate(self, conn: DbConnection, candidate_id: UUID) -> CandidateInput:
        row = conn.execute("SELECT * FROM memory_candidate WHERE candidate_id=%s", (candidate_id,)).fetchone()
        if not row:
            raise MmsError("review candidate not found", code="NOT_FOUND", status_code=404)
        return CandidateInput(
            candidate_id=row["candidate_id"],
            customer_id=row["customer_id"],
            source_system=row["source_system"],
            source_event_id=row["source_event_id"],
            memory_type=row["memory_type"],
            fact_key=row["fact_key"],
            subject=row["subject"],
            predicate=row["predicate"],
            entity_type=row["entity_type"],
            entity_ref=row["entity_ref"],
            value_slot=row["value_slot"],
            object_json=row["object_json"],
            content=row["content_redacted"],
            content_language=row["content_language"],
            language=row["language"],
            script=row["script"],
            language_confidence=row["language_confidence"],
            source_kind=row["source_kind"],
            source_refs=row["source_refs"],
            confidence=row["confidence"],
            sensitivity=row["sensitivity"],
            explicit_correction=row["explicit_correction"],
            valid_from=row["valid_from"],
            valid_to=row["valid_to"],
            observed_at=row["observed_at"],
            expires_at=row["expires_at"],
            policy_version=row["policy_version"],
        )
