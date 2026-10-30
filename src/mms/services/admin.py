from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from ..config import get_settings
from ..models import NormalizedCandidate
from ..utils.db import DbConnection, require_row
from ..utils.embedding import EmbeddingProvider
from ..utils.errors import MmsError
from ..utils.hashing import customer_hash, sha256_canonical, utcnow
from ..utils.metrics import (
    ADMIN_AUDIT_WRITE_FAILURES,
    DECISION_REPLAY_MISMATCH,
    DECISION_RULE_UNCOVERED,
)
from ..utils.query_cache import embed_query_cached
from .repository import MemoryRepository
from .rules import DecisionNotCovered, RuleContext, apply_decision_matrix
from .service import SystemOfRecordAuthoritativeResolver


@dataclass(frozen=True)
class AdminContext:
    actor: str
    reason_code: str
    ticket: str
    # Doc 12.6: the admin plane distinguishes actors by role, and the role is what the audit
    # trail records. Until SSO/OIDC supplies it, the gateway asserts it and the token stays the
    # credential — an unknown role is rejected so audits cannot be tagged arbitrarily.
    role: str = "ADMIN"


def require_admin_role(ctx: AdminContext, allowed: list[str]) -> None:
    if ctx.role not in set(allowed):
        raise MmsError(
            f"role {ctx.role} is not allowed to perform this admin action",
            code="ADMIN_ROLE_REQUIRED",
            status_code=403,
        )


class AdminService:
    """Docs 9.7/9.1: admin operations are audited, policy activation is four-eyes, and
    decision replay re-evaluates the rule matrix from the pinned policy/registry versions."""

    def __init__(
        self,
        repository: MemoryRepository | None = None,
        provider: EmbeddingProvider | None = None,
        resolver: SystemOfRecordAuthoritativeResolver | None = None,
    ):
        from ..utils.embedding import provider_from_settings

        self.settings = get_settings()
        self.repository = repository or MemoryRepository()
        self.provider = provider or provider_from_settings(self.settings)
        self.authoritative_resolver = resolver or SystemOfRecordAuthoritativeResolver()

    def audit(
        self,
        conn: DbConnection,
        *,
        ctx: AdminContext,
        customer_hash_value: str,
        action: str,
        scope: dict[str, Any],
        trace_id: str,
        actor_role: str | None = None,
    ) -> None:
        try:
            conn.execute(
                """INSERT INTO admin_access_audit
                     (audit_id,actor_id,actor_role,customer_hash,action,reason_code,reason_text_redacted,scope,trace_id)
                   VALUES (%s,%s,%s,%s,%s,%s,NULL,%s,%s)""",
                (
                    uuid4(),
                    ctx.actor,
                    actor_role or ctx.role,
                    customer_hash_value,
                    action,
                    ctx.reason_code,
                    Jsonb(scope),
                    trace_id,
                ),
            )
        except Exception as exc:
            ADMIN_AUDIT_WRITE_FAILURES.inc()
            raise MmsError("admin access audit write failed", code="AUDIT_WRITE_FAILED", status_code=503) from exc

    # ---- policies -----------------------------------------------------------

    def list_policies(self, conn: DbConnection) -> list[dict[str, Any]]:
        rows = conn.execute(
            """SELECT name,version,memory_type,status,config,config_hash,effective_from,
                      created_by,approved_by,approved_at,created_at
               FROM memory_policy ORDER BY name, effective_from DESC, version DESC"""
        ).fetchall()
        return [dict(row) for row in rows]

    def create_policy(
        self,
        conn: DbConnection,
        *,
        name: str,
        version: str,
        config: dict[str, Any],
        memory_type: str | None,
        effective_from: datetime | None,
        actor: str,
    ) -> dict[str, Any]:
        if not config:
            raise MmsError("policy config is required for CREATE", code="VALIDATION_FAILED", status_code=422)
        policy_id = uuid4()
        try:
            conn.execute(
                """INSERT INTO memory_policy
                     (id,name,version,memory_type,status,config,config_hash,effective_from,created_by)
                   VALUES (%s,%s,%s,%s,'DRAFT',%s,%s,%s,%s)""",
                (
                    policy_id,
                    name,
                    version,
                    memory_type,
                    Jsonb(config),
                    sha256_canonical(config),
                    effective_from or utcnow(),
                    actor,
                ),
            )
        except UniqueViolation as exc:
            raise MmsError("policy version already exists", code="VALIDATION_FAILED", status_code=409) from exc
        return {"id": str(policy_id), "name": name, "version": version, "status": "DRAFT"}

    def activate_policy(
        self,
        conn: DbConnection,
        *,
        name: str,
        version: str,
        actor: str,
        action: str,
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM memory_policy WHERE name=%s AND version=%s FOR UPDATE", (name, version)
        ).fetchone()
        if not row:
            raise MmsError("policy version not found", code="NOT_FOUND", status_code=404)
        if row["created_by"] == actor:
            raise MmsError(
                "four-eyes: the approver must differ from the policy creator",
                code="VALIDATION_FAILED",
                status_code=403,
            )
        if row["status"] == "ACTIVE":
            return {"id": str(row["id"]), "name": name, "version": version, "status": "ACTIVE", "action": action}
        # ux_policy_single_active_scope is not deferrable: retire the current holder of the
        # scope before activating this version, otherwise the UPDATE raises UniqueViolation.
        conn.execute(
            """UPDATE memory_policy SET status='RETIRED'
               WHERE memory_type IS NOT DISTINCT FROM %s AND status='ACTIVE' AND id<>%s""",
            (row["memory_type"], row["id"]),
        )
        conn.execute(
            "UPDATE memory_policy SET status='ACTIVE', approved_by=%s, approved_at=now() WHERE id=%s",
            (actor, row["id"]),
        )
        return {"id": str(row["id"]), "name": name, "version": version, "status": "ACTIVE", "action": action}

    # ---- fact keys / embeddings --------------------------------------------

    def list_fact_keys(self, conn: DbConnection) -> list[dict[str, Any]]:
        rows = conn.execute("SELECT * FROM fact_key_registry ORDER BY fact_key, version DESC").fetchall()
        return [dict(row) for row in rows]

    def rebuild_embeddings(self, conn: DbConnection, *, customer_id: str | None, memory_id: UUID | None) -> int:
        if not customer_id and not memory_id:
            raise MmsError(
                "customer_id or memory_id is required to bound a rebuild",
                code="VALIDATION_FAILED",
                status_code=422,
            )
        return self.repository.enqueue_missing_embeddings(conn, customer_id=customer_id, memory_id=memory_id)

    # ---- lineage ------------------------------------------------------------

    def lineage(self, conn: DbConnection, memory_id: UUID, *, ctx: AdminContext, trace_id: str) -> dict[str, Any]:
        memory = conn.execute("SELECT * FROM customer_memory WHERE id=%s", (memory_id,)).fetchone()
        if not memory:
            raise MmsError("memory not found", code="NOT_FOUND", status_code=404)
        self.audit(
            conn,
            ctx=ctx,
            customer_hash_value=customer_hash(memory["customer_id"]),
            action="READ_MEMORY",
            scope={"memory_id": str(memory_id)},
            trace_id=trace_id,
        )
        evidence = conn.execute(
            """SELECT id,source_kind,source_system,source_event_id,content_language,observed_at,
                      evidence_excerpt_redacted,anonymized
               FROM memory_evidence WHERE memory_id=%s ORDER BY observed_at""",
            (memory_id,),
        ).fetchall()
        decisions = conn.execute(
            """SELECT decision_id,outcome,reason_codes,primary_rule_id,policy_version,engine_version,
                      requires_review,attempt_no,new_memory_id,target_memory_id,created_at
               FROM memory_decision
               WHERE new_memory_id=%s OR target_memory_id=%s OR %s = ANY(matched_memory_ids)
               ORDER BY created_at""",
            (memory_id, memory_id, memory_id),
        ).fetchall()
        versions = conn.execute(
            """SELECT id,version_no,lifecycle_status,valid_from,valid_to,supersedes_memory_id,
                      current_flag,created_at
               FROM customer_memory
               WHERE customer_id=%s AND fact_key=%s AND entity_type=%s AND entity_ref=%s AND value_slot=%s
               ORDER BY version_no""",
            (
                memory["customer_id"],
                memory["fact_key"],
                memory["entity_type"],
                memory["entity_ref"],
                memory["value_slot"],
            ),
        ).fetchall()
        embeddings = conn.execute(
            """SELECT view_kind,view_language,embedding_model,embedding_version,status,
                      content_hash,view_text_hash,generated_at,updated_at
               FROM memory_embedding WHERE memory_id=%s ORDER BY view_kind,view_language""",
            (memory_id,),
        ).fetchall()
        usage = conn.execute(
            """SELECT request_id,purpose,rank_map,used,used_memory_ids,degraded,created_at
               FROM memory_usage_event
               WHERE customer_id=%s AND %s = ANY(retrieved_memory_ids)
               ORDER BY created_at DESC LIMIT 50""",
            (memory["customer_id"], memory_id),
        ).fetchall()
        return {
            "memory": dict(memory),
            "evidence": [dict(row) for row in evidence],
            "decisions": [dict(row) for row in decisions],
            "versions": [dict(row) for row in versions],
            "embeddings": [dict(row) for row in embeddings],
            "usage": [dict(row) for row in usage],
        }

    # ---- decision replay ----------------------------------------------------

    def replay_decision(
        self, conn: DbConnection, decision_id: UUID, *, ctx: AdminContext, trace_id: str
    ) -> dict[str, Any]:
        recorded = conn.execute("SELECT * FROM memory_decision WHERE decision_id=%s", (decision_id,)).fetchone()
        if not recorded:
            raise MmsError("decision not found", code="NOT_FOUND", status_code=404)
        self.audit(
            conn,
            ctx=ctx,
            customer_hash_value=recorded["customer_hash"],
            action="REPLAY",
            scope={"decision_id": str(decision_id)},
            trace_id=trace_id,
        )
        candidate = conn.execute(
            "SELECT * FROM memory_candidate WHERE candidate_id=%s", (recorded["candidate_id"],)
        ).fetchone()
        if candidate is None or candidate["customer_id"] != recorded["customer_id"]:
            raise MmsError(
                "decision is not replayable: candidate was removed or scrubbed",
                code="VALIDATION_FAILED",
                status_code=409,
            )
        normalized = self._normalized_candidate(candidate)
        policy, registry = self._pinned_policy_registry(conn, normalized, recorded)
        exact = self.repository.find_exact(conn, normalized)
        excluded = {fact.memory_id for fact in exact}
        semantic: list[dict[str, Any]] = []
        query_vector = None
        # Doc 3.2: replay re-derives the discovery vector, which is an outbound embedding
        # call, so it needs the same purpose check. Without consent the replay is exact-only;
        # the ACTIVE model is self-hosted by default (ADR-002), so this is a no-op there.
        if (
            self.repository.cross_border_embedding_blocked(
                conn,
                normalized.customer_id,
                model=self.settings.embedding_model,
                version=self.settings.embedding_version,
                stage="replay",
            )
            is None
        ):
            try:
                query_vector = embed_query_cached(
                    self.provider,
                    normalized.semantic_text,
                    self.settings.embedding_model,
                    version=self.settings.embedding_version,
                    language="und",
                )
            except Exception:
                query_vector = None
        if query_vector is not None:
            semantic = self.repository.semantic_candidates(
                conn,
                normalized.customer_id,
                query_vector,
                float(policy["semantic_match_threshold"]),
                excluded,
                normalized.content_language,
            )
        current = exact[0] if exact else None
        source_events: set[str] = set()
        if current is not None:
            source_events = {
                row["source_event_id"]
                for row in conn.execute(
                    "SELECT source_event_id FROM memory_evidence WHERE memory_id=%s",
                    (current.memory_id,),
                ).fetchall()
                if row["source_event_id"]
            }
        ctx_rule = RuleContext(
            candidate=normalized,
            value_score=normalized.value_score,
            score_breakdown=recorded["score_breakdown"] or {},
            exact_matches=exact,
            has_new_evidence=current is not None and normalized.source_event_id not in source_events,
            new_evidence_languages=(
                sorted({normalized.content_language} - current.evidence_languages) if current else []
            ),
            semantic_candidates=semantic,
            authoritative_fact=self.authoritative_resolver.resolve(candidate=normalized, registry=registry),
            policy=policy,
            registry=registry,
        )
        try:
            replayed = apply_decision_matrix(ctx_rule)
        except DecisionNotCovered as exc:
            DECISION_RULE_UNCOVERED.inc()
            raise MmsError(
                f"decision replay failed: {exc.__class__.__name__}",
                code="VALIDATION_FAILED",
                status_code=409,
            ) from exc
        except Exception as exc:
            raise MmsError(
                f"decision replay failed: {exc.__class__.__name__}",
                code="VALIDATION_FAILED",
                status_code=409,
            ) from exc
        drift = []
        for field in ("outcome", "primary_rule_id", "reason_codes"):
            if getattr(replayed, field) != recorded[field]:
                drift.append(field)
        if drift:
            DECISION_REPLAY_MISMATCH.inc()
        return {
            "decision_id": str(decision_id),
            "recorded": {
                "outcome": recorded["outcome"],
                "reason_codes": recorded["reason_codes"],
                "primary_rule_id": recorded["primary_rule_id"],
                "policy_version": recorded["policy_version"],
                "fact_key_registry_version": recorded["fact_key_registry_version"],
                "engine_version": recorded["engine_version"],
            },
            "replayed": {
                "outcome": replayed.outcome,
                "reason_codes": replayed.reason_codes,
                "primary_rule_id": replayed.primary_rule_id,
                "engine_version": replayed.engine_version,
            },
            "drift": drift,
            "context_snapshot": recorded["context_snapshot"],
        }

    def _normalized_candidate(self, candidate: dict[str, Any]) -> NormalizedCandidate:
        required = {
            "content": candidate["content_redacted"],
            "content_language": candidate["content_language"],
            "content_hash": candidate["content_hash"],
            "semantic_text": candidate["semantic_text"],
            "semantic_hash": candidate["semantic_hash"],
            "normalization_version": candidate["normalization_version"],
            "fact_key_registry_version": candidate["fact_key_registry_version"],
            "cardinality": candidate["cardinality"],
            "value_score": candidate["value_score"],
        }
        if any(value in (None, "") for value in required.values()):
            raise MmsError(
                "decision is not replayable: candidate snapshot is incomplete",
                code="VALIDATION_FAILED",
                status_code=409,
            )
        try:
            return NormalizedCandidate.model_validate(
                {
                    "candidate_id": candidate["candidate_id"],
                    "customer_id": candidate["customer_id"],
                    "source_system": candidate["source_system"],
                    "source_event_id": candidate["source_event_id"],
                    "memory_type": candidate["memory_type"],
                    "fact_key": candidate["fact_key"],
                    "subject": candidate["subject"],
                    "predicate": candidate["predicate"],
                    "entity_type": candidate["entity_type"],
                    "entity_ref": candidate["entity_ref"],
                    "value_slot": candidate["value_slot"],
                    "object_json": candidate["object_json"],
                    "content": candidate["content_redacted"],
                    "content_language": candidate["content_language"],
                    "language": candidate["language"],
                    "script": candidate["script"],
                    "language_confidence": candidate["language_confidence"],
                    "source_kind": candidate["source_kind"],
                    "source_refs": candidate["source_refs"],
                    "confidence": candidate["confidence"],
                    "sensitivity": candidate["sensitivity"],
                    "explicit_correction": candidate["explicit_correction"],
                    "valid_from": candidate["valid_from"],
                    "valid_to": candidate["valid_to"],
                    "observed_at": candidate["observed_at"],
                    "expires_at": candidate["expires_at"],
                    "policy_version": candidate["policy_version"],
                    "object_hash": candidate["object_hash"],
                    "candidate_fingerprint": candidate["candidate_fingerprint"],
                    "content_hash": candidate["content_hash"],
                    "semantic_text": candidate["semantic_text"],
                    "semantic_hash": candidate["semantic_hash"],
                    "normalization_version": candidate["normalization_version"],
                    "fact_key_registry_version": candidate["fact_key_registry_version"],
                    "cardinality": candidate["cardinality"],
                    "value_score": candidate["value_score"],
                    "policy_config_hash": "",
                }
            )
        except Exception as exc:
            raise MmsError(
                "decision is not replayable: candidate snapshot is invalid",
                code="VALIDATION_FAILED",
                status_code=409,
            ) from exc

    def _pinned_policy_registry(
        self, conn: DbConnection, normalized: NormalizedCandidate, recorded: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        policy_version = str(recorded["policy_version"] or normalized.policy_version)
        if "@" not in policy_version:
            raise MmsError(
                "decision is not replayable: unversioned policy",
                code="VALIDATION_FAILED",
                status_code=409,
            )
        name, version = policy_version.rsplit("@", 1)
        policy = conn.execute(
            "SELECT config FROM memory_policy WHERE name=%s AND version=%s", (name, version)
        ).fetchone()
        registry_version = int(recorded["fact_key_registry_version"] or normalized.fact_key_registry_version)
        registry = conn.execute(
            "SELECT * FROM fact_key_registry WHERE fact_key=%s AND version=%s",
            (normalized.fact_key, registry_version),
        ).fetchone()
        if not policy or not registry:
            raise MmsError(
                "decision is not replayable: pinned policy or fact-key version is gone",
                code="VALIDATION_FAILED",
                status_code=409,
            )
        return require_row(policy, "replay policy")["config"], dict(require_row(registry, "replay registry"))
