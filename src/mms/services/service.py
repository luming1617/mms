from __future__ import annotations

import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar, Protocol
from uuid import UUID

from ..config import get_settings
from ..models import CandidateInput, Decision, NormalizedCandidate
from ..utils.db import DbConnection, get_db, require_row
from ..utils.embedding import EmbeddingProvider
from ..utils.errors import BarrierError, ConsentError, MmsError
from ..utils.hashing import normalize_language, sha256_canonical, utcnow
from ..utils.language import query_language_confidence
from ..utils.metrics import (
    CANDIDATES,
    CROSS_CUSTOMER_HITS,
    DECISIONS,
    LANGUAGE_RETRIEVAL,
    LEXICAL_SKIPPED,
    RERANK_LANGUAGE_MISMATCH,
    RERANK_USED,
    RETRIEVAL_CANDIDATES,
    RETRIEVAL_DEGRADED,
    RETRIEVAL_FILTERED,
    RETRIEVAL_POST_FILTER_LOSS,
    RETRIEVAL_TOKENS,
    RETRIEVAL_VIEW_HITS,
    RETRIEVAL_ZERO_RESULTS,
    VIEW_COUNT,
)
from ..utils.query_cache import embed_query_cached
from ..utils.reranker import HttpReranker, Reranker
from ..utils.tokens import estimate_tokens
from .normalizer import HardGateError, enforce_registry_sensitivity, normalize_candidate, redact_text
from .quota import QuotaService
from .repository import PRIORITY_CORRECTION, PRIORITY_DEFAULT, MemoryRepository
from .rules import DECISION_RULE_SET_VERSION, RULES_BY_ID, RuleContext, apply_decision_matrix
from .translation import TranslationService

logger = logging.getLogger(__name__)


class AuthoritativeFactResolver(Protocol):
    def resolve(self, *, candidate: NormalizedCandidate, registry: dict[str, Any]) -> dict[str, Any] | None: ...


class SystemOfRecordAuthoritativeResolver:
    """Doc 5.2 #1: field authority comes from the business system. Business systems reach
    MMS as SYSTEM_OF_RECORD events, so a value reported through that channel is the
    authoritative fact for keys whose registry requires authority. CRM/order lookups can
    replace this resolver behind the same protocol when a sync client exists."""

    def resolve(self, *, candidate: NormalizedCandidate, registry: dict[str, Any]) -> dict[str, Any] | None:
        if not registry.get("requires_authority"):
            return None
        if candidate.source_kind != "SYSTEM_OF_RECORD":
            return None
        return {
            "object_hash": candidate.object_hash,
            "source_kind": candidate.source_kind,
            "source_system": candidate.source_system,
        }


@dataclass(frozen=True)
class CandidatePreparation:
    """Candidate discovery computed before the decision transaction (doc 4.12)."""

    semantic_text: str
    query_vector: list[float] | None
    semantic_candidates: list[dict[str, Any]]


class MemoryService:
    def __init__(
        self,
        repository: MemoryRepository,
        embedding_provider: EmbeddingProvider,
        translation_service: TranslationService | None = None,
        reranker: Reranker | None = None,
        authoritative_resolver: AuthoritativeFactResolver | None = None,
        quota: QuotaService | None = None,
    ):
        self.settings = get_settings()
        self.repository = repository
        self.embedding_provider = embedding_provider
        self.translation_service = translation_service or TranslationService(repository)
        reranker_url = self.settings.reranker_url
        self.reranker = reranker or (
            HttpReranker(reranker_url, self.settings.reranker_timeout_ms)
            if self.settings.reranker_enabled and reranker_url
            else None
        )
        self.authoritative_resolver = authoritative_resolver or SystemOfRecordAuthoritativeResolver()
        # Doc 11.5: HIGH sensitivity is registry-enforced to require an authority system, so it
        # is the read-side marker for a fact the answer layer must not assert unverified.
        self.high_impact_sensitivities = frozenset({"HIGH"})
        self.quota = quota or QuotaService()

    # Doc 2.6: consent failure outcomes map to terminal reason codes, not HTTP/exception
    # semantics — the decision itself must be recorded and replayable.
    _CONSENT_REASONS: ClassVar[dict[str, str]] = {
        "CONSENT_REQUIRED": "CONSENT_DENIED",
        "CONSENT_WITHDRAWN": "CONSENT_WITHDRAWN",
        "CONSENT_UNAVAILABLE": "CONSENT_UNAVAILABLE",
    }

    # Doc 5.3/6.5.1: each hard gate keeps its own rule id, so the decision audit no longer
    # labels schema, injection, credential and source failures as the sensitivity rule R010.
    _HARD_GATE_RULE_BY_REASON: ClassVar[dict[str, str]] = {
        "MISSING_EVIDENCE": "R007",
        "POLICY_FORBIDDEN": "R002",
        "SENSITIVE_DATA": "R003",
        "SCHEMA_VALIDATION_FAILED": "R004",
        "SOURCE_UNTRUSTED": "R006",
        "LANGUAGE_UNSUPPORTED_LEXICAL": "R030",
        "DELETE_REQUESTED": "R001",
    }

    def prepare_candidate(self, candidate: CandidateInput) -> CandidatePreparation:
        """Run candidate discovery outside the decision transaction (doc 4.12).

        The embedding provider is network I/O: calling it after the fact-instance and
        deletion-barrier advisory locks are taken would pin every write for this customer
        behind an external service. Callers therefore prepare first and pass the result
        into ``process_candidate``.

        Discovery is also the first place customer-derived text could leave MMS, so the
        receipt-side gates of doc 3.3 are evaluated before any provider call. They are
        plain reads here and they return an empty preparation instead of raising: the
        authoritative ``FOR UPDATE`` checks still run inside ``process_candidate``, which is
        what records the terminal REJECTED decision, and raising would turn a privacy
        rejection into a retried job (doc 3.3 forbids DLQ for consent denials).
        """
        with get_db().connection() as conn:
            policy, policy_version, policy_hash = self.repository.load_policy(conn, candidate.memory_type)
            registry = self.repository.load_registry(conn, candidate.fact_key)
            candidate = enforce_registry_sensitivity(candidate, registry)
            if self._receipt_gate_blocked(conn, candidate):
                return CandidatePreparation("", None, [])
            try:
                normalized, _ = normalize_candidate(candidate, registry, policy, policy_version, policy_hash)
            except HardGateError:
                return CandidatePreparation("", None, [])
            if (
                self.repository.cross_border_embedding_blocked(
                    conn,
                    normalized.customer_id,
                    model=self.settings.embedding_model,
                    version=self.settings.embedding_version,
                    stage="discovery",
                )
                is not None
            ):
                # No vector may be requested for this customer, so discovery keeps exact
                # matching only - the same degradation the Embedding Worker applies when it
                # cancels the job. The semantic text is returned so ``process_candidate``
                # reuses this preparation instead of re-embedding under the locks.
                return CandidatePreparation(normalized.semantic_text, None, [])
            query_vector = self._embed_query(normalized.semantic_text)
            semantic: list[dict[str, Any]] = []
            if query_vector is not None:
                semantic = self.repository.semantic_candidates(
                    conn,
                    normalized.customer_id,
                    query_vector,
                    float(policy["semantic_match_threshold"]),
                    set(),
                    normalized.content_language,
                )
        return CandidatePreparation(normalized.semantic_text, query_vector, semantic)

    def _receipt_gate_blocked(self, conn: DbConnection, candidate: CandidateInput) -> bool:
        """Barrier first, then the write purposes, in the order doc 3.3 specifies.

        ``SENSITIVE_MEMORY`` is decided after ``enforce_registry_sensitivity`` so the
        registry's floor - not the submitter's claim - decides whether it applies. Fail-closed:
        an unreadable or missing snapshot blocks the provider call.
        """
        barrier = self.repository.barrier_snapshot(conn, candidate.customer_id)
        if barrier is not None and barrier["status"] in {"ACTIVE", "COOLING_DOWN", "COMPLETED"}:
            return True
        purposes = ["MEMORY_WRITE"]
        if candidate.sensitivity in {"MEDIUM", "HIGH"}:
            purposes.append("SENSITIVE_MEMORY")
        for purpose in purposes:
            try:
                self.repository.check_consent(conn, candidate.customer_id, purpose)
            except ConsentError:
                return True
        return False

    def _embed_query(self, semantic_text: str) -> list[float] | None:
        try:
            return embed_query_cached(
                self.embedding_provider,
                semantic_text,
                self.settings.embedding_model,
                version=self.settings.embedding_version,
                language="und",
            )
        except Exception as exc:
            # Doc 4.4: vectors are a recall signal only. Degrade to exact-only discovery
            # instead of failing the write when the provider is unavailable.
            logger.warning(
                "candidate_embedding_unavailable",
                extra={"error": exc.__class__.__name__, "trace_id": getattr(exc, "trace_id", None)},
            )
            return None

    def process_candidate(
        self,
        conn: DbConnection,
        candidate: CandidateInput,
        *,
        review_override: bool = False,
        prepared: CandidatePreparation | None = None,
        carrier_created_at: datetime | None = None,
        carrier_barrier_generation: int | None = None,
    ) -> Decision:
        self.repository.lock_barrier(conn, candidate.customer_id)
        barrier = self.repository.barrier_snapshot(conn, candidate.customer_id)
        # Doc 8.3.1 rule 2: a queued carrier keeps the generation captured at receipt;
        # re-reading it here would make the comparison tautological.
        barrier_generation = (
            int(carrier_barrier_generation)
            if carrier_barrier_generation is not None
            else (0 if barrier is None else int(barrier["generation"]))
        )
        if carrier_created_at is None:
            # Doc 6.7.1 ③: carrier_created_at is the carrier row's created_at. observed_at is
            # submitter-controlled, so a future timestamp would walk through the cutover guard.
            carrier_row = conn.execute(
                "SELECT created_at FROM memory_candidate WHERE candidate_id=%s", (candidate.candidate_id,)
            ).fetchone()
            carrier_created_at = (
                carrier_row["created_at"]
                if carrier_row is not None
                else require_row(conn.execute("SELECT now() AS ts").fetchone(), "carrier clock")["ts"]
            )
        if carrier_created_at.tzinfo is None:
            carrier_created_at = carrier_created_at.replace(tzinfo=UTC)
        self.repository.verify_write_barrier(conn, candidate.customer_id, barrier_generation, carrier_created_at)
        # Doc 2.6 (P0-1): MEMORY_WRITE consent is a one-vote gate at candidate receipt.
        # A denied/withdrawn/unavailable snapshot is a permanent REJECTED decision
        # (reason CONSENT_DENIED/CONSENT_WITHDRAWN/CONSENT_UNAVAILABLE) — recorded, never
        # retried into the dead letter queue.
        try:
            self.repository.check_consent(conn, candidate.customer_id, "MEMORY_WRITE", write=True)
        except ConsentError as exc:
            return self._reject_for_consent(conn, candidate, exc, "MEMORY_WRITE", barrier_generation, review_override)
        policy, policy_version, policy_hash = self.repository.load_policy(conn, candidate.memory_type)
        registry = self.repository.load_registry(conn, candidate.fact_key)
        candidate = enforce_registry_sensitivity(candidate, registry)
        if candidate.sensitivity in {"MEDIUM", "HIGH"}:
            # Same terminal treatment for the SENSITIVE_MEMORY purpose (doc 2.6).
            try:
                self.repository.check_consent(conn, candidate.customer_id, "SENSITIVE_MEMORY", write=True)
            except ConsentError as exc:
                return self._reject_for_consent(
                    conn, candidate, exc, "SENSITIVE_MEMORY", barrier_generation, review_override
                )
        self.repository.lock_fact(
            conn,
            candidate.customer_id,
            candidate.fact_key,
            candidate.entity_type,
            candidate.entity_ref,
            candidate.value_slot,
        )

        try:
            normalized, breakdown = normalize_candidate(candidate, registry, policy, policy_version, policy_hash)
        except HardGateError as gate:
            normalized, _ = normalize_candidate(
                candidate, registry, policy, policy_version, policy_hash, for_rejection=True
            )
            # Hard-gated candidates must not persist raw text (docs 6.1/6.2): every
            # hard-gate outcome stores the credential-masked excerpt, and SLA expiry
            # scrubs it afterwards.
            inserted = self.repository.insert_candidate(
                conn,
                normalized,
                barrier_generation,
                content_override=redact_text(candidate.content),
            )
            if not inserted and not review_override:
                existing = self._existing_decision(conn, normalized)
                if existing:
                    return existing
            hard_gate_rule_id = self._HARD_GATE_RULE_BY_REASON.get(gate.reason_codes[0], "R010")
            decision = Decision(
                candidate_id=normalized.candidate_id,
                outcome=gate.outcome,  # type: ignore[arg-type]  # HardGateError.outcome: str; hard gates only raise Outcome literals
                reason_codes=gate.reason_codes,
                rule_trace=[
                    {
                        "rule_id": hard_gate_rule_id,
                        "condition_hash": sha256_canonical(gate.reason_codes),
                        "outcome": gate.outcome,
                    }
                ],
                primary_rule_id=hard_gate_rule_id,
                policy_version=policy_version,
                fact_key_registry_version=registry["version"],
                engine_version=DECISION_RULE_SET_VERSION,
                semantic_hash=normalized.semantic_hash,
                normalization_version=normalized.normalization_version,
                requires_review=gate.outcome == "NEEDS_VERIFICATION",
            )
            if gate.outcome == "NEEDS_VERIFICATION":
                from .review import ReviewService

                ReviewService(self.repository).ensure_candidate_task(
                    conn,
                    normalized,
                    decision,
                    review_sla_days=int((policy or {}).get("review_sla_days", 30)),
                )
            self._persist_non_write_decision(conn, normalized, decision)
            if gate.outcome == "DELETE_REQUESTED":
                # R001 (doc rule table): a user delete request must enter the reviewed
                # deletion workflow instead of lingering as an unactionable PENDING_REVIEW
                # candidate. Phase 2 scope is customer-wide, so this raises the barrier for
                # the whole customer; an existing request/barrier already covers it.
                from .deletion import DeletionService

                with suppress(BarrierError):
                    DeletionService(self.repository).request(
                        conn,
                        candidate.customer_id,
                        requested_by="customer",
                        idempotency_key=f"delete-request:{candidate.customer_id}:{candidate.source_event_id}",
                        request_source="DELETE_REQUESTED",
                    )
            self._check_write_consent(conn, normalized)
            return decision

        inserted = self.repository.insert_candidate(conn, normalized, barrier_generation)
        if not inserted and not review_override:
            existing = self._existing_decision(conn, normalized)
            if existing:
                return existing

        exact = self.repository.find_exact(conn, normalized)
        excluded = {fact.memory_id for fact in exact}
        semantic: list[dict[str, Any]] = []
        if prepared is not None:
            if prepared.semantic_text == normalized.semantic_text and prepared.query_vector is not None:
                semantic = [item for item in prepared.semantic_candidates if item["memory_id"] not in excluded]
        else:
            # Direct callers (tests, tooling) skip preparation; production entry points always
            # prepare before the transaction so no provider call runs under the locks (4.12).
            # Consent is already enforced above in this transaction; the cross-border purpose
            # is checked here too so this path cannot ship canonical text either (doc 3.2).
            blocked = self.repository.cross_border_embedding_blocked(
                conn,
                normalized.customer_id,
                model=self.settings.embedding_model,
                version=self.settings.embedding_version,
                stage="discovery",
            )
            qvec = None if blocked else self._embed_query(normalized.semantic_text)
            if qvec is not None:
                semantic = self.repository.semantic_candidates(
                    conn,
                    normalized.customer_id,
                    qvec,
                    float(policy["semantic_match_threshold"]),
                    excluded,
                    normalized.content_language,
                )
        current = exact[0] if exact else None
        has_new_evidence = current is not None and normalized.source_event_id not in self._memory_source_event_ids(
            conn, current.memory_id
        )
        new_languages = sorted({normalized.content_language} - current.evidence_languages) if current else []
        ctx = RuleContext(
            candidate=normalized,
            value_score=normalized.value_score,
            score_breakdown=breakdown,
            exact_matches=exact,
            has_new_evidence=has_new_evidence,
            new_evidence_languages=new_languages,
            semantic_candidates=semantic,
            authoritative_fact=self.authoritative_resolver.resolve(candidate=normalized, registry=registry),
            policy=policy,
            registry=registry,
            review_override=review_override,
        )
        decision = apply_decision_matrix(ctx)
        self._persist_decision(conn, normalized, decision, barrier_generation, policy)
        self._check_write_consent(conn, normalized)
        DECISIONS.labels(
            outcome=decision.outcome, rule_id=decision.primary_rule_id, reason_code=decision.reason_codes[0]
        ).inc()
        return decision

    def _reject_for_consent(
        self,
        conn: DbConnection,
        candidate: CandidateInput,
        exc: ConsentError,
        purpose: str,
        barrier_generation: int,
        review_override: bool,
    ) -> Decision:
        """Record a terminal REJECTED decision for a consent failure (doc 2.6).

        Normalization runs in ``for_rejection`` mode so the audit trail (candidate row,
        hashes, policy/registry versions, context snapshot) is always recordable even
        when the candidate would also fail structural gates. The consent row is locked
        FOR UPDATE by ``check_consent(write=True)``, so the status cannot change between
        the check and this transaction's commit.
        """
        policy, policy_version, policy_hash = self.repository.load_policy(conn, candidate.memory_type)
        registry = self.repository.load_registry(conn, candidate.fact_key)
        candidate = enforce_registry_sensitivity(candidate, registry)
        normalized, breakdown = normalize_candidate(
            candidate, registry, policy, policy_version, policy_hash, for_rejection=True
        )
        # Doc 6.2: a consent-blocked candidate is a privacy hard block — persist the
        # masked excerpt, never the raw body.
        inserted = self.repository.insert_candidate(
            conn, normalized, barrier_generation, content_override=redact_text(candidate.content)
        )
        if not inserted and not review_override:
            existing = self._existing_decision(conn, normalized)
            if existing:
                return existing
        reason_code = self._CONSENT_REASONS.get(exc.code, "CONSENT_UNAVAILABLE")
        spec = RULES_BY_ID["R005"]
        decision = Decision(
            candidate_id=normalized.candidate_id,
            outcome="REJECTED",
            reason_codes=[reason_code],
            rule_trace=[
                {
                    "rule_id": spec.rule_id,
                    "priority": spec.priority,
                    "condition_hash": sha256_canonical({"purpose": purpose, "consent_code": exc.code}),
                    "outcome": spec.outcome,
                }
            ],
            primary_rule_id=spec.rule_id,
            policy_version=policy_version,
            fact_key_registry_version=registry["version"],
            engine_version=DECISION_RULE_SET_VERSION,
            semantic_hash=normalized.semantic_hash,
            normalization_version=normalized.normalization_version,
            score_breakdown=breakdown,
            context_snapshot={
                "candidate": {
                    "object_hash": normalized.object_hash,
                    "semantic_hash": normalized.semantic_hash,
                    "normalization_version": normalized.normalization_version,
                },
                "policy": {"version": policy_version, "config_hash": policy_hash},
                "registry_version": registry["version"],
                "consent": {"purpose": purpose, "code": exc.code},
            },
        )
        self._persist_non_write_decision(conn, normalized, decision)
        DECISIONS.labels(outcome="REJECTED", rule_id=spec.rule_id, reason_code=reason_code).inc()
        return decision

    def _check_write_consent(self, conn: DbConnection, candidate: CandidateInput) -> None:
        self.repository.check_consent(conn, candidate.customer_id, "MEMORY_WRITE", write=True)
        if candidate.sensitivity in {"MEDIUM", "HIGH"}:
            self.repository.check_consent(conn, candidate.customer_id, "SENSITIVE_MEMORY", write=True)

    def _memory_source_event_ids(self, conn: DbConnection, memory_id: UUID) -> set[str]:
        rows = conn.execute("SELECT source_event_id FROM memory_evidence WHERE memory_id=%s", (memory_id,)).fetchall()
        return {row["source_event_id"] for row in rows if row["source_event_id"]}

    def _existing_decision(self, conn: DbConnection, candidate: NormalizedCandidate) -> Decision | None:
        row = conn.execute(
            """SELECT d.* FROM memory_candidate c
               JOIN memory_decision d ON d.candidate_id=c.candidate_id
               WHERE c.customer_id=%s AND c.source_system=%s AND c.source_event_id=%s AND c.candidate_fingerprint=%s
               ORDER BY d.created_at DESC LIMIT 1""",
            (
                candidate.customer_id,
                candidate.source_system,
                candidate.source_event_id,
                candidate.candidate_fingerprint,
            ),
        ).fetchone()
        if not row:
            return None
        return Decision(
            decision_id=row["decision_id"],
            candidate_id=row["candidate_id"],
            outcome=row["outcome"],
            reason_codes=row["reason_codes"],
            rule_trace=row["rule_trace"],
            primary_rule_id=row["primary_rule_id"],
            target_memory_id=row["target_memory_id"],
            new_memory_id=row["new_memory_id"],
            matched_memory_ids=row["matched_memory_ids"],
            policy_version=row["policy_version"],
            fact_key_registry_version=row["fact_key_registry_version"],
            engine_version=row["engine_version"],
            semantic_hash=row["semantic_hash"],
            normalization_version=row["normalization_version"],
            requires_review=row["requires_review"],
        )

    def _persist_non_write_decision(
        self, conn: DbConnection, candidate: NormalizedCandidate, decision: Decision
    ) -> None:
        self.repository.insert_decision(conn, decision, candidate.customer_id, candidate.candidate_fingerprint)
        status = {"REJECTED": "REJECTED", "IGNORE": "IGNORED", "DELETE_REQUESTED": "PENDING_REVIEW"}.get(
            decision.outcome, "PENDING_REVIEW"
        )
        self.repository.set_candidate_result(conn, candidate.candidate_id, status, decision.reason_codes)
        CANDIDATES.labels(source=candidate.source_system, memory_type=candidate.memory_type, status=status).inc()

    def _persist_decision(
        self,
        conn: DbConnection,
        candidate: NormalizedCandidate,
        decision: Decision,
        barrier_generation: int,
        policy: dict[str, Any] | None = None,
    ) -> None:
        review_sla_days = int((policy or {}).get("review_sla_days", 30))
        if decision.outcome in {"IGNORE", "REJECTED", "NEEDS_VERIFICATION", "CONFLICT", "DELETE_REQUESTED"}:
            if decision.outcome == "CONFLICT":
                from .review import ReviewService

                ReviewService(self.repository).create_conflict(
                    conn, candidate, decision, review_sla_days=review_sla_days
                )
            elif decision.outcome == "NEEDS_VERIFICATION":
                from .review import ReviewService

                ReviewService(self.repository).ensure_candidate_task(
                    conn, candidate, decision, review_sla_days=review_sla_days
                )
            self._persist_non_write_decision(conn, candidate, decision)
            return

        # Doc 12.6: admitting a fact consumes the storage and embedding budgets, so both are
        # gated at the single point where a memory row is about to be written. A rejection rolls
        # the write back instead of leaving a half-admitted fact.
        self.quota.check_storage(conn, candidate.customer_id)
        self.quota.check(conn, candidate.customer_id, "EMBEDDING")
        self.quota.record(conn, candidate.customer_id, "EMBEDDING", 1)

        target_id = decision.target_memory_id
        if decision.outcome in {"ADD", "ADD_TEMPORAL_VERSION"}:
            # version_no must continue past prior SUPERSEDED/EXPIRED versions:
            # exact_matches is status-filtered, so a rewritten fact key can have
            # historical rows while ADD fires - a hardcoded 1 would collide with
            # uq_memory_version (found by evals/lifecycle expiry-rewrite-recovery).
            row = conn.execute(
                "SELECT COALESCE(MAX(version_no), 0) + 1 AS version_no FROM customer_memory "
                "WHERE customer_id=%s AND fact_key=%s AND entity_type=%s AND entity_ref=%s AND value_slot=%s",
                (
                    candidate.customer_id,
                    candidate.fact_key,
                    candidate.entity_type,
                    candidate.entity_ref,
                    candidate.value_slot,
                ),
            ).fetchone()
            version_no = int(require_row(row, "memory version")["version_no"])
            memory_id = self.repository.insert_memory(conn, candidate, version_no=version_no)
            decision.new_memory_id = memory_id
            evidence_id = self.repository.insert_evidence(conn, candidate, memory_id)
            original_hash = self.repository.upsert_view(
                conn,
                memory_id,
                candidate.customer_id,
                candidate.content_hash,
                "ORIGINAL",
                candidate.content_language,
                candidate.content,
                evidence_id,
            )
            canonical_hash = self.repository.upsert_view(
                conn,
                memory_id,
                candidate.customer_id,
                candidate.content_hash,
                "CANONICAL",
                "und",
                candidate.semantic_text,
                None,
            )
            self.repository.enqueue_embedding_job(
                conn,
                memory_id,
                candidate.customer_id,
                candidate.content_hash,
                "ORIGINAL",
                candidate.content_language,
                original_hash,
                barrier_generation,
                priority=PRIORITY_CORRECTION if candidate.explicit_correction else PRIORITY_DEFAULT,
            )
            self.repository.enqueue_embedding_job(
                conn,
                memory_id,
                candidate.customer_id,
                candidate.content_hash,
                "CANONICAL",
                "und",
                canonical_hash,
                barrier_generation,
                priority=PRIORITY_CORRECTION if candidate.explicit_correction else PRIORITY_DEFAULT,
            )
            self.translation_service.enqueue_for_memory(conn, memory_id, barrier_generation)
            VIEW_COUNT.observe(2)
            # Doc 7.3.1/7.4.2: only ADD and SUPERSEDE move the SINGLE_VALUE pointer. A temporal
            # version is a past/future window, so replacing the pointer with it would hide the
            # actually-current value from exact matching (and later supersedes would key off it).
            if decision.outcome == "ADD" and candidate.cardinality == "SINGLE_VALUE":
                self.repository.update_current(conn, candidate, memory_id, version_no)
            self.repository.insert_outbox(
                conn,
                memory_id,
                "memory.created.v1",
                {"memory_id": str(memory_id), "source_system": candidate.source_system},
                f"memory-created:{memory_id}",
                barrier_generation,
                customer_id=candidate.customer_id,
            )
            self.repository.set_candidate_result(conn, candidate.candidate_id, "ACCEPTED", decision.reason_codes)
        elif decision.outcome == "SUPERSEDE":
            if target_id is None:
                raise ConsentError("CONSENT_UNAVAILABLE", "SUPERSEDE without target")
            old = require_row(
                conn.execute("SELECT version_no FROM customer_memory WHERE id=%s FOR UPDATE", (target_id,)).fetchone(),
                "supersede target",
            )
            expected_version = decision.expected_versions.get(str(target_id))
            if expected_version is not None and int(old["version_no"]) != int(expected_version):
                raise MmsError("current memory version changed", code="CONCURRENT_MODIFICATION", status_code=409)
            self.repository.supersede_old(conn, target_id, candidate.customer_id, candidate)
            memory_id = self.repository.insert_memory(
                conn, candidate, supersedes=target_id, version_no=int(old["version_no"]) + 1
            )
            decision.new_memory_id = memory_id
            evidence_id = self.repository.insert_evidence(conn, candidate, memory_id)
            original_hash = self.repository.upsert_view(
                conn,
                memory_id,
                candidate.customer_id,
                candidate.content_hash,
                "ORIGINAL",
                candidate.content_language,
                candidate.content,
                evidence_id,
            )
            canonical_hash = self.repository.upsert_view(
                conn,
                memory_id,
                candidate.customer_id,
                candidate.content_hash,
                "CANONICAL",
                "und",
                candidate.semantic_text,
                None,
            )
            self.repository.enqueue_embedding_job(
                conn,
                memory_id,
                candidate.customer_id,
                candidate.content_hash,
                "ORIGINAL",
                candidate.content_language,
                original_hash,
                barrier_generation,
                priority=PRIORITY_CORRECTION if candidate.explicit_correction else PRIORITY_DEFAULT,
            )
            self.repository.enqueue_embedding_job(
                conn,
                memory_id,
                candidate.customer_id,
                candidate.content_hash,
                "CANONICAL",
                "und",
                canonical_hash,
                barrier_generation,
                priority=PRIORITY_CORRECTION if candidate.explicit_correction else PRIORITY_DEFAULT,
            )
            self.translation_service.enqueue_for_memory(conn, memory_id, barrier_generation)
            VIEW_COUNT.observe(2)
            if candidate.cardinality == "SINGLE_VALUE":
                self.repository.update_current(conn, candidate, memory_id, int(old["version_no"]) + 1)
            self.repository.insert_outbox(
                conn,
                memory_id,
                "memory.superseded.v1",
                {
                    "memory_id": str(memory_id),
                    "old_memory_id": str(target_id),
                    "source_system": candidate.source_system,
                },
                f"memory-superseded:{memory_id}",
                barrier_generation,
                customer_id=candidate.customer_id,
                aggregate_version=int(old["version_no"]) + 1,
            )
            self.repository.set_candidate_result(conn, candidate.candidate_id, "ACCEPTED", decision.reason_codes)
        elif decision.outcome == "REINFORCE":
            if target_id is None:
                raise ConsentError("CONSENT_UNAVAILABLE", "REINFORCE without target")
            evidence_id = self.repository.insert_evidence(conn, candidate, target_id)
            for action in decision.view_actions:
                if action["view_kind"] == "ORIGINAL":
                    # Doc 8.4 1a: a new-language ORIGINAL must equal the linked evidence
                    # excerpt (stored redacted); the 7.13.4 trigger enforces the equality.
                    view_hash = self.repository.upsert_view(
                        conn,
                        target_id,
                        candidate.customer_id,
                        self._memory_content_hash(conn, target_id),
                        "ORIGINAL",
                        action["view_language"],
                        redact_text(candidate.content),
                        evidence_id,
                    )
                    self.repository.enqueue_embedding_job(
                        conn,
                        target_id,
                        candidate.customer_id,
                        self._memory_content_hash(conn, target_id),
                        "ORIGINAL",
                        action["view_language"],
                        view_hash,
                        barrier_generation,
                        priority=PRIORITY_CORRECTION if candidate.explicit_correction else PRIORITY_DEFAULT,
                    )
            self.repository.insert_outbox(
                conn,
                target_id,
                "memory.reinforced.v1",
                {"memory_id": str(target_id), "source_system": candidate.source_system},
                f"memory-reinforced:{target_id}:{candidate.candidate_id}",
                barrier_generation,
                customer_id=candidate.customer_id,
                aggregate_version=int(decision.expected_versions.get(str(target_id), 1)),
            )
            self.repository.set_candidate_result(conn, candidate.candidate_id, "ACCEPTED", decision.reason_codes)
        elif decision.outcome == "NOOP":
            self.repository.set_candidate_result(conn, candidate.candidate_id, "PROCESSED", decision.reason_codes)

        self.repository.insert_decision(conn, decision, candidate.customer_id, candidate.candidate_fingerprint)
        CANDIDATES.labels(
            source=candidate.source_system,
            memory_type=candidate.memory_type,
            status="ACCEPTED" if decision.new_memory_id else "PROCESSED",
        ).inc()

    def _memory_content_hash(self, conn: DbConnection, memory_id: UUID) -> str:
        row = conn.execute("SELECT content_hash FROM customer_memory WHERE id=%s", (memory_id,)).fetchone()
        return str(require_row(row, "memory content hash")["content_hash"])

    def retrieve(
        self,
        conn: DbConnection,
        *,
        customer_id: str,
        query: str,
        language: str,
        purpose: str,
        max_items: int,
        fact_key: str | None = None,
        entity_ref: str | None = None,
        as_of=None,
        include_types: list[str] | None = None,
        max_tokens: int = 1200,
        primary_conn: DbConnection | None = None,
        replica_degraded: bool = False,
        replica_in_use: bool = False,
        precomputed_vector: list[float] | None = None,
        status_override: list[str] | None = None,
        include_channels: bool = True,
    ) -> dict[str, Any]:
        primary = primary_conn or conn
        self.repository.check_read_barrier(primary, customer_id)
        try:
            self.repository.check_consent(primary, customer_id, "MEMORY_READ")
        except ConsentError as exc:
            return {
                "memories": [],
                "conflicted_memories": [],
                "requires_confirmation": False,
                "consent_denied": exc.code in {"CONSENT_REQUIRED", "CONSENT_WITHDRAWN"},
                "consent_unavailable": exc.code == "CONSENT_UNAVAILABLE",
                "degraded": False,
                "degraded_reason": None,
                "semantic_index_pending": False,
                "lexical_used": False,
                "lexical_mode": None,
                "lexical_skip_reason": "language_not_configured",
                "rerank_used": False,
                "filtered_reasons": {},
            }
        sensitive = primary.execute(
            "SELECT status,expires_at FROM customer_memory_consent WHERE customer_id=%s AND purpose='SENSITIVE_MEMORY'",
            (customer_id,),
        ).fetchone()
        sensitive_granted = bool(
            sensitive
            and sensitive["status"] == "GRANTED"
            and (sensitive["expires_at"] is None or sensitive["expires_at"] > utcnow())
        )
        exact_only = bool(fact_key) or purpose == "AUDIT"
        if purpose == "CUSTOMER_SELF_SERVICE":
            policy = primary.execute(
                "SELECT config FROM memory_policy WHERE status='ACTIVE' AND memory_type IS NULL LIMIT 1"
            ).fetchone()
            visible = set(policy["config"].get("customer_visible_types", [])) if policy else set()
            include_types = sorted(visible & set(include_types)) if include_types else sorted(visible)
        RETRIEVAL_TOKENS.labels(direction="input").inc(len(query))
        language = normalize_language(language, "") or language
        low_confidence = not exact_only and query_language_confidence(query, language) < 0.5
        lexical_mode, lexical_config, lexical_skip_reason = (None, None, None)
        if not exact_only:
            if low_confidence:
                # Doc 8.15 step 1: a low-confidence language detection disables lexical and
                # TRANSLATED views; semantic recall stays on ORIGINAL+CANONICAL.
                lexical_skip_reason = "language_not_configured"
            else:
                lexical_mode, lexical_config, lexical_skip_reason = self._lexical_options(conn, query, language)
            if lexical_skip_reason:
                LEXICAL_SKIPPED.labels(reason=lexical_skip_reason).inc()
        degraded_reasons = ["replica_unavailable"] if replica_degraded else []
        qvec = precomputed_vector if not exact_only else None
        semantic_closed_by_consent = False
        if not exact_only and qvec is None:
            qvec, semantic_closed_by_consent = self._retrieval_query_vector(
                primary, customer_id, query, language, degraded_reasons
            )
        try:
            rows = self.repository.retrieve(
                conn,
                customer_id,
                query,
                qvec,
                language,
                purpose,
                max_items,
                fact_key=fact_key,
                entity_ref=entity_ref,
                as_of=as_of,
                include_types=include_types,
                include_sensitive=sensitive_granted,
                lexical_mode=lexical_mode,
                lexical_config=lexical_config,
                trgm_threshold=self.settings.lexical_trgm_threshold,
                include_translated=not low_confidence,
                # Doc 11.2 rule 1: both channels must use the same pool. When the request is
                # served by a replica the lexical thread would otherwise hit the primary, so
                # the two channels could disagree on freshness; run them on one connection.
                parallel=self.settings.retrieval_parallel_lexical and not replica_in_use,
                status_override=status_override,
                include_channels=include_channels,
            )
        except Exception:
            if not lexical_mode:
                raise
            degraded_reasons.append("lexical_unavailable")
            rows = self.repository.retrieve(
                conn,
                customer_id,
                query,
                qvec,
                language,
                purpose,
                max_items,
                fact_key=fact_key,
                entity_ref=entity_ref,
                as_of=as_of,
                include_types=include_types,
                include_sensitive=sensitive_granted,
                include_translated=not low_confidence,
                parallel=False,
                status_override=status_override,
                include_channels=include_channels,
            )
        cross_customer = [row for row in rows if row.get("customer_id") != customer_id]
        if cross_customer:
            CROSS_CUSTOMER_HITS.labels(purpose=purpose).inc(len(cross_customer))
            rows = [row for row in rows if row.get("customer_id") == customer_id]
        RETRIEVAL_CANDIDATES.observe(len(rows))
        rows, duplicate_count = self._rank_rows(rows, exact_only)
        if duplicate_count:
            RETRIEVAL_POST_FILTER_LOSS.labels(reason="duplicate_view").inc(duplicate_count)
        for row in rows:
            RETRIEVAL_VIEW_HITS.labels(
                view_kind=str(row.get("matched_view_kind") or "UNKNOWN"),
                language=str(row.get("matched_view_language") or "und"),
            ).inc()
            if row.get("semantic_score") is not None:
                LANGUAGE_RETRIEVAL.labels(language=row["matched_view_language"], channel="semantic").inc()
            if row.get("lexical_score") is not None:
                LANGUAGE_RETRIEVAL.labels(language=row["matched_view_language"], channel="lexical").inc()
        lexical_used = bool(lexical_mode)
        if (
            lexical_used
            and rows
            and not semantic_closed_by_consent
            and not any(row.get("semantic_score") is not None for row in rows)
        ):
            degraded_reasons.append("semantic_unavailable")
        rerank_used = False
        # Doc 11.2: rerank is opt-in per purpose and only ever scores the first RERANK_TOP_N
        # candidates, so a broad purpose cannot turn every read into a provider call.
        if self.reranker and rows and not exact_only and purpose in set(self.settings.rerank_enabled_purposes):
            # Doc 3.3: consent judgments always read the primary - `conn` may be the replica.
            blocked = self._rerank_consent_block(primary, customer_id)
            if blocked:
                RERANK_USED.labels(result=blocked).inc()
            else:
                top_n = self.settings.rerank_top_n
                mismatch = sum(
                    1
                    for row in rows[:top_n]
                    if row.get("matched_view_kind") != "CANONICAL" and row.get("matched_view_language") != language
                )
                if mismatch:
                    RERANK_LANGUAGE_MISMATCH.inc(mismatch)
                try:
                    rows = self._rerank(query, rows)
                    rerank_used = True
                    RERANK_USED.labels(result="success").inc()
                except Exception:
                    degraded_reasons.append("reranker_unavailable")
                    RERANK_USED.labels(result="fallback").inc()
        translation_failed = primary.execute(
            "SELECT 1 FROM memory_translation_job WHERE customer_id=%s AND target_language=%s AND status IN ('RETRY','DEAD_LETTER') LIMIT 1",
            (customer_id, language),
        ).fetchone()
        if translation_failed:
            degraded_reasons.append("translation_unavailable")
        if low_confidence:
            # Query-quality signal, reported after concrete runtime degradations.
            degraded_reasons.append("language_low_confidence")
        conflicted_rows = []
        if purpose in {"CURRENT_ANSWER", "HISTORY_DEBUG"} and include_channels:
            # Doc 11.3 budget closure: the three-row conflict lookup runs exactly one channel
            # instead of repeating the full hybrid scan. With a query vector it takes semantic
            # (the only channel that can surface a conflict written in another language); the
            # lexical channel is the fallback when embedding is unavailable.
            conflicted_rows = self.repository.retrieve(
                conn,
                customer_id,
                query,
                qvec,
                language,
                purpose,
                3,
                fact_key=fact_key,
                entity_ref=entity_ref,
                as_of=as_of,
                include_types=include_types,
                include_sensitive=sensitive_granted,
                lexical_mode=None if qvec is not None else lexical_mode,
                lexical_config=lexical_config,
                trgm_threshold=self.settings.lexical_trgm_threshold,
                include_translated=not low_confidence,
                status_override=["CONFLICTED"],
            )
        memories = []
        token_count = 0
        token_dropped = 0
        high_impact_ids: list[str] = []
        for row in rows:
            content_tokens = estimate_tokens(row["content"])
            if token_count + content_tokens > max_tokens:
                token_dropped += 1
                RETRIEVAL_FILTERED.labels(reason="token_budget").inc()
                RETRIEVAL_POST_FILTER_LOSS.labels(reason="token_budget").inc()
                continue
            token_count += content_tokens
            if row["sensitivity"] in self.high_impact_sensitivities:
                high_impact_ids.append(str(row["memory_id"]))
            memories.append(self._memory_response(row, purpose))
        conflicted = [self._memory_response(row, purpose, context_only=True) for row in conflicted_rows]
        if not memories and not conflicted:
            RETRIEVAL_ZERO_RESULTS.labels(reason="token_budget" if token_dropped else "no_candidates").inc()
        if degraded_reasons:
            # One increment per request: the SLI numerator is degraded requests, not reasons
            # (docs 18.4). The label keeps the causes visible without inflating the ratio.
            RETRIEVAL_DEGRADED.labels(reason="+".join(sorted(set(degraded_reasons)))).inc()
        return {
            "memories": memories,
            "conflicted_memories": conflicted,
            # Doc 11.5: a high-impact fact is usable only after the business system confirms it,
            # so the answer layer gets the same signal whether or not a CONFLICT row exists.
            "requires_confirmation": bool(conflicted) or bool(high_impact_ids),
            "high_impact_memory_ids": high_impact_ids,
            "consent_denied": False,
            "consent_unavailable": False,
            "degraded": bool(degraded_reasons),
            "degraded_reason": degraded_reasons[0] if degraded_reasons else None,
            "semantic_index_pending": self._has_pending_embeddings(primary, customer_id),
            "lexical_used": lexical_used,
            "lexical_mode": lexical_mode if any(row.get("lexical_score") is not None for row in rows) else None,
            "lexical_skip_reason": lexical_skip_reason,
            "rerank_used": rerank_used,
            "filtered_reasons": {
                "expired": self._expired_filtered_count(conn, customer_id, include_types, sensitive_granted),
                "conflicted": len(conflicted_rows),
                "duplicates": duplicate_count,
                "token_budget": token_dropped,
            },
        }

    def _rank_rows(self, rows: list[dict[str, Any]], exact_only: bool) -> tuple[list[dict[str, Any]], int]:
        if exact_only:
            return rows, 0
        deduplicated = []
        seen = set()
        duplicate_count = 0
        for row in rows:
            identity = row.get("semantic_hash") or row["memory_id"]
            if identity in seen:
                duplicate_count += 1
                continue
            seen.add(identity)
            deduplicated.append(row)
        if duplicate_count:
            RETRIEVAL_FILTERED.labels(reason="duplicate").inc(duplicate_count)
        type_rank = {
            "ACTIVE_ISSUE": 6,
            "COMMITMENT": 5,
            "ACCOUNT_FACT": 4,
            "CUSTOMER_PROFILE": 3,
            "SERVICE_HISTORY": 2,
            "EPHEMERAL_CONTEXT": 1,
        }
        source_rank = {
            "SYSTEM_OF_RECORD": 6,
            "USER_EXPLICIT": 5,
            "AGENT_CONFIRMED": 4,
            "USER_IMPLICIT": 3,
            "MODEL_INFERRED": 2,
        }
        deduplicated.sort(
            key=lambda row: (
                row.get("rrf_score") or row["score"],
                type_rank.get(row["memory_type"], 0),
                source_rank.get(str(row.get("source_kind") or ""), 1),
                row["updated_at"].timestamp(),
                row.get("feedback_score") or 0,
                -len(row["content"]),
            ),
            reverse=True,
        )
        return deduplicated, duplicate_count

    def _expired_filtered_count(
        self, conn: DbConnection, customer_id: str, include_types: list[str] | None, include_sensitive: bool
    ) -> int:
        row = conn.execute(
            """SELECT count(*) AS n FROM customer_memory
               WHERE customer_id=%s AND lifecycle_status IN ('ACTIVE','RESOLVED')
                 AND expires_at IS NOT NULL AND expires_at <= now()
                 AND (%s::text[] IS NULL OR memory_type = ANY(%s::text[]))
                 AND (sensitivity='LOW' OR %s)""",
            (customer_id, include_types, include_types, include_sensitive),
        ).fetchone()
        return int(require_row(row, "expired filtered count")["n"])

    def _retrieval_query_vector(
        self,
        conn: DbConnection,
        customer_id: str,
        query: str,
        language: str,
        degraded_reasons: list[str],
    ) -> tuple[list[float] | None, bool]:
        """Embed the query, or report that the channel is closed by consent (doc 3.2/12.5).

        The second element is what keeps a policy closure out of `degraded`: consent posture
        must not move the availability SLI (18.4), so a customer who never granted
        CROSS_BORDER_EMBEDDING is served by the exact and lexical channels without being
        counted as a failed request.
        """
        if not self._query_vector_allowed(conn, customer_id):
            return None, True
        try:
            return (
                embed_query_cached(
                    self.embedding_provider,
                    query,
                    self.settings.embedding_model,
                    version=self.settings.embedding_version,
                    language=language,
                ),
                False,
            )
        except Exception:
            degraded_reasons.append("embedding_provider_unavailable")
            return None, False

    def _query_vector_allowed(self, conn: DbConnection, customer_id: str) -> bool:
        """Doc 3.2: the query text is customer content, so an external/cross-border ACTIVE model
        needs ``CROSS_BORDER_EMBEDDING`` here too, judged on the primary like every other consent
        check. A closed channel is policy, not a failure, so it must not enter ``degraded`` - that
        feeds the availability SLI (12.5/18.4). Exact and lexical recall stay available.
        """
        return (
            self.repository.cross_border_embedding_blocked(
                conn,
                customer_id,
                model=self.settings.embedding_model,
                version=self.settings.embedding_version,
                stage="retrieval",
            )
            is None
        )

    def _lexical_options(
        self, conn: DbConnection, query: str, language: str
    ) -> tuple[str | None, str | None, str | None]:
        if language not in self.settings.lexical_enabled_languages:
            return None, None, "language_not_configured"
        if len(query.strip()) < self.settings.lexical_min_query_chars:
            return None, None, "query_too_short"
        mode = self.settings.lexical_mode_map.get(language)
        config = self.settings.ts_config_map.get(language)
        if mode not in {"fts", "trgm"} or (mode == "fts" and not config):
            return None, None, "tokenizer_unavailable"
        if mode == "fts":
            if conn.execute("SELECT 1 FROM pg_ts_config WHERE cfgname=%s", (config,)).fetchone() is None:
                return None, None, "tokenizer_unavailable"
        else:
            extension = conn.execute("SELECT 1 FROM pg_extension WHERE extname='pg_trgm'").fetchone()
            if not extension:
                return None, None, "index_unavailable"
        return mode, config, None

    def _rerank_consent_block(self, conn: DbConnection, customer_id: str) -> str | None:
        """Doc 12.2/12.5: the reranker receives memory bodies, so a reranker that is not
        declared self-hosted is a cross-border channel and needs its own consent. Without it
        the RRF order is kept — reranking is an optimisation, never a reason to leak content."""
        if self.settings.reranker_provider == "self-hosted":
            return None
        try:
            self.repository.check_consent(conn, customer_id, "CROSS_BORDER_RERANK")
            return None
        except ConsentError as exc:
            return (
                "cross_border_consent_unavailable"
                if exc.code == "CONSENT_UNAVAILABLE"
                else "cross_border_consent_denied"
            )

    def _rerank(self, query: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        documents = [
            {
                "memory_id": str(row["memory_id"]),
                "content": row["content"],
                "matched_view": row["matched_content"],
                "matched_view_kind": row["matched_view_kind"],
                "translation_quality": row.get("translation_quality"),
            }
            for row in rows[: self.settings.rerank_top_n]
        ]
        reranker = self.reranker
        if reranker is None:
            return rows
        scores = reranker.rerank(query, documents)
        if not scores:
            raise ValueError("reranker returned no scores")
        reranked = []
        for row in rows:
            quality = row.get("translation_quality")
            quality_weight = (
                float(quality)
                if row["matched_view_kind"] == "TRANSLATED"
                and quality is not None
                and quality < self.settings.translation_quality_threshold
                else 1.0
            )
            row["score"] = scores.get(str(row["memory_id"]), row["rrf_score"]) * quality_weight
            reranked.append(row)
        reranked.sort(key=lambda item: item["score"], reverse=True)
        return reranked

    def _memory_response(self, row: dict[str, Any], purpose: str, context_only: bool = False) -> dict[str, Any]:
        if context_only:
            use_policy = "context_only"
        elif purpose == "AUDIT":
            use_policy = "audit_only"
        elif purpose == "HISTORY_DEBUG":
            use_policy = "history_only"
        else:
            use_policy = "fact"
        return {
            "memory_id": str(row["memory_id"]),
            "memory_type": row["memory_type"],
            "content": "[REDACTED_SENSITIVE_MEMORY]"
            if purpose == "CUSTOMER_SELF_SERVICE" and row["sensitivity"] != "LOW"
            else row["content"],
            "content_language": row["content_language"],
            "language": row["language"],
            "matched_view_kind": row["matched_view_kind"],
            "matched_view_language": row["matched_view_language"],
            "valid_at": row["valid_from"],
            "expires_at": row["expires_at"],
            "confidence_label": "unverified" if context_only else "verified",
            "source_summary": row.get("source_summary"),
            "use_policy": use_policy,
            "score": row["score"],
        }

    def _has_pending_embeddings(self, conn: DbConnection, customer_id: str) -> bool:
        row = conn.execute(
            "SELECT EXISTS(SELECT 1 FROM memory_embedding_job WHERE customer_id=%s AND status IN ('PENDING','RUNNING','RETRY')) AS pending",
            (customer_id,),
        ).fetchone()
        return bool(require_row(row, "pending embeddings")["pending"])
