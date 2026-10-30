from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar
from uuid import UUID, uuid4

from psycopg import sql
from psycopg.types.json import Jsonb

from ..config import get_settings
from ..models import Decision, ExistingFact, NormalizedCandidate
from ..utils.db import DbConnection, get_db, require_row
from ..utils.embedding import vector_literal
from ..utils.errors import BarrierCheckFailedError, BarrierError, CandidateRejectedError, ConsentError
from ..utils.hashing import customer_hash, customer_ref, sha256_canonical, sha256_text, utcnow
from ..utils.metrics import (
    BARRIER_BLOCKED,
    BARRIER_CACHE_AGE,
    BARRIER_CACHE_MISS,
    BARRIER_FAILURES,
    CONSENT_CHECKS,
    DELETION_COMPLETION,
    EMBEDDING_CROSS_BORDER_BLOCKED,
    RETRIEVAL_BLOCKED,
    RETRIEVAL_DUPLICATE_VIEWS_COLLAPSED,
    RETRIEVAL_VIEWS_SCANNED,
    TOMBSTONE_LEDGER_FAILURES,
    VECTOR_SEARCH_LATENCY,
)
from .normalizer import redact_text

logger = logging.getLogger(__name__)

# Doc 10.4: priority bands. A correction has to be re-embedded ahead of everything else;
# rebuild/backfill work only gets leftover capacity.
PRIORITY_CORRECTION = 10
PRIORITY_DEFAULT = 100
PRIORITY_TRANSLATED = 120
PRIORITY_BACKFILL = 500


class _BarrierCache:
    """Doc 6.7.6 read-path cache: LRU + TTL. The write path never reads this cache."""

    def __init__(self) -> None:
        self._entries: OrderedDict[str, tuple[float, dict[str, Any] | None]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, customer_id: str) -> tuple[dict[str, Any] | None, float] | None:
        ttl = get_settings().barrier_cache_ttl_seconds
        with self._lock:
            entry = self._entries.get(customer_id)
            if entry is None:
                return None
            fetched_at, barrier = entry
            age = time.monotonic() - fetched_at
            if age >= ttl:
                self._entries.pop(customer_id, None)
                return None
            self._entries.move_to_end(customer_id)
            return barrier, age

    def put(self, customer_id: str, barrier: dict[str, Any] | None) -> None:
        limit = get_settings().barrier_cache_max_entries
        with self._lock:
            self._entries[customer_id] = (time.monotonic(), barrier)
            self._entries.move_to_end(customer_id)
            while len(self._entries) > limit:
                self._entries.popitem(last=False)

    def invalidate(self, customer_id: str) -> None:
        with self._lock:
            self._entries.pop(customer_id, None)


_BARRIER_CACHE = _BarrierCache()
_LEXICAL_EXECUTOR: ThreadPoolExecutor | None = None


def _lexical_executor() -> ThreadPoolExecutor:
    global _LEXICAL_EXECUTOR
    if _LEXICAL_EXECUTOR is None:
        _LEXICAL_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="mms-lexical")
    return _LEXICAL_EXECUTOR


def _row_to_existing(row: dict[str, Any], languages: set[str]) -> ExistingFact:
    return ExistingFact(
        memory_id=row["id"],
        version_no=row["version_no"],
        lifecycle_status=row["lifecycle_status"],
        object_json=row["object_json"],
        object_hash=row["object_hash"],
        semantic_hash=row["semantic_hash"],
        content_hash=row["content_hash"],
        content_language=row["content_language"],
        source_kind=row["source_kind"],
        valid_from=row["valid_from"],
        valid_to=row["valid_to"],
        evidence_languages=languages,
    )


class MemoryRepository:
    def __init__(self, ts_config_map: dict[str, str] | None = None):
        # Read settings here so every construction site (API, workers, services) shares
        # the same ts_config_map / embedding model instead of drifting to defaults.
        settings = get_settings()
        self.settings = settings
        self.ts_config_map = ts_config_map or settings.ts_config_map
        self.embedding_model = settings.embedding_model
        self.embedding_version = settings.embedding_version

    def lock_barrier(self, conn: DbConnection, customer_id: str) -> None:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"mms_barrier:{customer_id}",))

    def lock_fact(
        self, conn: DbConnection, customer_id: str, fact_key: str, entity_type: str, entity_ref: str, value_slot: str
    ) -> None:
        key = sha256_canonical([customer_id, fact_key, entity_type, entity_ref, value_slot])
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"mms_fact:{key}",))

    def barrier_snapshot(self, conn: DbConnection, customer_id: str) -> dict[str, Any] | None:
        return conn.execute(
            "SELECT status, generation, cutover_at FROM customer_deletion_barrier WHERE customer_id=%s", (customer_id,)
        ).fetchone()

    def verify_write_barrier(
        self, conn: DbConnection, customer_id: str, barrier_generation: int, carrier_created_at: datetime
    ) -> int:
        self.lock_barrier(conn, customer_id)
        try:
            barrier = self.barrier_snapshot(conn, customer_id)
            required = 0 if barrier is None else int(barrier["generation"])
            if barrier and barrier["status"] in {"ACTIVE", "COOLING_DOWN", "COMPLETED"}:
                BARRIER_BLOCKED.labels(status=barrier["status"]).inc()
                raise BarrierError(barrier["status"])
            # Docs 6.7.1 ②③: a carrier older than the barrier is a stale-generation
            # rejection, not a "customer deleted" verdict.
            if barrier_generation != required or (barrier and carrier_created_at < barrier["cutover_at"]):
                BARRIER_BLOCKED.labels(status="STALE_GENERATION").inc()
                raise BarrierError("STALE_GENERATION")
            return required
        except (BarrierError, BarrierCheckFailedError):
            raise
        except Exception as exc:
            # Fail closed: the transaction rolls back (doc 6.7.1 BARRIER_CHECK_FAILED).
            BARRIER_FAILURES.inc()
            raise BarrierCheckFailedError(exc.__class__.__name__) from exc

    def check_read_barrier(self, conn: DbConnection, customer_id: str) -> None:
        cached = _BARRIER_CACHE.get(customer_id)
        if cached is None:
            # Doc 6.7.6: miss/expiry re-reads the primary; an absent row is cached as
            # "no barrier" and still counted as a cache miss.
            BARRIER_CACHE_MISS.inc()
            barrier = self.barrier_snapshot(conn, customer_id)
            _BARRIER_CACHE.put(customer_id, barrier)
            BARRIER_CACHE_AGE.set(0.0)
        else:
            barrier, age = cached
            BARRIER_CACHE_AGE.set(age)
        if barrier and barrier["status"] in {"ACTIVE", "COOLING_DOWN", "COMPLETED"}:
            RETRIEVAL_BLOCKED.labels(status=barrier["status"]).inc()
            raise BarrierError(barrier["status"])

    def invalidate_barrier_cache(self, customer_id: str) -> None:
        _BARRIER_CACHE.invalidate(customer_id)

    def notify_barrier_changed(self, conn: DbConnection, customer_id: str) -> None:
        # Doc 6.7.6: NOTIFY is delivered on commit to every API replica's LISTEN loop.
        conn.execute("SELECT pg_notify('mms_barrier_changed', %s)", (customer_id,))

    def has_agent_customer_grant(self, conn: DbConnection, actor_id: str, customer_id: str) -> bool:
        """Doc 12.1: the acting subject must hold an explicit grant for the target customer."""
        return (
            conn.execute(
                """SELECT 1 FROM agent_customer_grant
                   WHERE actor_id=%s AND customer_id=%s AND revoked_at IS NULL""",
                (actor_id, customer_id),
            ).fetchone()
            is not None
        )

    def check_consent(self, conn: DbConnection, customer_id: str, purpose: str, *, write: bool = False) -> None:
        query = sql.SQL(
            """SELECT status, expires_at FROM customer_memory_consent
               WHERE customer_id=%s AND purpose=%s{for_update}"""
        ).format(for_update=sql.SQL(" FOR UPDATE" if write else ""))
        row = conn.execute(query, (customer_id, purpose)).fetchone()
        now = utcnow()
        if row is None or (row["expires_at"] is not None and row["expires_at"] <= now):
            CONSENT_CHECKS.labels(result="unavailable").inc()
            raise ConsentError("CONSENT_UNAVAILABLE")
        if row["status"] != "GRANTED":
            CONSENT_CHECKS.labels(result=row["status"].lower()).inc()
            raise ConsentError("CONSENT_WITHDRAWN" if row["status"] == "WITHDRAWN" else "CONSENT_REQUIRED")
        CONSENT_CHECKS.labels(result="granted").inc()

    def embedding_needs_cross_border(self, conn: DbConnection, *, model: str, version: int) -> bool:
        """Doc 3.2/12.3: whether an embedding call leaves MMS is decided by
        `embedding_model_registry.provider`, never guessed at runtime. A missing registry
        row fails closed as external.
        """
        row = conn.execute(
            "SELECT provider FROM embedding_model_registry WHERE model_id=%s AND version=%s",
            (model, version),
        ).fetchone()
        return row is None or row["provider"] != "self-hosted"

    def cross_border_embedding_blocked(
        self, conn: DbConnection, customer_id: str, *, model: str, version: int, stage: str = "worker"
    ) -> str | None:
        """The one CROSS_BORDER_EMBEDDING gate, shared by every outbound embedding call.

        Discovery vectors, retrieval query vectors, decision replay and the semantic canary
        are provider calls just like the Worker's, so they need the same verdict. Returning
        the reason code instead of raising lets each caller close only its own channel
        (doc 3.2: no vector, exact and lexical recall remain). Every closed channel is
        counted per stage, because a policy-driven skip must not be mistaken later for a
        provider outage.
        """
        if not self.embedding_needs_cross_border(conn, model=model, version=version):
            return None
        try:
            self.check_consent(conn, customer_id, "CROSS_BORDER_EMBEDDING")
        except ConsentError as exc:
            EMBEDDING_CROSS_BORDER_BLOCKED.labels(stage=stage).inc()
            return (
                "CROSS_BORDER_CONSENT_UNAVAILABLE"
                if exc.code == "CONSENT_UNAVAILABLE"
                else "CROSS_BORDER_CONSENT_DENIED"
            )
        return None

    def load_policy(self, conn: DbConnection, memory_type: str) -> tuple[dict[str, Any], str, str]:
        row = conn.execute(
            """SELECT name, version, config, config_hash FROM memory_policy
               WHERE status='ACTIVE' AND (memory_type=%s OR memory_type IS NULL)
               ORDER BY memory_type NULLS LAST, effective_from DESC LIMIT 1""",
            (memory_type,),
        ).fetchone()
        if not row:
            raise ConsentError("CONSENT_UNAVAILABLE", "no active memory policy")
        required = {
            "ignore_threshold",
            "auto_accept_threshold",
            "scoring_weights",
            "semantic_match_threshold",
            "auto_write",
            "review_rules",
            "retention_days",
            "sensitive_policy",
            "language_confidence_min",
            "translatable",
            "max_views_per_memory",
            "customer_visible_types",
            "review_sla_days",
            "conflict_max_age_days",
        }
        missing = required - set(row["config"])
        if missing:
            raise ValueError(f"active policy misses required keys: {sorted(missing)}")
        ignore_threshold = row["config"]["ignore_threshold"]
        auto_threshold = row["config"]["auto_accept_threshold"]
        if not (0 <= ignore_threshold < auto_threshold <= 100):
            raise ValueError(
                f"active policy thresholds invalid (need 0 <= ignore < auto <= 100): "
                f"ignore={ignore_threshold}, auto={auto_threshold}"
            )
        self._validate_scoring_weights(row["config"]["scoring_weights"])
        return row["config"], f"{row['name']}@{row['version']}", row["config_hash"]

    @staticmethod
    def _validate_scoring_weights(weights: Any) -> None:
        # Unvalidated signs silently invert the score (a positive risk_penalty rewards risk).
        if not isinstance(weights, dict) or not weights:
            raise ValueError("active policy scoring_weights must be a non-empty object")
        for key, value in weights.items():
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ValueError(f"scoring weight {key} must be numeric")
            if key == "risk_penalty" and value > 0:
                raise ValueError("scoring weight risk_penalty must be <= 0")
            if key != "risk_penalty" and value < 0:
                raise ValueError(f"scoring weight {key} must be >= 0")

    def load_registry(self, conn: DbConnection, fact_key: str) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM fact_key_registry WHERE fact_key=%s AND status='ACTIVE' ORDER BY version DESC LIMIT 1",
            (fact_key,),
        ).fetchone()
        if not row:
            # Permanent candidate-level failure: an unregistered fact key can never be
            # admitted, so the job must terminate instead of retrying (docs 4.11).
            raise CandidateRejectedError("UNKNOWN_FACT_KEY", f"unknown or inactive fact_key: {fact_key}")
        if row["cardinality"] == "SINGLE_VALUE" and not row["mutual_exclusion"]:
            raise CandidateRejectedError(
                "REGISTRY_MISCONFIGURED", f"SINGLE_VALUE fact_key {fact_key} must declare mutual_exclusion"
            )
        if row["sensitivity"] == "HIGH" and not row["requires_authority"]:
            # High-impact facts must be authority-backed (docs 3.1/4.x): a registry row that
            # allows un-authoritative HIGH-sensitivity writes is a misconfiguration, and the
            # candidate can never satisfy it — terminate, do not retry.
            raise CandidateRejectedError(
                "REGISTRY_MISCONFIGURED",
                f"HIGH-sensitivity fact_key {fact_key} must declare requires_authority",
            )
        return row

    def insert_candidate(
        self,
        conn: DbConnection,
        candidate: NormalizedCandidate,
        barrier_generation: int,
        *,
        content_override: str | None = None,
    ) -> bool:
        expires_at = candidate.expires_at or (candidate.observed_at + timedelta(days=365))
        row = conn.execute(
            """INSERT INTO memory_candidate (
                 candidate_id, customer_id, source_system, source_event_id, candidate_fingerprint,
                 memory_type, fact_key, fact_key_registry_version, cardinality, subject, predicate,
                 entity_type, entity_ref, value_slot, object_json, object_hash, content_redacted,
                 content_language, language, script, content_hash, semantic_text, semantic_hash,
                 normalization_version, language_confidence, source_kind, source_refs, confidence,
                 value_score, sensitivity, explicit_correction, valid_from, valid_to, observed_at,
                 expires_at, candidate_status, last_reason_codes, policy_version, extractor_version,
                 barrier_generation
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (customer_id, source_system, source_event_id, candidate_fingerprint) DO NOTHING
               RETURNING candidate_id""",
            (
                candidate.candidate_id,
                candidate.customer_id,
                candidate.source_system,
                candidate.source_event_id,
                candidate.candidate_fingerprint,
                candidate.memory_type,
                candidate.fact_key,
                candidate.fact_key_registry_version,
                candidate.cardinality,
                candidate.subject,
                candidate.predicate,
                candidate.entity_type,
                candidate.entity_ref,
                candidate.value_slot,
                Jsonb(candidate.object_json),
                candidate.object_hash,
                content_override if content_override is not None else redact_text(candidate.content),
                candidate.content_language,
                candidate.language,
                candidate.script,
                candidate.content_hash,
                candidate.semantic_text,
                candidate.semantic_hash,
                candidate.normalization_version,
                candidate.language_confidence,
                candidate.source_kind,
                Jsonb(candidate.source_refs),
                candidate.confidence,
                candidate.value_score,
                candidate.sensitivity,
                candidate.explicit_correction,
                candidate.valid_from,
                candidate.valid_to,
                candidate.observed_at,
                expires_at,
                "RECEIVED",
                Jsonb([]),
                candidate.policy_version,
                "api-2026-10-26",
                barrier_generation,
            ),
        ).fetchone()
        return row is not None

    def find_exact(self, conn: DbConnection, candidate: NormalizedCandidate) -> list[ExistingFact]:
        if candidate.cardinality == "SINGLE_VALUE":
            rows = conn.execute(
                """SELECT m.* FROM customer_memory_current c
                   JOIN customer_memory m ON m.id=c.memory_id AND m.customer_id=c.customer_id
                   WHERE c.customer_id=%s AND c.fact_key=%s AND c.entity_type=%s AND c.entity_ref=%s AND c.value_slot=%s
                     AND m.lifecycle_status IN ('ACTIVE','CONFLICTED')""",
                (
                    candidate.customer_id,
                    candidate.fact_key,
                    candidate.entity_type,
                    candidate.entity_ref,
                    candidate.value_slot,
                ),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT m.* FROM customer_memory m
                   WHERE m.customer_id=%s AND m.fact_key=%s AND m.entity_type=%s AND m.entity_ref=%s
                     AND m.value_slot=%s AND m.cardinality=%s AND m.current_flag
                     AND m.lifecycle_status IN ('ACTIVE','CONFLICTED')
                   ORDER BY (m.object_hash=%s) DESC, m.updated_at DESC""",
                (
                    candidate.customer_id,
                    candidate.fact_key,
                    candidate.entity_type,
                    candidate.entity_ref,
                    candidate.value_slot,
                    candidate.cardinality,
                    candidate.object_hash,
                ),
            ).fetchall()
        facts = []
        for row in rows:
            languages = {
                item["view_language"]
                for item in conn.execute(
                    "SELECT view_language FROM memory_search_document WHERE customer_id=%s AND memory_id=%s AND view_kind='ORIGINAL' AND status='READY'",
                    (candidate.customer_id, row["id"]),
                ).fetchall()
            }
            facts.append(_row_to_existing(row, languages))
        return facts

    def semantic_candidates(
        self,
        conn: DbConnection,
        customer_id: str,
        query_vector: list[float],
        threshold: float,
        exclude_ids: set[UUID],
        language: str = "und",
    ) -> list[dict[str, Any]]:
        rows = conn.execute(
            """WITH candidate_views AS (
                 SELECT e.memory_id, 1 - (e.embedding <=> %s::vector) AS score,
                        ROW_NUMBER() OVER (PARTITION BY e.memory_id ORDER BY e.embedding <=> %s::vector,
                          CASE e.view_kind WHEN 'ORIGINAL' THEN 0 WHEN 'CANONICAL' THEN 1 ELSE 2 END) rn
                 FROM memory_embedding e
                 JOIN customer_memory m
                   ON m.id=e.memory_id AND m.customer_id=e.customer_id AND m.content_hash=e.content_hash
                 JOIN memory_search_document d
                   ON d.customer_id=e.customer_id AND d.memory_id=e.memory_id
                  AND d.view_kind=e.view_kind AND d.view_language=e.view_language
                  AND d.view_text_hash=e.view_text_hash AND d.status='READY'
                 WHERE e.customer_id=%s AND e.status='READY' AND e.embedding_model=%s AND e.embedding_version=%s
                   AND (e.view_kind IN ('ORIGINAL','CANONICAL')
                        OR (e.view_kind='TRANSLATED' AND e.view_language=%s))
                   AND m.lifecycle_status IN ('ACTIVE','CONFLICTED')
               ) SELECT memory_id, score FROM candidate_views WHERE rn=1 AND score >= %s ORDER BY score DESC LIMIT 20""",
            (
                vector_literal(query_vector),
                vector_literal(query_vector),
                customer_id,
                self.embedding_model,
                self.embedding_version,
                language,
                threshold,
            ),
        ).fetchall()
        return [
            {"memory_id": row["memory_id"], "score": float(row["score"])}
            for row in rows
            if row["memory_id"] not in exclude_ids
        ]

    def insert_memory(
        self, conn: DbConnection, candidate: NormalizedCandidate, *, supersedes: UUID | None = None, version_no: int = 1
    ) -> UUID:
        memory_id = uuid4()
        conn.execute(
            """INSERT INTO customer_memory (
                 id, customer_id, memory_type, fact_key, subject, predicate, cardinality, entity_type,
                 entity_ref, value_slot, object_json, object_hash, content, content_language, language,
                 script, content_hash, semantic_text, semantic_hash, normalization_version,
                 language_confidence, source_kind, confidence, value_score, sensitivity, valid_from,
                 valid_to, observed_at, expires_at, lifecycle_status, version_no, supersedes_memory_id,
                 current_flag, policy_version, fact_key_registry_version, extractor_version
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s,true,%s,%s,%s)""",
            (
                memory_id,
                candidate.customer_id,
                candidate.memory_type,
                candidate.fact_key,
                candidate.subject,
                candidate.predicate,
                candidate.cardinality,
                candidate.entity_type,
                candidate.entity_ref,
                candidate.value_slot,
                Jsonb(candidate.object_json),
                candidate.object_hash,
                candidate.content,
                candidate.content_language,
                candidate.language,
                candidate.script,
                candidate.content_hash,
                candidate.semantic_text,
                candidate.semantic_hash,
                candidate.normalization_version,
                candidate.language_confidence,
                candidate.source_kind,
                candidate.confidence,
                candidate.value_score,
                candidate.sensitivity,
                candidate.valid_from,
                candidate.valid_to,
                candidate.observed_at,
                candidate.expires_at,
                version_no,
                supersedes,
                candidate.policy_version,
                candidate.fact_key_registry_version,
                "api-2026-10-26",
            ),
        )
        return memory_id

    def insert_evidence(self, conn: DbConnection, candidate: NormalizedCandidate, memory_id: UUID | None) -> UUID:
        evidence_id = uuid4()
        first_ref = candidate.source_refs[0] if candidate.source_refs else {}
        evidence_hash = sha256_canonical(
            {
                "source_system": candidate.source_system,
                "source_event_id": candidate.source_event_id,
                "message_id": first_ref.get("message_id"),
                "ticket_id": first_ref.get("ticket_id"),
                "evidence_excerpt_hash": sha256_text(candidate.content),
            }
        )
        conn.execute(
            """INSERT INTO memory_evidence (
                 id, customer_id, memory_id, candidate_id, anonymized, source_kind, source_system,
                 source_event_id, content_language, conversation_id, message_id, ticket_id,
                 evidence_excerpt_redacted, evidence_hash, observed_at
               ) VALUES (%s,%s,%s,%s,false,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (customer_id, source_system, source_event_id, evidence_hash) DO NOTHING
               RETURNING id""",
            (
                evidence_id,
                candidate.customer_id,
                memory_id,
                candidate.candidate_id,
                candidate.source_kind,
                candidate.source_system,
                candidate.source_event_id,
                candidate.content_language,
                first_ref.get("conversation_id"),
                first_ref.get("message_id"),
                first_ref.get("ticket_id"),
                redact_text(candidate.content),
                evidence_hash,
                candidate.observed_at,
            ),
        )
        row = conn.execute(
            "SELECT id FROM memory_evidence WHERE customer_id=%s AND source_system=%s AND source_event_id=%s AND evidence_hash=%s",
            (candidate.customer_id, candidate.source_system, candidate.source_event_id, evidence_hash),
        ).fetchone()
        return UUID(str(require_row(row, "insert_evidence")["id"]))

    @staticmethod
    def search_vector(conn: DbConnection, config: str | None, content: str):
        # Doc 8.4: a language without a usable tokenizer keeps search_vector NULL; trgm and
        # semantic retrieval still apply, so the write must not fail on a missing ts config.
        if not config or conn.execute("SELECT 1 FROM pg_ts_config WHERE cfgname=%s", (config,)).fetchone() is None:
            return None
        return require_row(
            conn.execute("SELECT to_tsvector(%s::regconfig, %s) AS value", (config, content)).fetchone(),
            "search vector",
        )["value"]

    def upsert_view(
        self,
        conn: DbConnection,
        memory_id: UUID,
        customer_id: str,
        content_hash: str,
        view_kind: str,
        language: str,
        content: str,
        evidence_id: UUID | None,
    ) -> str:
        view_hash = sha256_text(content)
        search_vector = (
            self.search_vector(conn, self.ts_config_map.get(language), content) if view_kind == "ORIGINAL" else None
        )
        conn.execute(
            """INSERT INTO memory_search_document (
                 memory_id, customer_id, view_kind, view_language, content, source_evidence_id,
                 content_hash, view_text_hash, search_vector, analyzer_version, status
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'phase1-2026-10-26','READY')
               ON CONFLICT (customer_id, memory_id, view_kind, view_language)
               DO UPDATE SET content=EXCLUDED.content, source_evidence_id=EXCLUDED.source_evidence_id,
                 view_text_hash=EXCLUDED.view_text_hash, search_vector=EXCLUDED.search_vector,
                 status='READY', updated_at=now()""",
            (memory_id, customer_id, view_kind, language, content, evidence_id, content_hash, view_hash, search_vector),
        )
        return view_hash

    def enqueue_embedding_job(
        self,
        conn: DbConnection,
        memory_id: UUID,
        customer_id: str,
        content_hash: str,
        view_kind: str,
        language: str,
        view_hash: str,
        barrier_generation: int,
        *,
        priority: int = 100,
    ) -> None:
        existing = conn.execute(
            """UPDATE memory_embedding_job SET status='PENDING', attempts=0, available_at=now(),
                 content_hash=%s, view_text_hash=%s, generation=generation+1, barrier_generation=%s,
                 priority=LEAST(memory_embedding_job.priority, %s),
                 locked_by=NULL, locked_at=NULL, lease_expires_at=NULL, updated_at=now()
               WHERE memory_id=%s AND embedding_model=%s AND embedding_version=%s
                 AND view_kind=%s AND view_language=%s AND operation='UPSERT'
                 AND status IN ('PENDING','RUNNING','RETRY') RETURNING id""",
            (
                content_hash,
                view_hash,
                barrier_generation,
                priority,
                memory_id,
                self.embedding_model,
                self.embedding_version,
                view_kind,
                language,
            ),
        ).fetchone()
        if existing:
            return
        conn.execute(
            """INSERT INTO memory_embedding_job (
                 id, memory_id, customer_id, operation, embedding_model, embedding_version,
                 content_hash, view_kind, view_language, view_text_hash, generation,
                 barrier_generation, status, priority
               ) VALUES (%s,%s,%s,'UPSERT',%s,%s,%s,%s,%s,%s,1,%s,'PENDING',%s)""",
            (
                uuid4(),
                memory_id,
                customer_id,
                self.embedding_model,
                self.embedding_version,
                content_hash,
                view_kind,
                language,
                view_hash,
                barrier_generation,
                priority,
            ),
        )

    def replay_dead_letter(self, conn: DbConnection, job_type: str, job_id: UUID) -> int:
        table = {"EMBEDDING": "memory_embedding_job", "TRANSLATION": "memory_translation_job"}[job_type]
        result = conn.execute(
            sql.SQL(
                """UPDATE {} SET status='PENDING', attempts=0, available_at=now(), last_error_code=NULL,
                     locked_by=NULL, locked_at=NULL, lease_expires_at=NULL, updated_at=now()
                   WHERE id=%s AND status='DEAD_LETTER'"""
            ).format(sql.Identifier(table)),
            (job_id,),
        )
        return result.rowcount

    def repair_missing_views(self, conn: DbConnection) -> dict[str, int]:
        """Doc 8.10/12.10: rebuild READY view rows that recon found missing.

        Only documents are rebuilt here; ``enqueue_missing_embeddings`` then queues the
        corresponding vectors, so a missing view converges without manual SQL.
        """
        canonical = conn.execute(
            """SELECT m.id, m.customer_id, m.content_hash, m.semantic_text
               FROM customer_memory m
               WHERE m.lifecycle_status IN ('ACTIVE','RESOLVED','CONFLICTED')
                 AND NOT EXISTS (
                   SELECT 1 FROM memory_search_document d
                   WHERE d.customer_id=m.customer_id AND d.memory_id=m.id
                     AND d.view_kind='CANONICAL' AND d.status='READY')"""
        ).fetchall()
        for row in canonical:
            self.upsert_view(
                conn, row["id"], row["customer_id"], row["content_hash"], "CANONICAL", "und", row["semantic_text"], None
            )
        primary = conn.execute(
            """SELECT DISTINCT ON (m.id)
                      m.id, m.customer_id, m.content_hash, m.content_language, m.content AS view_content,
                      e.id AS evidence_id
               FROM customer_memory m
               JOIN memory_evidence e
                 ON e.memory_id=m.id AND e.customer_id=m.customer_id
                AND e.content_language=m.content_language
               WHERE m.lifecycle_status IN ('ACTIVE','RESOLVED','CONFLICTED')
                 AND NOT EXISTS (
                   SELECT 1 FROM memory_search_document d
                   WHERE d.customer_id=m.customer_id AND d.memory_id=m.id
                     AND d.view_kind='ORIGINAL' AND d.view_language=m.content_language AND d.status='READY')
               ORDER BY m.id, e.observed_at"""
        ).fetchall()
        secondary = conn.execute(
            """SELECT DISTINCT ON (m.id, e.content_language)
                      m.id, m.customer_id, m.content_hash, e.content_language AS view_language,
                      e.evidence_excerpt_redacted AS view_content, e.id AS evidence_id
               FROM customer_memory m
               JOIN memory_evidence e
                 ON e.memory_id=m.id AND e.customer_id=m.customer_id
               WHERE m.lifecycle_status IN ('ACTIVE','RESOLVED','CONFLICTED')
                 AND e.content_language IS NOT NULL
                 AND e.content_language <> m.content_language
                 AND e.evidence_excerpt_redacted IS NOT NULL
                 AND NOT EXISTS (
                   SELECT 1 FROM memory_search_document d
                   WHERE d.customer_id=m.customer_id AND d.memory_id=m.id
                     AND d.view_kind='ORIGINAL' AND d.view_language=e.content_language AND d.status='READY')
               ORDER BY m.id, e.content_language, e.observed_at"""
        ).fetchall()
        for row in primary:
            self.upsert_view(
                conn,
                row["id"],
                row["customer_id"],
                row["content_hash"],
                "ORIGINAL",
                row["content_language"],
                row["view_content"],
                row["evidence_id"],
            )
        for row in secondary:
            self.upsert_view(
                conn,
                row["id"],
                row["customer_id"],
                row["content_hash"],
                "ORIGINAL",
                row["view_language"],
                row["view_content"],
                row["evidence_id"],
            )
        return {"canonical": len(canonical), "original_primary": len(primary), "original_secondary": len(secondary)}

    def enqueue_missing_embeddings(
        self,
        conn: DbConnection,
        *,
        customer_id: str | None = None,
        memory_id: UUID | None = None,
    ) -> int:
        """Doc 8.10: re-queue READY views that lack a READY vector for the ACTIVE model."""
        active = conn.execute(
            "SELECT model_id, version FROM embedding_model_registry WHERE status='ACTIVE' LIMIT 1"
        ).fetchone()
        if not active:
            return 0
        if customer_id is not None:
            barrier = self.barrier_snapshot(conn, customer_id)
            if barrier and barrier["status"] in {"ACTIVE", "COOLING_DOWN", "COMPLETED"}:
                return 0
        require_cross_border = self.settings.translation_provider == "http"
        result = conn.execute(
            """INSERT INTO memory_embedding_job (
                 id, memory_id, customer_id, operation, embedding_model, embedding_version,
                 content_hash, view_kind, view_language, view_text_hash, generation,
                 barrier_generation, status, priority
               )
               SELECT gen_random_uuid(), d.memory_id, d.customer_id, 'UPSERT', %s, %s,
                      d.content_hash, d.view_kind, d.view_language, d.view_text_hash, 1,
                      COALESCE(b.generation, 0), 'PENDING',
                      CASE WHEN d.view_kind='TRANSLATED' THEN %s ELSE %s END
               FROM memory_search_document d
               JOIN customer_memory m
                 ON m.id=d.memory_id AND m.customer_id=d.customer_id AND m.content_hash=d.content_hash
               LEFT JOIN customer_deletion_barrier b ON b.customer_id=d.customer_id
               WHERE d.status='READY'
                 AND m.lifecycle_status IN ('ACTIVE','RESOLVED','CONFLICTED')
                 AND (b.customer_id IS NULL OR b.status NOT IN ('ACTIVE','COOLING_DOWN','COMPLETED'))
                 AND (d.view_kind IN ('ORIGINAL','CANONICAL')
                      OR (d.view_kind='TRANSLATED' AND d.view_language = ANY(%s)))
                 AND (d.view_kind <> 'ORIGINAL' OR EXISTS (
                       SELECT 1 FROM memory_evidence ev
                       WHERE ev.memory_id=d.memory_id AND ev.customer_id=d.customer_id
                         AND ev.content_language=d.view_language))
                 AND (%s::text IS NULL OR d.customer_id=%s)
                 AND (%s::uuid IS NULL OR d.memory_id=%s)
                 AND (d.view_kind <> 'TRANSLATED' OR %s = false OR EXISTS (
                       SELECT 1 FROM customer_memory_consent c
                       WHERE c.customer_id=d.customer_id AND c.purpose='CROSS_BORDER_EMBEDDING'
                         AND c.status='GRANTED' AND (c.expires_at IS NULL OR c.expires_at > now())))
                 AND NOT EXISTS (
                       SELECT 1 FROM memory_embedding e
                       WHERE e.customer_id=d.customer_id AND e.memory_id=d.memory_id
                         AND e.view_kind=d.view_kind AND e.view_language=d.view_language
                         AND e.view_text_hash=d.view_text_hash
                         AND e.embedding_model=%s AND e.embedding_version=%s AND e.status='READY')
                 AND NOT EXISTS (
                       SELECT 1 FROM memory_embedding_job j
                       WHERE j.memory_id=d.memory_id AND j.embedding_model=%s AND j.embedding_version=%s
                         AND j.view_kind=d.view_kind AND j.view_language=d.view_language
                         AND j.operation='UPSERT' AND j.status IN ('PENDING','RUNNING','RETRY'))
               ON CONFLICT DO NOTHING""",
            (
                active["model_id"],
                int(active["version"]),
                PRIORITY_TRANSLATED,
                PRIORITY_BACKFILL,
                self.settings.translation_target_languages,
                customer_id,
                customer_id,
                memory_id,
                memory_id,
                require_cross_border,
                active["model_id"],
                int(active["version"]),
                active["model_id"],
                int(active["version"]),
            ),
        )
        return result.rowcount

    def insert_outbox(
        self,
        conn: DbConnection,
        aggregate_id: UUID,
        event_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
        barrier_generation: int,
        *,
        customer_id: str | None = None,
        aggregate_version: int = 1,
    ) -> None:
        customer_ref = self.ensure_identity(conn, customer_id) if customer_id else payload.get("customer_ref", "")
        if not customer_ref:
            raise ValueError("outbox event requires a customer_ref partition key")
        conn.execute(
            """INSERT INTO memory_outbox (id, aggregate_id, event_type, payload_json, idempotency_key,
                 event_id, partition_key, aggregate_version, barrier_generation, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDING')
               ON CONFLICT (idempotency_key) DO NOTHING""",
            (
                uuid4(),
                aggregate_id,
                event_type,
                Jsonb(payload),
                idempotency_key,
                uuid4(),
                customer_ref,
                aggregate_version,
                barrier_generation,
            ),
        )

    def insert_decision(self, conn: DbConnection, decision: Decision, customer_id: str, candidate_hash: str) -> None:
        # attempt_no is MAX+1: serialize per candidate so a concurrent insert cannot lose
        # its append-only audit row to ON CONFLICT DO NOTHING (docs/7 7.4).
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"mms_decision:{decision.candidate_id}",),
        )
        attempt_no = require_row(
            conn.execute(
                "SELECT COALESCE(MAX(attempt_no),0)+1 AS attempt_no FROM memory_decision WHERE candidate_id=%s",
                (decision.candidate_id,),
            ).fetchone(),
            "insert_decision attempt number",
        )["attempt_no"]
        conn.execute(
            """INSERT INTO memory_decision (
                 decision_id, candidate_id, customer_id, customer_hash, outcome, reason_codes,
                 rule_trace, primary_rule_id, candidate_hash, semantic_hash, normalization_version,
                 target_memory_id, new_memory_id, matched_memory_ids, expected_versions,
                 policy_version, fact_key_registry_version, engine_version, score_breakdown,
                 context_snapshot, requires_review, attempt_no
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (candidate_id, attempt_no) DO NOTHING""",
            (
                decision.decision_id,
                decision.candidate_id,
                customer_id,
                customer_hash(customer_id),
                decision.outcome,
                Jsonb(decision.reason_codes),
                Jsonb(decision.rule_trace),
                decision.primary_rule_id,
                candidate_hash,
                decision.semantic_hash,
                decision.normalization_version,
                decision.target_memory_id,
                decision.new_memory_id,
                decision.matched_memory_ids,
                Jsonb(decision.expected_versions),
                decision.policy_version,
                decision.fact_key_registry_version,
                decision.engine_version,
                Jsonb(decision.score_breakdown),
                Jsonb(decision.context_snapshot),
                decision.requires_review,
                attempt_no,
            ),
        )

    def set_candidate_result(
        self, conn: DbConnection, candidate_id: UUID, status: str, reason_codes: list[str]
    ) -> None:
        conn.execute(
            "UPDATE memory_candidate SET candidate_status=%s, last_reason_codes=%s, updated_at=now() WHERE candidate_id=%s",
            (status, Jsonb(reason_codes), candidate_id),
        )

    def supersede_old(self, conn: DbConnection, old_id: UUID, customer_id: str, candidate: NormalizedCandidate) -> None:
        conn.execute(
            """UPDATE customer_memory SET lifecycle_status='SUPERSEDED', current_flag=false,
                 valid_to=COALESCE(%s, now()), updated_at=now()
               WHERE id=%s AND customer_id=%s AND lifecycle_status IN ('ACTIVE','CONFLICTED')""",
            (candidate.valid_from, old_id, customer_id),
        )
        # Vectors are removed physically: a superseded memory is never re-embedded, and the
        # API account holds no write grant on memory_embedding (docs 12.3/12.5).
        conn.execute("DELETE FROM memory_embedding WHERE memory_id=%s AND customer_id=%s", (old_id, customer_id))
        conn.execute(
            "UPDATE memory_search_document SET status='DELETED', updated_at=now() WHERE memory_id=%s AND customer_id=%s",
            (old_id, customer_id),
        )
        conn.execute(
            "UPDATE memory_embedding_job SET status='CANCELLED', updated_at=now() WHERE memory_id=%s AND customer_id=%s AND status IN ('PENDING','RUNNING','RETRY')",
            (old_id, customer_id),
        )

    def update_current(
        self, conn: DbConnection, candidate: NormalizedCandidate, memory_id: UUID, generation: int
    ) -> None:
        conn.execute(
            """INSERT INTO customer_memory_current (customer_id, fact_key, entity_type, entity_ref,
                 cardinality, value_slot, memory_id, generation, updated_at)
               VALUES (%s,%s,%s,%s,'SINGLE_VALUE','',%s,%s,now())
               ON CONFLICT (customer_id, fact_key, entity_type, entity_ref, value_slot)
               DO UPDATE SET memory_id=EXCLUDED.memory_id, generation=EXCLUDED.generation, updated_at=now()""",
            (
                candidate.customer_id,
                candidate.fact_key,
                candidate.entity_type,
                candidate.entity_ref,
                memory_id,
                generation,
            ),
        )

    def _status_set(self, purpose: str) -> list[str]:
        return {
            "CURRENT_ANSWER": ["ACTIVE"],
            "CUSTOMER_SELF_SERVICE": ["ACTIVE"],
            "CANDIDATE_DISCOVERY": ["ACTIVE", "CONFLICTED"],
            "HISTORY_DEBUG": ["ACTIVE", "RESOLVED"],
            "AUDIT": ["ACTIVE", "RESOLVED", "CONFLICTED", "SUPERSEDED", "EXPIRED"],
        }[purpose]

    _ALLOWED_STATUSES: ClassVar[dict[str, frozenset[str]]] = {
        "CURRENT_ANSWER": frozenset({"ACTIVE", "CONFLICTED"}),
        "HISTORY_DEBUG": frozenset({"ACTIVE", "RESOLVED", "CONFLICTED", "SUPERSEDED", "EXPIRED"}),
        "CUSTOMER_SELF_SERVICE": frozenset({"ACTIVE"}),
        "CANDIDATE_DISCOVERY": frozenset({"ACTIVE", "CONFLICTED"}),
        "AUDIT": frozenset({"ACTIVE", "RESOLVED", "CONFLICTED", "SUPERSEDED", "EXPIRED"}),
    }

    def _validated_status_set(self, purpose: str, override: list[str] | None) -> list[str]:
        """Doc 8.13: callers may only narrow the purpose's status set; anything wider is a bug."""
        allowed = self._ALLOWED_STATUSES.get(purpose)
        if allowed is None:
            raise ValueError(f"unsupported retrieval purpose: {purpose}")
        status_set = list(override) if override is not None else self._status_set(purpose)
        invalid = set(status_set) - allowed
        if invalid:
            raise ValueError(f"status_set {sorted(invalid)} is not allowed for purpose {purpose}")
        return status_set

    def retrieve(
        self,
        conn: DbConnection,
        customer_id: str,
        query: str,
        query_vector: list[float] | None,
        language: str,
        purpose: str,
        max_items: int,
        fact_key: str | None = None,
        entity_ref: str | None = None,
        as_of: datetime | None = None,
        include_types: list[str] | None = None,
        include_sensitive: bool = False,
        lexical_mode: str | None = None,
        lexical_config: str | None = None,
        trgm_threshold: float = 0.3,
        include_translated: bool = True,
        parallel: bool = False,
        status_override: list[str] | None = None,
        include_channels: bool = True,
    ) -> list[dict[str, Any]]:
        status_set = self._validated_status_set(purpose, status_override)
        if fact_key or purpose == "AUDIT":
            exact_statuses = status_set
            if purpose == "HISTORY_DEBUG" and as_of is not None and include_channels:
                exact_statuses = [*status_set, "SUPERSEDED", "EXPIRED"]
            return self._exact_retrieve(
                conn,
                customer_id,
                max_items,
                fact_key,
                entity_ref,
                as_of,
                include_types,
                include_sensitive,
                exact_statuses,
            )
        semantic_args = (
            conn,
            customer_id,
            query_vector,
            language,
            status_set,
            as_of,
            include_types,
            include_sensitive,
            4 * max_items,
            include_translated,
        )
        lexical_args = (
            customer_id,
            query,
            language,
            lexical_mode,
            lexical_config,
            trgm_threshold,
            status_set,
            as_of,
            include_types,
            include_sensitive,
            4 * max_items,
        )
        if parallel and query_vector is not None and lexical_mode:
            future = _lexical_executor().submit(self._lexical_new_connection, lexical_args)
            semantic = self._semantic_retrieve(*semantic_args)
            lexical = future.result()
        else:
            semantic = self._semantic_retrieve(*semantic_args) if query_vector is not None else []
            lexical = self._lexical_retrieve(conn, *lexical_args) if lexical_mode else []
        rows = self._fuse_rrf(semantic, lexical, max_items)
        if purpose == "HISTORY_DEBUG" and as_of is not None and include_channels:
            rows = self._append_historic(
                conn,
                rows,
                customer_id,
                max_items,
                entity_ref,
                as_of,
                include_types,
                include_sensitive,
            )
        return rows

    def _exact_retrieve(
        self,
        conn,
        customer_id,
        max_items,
        fact_key,
        entity_ref,
        as_of,
        include_types,
        include_sensitive,
        status_set,
        fact_keys: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        started = time.perf_counter()
        rows = conn.execute(
            """SELECT m.id memory_id, m.customer_id, m.content, m.content_language, m.language, m.memory_type,
                      m.expires_at, m.valid_from, m.sensitivity, m.source_kind, m.confidence, m.updated_at, m.semantic_hash, 1.0 score, 1.0 semantic_score,
                      NULL::float lexical_score, m.content matched_content, NULL::real translation_quality,
                      (SELECT count(*) FILTER (WHERE f.feedback_type='HELPFUL') - count(*) FILTER (WHERE f.feedback_type='INCORRECT')
                       FROM memory_feedback f WHERE f.memory_id=m.id) feedback_score,
                      'ORIGINAL'::text matched_view_kind, m.content_language matched_view_language,
                      (SELECT ev.source_system || ':' || COALESCE(ev.ticket_id, ev.message_id, ev.source_event_id, ev.id::text)
                         FROM memory_evidence ev WHERE ev.memory_id=m.id
                        ORDER BY ev.observed_at LIMIT 1) AS source_summary
               FROM customer_memory m
               WHERE m.customer_id=%s AND (%s::text IS NULL OR m.fact_key=%s)
                 AND (%s::text IS NULL OR m.entity_ref=%s)
                 AND (%s::text[] IS NULL OR m.memory_type = ANY(%s::text[]))
                 AND (%s::text[] IS NULL OR m.fact_key = ANY(%s::text[]))
                 AND (m.sensitivity='LOW' OR %s)
                 AND m.lifecycle_status = ANY(%s)
                 AND (m.valid_from IS NULL OR m.valid_from <= COALESCE(%s, now()))
                 AND (m.valid_to IS NULL OR m.valid_to > COALESCE(%s, now()))
                 AND (m.expires_at IS NULL OR m.expires_at > COALESCE(%s, now()))
               ORDER BY m.updated_at DESC LIMIT %s""",
            (
                customer_id,
                fact_key,
                fact_key,
                entity_ref,
                entity_ref,
                include_types,
                include_types,
                fact_keys,
                fact_keys,
                include_sensitive,
                status_set,
                as_of,
                as_of,
                as_of,
                max_items,
            ),
        ).fetchall()
        VECTOR_SEARCH_LATENCY.labels(path="exact").observe(time.perf_counter() - started)
        return [dict(row) for row in rows]

    def _semantic_retrieve(
        self,
        conn,
        customer_id,
        query_vector,
        language,
        status_set,
        as_of,
        include_types,
        include_sensitive,
        limit: int = 20,
        include_translated: bool = True,
    ) -> list[dict[str, Any]]:
        from ..config import get_settings
        from ..utils.metrics import EXACT_SCAN_LARGE_CUSTOMER

        soft_limit = get_settings().exact_scan_soft_limit
        scanned = require_row(
            conn.execute(
                "SELECT count(*) AS n FROM (SELECT 1 FROM memory_embedding WHERE customer_id=%s AND status='READY' LIMIT %s) sample",
                (customer_id, soft_limit + 1),
            ).fetchone(),
            "exact scan sample",
        )["n"]
        if scanned > soft_limit:
            EXACT_SCAN_LARGE_CUSTOMER.inc()
        started = time.perf_counter()
        rows = conn.execute(
            """WITH candidate_views AS (
                 SELECT e.memory_id, 1 - (e.embedding <=> %s::vector) AS semantic_score,
                        e.view_kind, e.view_language, d.content matched_content, d.translation_quality,
                        COUNT(*) OVER () AS views_scanned,
                        DENSE_RANK() OVER (ORDER BY e.memory_id)
                          + DENSE_RANK() OVER (ORDER BY e.memory_id DESC) - 1 AS memories_scanned,
                        ROW_NUMBER() OVER (PARTITION BY e.memory_id ORDER BY e.embedding <=> %s::vector,
                          CASE e.view_kind WHEN 'ORIGINAL' THEN 0 WHEN 'CANONICAL' THEN 1 ELSE 2 END) rn
                 FROM memory_embedding e
                 JOIN customer_memory m ON m.id=e.memory_id AND m.customer_id=e.customer_id AND m.content_hash=e.content_hash
                 JOIN memory_search_document d ON d.customer_id=e.customer_id AND d.memory_id=e.memory_id
                   AND d.view_kind=e.view_kind AND d.view_language=e.view_language
                   AND d.view_text_hash=e.view_text_hash AND d.status='READY'
                 WHERE e.customer_id=%s AND e.status='READY' AND e.embedding_model=%s
                   AND e.embedding_version=%s
                   AND (e.view_kind IN ('ORIGINAL','CANONICAL')
                        OR (%s AND e.view_kind='TRANSLATED' AND e.view_language=%s))
                   AND (%s::text[] IS NULL OR m.memory_type = ANY(%s::text[]))
                   AND (m.sensitivity='LOW' OR %s)
                   AND m.lifecycle_status = ANY(%s)
                   AND (m.valid_from IS NULL OR m.valid_from <= COALESCE(%s, now()))
                   AND (m.valid_to IS NULL OR m.valid_to > COALESCE(%s, now()))
                   AND (m.expires_at IS NULL OR m.expires_at > COALESCE(%s, now()))
               )
               SELECT m.id memory_id, m.customer_id, m.content, m.content_language, m.language, m.memory_type,
                      m.expires_at, m.valid_from, m.sensitivity, m.source_kind, m.confidence, m.updated_at, m.semantic_hash, c.semantic_score,
                      c.matched_content, c.translation_quality,
                      (SELECT count(*) FILTER (WHERE f.feedback_type='HELPFUL') - count(*) FILTER (WHERE f.feedback_type='INCORRECT')
                       FROM memory_feedback f WHERE f.memory_id=m.id) feedback_score,
                      c.view_kind matched_view_kind, c.view_language matched_view_language,
                      c.views_scanned, c.memories_scanned,
                      (SELECT ev.source_system || ':' || COALESCE(ev.ticket_id, ev.message_id, ev.source_event_id, ev.id::text)
                         FROM memory_evidence ev WHERE ev.memory_id=m.id
                        ORDER BY ev.observed_at LIMIT 1) AS source_summary
               FROM candidate_views c JOIN customer_memory m ON m.id=c.memory_id
               WHERE c.rn=1 ORDER BY c.semantic_score DESC LIMIT %s""",
            (
                vector_literal(query_vector),
                vector_literal(query_vector),
                customer_id,
                self.embedding_model,
                self.embedding_version,
                include_translated,
                language,
                include_types,
                include_types,
                include_sensitive,
                status_set,
                as_of,
                as_of,
                as_of,
                limit,
            ),
        ).fetchall()
        VECTOR_SEARCH_LATENCY.labels(path="semantic").observe(time.perf_counter() - started)
        self._observe_scan(rows)
        return [dict(row) for row in rows]

    def _append_historic(
        self,
        conn: DbConnection,
        rows: list[dict[str, Any]],
        customer_id: str,
        max_items: int,
        entity_ref: str | None,
        as_of: datetime,
        include_types: list[str] | None,
        include_sensitive: bool,
    ) -> list[dict[str, Any]]:
        # Doc 9.4 step 7: SUPERSEDED/EXPIRED carry no vectors; history is reconstructed from
        # the version chain at as_of and must not consume the vector top-K slots. Scope the
        # chain to the recalled memories' fact keys so unrelated history cannot leak in.
        fact_keys: list[str] | None = None
        if rows:
            fk_rows = conn.execute(
                "SELECT DISTINCT fact_key FROM customer_memory WHERE customer_id=%s AND id=ANY(%s)",
                (customer_id, [row["memory_id"] for row in rows]),
            ).fetchall()
            fact_keys = [row["fact_key"] for row in fk_rows]
        historic = self._exact_retrieve(
            conn,
            customer_id,
            max_items,
            None,
            entity_ref,
            as_of,
            include_types,
            include_sensitive,
            ["SUPERSEDED", "EXPIRED"],
            fact_keys,
        )
        seen = {row["memory_id"] for row in rows}
        appended: list[dict[str, Any]] = []
        for row in historic:
            if row["memory_id"] in seen or len(rows) + len(appended) >= max_items:
                continue
            row["score"] = 0.0
            row["semantic_score"] = None
            appended.append(row)
        return [*rows, *appended]

    def _lexical_new_connection(self, args) -> list[dict[str, Any]]:
        from ..utils.db import get_db

        with get_db().connection() as conn:
            return self._lexical_retrieve(conn, *args)

    def _lexical_retrieve(
        self,
        conn,
        customer_id,
        query,
        language,
        mode,
        config,
        trgm_threshold,
        status_set,
        as_of,
        include_types,
        include_sensitive,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        started = time.perf_counter()
        if mode == "fts":
            score_sql = "ts_rank_cd(d.search_vector,websearch_to_tsquery(%s::regconfig,%s))"
            condition_sql = "d.search_vector @@ websearch_to_tsquery(%s::regconfig,%s)"
            lateral_sql = ""
            params = [config, query, customer_id, language, config, query]
        elif mode == "trgm":
            # similarity() is computed once in the lateral and then reused for scoring,
            # dedup ordering and the threshold filter.
            score_sql = "sx.sim"
            condition_sql = "d.content %% %s AND sx.sim >= %s"
            lateral_sql = "CROSS JOIN LATERAL (SELECT similarity(d.content,%s) AS sim) sx"
            params = [query, customer_id, language, query, trgm_threshold]
        else:
            raise ValueError(f"unsupported lexical mode: {mode}")
        query = sql.SQL("""
            WITH scored AS (
              SELECT d.memory_id,d.view_kind,d.view_language,d.content matched_content,d.translation_quality,
                     {score_sql} AS lexical_score,
                     COUNT(*) OVER () AS views_scanned,
                     DENSE_RANK() OVER (ORDER BY d.memory_id)
                       + DENSE_RANK() OVER (ORDER BY d.memory_id DESC) - 1 AS memories_scanned
              FROM memory_search_document d
              JOIN customer_memory m ON m.id=d.memory_id AND m.customer_id=d.customer_id AND m.content_hash=d.content_hash
              {lateral_sql}
              WHERE d.customer_id=%s AND d.view_language=%s AND d.view_kind IN ('ORIGINAL','TRANSLATED')
                AND d.status='READY' AND {condition_sql}
                AND (%s::text[] IS NULL OR m.memory_type = ANY(%s::text[]))
                AND (m.sensitivity='LOW' OR %s)
                AND m.lifecycle_status = ANY(%s)
                AND (m.valid_from IS NULL OR m.valid_from <= COALESCE(%s, now()))
                AND (m.valid_to IS NULL OR m.valid_to > COALESCE(%s, now()))
                AND (m.expires_at IS NULL OR m.expires_at > COALESCE(%s, now()))
            ), ranked AS (
              SELECT scored.*,ROW_NUMBER() OVER (PARTITION BY memory_id ORDER BY lexical_score DESC,
                CASE view_kind WHEN 'ORIGINAL' THEN 0 ELSE 1 END) rn FROM scored
            )
            SELECT m.id memory_id,m.customer_id,m.content,m.content_language,m.language,m.memory_type,m.expires_at,m.valid_from,
                   m.sensitivity,m.source_kind,m.confidence,m.updated_at,m.semantic_hash,r.lexical_score,r.matched_content,r.translation_quality,
                   (SELECT count(*) FILTER (WHERE f.feedback_type='HELPFUL') - count(*) FILTER (WHERE f.feedback_type='INCORRECT')
                    FROM memory_feedback f WHERE f.memory_id=m.id) feedback_score,
                   r.view_kind matched_view_kind,r.view_language matched_view_language,r.views_scanned,r.memories_scanned,
                   (SELECT ev.source_system || ':' || COALESCE(ev.ticket_id, ev.message_id, ev.source_event_id, ev.id::text)
                      FROM memory_evidence ev WHERE ev.memory_id=m.id
                     ORDER BY ev.observed_at LIMIT 1) AS source_summary
            FROM ranked r JOIN customer_memory m ON m.id=r.memory_id
            WHERE r.rn=1 ORDER BY r.lexical_score DESC LIMIT %s""").format(
            score_sql=sql.SQL(score_sql),
            condition_sql=sql.SQL(condition_sql),
            lateral_sql=sql.SQL(lateral_sql),
        )
        params.extend([include_types, include_types, include_sensitive, status_set, as_of, as_of, as_of, limit])
        rows = conn.execute(query, params).fetchall()
        VECTOR_SEARCH_LATENCY.labels(path="lexical").observe(time.perf_counter() - started)
        self._observe_scan(rows)
        return [dict(row) for row in rows]

    @staticmethod
    def _observe_scan(rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        scanned = int(rows[0]["views_scanned"])
        RETRIEVAL_VIEWS_SCANNED.observe(scanned)
        collapsed = scanned - int(rows[0]["memories_scanned"])
        if collapsed > 0:
            RETRIEVAL_DUPLICATE_VIEWS_COLLAPSED.inc(collapsed)

    def _fuse_rrf(self, semantic, lexical, max_items: int, constant: int = 60) -> list[dict[str, Any]]:
        merged: dict[UUID, dict[str, Any]] = {}
        scores: dict[UUID, float] = {}
        for channel, rows in (("semantic", semantic), ("lexical", lexical)):
            for rank, row in enumerate(rows, start=1):
                memory_id = row["memory_id"]
                scores[memory_id] = scores.get(memory_id, 0.0) + 1.0 / (constant + rank)
                current = merged.setdefault(memory_id, dict(row))
                current[f"{channel}_score"] = float(row[f"{channel}_score"])
                if channel == "semantic":
                    current.update(
                        {
                            "matched_view_kind": row["matched_view_kind"],
                            "matched_view_language": row["matched_view_language"],
                            "matched_content": row["matched_content"],
                        }
                    )
                other = "lexical" if channel == "semantic" else "semantic"
                current.setdefault(f"{other}_score", None)
        result = []
        for memory_id, row in merged.items():
            row["score"] = scores[memory_id]
            row["rrf_score"] = scores[memory_id]
            result.append(row)
        result.sort(key=lambda item: (item["rrf_score"], item.get("semantic_score") or 0.0), reverse=True)
        return result[:max_items]

    def ensure_identity(self, conn: DbConnection, customer_id: str) -> str:
        row = conn.execute(
            "SELECT customer_ref FROM customer_identity WHERE customer_id=%s AND is_current=true", (customer_id,)
        ).fetchone()
        if row:
            return str(row["customer_ref"])
        ref = customer_ref(customer_id)
        # DO UPDATE (not DO NOTHING) so a re-granted, previously deleted customer gets a
        # usable identity mapping again on their next write (doc 6.7.6 re-consent path).
        conn.execute(
            """INSERT INTO customer_identity (customer_id, customer_ref, ref_key_version, status, is_current)
               VALUES (%s,%s,%s,'ACTIVE',true)
               ON CONFLICT (customer_ref) DO UPDATE SET status='ACTIVE', is_current=true, retired_at=NULL""",
            (customer_id, ref, self.settings.kafka_ref_key_version),
        )
        return ref

    def customer_ref_for(self, conn: DbConnection, customer_id: str) -> str:
        """The pseudonym this customer's events actually used (any key version)."""
        row = conn.execute(
            "SELECT customer_ref FROM customer_identity WHERE customer_id=%s ORDER BY ref_key_version DESC LIMIT 1",
            (customer_id,),
        ).fetchone()
        return str(row["customer_ref"]) if row else customer_ref(customer_id)

    def _mark_delete_pending(self, conn: DbConnection, customer_id: str) -> dict[str, int]:
        """Doc 6.7.2 step 3 / 6.7.3: DELETE_PENDING + projection invalidation + job cancel.

        Runs in the caller's deletion transaction so an approval failure rolls the
        invalidation back together with the barrier/request (all-or-nothing). The
        trg_memory_views_present guard skips DELETE_PENDING/DELETED rows, which is what
        makes the same-transaction projection teardown legal.
        Returns per-table mutation counts for the deletion proof (doc 8.3.3).
        """
        counts: dict[str, int] = {}
        counts["customer_memory"] = conn.execute(
            """UPDATE customer_memory SET lifecycle_status='DELETE_PENDING',current_flag=false,updated_at=now()
               WHERE customer_id=%s AND lifecycle_status <> 'DELETED'""",
            (customer_id,),
        ).rowcount
        # Embeddings first: the translated-view trigger needs the READY document.
        counts["memory_embedding"] = conn.execute(
            "DELETE FROM memory_embedding WHERE customer_id=%s", (customer_id,)
        ).rowcount
        counts["memory_search_document"] = conn.execute(
            "UPDATE memory_search_document SET status='DELETED',updated_at=now() "
            "WHERE customer_id=%s AND status<>'DELETED'",
            (customer_id,),
        ).rowcount
        for table in ("memory_embedding_job", "memory_translation_job"):
            counts[table] = conn.execute(
                sql.SQL(
                    "UPDATE {} SET status='CANCELLED',last_error_code='DELETE_REQUESTED',updated_at=now(),"
                    "locked_by=NULL,locked_at=NULL,lease_expires_at=NULL "
                    "WHERE customer_id=%s AND status IN ('PENDING','RUNNING','RETRY')"
                ).format(sql.Identifier(table)),
                (customer_id,),
            ).rowcount
        counts["memory_ingestion_job"] = conn.execute(
            "UPDATE memory_ingestion_job SET status='CANCELLED',last_error_code='DELETE_REQUESTED',updated_at=now(),"
            "locked_by=NULL,locked_at=NULL,lease_expires_at=NULL "
            "WHERE customer_id=%s AND status IN ('RECEIVED','PROCESSING','RETRY')",
            (customer_id,),
        ).rowcount
        counts["customer_memory_current"] = conn.execute(
            "DELETE FROM customer_memory_current WHERE customer_id=%s", (customer_id,)
        ).rowcount
        return counts

    def delete_customer_data(
        self,
        conn: DbConnection,
        customer_id: str,
        requested_by: str,
        idempotency_key: str,
        *,
        existing_request_id: UUID | None = None,
        approved_by: str | None = None,
        ledger_writer=None,
    ) -> dict[str, Any]:
        self.lock_barrier(conn, customer_id)
        self.invalidate_barrier_cache(customer_id)
        customer_ref_value = self.customer_ref_for(conn, customer_id)
        scope = {"all": True}
        scope_hash = sha256_canonical(scope)
        previous_counts: dict[str, int] = {}
        # Tombstone audit reason differs by deletion origin: consent withdrawal loses the
        # legal basis rather than being a data-subject erase request (docs 2.6).
        tombstone_reason = "USER_DELETE"
        if existing_request_id:
            request = conn.execute(
                "SELECT * FROM memory_deletion_request WHERE id=%s AND customer_id=%s FOR UPDATE",
                (existing_request_id, customer_id),
            ).fetchone()
            if not request:
                raise ValueError(f"deletion request not found: {existing_request_id}")
            if request["request_source"] == "CONSENT_WITHDRAWN":
                tombstone_reason = "CONSENT_WITHDRAWN"
            if request["status"] == "COMPLETED":
                return {
                    "request_id": existing_request_id,
                    "status": "COMPLETED",
                    "counts": (request["proof"] or {}).get("counts", {}),
                }
            request_id = existing_request_id
            idempotency_key = request["request_idempotency_key"]
            previous_counts = (request["proof"] or {}).get("counts", {}) or {}
            current = self.barrier_snapshot(conn, customer_id)
            generation = int(require_row(current, "deletion barrier")["generation"])
            conn.execute(
                "UPDATE memory_deletion_request SET status='IN_PROGRESS',approved_by=%s,approved_at=now(),updated_at=now() WHERE id=%s",
                (approved_by, request_id),
            )
        else:
            existing = conn.execute(
                "SELECT id, status, proof FROM memory_deletion_request WHERE customer_id=%s AND request_idempotency_key=%s FOR UPDATE",
                (customer_id, idempotency_key),
            ).fetchone()
            if existing:
                return {
                    "request_id": existing["id"],
                    "status": existing["status"],
                    "counts": (existing["proof"] or {}).get("counts", {}),
                }
            current = self.barrier_snapshot(conn, customer_id)
            generation = 1 if current is None else int(current["generation"]) + 1
            request_id = uuid4()
            conn.execute(
                """INSERT INTO memory_deletion_request (id, customer_id, request_idempotency_key, request_source,
                     requested_by, approved_by, approved_at, scope, scope_hash, status, cutover_at, completed_at)
                   VALUES (%s,%s,%s,'USER',%s,'mms-reviewer',now(),%s,%s,'IN_PROGRESS',now(),now())""",
                (request_id, customer_id, idempotency_key, requested_by, Jsonb(scope), scope_hash),
            )
            conn.execute(
                """INSERT INTO customer_deletion_barrier (customer_id, request_id, status, requested_at, cutover_at, completed_at, generation, scope_type)
                   VALUES (%s,%s,'ACTIVE',now(),now(),NULL,%s,'CUSTOMER')
                   ON CONFLICT (customer_id) DO UPDATE SET request_id=EXCLUDED.request_id, status='ACTIVE',
                     requested_at=now(), cutover_at=now(), generation=EXCLUDED.generation, updated_at=now()""",
                (customer_id, request_id, generation),
            )
            self.insert_outbox(
                conn,
                request_id,
                "customer.deletion.barrier.changed.v1",
                {"customer_ref": self.ensure_identity(conn, customer_id), "generation": generation},
                f"barrier:{customer_id}:{generation}",
                generation,
            )

        counts: dict[str, int] = self._mark_delete_pending(conn, customer_id)
        result = conn.execute(
            """DELETE FROM memory_inbox_event i USING memory_ingestion_job j
               WHERE j.customer_id=%s AND i.consumer_group=j.payload_ref->'inbox'->>'consumer_group'
                 AND i.event_id=(j.payload_ref->'inbox'->>'event_id')::uuid""",
            (customer_id,),
        )
        counts["memory_inbox_event"] = result.rowcount
        for table in (
            "memory_embedding",
            "memory_search_document",
            "memory_embedding_job",
            "memory_translation_job",
            "customer_memory_current",
            "memory_review_task",
            "memory_conflict_item",
            "memory_conflict",
            "memory_feedback",
            "memory_usage_event",
            "memory_evidence",
        ):
            result = conn.execute(
                sql.SQL("DELETE FROM {} WHERE customer_id=%s").format(sql.Identifier(table)), (customer_id,)
            )
            # A projection is invalidated first and physically removed later; keep the
            # larger count so the proof does not collapse to zero.
            counts[table] = max(counts.get(table, 0), result.rowcount)
        # Doc 6.7.5 step 6: scrub the decision audit on the independent
        # mms_audit_scrubber connection *before* memory versions are deleted. Running it
        # afterwards would contend with the ON DELETE SET NULL row updates that the
        # composite FKs apply to memory_decision while this deletion transaction is
        # still open, and the scrubber connection would block forever behind it.
        with get_db().scrubber_transaction() as scrub_conn:
            result = scrub_conn.execute(
                """UPDATE memory_decision SET customer_id='scrubbed',candidate_id=NULL,target_memory_id=NULL,new_memory_id=NULL,
                     matched_memory_ids='{}',context_snapshot='{}',score_breakdown='{}',expected_versions='{}',
                     rule_trace='[]',pii_scrubbed_at=now() WHERE customer_id=%s""",
                (customer_id,),
            )
            counts["memory_decision"] = result.rowcount
            result = scrub_conn.execute(
                "UPDATE admin_access_audit SET reason_text_redacted=NULL,pii_scrubbed_at=now() WHERE customer_hash=%s",
                (customer_hash(customer_id),),
            )
            counts["admin_access_audit"] = result.rowcount
        # Only memory-domain outbox rows are removed; deletion-domain events
        # (aggregate_id=request_id, e.g. the activation barrier.changed) are kept so the
        # outbox publisher can still emit them instead of swallowing dead events.
        result = conn.execute(
            "DELETE FROM memory_outbox WHERE aggregate_id IN (SELECT id FROM customer_memory WHERE customer_id=%s)",
            (customer_id,),
        )
        counts["memory_outbox"] = result.rowcount
        result = conn.execute("DELETE FROM customer_memory WHERE customer_id=%s", (customer_id,))
        counts["customer_memory"] = result.rowcount
        # Usage counters are keyed by customer_id too. They are operational telemetry rather
        # than memory content, so they are erased without claiming a tombstone item (docs 12.2).
        conn.execute("DELETE FROM customer_quota_usage WHERE customer_id=%s", (customer_id,))
        for table in ("memory_candidate", "memory_ingestion_job"):
            result = conn.execute(
                sql.SQL("DELETE FROM {} WHERE customer_id=%s").format(sql.Identifier(table)), (customer_id,)
            )
            counts[table] = max(counts.get(table, 0), result.rowcount)
        for table, value in previous_counts.items():
            # Doc 6.7.5: retries are re-entrant; the deletion proof keeps the original counts.
            counts[table] = max(int(counts.get(table, 0)), int(value))

        result = conn.execute(
            "UPDATE customer_memory_consent SET status='WITHDRAWN', evidence_ref=NULL, updated_at=now() WHERE customer_id=%s AND status='GRANTED'",
            (customer_id,),
        )
        counts["customer_memory_consent"] = result.rowcount
        result = conn.execute(
            "UPDATE customer_identity SET status='RETIRED', is_current=false, retired_at=now() WHERE customer_id=%s",
            (customer_id,),
        )
        counts["customer_identity"] = result.rowcount

        target_types = {
            "memory_embedding": "EMBEDDING",
            "memory_search_document": "SEARCH_DOCUMENT",
            "memory_embedding_job": "EMBEDDING_JOB",
            "memory_translation_job": "EMBEDDING_JOB",
            "customer_memory_current": "CUSTOMER_MEMORY_CURRENT",
            "memory_review_task": "REVIEW_TASK",
            "memory_conflict_item": "CONFLICT",
            "memory_conflict": "CONFLICT",
            "memory_feedback": "FEEDBACK",
            "memory_usage_event": "USAGE_EVENT",
            "memory_evidence": "EVIDENCE",
            "memory_outbox": "OUTBOX",
            "customer_memory": "CUSTOMER_MEMORY",
            "memory_candidate": "CANDIDATE",
            "memory_ingestion_job": "INGESTION_JOB",
            "memory_inbox_event": "INBOX",
            "memory_decision": "DECISION",
            "admin_access_audit": "AUDIT_SCRUB",
            "customer_memory_consent": "CONSENT",
            "customer_identity": "CUSTOMER_IDENTITY",
        }
        for table in counts:
            conn.execute(
                """INSERT INTO memory_deletion_item
                     (id,deletion_request_id,customer_id,target_type,target_ref,status,verified_at)
                   VALUES (%s,%s,%s,%s,%s,'VERIFIED',now())
                   ON CONFLICT DO NOTHING""",
                (uuid4(), request_id, customer_id, target_types[table], sha256_text(f"{customer_id}:{table}")),
            )
        ledger_key = f"deletion-tombstones/{request_id}/{scope_hash}.json"
        proof = {"counts": counts, "verified": True, "ledger_object_key": ledger_key}
        ledger_ok = True
        if ledger_writer:
            try:
                ledger_writer(
                    ledger_key,
                    {
                        "request_id": str(request_id),
                        "customer_ref": customer_ref_value,
                        "proof": proof,
                        "completed_at": datetime.now(UTC).isoformat(),
                    },
                )
            except Exception:
                # Doc 6.8: a failed WORM ledger write must never be reported as a completed
                # deletion. Keep the request VERIFYING (barrier stays ACTIVE, so the customer
                # cannot resurrect) and retry the tombstone against the same request id.
                ledger_ok = False
                TOMBSTONE_LEDGER_FAILURES.inc()
                logger.exception("deletion_tombstone_ledger_write_failed request_id=%s", request_id)
                proof["verified"] = False
                proof["ledger_error"] = "LEDGER_WRITE_FAILED"
        if not ledger_ok:
            conn.execute(
                "UPDATE memory_deletion_request SET status='VERIFYING', proof=%s, updated_at=now() WHERE id=%s",
                (Jsonb(proof), request_id),
            )
            return {"request_id": request_id, "status": "VERIFYING", "counts": counts}
        conn.execute(
            """INSERT INTO deletion_tombstone (tombstone_id, deletion_request_id, customer_id, customer_ref,
                 scope_hash, scope, reason_code, completed_at, backup_generation, kms_key_id,
                 ledger_object_key, ledger_written_at, retain_until)
               VALUES (%s,%s,%s,%s,%s,%s,%s,now(),%s,%s,%s,now(),now()+(%s || ' days')::interval)""",
            (
                uuid4(),
                request_id,
                customer_id,
                customer_ref_value,
                scope_hash,
                Jsonb(scope),
                tombstone_reason,
                self.settings.deletion_backup_generation,
                self.settings.kms_key_id,
                ledger_key,
                self.settings.tombstone_retention_days,
            ),
        )
        completed = require_row(
            conn.execute(
                "UPDATE memory_deletion_request SET status='COMPLETED', completed_at=now(), proof=%s, updated_at=now() "
                "WHERE id=%s RETURNING extract(epoch from (completed_at-created_at)) AS duration",
                (Jsonb(proof), request_id),
            ).fetchone(),
            "deletion completion",
        )
        # Docs 15.4 deletion completion SLI: request creation to full completion, by scope.
        DELETION_COMPLETION.labels(scope=tombstone_reason).observe(float(completed["duration"] or 0))
        # Doc 6.7.1/6.7.6: completion enters the cooling-down window. Reads stay at 410 and
        # writes stay blocked; only the lifecycle scheduler may later mark it COMPLETED, and
        # a fresh GRANTED consent may cancel it early.
        conn.execute(
            "UPDATE customer_deletion_barrier SET status='COOLING_DOWN', completed_at=now(), updated_at=now() WHERE customer_id=%s",
            (customer_id,),
        )
        self.invalidate_barrier_cache(customer_id)
        # Doc 6.7.6: the deletion transaction itself must publish the barrier transition so
        # downstream barrier caches invalidate. The identity row is already retired here, so
        # the ref captured at entry is reused instead of resurrecting a current identity row.
        self.insert_outbox(
            conn,
            request_id,
            "customer.deletion.barrier.changed.v1",
            {
                "customer_ref": customer_ref_value,
                "generation": generation,
                "status": "COOLING_DOWN",
            },
            f"barrier:{customer_id}:{generation}:completed",
            generation,
        )
        self.notify_barrier_changed(conn, customer_id)
        return {"request_id": request_id, "status": "COMPLETED", "counts": counts}
