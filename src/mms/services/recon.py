from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from ..config import get_settings
from ..utils.db import DbConnection, get_db, require_row
from ..utils.embedding import provider_from_settings
from ..utils.metrics import CURRENT_CONFLICTS, CURRENT_DUPLICATE, RECON_MISMATCHES, RENORM_LAG_RATIO, VIEW_MISSING
from .repository import MemoryRepository

_MISSING_VIEW_LABELS: set[tuple[str, str]] = set()


class ReconciliationService:
    TYPES = frozenset({"JOB", "VECTOR", "RECALL", "BUSINESS"})

    def __init__(self):
        self.settings = get_settings()
        self.provider = provider_from_settings(self.settings)
        self.repository = MemoryRepository()

    def run(self, recon_type: str) -> dict[str, Any]:
        if recon_type not in self.TYPES:
            raise ValueError(f"unsupported reconciliation type: {recon_type}")
        db = get_db()
        with db.transaction() as conn:
            run_id = require_row(
                conn.execute(
                    "INSERT INTO recon_run (id,recon_type,status,started_at,triggered_by) VALUES (gen_random_uuid(),%s,'RUNNING',now(),'scheduler') RETURNING id",
                    (recon_type,),
                ).fetchone(),
                "recon run",
            )["id"]
        stats: dict[str, Any] = {
            "scanned_count": 0,
            "mismatch_count": 0,
            "fixed_count": 0,
            "error_count": 0,
            "details": {},
        }
        try:
            if recon_type == "RECALL":
                # Doc 10.8/4.12: the canary makes a provider call per sample, so it runs on
                # short-lived connections instead of pinning the reconciliation transaction.
                self._recall_recon(stats)
            else:
                with db.transaction() as conn:
                    if recon_type == "JOB":
                        self._job_recon(conn, stats)
                    elif recon_type == "VECTOR":
                        self._vector_recon(conn, stats)
                    else:
                        self._business_recon(conn, stats)
            status = "COMPLETED" if stats["mismatch_count"] == stats["fixed_count"] else "PARTIAL"
            with db.transaction() as conn:
                self._finish(conn, run_id, status, stats)
        except Exception as exc:
            stats["error_count"] += 1
            with db.transaction() as conn:
                self._finish(conn, run_id, "FAILED", stats, exc.__class__.__name__)
            raise
        RECON_MISMATCHES.labels(recon_type=recon_type).inc(stats["mismatch_count"])
        return {"run_id": run_id, "recon_type": recon_type, "status": status, **stats}

    def _observe(self, conn: DbConnection, stats: dict[str, Any], name: str, sql: str, params=()) -> int:
        """Count a mismatch MMS cannot repair by itself (docs 10.8): it stays unfixed, so the run
        reports PARTIAL and the reconciliation alert fires instead of a green sheet."""
        count = int(require_row(conn.execute(sql, params).fetchone(), name)["n"])
        stats["mismatch_count"] += count
        stats["details"][name] = count
        return count

    def _fix(self, conn: DbConnection, stats: dict[str, Any], name: str, sql: str, params=(), *, fixes=True) -> None:
        result = conn.execute(sql, params)
        count = result.rowcount
        stats["scanned_count"] += count
        stats["mismatch_count"] += count
        if fixes:
            stats["fixed_count"] += count
        stats["details"][name] = count

    def _observe_dead_letters(self, conn: DbConnection, stats: dict[str, Any]) -> None:
        # Doc 10.7/10.8: terminal job states are an operator queue, so recon reports the stock and
        # not only what it repaired. FAILED outbox rows are invisible to the pending gauge.
        self._observe(
            conn,
            stats,
            "dead_letter_stock",
            """SELECT (SELECT count(*) FROM memory_embedding_job WHERE status='DEAD_LETTER')
                    +(SELECT count(*) FROM memory_translation_job WHERE status='DEAD_LETTER')
                    +(SELECT count(*) FROM memory_ingestion_job WHERE status='DEAD_LETTER')
                    +(SELECT count(*) FROM memory_outbox WHERE status='FAILED') AS n""",
        )

    def _job_recon(self, conn: DbConnection, stats: dict[str, Any]) -> None:
        self._fix(
            conn,
            stats,
            "embedding_lease",
            """UPDATE memory_embedding_job SET status='RETRY',locked_by=NULL,locked_at=NULL,updated_at=now()
            WHERE status='RUNNING' AND lease_expires_at<=now()""",
        )
        self._fix(
            conn,
            stats,
            "ingestion_lease",
            """UPDATE memory_ingestion_job SET status='RETRY',locked_by=NULL,locked_at=NULL,updated_at=now()
            WHERE status='PROCESSING' AND lease_expires_at<=now()""",
        )
        self._fix(
            conn,
            stats,
            "translation_lease",
            """UPDATE memory_translation_job SET status='RETRY',locked_by=NULL,locked_at=NULL,updated_at=now()
            WHERE status='RUNNING' AND lease_expires_at<=now()""",
        )
        self._fix(
            conn,
            stats,
            "outbox_lease",
            """UPDATE memory_outbox SET status='RETRY',locked_by=NULL,locked_at=NULL
            WHERE status IN ('PENDING','RETRY') AND locked_at<now()-(%s || ' seconds')::interval""",
            (self.settings.outbox_lease_seconds,),
        )
        self._fix(
            conn,
            stats,
            "ready_job_missing_vector",
            """UPDATE memory_embedding_job j SET status='PENDING',attempts=0,available_at=now(),locked_by=NULL,locked_at=NULL,updated_at=now()
            WHERE j.status='READY' AND NOT EXISTS (
              SELECT 1 FROM memory_embedding e WHERE e.customer_id=j.customer_id AND e.memory_id=j.memory_id
                AND e.embedding_model=j.embedding_model AND e.embedding_version=j.embedding_version
                AND e.view_kind=j.view_kind AND e.view_language=j.view_language AND e.status='READY')
              AND EXISTS (
                SELECT 1 FROM customer_memory m WHERE m.id=j.memory_id AND m.customer_id=j.customer_id
                  AND m.lifecycle_status IN ('ACTIVE','RESOLVED','CONFLICTED'))
              AND EXISTS (
                SELECT 1 FROM memory_search_document d WHERE d.customer_id=j.customer_id AND d.memory_id=j.memory_id
                  AND d.view_kind=j.view_kind AND d.view_language=j.view_language
                  AND d.view_text_hash=j.view_text_hash AND d.status='READY')""",
        )
        self._observe_dead_letters(conn, stats)

    def _observe_decision_safety(self, conn: DbConnection) -> None:
        # Docs 15.4 decision-safety SLO sources: duplicate current slots, open conflicts and
        # normalization lag are state snapshots (gauges), so they stay out of mismatch_count
        # and are surfaced for the dedicated alerts instead.
        duplicate = require_row(
            conn.execute(
                """SELECT count(*) AS n FROM (
                     SELECT customer_id, fact_key, entity_type, entity_ref
                     FROM customer_memory
                     WHERE current_flag AND cardinality='SINGLE_VALUE'
                       AND lifecycle_status IN ('ACTIVE','CONFLICTED')
                     GROUP BY customer_id, fact_key, entity_type, entity_ref
                     HAVING count(*) > 1) s"""
            ).fetchone(),
            "current duplicate count",
        )["n"]
        CURRENT_DUPLICATE.set(int(duplicate))
        conflicts = require_row(
            conn.execute("SELECT count(*) AS n FROM memory_conflict WHERE status IN ('OPEN','IN_REVIEW')").fetchone(),
            "open conflict count",
        )["n"]
        CURRENT_CONFLICTS.set(int(conflicts))
        lag = require_row(
            conn.execute(
                """SELECT count(*) FILTER (WHERE r.normalization_version <> m.normalization_version) AS lag,
                          count(m.id) AS total
                   FROM customer_memory m
                   JOIN LATERAL (SELECT normalization_version FROM fact_key_registry r2
                                 WHERE r2.fact_key=m.fact_key AND r2.status='ACTIVE'
                                 ORDER BY r2.version DESC LIMIT 1) r ON true
                   WHERE m.lifecycle_status IN ('ACTIVE','CONFLICTED')"""
            ).fetchone(),
            "renorm lag count",
        )
        RENORM_LAG_RATIO.set(int(lag["lag"]) / int(lag["total"]) if int(lag["total"]) else 0.0)

    def _observe_missing_views(self, conn: DbConnection, stats: dict[str, Any]) -> None:
        # Doc 8.10: a READY vector is missing per view kind; ORIGINAL is reconciled against
        # every evidence language, not only the main original.
        missing: dict[tuple[str, str], int] = {}
        canonical = require_row(
            conn.execute(
                """SELECT count(*) AS n FROM customer_memory m
                   WHERE m.lifecycle_status IN ('ACTIVE','RESOLVED','CONFLICTED')
                     AND NOT EXISTS (
                       SELECT 1 FROM memory_search_document d
                       JOIN memory_embedding em
                         ON em.customer_id=d.customer_id AND em.memory_id=d.memory_id
                        AND em.view_kind=d.view_kind AND em.view_language=d.view_language
                        AND em.view_text_hash=d.view_text_hash AND em.status='READY'
                       WHERE d.customer_id=m.customer_id AND d.memory_id=m.id
                         AND d.view_kind='CANONICAL' AND d.status='READY')"""
            ).fetchone(),
            "missing canonical views",
        )["n"]
        missing[("CANONICAL", "und")] = int(canonical)
        original = conn.execute(
            """SELECT e.content_language AS language, count(DISTINCT m.id) AS n
               FROM customer_memory m
               JOIN memory_evidence e ON e.memory_id=m.id AND e.customer_id=m.customer_id
               WHERE m.lifecycle_status IN ('ACTIVE','RESOLVED','CONFLICTED')
                 AND e.content_language IS NOT NULL
                 AND NOT EXISTS (
                   SELECT 1 FROM memory_search_document d
                   JOIN memory_embedding em
                     ON em.customer_id=d.customer_id AND em.memory_id=d.memory_id
                    AND em.view_kind=d.view_kind AND em.view_language=d.view_language
                    AND em.view_text_hash=d.view_text_hash AND em.status='READY'
                   WHERE d.customer_id=m.customer_id AND d.memory_id=m.id
                     AND d.view_kind='ORIGINAL' AND d.view_language=e.content_language AND d.status='READY')
               GROUP BY e.content_language"""
        ).fetchall()
        for row in original:
            missing[("ORIGINAL", str(row["language"]))] = int(row["n"])
        for view_kind, language in _MISSING_VIEW_LABELS - set(missing):
            VIEW_MISSING.labels(view_kind=view_kind, language=language).set(0)
        for (view_kind, language), count in missing.items():
            VIEW_MISSING.labels(view_kind=view_kind, language=language).set(count)
        _MISSING_VIEW_LABELS.update(missing)
        stats["mismatch_count"] += sum(missing.values())
        stats["details"]["missing_ready_views"] = {
            f"{view_kind}:{language}": count for (view_kind, language), count in missing.items()
        }

    def _vector_recon(self, conn: DbConnection, stats: dict[str, Any]) -> None:
        self._observe_missing_views(conn, stats)
        stats["details"]["repaired_views"] = self.repository.repair_missing_views(conn)
        self._fix(
            conn,
            stats,
            "orphan_original_view",
            """UPDATE memory_search_document d SET status='DELETED',updated_at=now()
            FROM customer_memory m WHERE m.id=d.memory_id AND m.customer_id=d.customer_id AND d.status='READY'
              AND d.view_kind='ORIGINAL'
              AND NOT EXISTS (SELECT 1 FROM memory_evidence e
                              WHERE e.memory_id=d.memory_id AND e.customer_id=d.customer_id
                                AND e.content_language=d.view_language)""",
        )
        self._fix(
            conn,
            stats,
            "memory_hash_or_state",
            """DELETE FROM memory_embedding e USING customer_memory m
            WHERE m.id=e.memory_id AND m.customer_id=e.customer_id
              AND (m.content_hash<>e.content_hash OR m.lifecycle_status NOT IN ('ACTIVE','RESOLVED','CONFLICTED'))""",
        )
        self._fix(
            conn,
            stats,
            "missing_or_stale_view",
            """DELETE FROM memory_embedding e WHERE NOT EXISTS (
              SELECT 1 FROM memory_search_document d WHERE d.customer_id=e.customer_id AND d.memory_id=e.memory_id
                AND d.view_kind=e.view_kind AND d.view_language=e.view_language
                AND d.view_text_hash=e.view_text_hash AND d.status='READY')""",
        )
        self._fix(
            conn,
            stats,
            "unindexable_document",
            """UPDATE memory_search_document d SET status='DELETED',updated_at=now()
            FROM customer_memory m WHERE m.id=d.memory_id AND m.customer_id=d.customer_id AND d.status='READY'
              AND (m.content_hash<>d.content_hash OR m.lifecycle_status NOT IN ('ACTIVE','RESOLVED','CONFLICTED'))""",
        )
        active = conn.execute("SELECT 1 FROM embedding_model_registry WHERE status='ACTIVE' LIMIT 1").fetchone()
        if active:
            self._fix(
                conn,
                stats,
                "retired_embedding_model",
                """DELETE FROM memory_embedding e WHERE EXISTS (
                  SELECT 1 FROM embedding_model_registry r WHERE r.model_id=e.embedding_model
                    AND r.version=e.embedding_version AND r.status='RETIRED')
                  OR NOT EXISTS (
                  SELECT 1 FROM embedding_model_registry r WHERE r.model_id=e.embedding_model
                    AND r.version=e.embedding_version AND r.status IN ('ACTIVE','SHADOW'))""",
            )
            stats["details"]["enqueued_missing_vectors"] = self.repository.enqueue_missing_embeddings(conn)

    def _recall_recon(self, stats: dict[str, Any]) -> None:
        """Doc 10.8: the semantic canary samples every language that has READY vectors and runs
        against the ACTIVE registry model. Newest-N sampling lets one hot language hide all the
        others, and settings.embedding_model reports false mismatches during a model switch."""
        per_language = max(1, self.settings.recon_batch_size // 10)
        with get_db().connection() as conn:
            model = conn.execute(
                """SELECT model_id,version FROM embedding_model_registry
                   WHERE status='ACTIVE' ORDER BY activated_at DESC LIMIT 1"""
            ).fetchone()
            if model is None:
                stats["details"]["semantic_canary"] = {"skipped": "no_active_model"}
                return
            rows = conn.execute(
                """SELECT customer_id,memory_id,content,view_kind,view_language
                   FROM (
                     SELECT d.customer_id,d.memory_id,d.content,d.view_kind,d.view_language,
                            row_number() OVER (PARTITION BY d.view_language ORDER BY d.updated_at DESC) AS rn
                     FROM memory_search_document d
                     JOIN customer_memory m ON m.id=d.memory_id AND m.customer_id=d.customer_id
                     JOIN memory_embedding e ON e.customer_id=d.customer_id AND e.memory_id=d.memory_id
                       AND e.view_kind=d.view_kind AND e.view_language=d.view_language
                       AND e.view_text_hash=d.view_text_hash AND e.content_hash=d.content_hash AND e.status='READY'
                     WHERE d.status='READY' AND d.view_kind IN ('ORIGINAL','CANONICAL')
                       AND m.lifecycle_status IN ('ACTIVE','RESOLVED')
                   ) sampled
                   WHERE rn <= %s
                   ORDER BY view_language, rn""",
                (per_language,),
            ).fetchall()
            # Doc 3.2: a canary sample is a provider call carrying stored memory text, so an
            # external/cross-border ACTIVE model needs the same verdict the writes get. Checked
            # on the sampling connection: no provider call may hold a connection.
            blocked_customers = {
                row["customer_id"]
                for row in rows
                if self.repository.cross_border_embedding_blocked(
                    conn,
                    row["customer_id"],
                    model=model["model_id"],
                    version=int(model["version"]),
                    stage="canary",
                )
            }
        by_language: dict[str, dict[str, int]] = {}
        for row in rows:
            if row["customer_id"] in blocked_customers:
                continue
            entry = by_language.setdefault(row["view_language"], {"scanned": 0, "misses": 0})
            entry["scanned"] += 1
            vector = self.provider.embed_query(row["content"], model["model_id"])
            with get_db().connection() as conn:
                hits = self.repository.retrieve(
                    conn,
                    row["customer_id"],
                    row["content"],
                    vector,
                    row["view_language"],
                    "HISTORY_DEBUG",
                    10,
                    include_sensitive=True,
                )
            if row["memory_id"] not in {hit["memory_id"] for hit in hits}:
                entry["misses"] += 1
        stats["scanned_count"] += sum(entry["scanned"] for entry in by_language.values())
        stats["mismatch_count"] += sum(entry["misses"] for entry in by_language.values())
        stats["details"]["semantic_canary"] = {
            "model": model["model_id"],
            "version": model["version"],
            "per_language": by_language,
            # Skipped samples must stay visible: a consent-closed cross-border channel shrinks
            # canary coverage, which otherwise looks identical to "no samples for this language".
            "scanned_total": sum(entry["scanned"] for entry in by_language.values()),
            "skipped_cross_border_customers": len(blocked_customers),
        }

    def _business_recon(self, conn: DbConnection, stats: dict[str, Any]) -> None:
        self._observe_decision_safety(conn)
        self._fix(
            conn,
            stats,
            "invalid_current_pointer",
            """DELETE FROM customer_memory_current c
            WHERE NOT EXISTS (SELECT 1 FROM customer_memory m WHERE m.id=c.memory_id AND m.customer_id=c.customer_id
              AND m.fact_key=c.fact_key AND m.entity_type=c.entity_type AND m.entity_ref=c.entity_ref
              AND m.cardinality='SINGLE_VALUE' AND m.current_flag AND m.lifecycle_status IN ('ACTIVE','CONFLICTED'))""",
        )
        self._fix(
            conn,
            stats,
            "missing_current_pointer",
            """INSERT INTO customer_memory_current
            (customer_id,fact_key,entity_type,entity_ref,cardinality,value_slot,memory_id,generation,updated_at)
            SELECT m.customer_id,m.fact_key,m.entity_type,m.entity_ref,'SINGLE_VALUE','',m.id,m.version_no,now()
            FROM customer_memory m WHERE m.cardinality='SINGLE_VALUE' AND m.current_flag
              AND m.lifecycle_status IN ('ACTIVE','CONFLICTED')
              AND NOT EXISTS (SELECT 1 FROM customer_memory_current c WHERE c.customer_id=m.customer_id
                AND c.fact_key=m.fact_key AND c.entity_type=m.entity_type AND c.entity_ref=m.entity_ref)
            ON CONFLICT (customer_id,fact_key,entity_type,entity_ref,value_slot) DO UPDATE SET
              memory_id=EXCLUDED.memory_id,generation=EXCLUDED.generation,updated_at=now()""",
        )
        # Doc 10.8: "an ACTIVE memory must have evidence, decision, current and embedding"; the
        # first pass only reconciled the current pointer.
        self._observe(
            conn,
            stats,
            "memory_without_evidence",
            """SELECT count(*) AS n FROM customer_memory m
               WHERE m.lifecycle_status IN ('ACTIVE','CONFLICTED','RESOLVED')
                 AND NOT EXISTS (SELECT 1 FROM memory_evidence e
                                 WHERE e.memory_id=m.id AND e.customer_id=m.customer_id)""",
        )
        self._observe(
            conn,
            stats,
            "memory_without_decision",
            """SELECT count(*) AS n FROM customer_memory m
               WHERE m.lifecycle_status IN ('ACTIVE','CONFLICTED','RESOLVED')
                 AND NOT EXISTS (SELECT 1 FROM memory_decision d
                                 WHERE d.customer_id=m.customer_id AND d.new_memory_id=m.id)""",
        )
        # Doc 6.7: DELETE_PENDING means the erasure is in flight. Once the barrier says the
        # deletion completed, finishing the projection is the only consistent state.
        self._fix(
            conn,
            stats,
            "delete_pending_after_completed_barrier",
            """UPDATE customer_memory m SET lifecycle_status='DELETED',updated_at=now()
               WHERE m.lifecycle_status='DELETE_PENDING'
                 AND EXISTS (SELECT 1 FROM customer_deletion_barrier b
                             WHERE b.customer_id=m.customer_id AND b.status='COMPLETED')""",
        )
        # A DELETE_PENDING row with no live barrier is the window the independent
        # _mark_delete_pending transaction leaves behind: writes stop honouring the fence, so it
        # has to surface rather than be guessed at in either direction.
        self._observe(
            conn,
            stats,
            "delete_pending_without_barrier",
            """SELECT count(*) AS n FROM customer_memory m
               WHERE m.lifecycle_status='DELETE_PENDING'
                 AND NOT EXISTS (SELECT 1 FROM customer_deletion_barrier b
                                 WHERE b.customer_id=m.customer_id AND b.status IN ('ACTIVE','COOLING_DOWN'))""",
        )

    def _finish(self, conn: DbConnection, run_id: UUID, status: str, stats: dict[str, Any], error_code=None) -> None:
        details = stats.pop("details", {})
        conn.execute(
            """UPDATE recon_run SET status=%s,completed_at=now(),scanned_count=%s,mismatch_count=%s,
                 fixed_count=%s,error_count=%s,details=%s,error_code=%s WHERE id=%s""",
            (
                status,
                stats["scanned_count"],
                stats["mismatch_count"],
                stats["fixed_count"],
                stats["error_count"],
                Jsonb(details),
                error_code,
                run_id,
            ),
        )
        stats["details"] = details

    def run_all(self) -> list[dict[str, Any]]:
        return [self.run(recon_type) for recon_type in ("JOB", "VECTOR", "RECALL", "BUSINESS")]


def main() -> None:
    print(json.dumps(ReconciliationService().run_all(), default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
