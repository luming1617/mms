from __future__ import annotations

import math
import time
from collections import defaultdict
from typing import Any
from uuid import uuid4

from opentelemetry import trace
from psycopg import sql

from ..config import get_settings
from ..observability.bootstrap import bootstrap_worker
from ..observability.tracing import record_error
from ..utils.backoff import jittered
from ..utils.db import DbConnection, get_db
from ..utils.embedding import provider_from_settings, vector_literal
from ..utils.hashing import utcnow
from ..utils.metrics import (
    EMBEDDING_FAILURES,
    EMBEDDING_INVALID_VECTOR,
    EMBEDDING_JOBS,
    EMBEDDING_LEASE_EXPIRED,
    EMBEDDING_OLDEST_AGE,
    EMBEDDING_READY_AGE,
)
from .repository import MemoryRepository


class EmbeddingWorker:
    def __init__(self, provider=None, worker_id: str | None = None):
        settings = get_settings()
        self.settings = settings
        self.provider = provider or provider_from_settings(settings)
        self.worker_id = worker_id or f"worker-{uuid4()}"
        self.repository = MemoryRepository()

    def _cross_border_consent_block(self, conn: DbConnection, job: dict[str, Any]) -> str | None:
        """Doc 2.6 (P0-1): the CROSS_BORDER_EMBEDDING purpose gates calls to an external /
        cross-border embedding provider. The verdict is the repository's, shared with the
        candidate-discovery path, so the registry rule cannot drift between call sites.
        Without consent only exact/lexical recall remains: no vector is generated.
        """
        return self.repository.cross_border_embedding_blocked(
            conn,
            job["customer_id"],
            model=job["embedding_model"],
            version=int(job["embedding_version"]),
            stage="worker",
        )

    def _pre_provider_block(self, conn: DbConnection, job: dict[str, Any]) -> str | None:
        """Doc 8.4 step 5 / 8.3.4: barrier, generation and lifecycle are checked before the
        provider call, not only before the vector write."""
        self.repository.lock_barrier(conn, job["customer_id"])
        barrier = self.repository.barrier_snapshot(conn, job["customer_id"])
        required = 0 if barrier is None else int(barrier["generation"])
        if barrier and barrier["status"] in {"ACTIVE", "COOLING_DOWN", "COMPLETED"}:
            return f"BARRIER_{barrier['status']}"
        if job["barrier_generation"] != required or (barrier is not None and job["created_at"] < barrier["cutover_at"]):
            return "STALE_GENERATION"
        memory = conn.execute(
            "SELECT lifecycle_status,content_hash FROM customer_memory WHERE id=%s AND customer_id=%s",
            (job["memory_id"], job["customer_id"]),
        ).fetchone()
        if (
            memory is None
            or memory["content_hash"] != job["content_hash"]
            or memory["lifecycle_status"] not in {"ACTIVE", "RESOLVED", "CONFLICTED"}
        ):
            return "STALE_GENERATION"
        return None

    def claim(self, conn: DbConnection) -> dict[str, Any] | None:
        jobs = self.claim_batch(conn, 1)
        return jobs[0] if jobs else None

    def claim_batch(self, conn: DbConnection, limit: int) -> list[dict[str, Any]]:
        requested = conn.execute(
            """SELECT id, status FROM memory_embedding_job
               WHERE (status IN ('PENDING','RETRY') AND available_at <= now())
                  OR (status = 'RUNNING' AND lease_expires_at <= now())
               ORDER BY priority, available_at, created_at
               FOR UPDATE SKIP LOCKED LIMIT %s""",
            (limit,),
        ).fetchall()
        if not requested:
            return []
        # Docs 15.3: a RUNNING row past its lease is a worker that died mid-job; count the
        # reclaim so lease-expiry growth is visible (alert MMSEmbeddingLeaseExpiryGrowing).
        reclaimed = sum(1 for row in requested if row["status"] == "RUNNING")
        if reclaimed:
            EMBEDDING_LEASE_EXPIRED.inc(reclaimed)
        claimed = conn.execute(
            """UPDATE memory_embedding_job SET status='RUNNING', locked_by=%s, locked_at=now(),
                 lease_expires_at=now()+(%s || ' seconds')::interval, attempts=attempts+1, updated_at=now()
               WHERE id = ANY(%s)
               RETURNING *""",
            (self.worker_id, self.settings.embedding_lease_seconds, [row["id"] for row in requested]),
        ).fetchall()
        return [dict(row) for row in claimed]

    def process_one(self) -> bool:
        return self.process_batch(limit=1)

    def process_batch(self, limit: int | None = None) -> bool:
        """Doc 8.4 step 5: claim up to embedding_batch_size jobs and call the provider once
        per (model, version). Each job still commits its vector in its own short transaction."""
        db = get_db()
        with db.transaction() as conn:
            jobs = self.claim_batch(conn, limit or self.settings.embedding_batch_size)
        if not jobs:
            return False
        with trace.get_tracer("mms.embedding-worker").start_as_current_span("embedding.job") as span:
            span.set_attribute("mms.job.status", "claimed")
            span.set_attribute("mms.job.count", len(jobs))
            try:
                self._process_claimed(jobs)
            except Exception as exc:
                record_error(span, exc)
                raise
        return True

    def _process_claimed(self, jobs: list[dict[str, Any]]) -> None:
        db = get_db()
        specs = self._registry_specs(jobs)
        batches: dict[tuple[str, int], list[tuple[dict[str, Any], str]]] = defaultdict(list)
        for job in jobs:
            try:
                # Doc 2.6: the consent gate runs BEFORE the provider call so that text of a
                # customer without CROSS_BORDER_EMBEDDING consent never leaves MMS.
                with db.connection() as conn:
                    blocked = self._cross_border_consent_block(conn, job)
                if blocked:
                    self._cancel_job(job, blocked)
                    continue
                with db.transaction() as conn:
                    blocked = self._pre_provider_block(conn, job)
                if blocked:
                    self._cancel_job(job, blocked)
                    continue
                with db.connection() as conn:
                    document = conn.execute(
                        """SELECT content FROM memory_search_document
                           WHERE customer_id=%s AND memory_id=%s AND view_kind=%s AND view_language=%s
                             AND view_text_hash=%s AND status='READY'""",
                        (
                            job["customer_id"],
                            job["memory_id"],
                            job["view_kind"],
                            job["view_language"],
                            job["view_text_hash"],
                        ),
                    ).fetchone()
                if not document:
                    self._cancel_job(job, "VIEW_NOT_READY")
                    continue
                # Doc 8.4 step 10: an unknown provider/write outcome is resolved by reading
                # back the vector, never by blindly calling the provider again.
                with db.connection() as conn:
                    if self._embedding_written(conn, job):
                        self._mark_ready(job)
                        continue
                batches[(job["embedding_model"], int(job["embedding_version"]))].append((job, document["content"]))
            except Exception as exc:
                self._retry_job(job, exc)
        for (model, version), group in batches.items():
            try:
                vectors = self.provider.embed_documents([content for _, content in group], model)
                if len(vectors) != len(group):
                    raise ValueError("EMBEDDING_COUNT_MISMATCH")
            except Exception as exc:
                for job, _ in group:
                    self._retry_job(job, exc)
                continue
            spec = specs.get((model, version))
            dimension = int(spec["dimension"]) if spec else self.settings.embedding_dimension
            for (job, _), vector in zip(group, vectors, strict=True):
                try:
                    self._validate_vector(vector, spec)
                except Exception as exc:
                    self._retry_job(job, exc)
                    continue
                try:
                    with db.transaction() as conn:
                        ready = self._final_write(conn, job, vector, dimension)
                except Exception as exc:
                    self._retry_job(job, exc)
                    continue
                if ready:
                    EMBEDDING_JOBS.labels(status="READY", model=model, version=str(version)).inc()
                    self._observe_ready_age(job)

    def _cancel_job(self, job: dict[str, Any], reason: str) -> None:
        with get_db().transaction() as tx:
            self._cancel(tx, job, reason)
        EMBEDDING_JOBS.labels(
            status="CANCELLED", model=job["embedding_model"], version=str(job["embedding_version"])
        ).inc()

    def _retry_job(self, job: dict[str, Any], exc: Exception) -> None:
        with get_db().connection() as conn:
            if self._embedding_written(conn, job):
                self._mark_ready(job)
                return
        EMBEDDING_FAILURES.labels(
            model=job["embedding_model"], version=str(job["embedding_version"]), error=exc.__class__.__name__
        ).inc()
        with get_db().transaction() as conn:
            self._retry_or_dead(conn, job)

    def _observe_ready_age(self, job: dict[str, Any]) -> None:
        with get_db().connection() as conn:
            created = conn.execute("SELECT created_at FROM customer_memory WHERE id=%s", (job["memory_id"],)).fetchone()
        if created:
            EMBEDDING_READY_AGE.labels(view_kind=job["view_kind"], model=job["embedding_model"]).observe(
                max(0.0, (utcnow() - created["created_at"]).total_seconds())
            )

    def _registry_specs(self, jobs: list[dict[str, Any]]) -> dict[tuple[str, int], dict[str, Any] | None]:
        keys = sorted({(job["embedding_model"], int(job["embedding_version"])) for job in jobs})
        specs: dict[tuple[str, int], dict[str, Any] | None] = {}
        with get_db().connection() as conn:
            for model, version in keys:
                row = conn.execute(
                    "SELECT dimension, config FROM embedding_model_registry WHERE model_id=%s AND version=%s",
                    (model, version),
                ).fetchone()
                specs[(model, version)] = dict(row) if row else None
        return specs

    def _embedding_written(self, conn: DbConnection, job: dict[str, Any]) -> bool:
        return (
            conn.execute(
                """SELECT 1 FROM memory_embedding
               WHERE customer_id=%s AND memory_id=%s AND embedding_model=%s AND embedding_version=%s
                 AND view_kind=%s AND view_language=%s AND view_text_hash=%s AND status='READY'""",
                (
                    job["customer_id"],
                    job["memory_id"],
                    job["embedding_model"],
                    job["embedding_version"],
                    job["view_kind"],
                    job["view_language"],
                    job["view_text_hash"],
                ),
            ).fetchone()
            is not None
        )

    def _mark_ready(self, job: dict[str, Any]) -> None:
        with get_db().transaction() as conn:
            conn.execute(
                "UPDATE memory_embedding_job SET status='READY', updated_at=now(), lease_expires_at=NULL "
                "WHERE id=%s AND status='RUNNING' AND locked_by=%s",
                (job["id"], self.worker_id),
            )
        EMBEDDING_JOBS.labels(status="READY", model=job["embedding_model"], version=str(job["embedding_version"])).inc()

    def _validate_vector(self, vector: list[float], spec: dict[str, Any] | None = None) -> None:
        # Doc 8.4 step 6: dimension and the L2 norm range come from the model registry,
        # not from process settings.
        dimension = int(spec["dimension"]) if spec else self.settings.embedding_dimension
        if len(vector) != dimension:
            EMBEDDING_INVALID_VECTOR.labels(reason="dimension").inc()
            raise ValueError("EMBEDDING_INVALID_VECTOR: dimension")
        if any(not math.isfinite(value) for value in vector):
            EMBEDDING_INVALID_VECTOR.labels(reason="non_finite").inc()
            raise ValueError("EMBEDDING_INVALID_VECTOR: non-finite")
        config = (spec or {}).get("config") or {}
        if config.get("normalize", True):
            bounds = config.get("norm_l2_range", [0.99, 1.01])
            norm = math.sqrt(sum(value * value for value in vector))
            if not (float(bounds[0]) <= norm <= float(bounds[1])):
                EMBEDDING_INVALID_VECTOR.labels(reason="norm_l2_range").inc()
                raise ValueError("EMBEDDING_INVALID_VECTOR: norm_l2_range")

    def _final_write(
        self, conn: DbConnection, job: dict[str, Any], vector: list[float], dimension: int | None = None
    ) -> bool:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"mms_barrier:{job['customer_id']}",))
        barrier = conn.execute(
            "SELECT status, generation, cutover_at FROM customer_deletion_barrier WHERE customer_id=%s",
            (job["customer_id"],),
        ).fetchone()
        required_generation = 0 if barrier is None else int(barrier["generation"])
        if barrier and barrier["status"] in {"ACTIVE", "COOLING_DOWN", "COMPLETED"}:
            self._cancel(conn, job, f"BARRIER_{barrier['status']}")
            return False
        if job["barrier_generation"] != required_generation or (
            barrier is not None and job["created_at"] < barrier["cutover_at"]
        ):
            self._cancel(conn, job, "STALE_GENERATION")
            return False
        owned = conn.execute(
            "SELECT 1 FROM memory_embedding_job WHERE id=%s AND status='RUNNING' AND locked_by=%s "
            "AND lease_expires_at > now() FOR UPDATE",
            (job["id"], self.worker_id),
        ).fetchone()
        if not owned:
            return False
        result = conn.execute(
            """INSERT INTO memory_embedding (memory_id, customer_id, embedding_model, embedding_version,
                 embedding_dim, content_hash, view_kind, view_language, view_text_hash, embedding, status, generated_at)
               SELECT m.id, m.customer_id, %s, %s, %s, %s, %s, %s, %s, %s::vector, 'READY', now()
               FROM customer_memory m
               WHERE m.id=%s AND m.customer_id=%s AND m.content_hash=%s
                 AND m.lifecycle_status IN ('ACTIVE','RESOLVED','CONFLICTED')
                 AND EXISTS (SELECT 1 FROM memory_search_document d
                   WHERE d.customer_id=m.customer_id AND d.memory_id=m.id AND d.view_kind=%s
                     AND d.view_language=%s AND d.view_text_hash=%s AND d.status='READY')
               ON CONFLICT (customer_id, memory_id, embedding_model, embedding_version, view_kind, view_language)
               DO UPDATE SET embedding=EXCLUDED.embedding, content_hash=EXCLUDED.content_hash,
                 view_text_hash=EXCLUDED.view_text_hash, status='READY', generated_at=now(), updated_at=now()
               RETURNING memory_id""",
            (
                job["embedding_model"],
                job["embedding_version"],
                dimension or self.settings.embedding_dimension,
                job["content_hash"],
                job["view_kind"],
                job["view_language"],
                job["view_text_hash"],
                vector_literal(vector),
                job["memory_id"],
                job["customer_id"],
                job["content_hash"],
                job["view_kind"],
                job["view_language"],
                job["view_text_hash"],
            ),
        ).fetchone()
        if not result:
            self._cancel(conn, job, "STALE_GENERATION")
            return False
        conn.execute(
            "UPDATE memory_embedding_job SET status='READY', updated_at=now(), lease_expires_at=NULL WHERE id=%s AND locked_by=%s",
            (job["id"], self.worker_id),
        )
        return True

    def _cancel(self, conn: DbConnection, job: dict[str, Any], reason: str) -> None:
        conn.execute(
            "UPDATE memory_embedding_job SET status='CANCELLED', last_error_code=%s, updated_at=now(), lease_expires_at=NULL "
            "WHERE id=%s AND status='RUNNING' AND locked_by=%s",
            (reason, job["id"], self.worker_id),
        )

    def _retry_or_dead(self, conn: DbConnection, job: dict[str, Any]) -> None:
        # attempts was already incremented on claim; +1 would burn a retry early.
        if int(job["attempts"]) >= self.settings.embedding_max_attempts:
            status = "DEAD_LETTER"
            available_at = None
        else:
            status = "RETRY"
            # Doc 10.7: exponential backoff carries jitter so a shared outage does not make
            # every worker retry in lockstep.
            available_at = f"{jittered(min(300, 2 ** int(job['attempts'])))} seconds"
        available_sql = sql.SQL("now()+(%s)::interval") if available_at else sql.SQL("now()")
        params = (
            (status, available_at, job["id"], self.worker_id) if available_at else (status, job["id"], self.worker_id)
        )
        conn.execute(
            sql.SQL(
                """UPDATE memory_embedding_job SET status=%s, available_at={available_at},
                      locked_by=NULL, locked_at=NULL, lease_expires_at=NULL, updated_at=now()
                    WHERE id=%s AND status='RUNNING' AND locked_by=%s"""
            ).format(available_at=available_sql),
            params,
        )

    def _refresh_oldest_age(self, last_refresh: float) -> float:
        # Doc 8.4 freshness SLO: PENDING oldest >5min must alert, so the gauge has to be
        # exported by the worker process, not only the API process.
        now = time.monotonic()
        if now - last_refresh < 5.0:
            return last_refresh
        bands = {"primary": 0.0, "translation": 0.0, "backfill": 0.0}
        with get_db().connection() as conn:
            rows = conn.execute(
                """SELECT CASE WHEN priority <= 100 THEN 'primary'
                          WHEN priority <= 120 THEN 'translation' ELSE 'backfill' END AS band,
                          EXTRACT(EPOCH FROM now()-min(created_at)) AS age
                   FROM memory_embedding_job WHERE status IN ('PENDING','RETRY')
                   GROUP BY band"""
            ).fetchall()
        for row in rows:
            bands[row["band"]] = max(0.0, float(row["age"] or 0.0))
        for band, age in bands.items():
            EMBEDDING_OLDEST_AGE.labels(priority_band=band).set(age)
        return now

    def run_forever(self) -> None:
        # Job-level spans are created per claimed batch; DB sub-spans come from psycopg
        # auto-instrumentation (docs/2 §3.4/§7.3, §9.1).
        last_refresh = 0.0
        while True:
            processed = self.process_batch()
            last_refresh = self._refresh_oldest_age(last_refresh)
            if not processed:
                time.sleep(self.settings.worker_poll_seconds)


def main() -> None:
    bootstrap_worker(get_settings())
    EmbeddingWorker().run_forever()


if __name__ == "__main__":
    main()
