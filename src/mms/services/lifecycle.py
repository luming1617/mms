from __future__ import annotations

import logging
import time

from ..config import get_settings
from ..observability.bootstrap import bootstrap_worker
from ..utils.db import SchedulerLock, get_db, require_row
from ..utils.errors import BarrierError
from ..utils.metrics import (
    CANDIDATE_EXPIRED_BY_SLA,
    CONFLICT_TIMEOUT_EXPIRED,
    DELETION_RETRY,
    DELETION_VERIFYING,
    LIFECYCLE_TRANSITIONS,
    TOMBSTONE_LEDGER_LAG,
)
from .deletion import DeletionService

logger = logging.getLogger(__name__)


class LifecycleService:
    def __init__(self, deletion_service: DeletionService | None = None):
        self.settings = get_settings()
        self.deletion_service = deletion_service or DeletionService()

    def run_once(self) -> dict[str, int]:
        counts = {}
        with get_db().transaction() as conn:
            result = conn.execute(
                """WITH expired AS (
                     SELECT c.candidate_id FROM memory_candidate c
                     LEFT JOIN memory_policy p ON p.name||'@'||p.version=c.policy_version
                     WHERE c.candidate_status IN ('RECEIVED','VALIDATED','PENDING_REVIEW')
                       AND c.created_at<now()-COALESCE((p.config->>'review_sla_days')::int,30)*interval '1 day'
                   ) UPDATE memory_candidate c SET candidate_status='EXPIRED',content_redacted=NULL,
                       source_refs='[]'::jsonb,semantic_text=NULL,updated_at=now()
                   FROM expired WHERE c.candidate_id=expired.candidate_id"""
            )
            counts["candidates_expired"] = result.rowcount
            result = conn.execute(
                """WITH ttl AS (
                     SELECT candidate_id FROM memory_candidate
                     WHERE candidate_status IN ('RECEIVED','VALIDATED') AND expires_at<=now()
                   ) UPDATE memory_candidate c SET candidate_status='EXPIRED',content_redacted=NULL,
                       source_refs='[]'::jsonb,semantic_text=NULL,updated_at=now()
                   FROM ttl WHERE c.candidate_id=ttl.candidate_id"""
            )
            counts["candidates_expired_by_ttl"] = result.rowcount
            result = conn.execute(
                """UPDATE memory_review_task t SET status='EXPIRED',resolved_at=now(),updated_at=now()
                   FROM memory_candidate c WHERE c.candidate_id=t.candidate_id AND c.candidate_status='EXPIRED'
                     AND t.status IN ('PENDING','IN_REVIEW')"""
            )
            counts["candidate_reviews_expired"] = result.rowcount
            result = conn.execute(
                """UPDATE customer_memory SET lifecycle_status='RESOLVED',current_flag=false,updated_at=now()
                   WHERE lifecycle_status IN ('ACTIVE','CONFLICTED') AND valid_to<=now()
                     AND memory_type IN ('SERVICE_HISTORY','ACTIVE_ISSUE','COMMITMENT')"""
            )
            counts["memories_resolved"] = result.rowcount
            result = conn.execute(
                """UPDATE memory_conflict c SET status='RESOLVED',resolution_reason_code='RESOLVED_BY_EVENT',resolved_at=now(),updated_at=now()
                   FROM customer_memory m WHERE m.id=c.current_memory_id AND m.lifecycle_status='RESOLVED'
                     AND c.status IN ('OPEN','IN_REVIEW')"""
            )
            counts["conflicts_resolved_by_event"] = result.rowcount
            result = conn.execute(
                """UPDATE memory_conflict SET status='RESOLVED',resolution_reason_code='CONFLICT_TIMEOUT_EXPIRED',resolved_at=now(),updated_at=now()
                   WHERE status IN ('OPEN','IN_REVIEW')
                     AND opened_at<now()-COALESCE((SELECT (config->>'conflict_max_age_days')::int
                       FROM memory_policy WHERE status='ACTIVE' AND memory_type IS NULL
                       ORDER BY effective_from DESC LIMIT 1),90)*interval '1 day'"""
            )
            counts["conflicts_resolved_by_timeout"] = result.rowcount
            result = conn.execute(
                """UPDATE memory_review_task t SET status='EXPIRED',resolved_at=now(),updated_at=now()
                   FROM memory_conflict c WHERE c.id=t.conflict_id AND c.status IN ('RESOLVED','DISMISSED')
                     AND t.status IN ('PENDING','IN_REVIEW')"""
            )
            counts["conflict_reviews_closed"] = result.rowcount
            completed_barriers = conn.execute(
                """UPDATE customer_deletion_barrier SET status='COMPLETED',updated_at=now()
                   WHERE status='COOLING_DOWN'
                     AND COALESCE(completed_at,updated_at)<now()-(%s || ' days')::interval
                   RETURNING customer_id""",
                (self.settings.deletion_cooldown_days,),
            ).fetchall()
            counts["barriers_completed"] = len(completed_barriers)
            for row in completed_barriers:
                conn.execute("SELECT pg_notify('mms_barrier_changed', %s)", (row["customer_id"],))
            result = conn.execute(
                """UPDATE customer_memory m SET lifecycle_status='EXPIRED',current_flag=false,updated_at=now()
                   WHERE (m.expires_at<=now() AND m.lifecycle_status IN ('ACTIVE','CONFLICTED','RESOLVED','SUPERSEDED'))
                      OR (m.lifecycle_status='CONFLICTED' AND EXISTS (
                        SELECT 1 FROM memory_conflict c
                        WHERE c.current_memory_id=m.id AND c.customer_id=m.customer_id
                          AND c.status IN ('OPEN','IN_REVIEW')
                          AND c.opened_at<now()-COALESCE((SELECT (config->>'conflict_max_age_days')::int
                            FROM memory_policy WHERE status='ACTIVE' AND memory_type IS NULL
                            ORDER BY effective_from DESC LIMIT 1),90)*interval '1 day'))"""
            )
            counts["memories_expired"] = result.rowcount
            result = conn.execute(
                """DELETE FROM customer_memory_current c USING customer_memory m
                   WHERE m.id=c.memory_id AND m.customer_id=c.customer_id
                     AND (NOT m.current_flag OR m.lifecycle_status NOT IN ('ACTIVE','CONFLICTED'))"""
            )
            counts["current_pointers_removed"] = result.rowcount
            result = conn.execute(
                """DELETE FROM memory_embedding e USING customer_memory m
                   WHERE m.id=e.memory_id AND m.customer_id=e.customer_id
                     AND m.lifecycle_status IN ('SUPERSEDED','EXPIRED','DELETE_PENDING','DELETED')"""
            )
            counts["embeddings_deleted"] = result.rowcount
            result = conn.execute(
                """UPDATE memory_search_document d SET status='DELETED',updated_at=now()
                   FROM customer_memory m WHERE m.id=d.memory_id AND m.customer_id=d.customer_id
                     AND m.lifecycle_status IN ('SUPERSEDED','EXPIRED','DELETE_PENDING','DELETED') AND d.status<>'DELETED'"""
            )
            counts["documents_deleted"] = result.rowcount
            result = conn.execute(
                """UPDATE memory_embedding_job j SET status='CANCELLED',last_error_code='LIFECYCLE_CLOSED',updated_at=now(),
                     locked_by=NULL,locked_at=NULL,lease_expires_at=NULL
                   FROM customer_memory m WHERE m.id=j.memory_id AND m.customer_id=j.customer_id
                     AND m.lifecycle_status IN ('SUPERSEDED','EXPIRED','DELETE_PENDING','DELETED')
                     AND j.status IN ('PENDING','RUNNING','RETRY')"""
            )
            counts["jobs_cancelled"] = result.rowcount
            result = conn.execute(
                """UPDATE memory_translation_job j SET status='CANCELLED',last_error_code='LIFECYCLE_CLOSED',updated_at=now(),
                     locked_by=NULL,locked_at=NULL,lease_expires_at=NULL
                   FROM customer_memory m WHERE m.id=j.memory_id AND m.customer_id=j.customer_id
                     AND m.lifecycle_status IN ('SUPERSEDED','EXPIRED','DELETE_PENDING','DELETED')
                     AND j.status IN ('PENDING','RUNNING','RETRY')"""
            )
            counts["translation_jobs_cancelled"] = result.rowcount
            result = conn.execute(
                "DELETE FROM memory_inbox_event WHERE status='COMPLETED' AND updated_at<now()-(%s || ' seconds')::interval",
                (self.settings.inbox_retention_days * 86400,),
            )
            counts["inbox_events_purged"] = result.rowcount
        for name, count in counts.items():
            if count:
                LIFECYCLE_TRANSITIONS.labels(transition=name).inc(count)
        if counts["candidates_expired"]:
            # Doc 6.2: SLA-expired candidates must be reported to the review-queue owner
            # (alert MMSAutomaticExpiryGrowing) and keep only the fingerprint + decision audit.
            CANDIDATE_EXPIRED_BY_SLA.inc(counts["candidates_expired"])
        if counts["conflicts_resolved_by_timeout"]:
            CONFLICT_TIMEOUT_EXPIRED.inc(counts["conflicts_resolved_by_timeout"])
        deletion_retries = self._retry_verifying_deletions()
        if deletion_retries:
            counts["deletion_retries_completed"] = deletion_retries
            LIFECYCLE_TRANSITIONS.labels(transition="deletion_retries_completed").inc(deletion_retries)
        retention_deletions = self._retention_consent_deletions()
        if retention_deletions:
            counts["retention_consent_deletions"] = retention_deletions
            LIFECYCLE_TRANSITIONS.labels(transition="retention_consent_deletions").inc(retention_deletions)
        purged = self._purge_expired_tombstones()
        if purged:
            LIFECYCLE_TRANSITIONS.labels(transition="tombstones_purged").inc(purged)
        return counts

    def _purge_expired_tombstones(self) -> int:
        """Doc 8.3.3: drop tombstone rows whose retention elapsed, on the deletion account.

        This must stay outside the scan transaction. The scan runs as ``mms_worker``, which
        has no DELETE grant on ``deletion_tombstone``, so running the purge there used to
        abort every expiry duty with ``InsufficientPrivilege``. Running it afterwards keeps
        the committed scan work intact and lets a purge failure surface on its own instead of
        silently skipping the retention duty.
        """
        with get_db().deletion_transaction() as conn:
            result = conn.execute("DELETE FROM deletion_tombstone WHERE retain_until<=now()")
        return result.rowcount

    def _retry_verifying_deletions(self) -> int:
        """Doc 6.8/10.7: a failed WORM ledger write must not strand a deletion request.

        The request stays VERIFYING and the barrier stays ACTIVE, so retrying the same
        request id (the tombstone key is idempotent per request+scope) against a healthy
        ledger is the only way the customer becomes deletable again. Retries are throttled
        by ``deletion_retry_seconds`` and never stop on their own; the gauge keeps a
        request stuck beyond the interval visible to alerting.
        """
        db = get_db()
        with db.connection() as conn:
            verifying_row = require_row(
                conn.execute(
                    """SELECT count(*) AS n,
                              coalesce(extract(epoch from (now()-min(updated_at))),0) AS lag
                       FROM memory_deletion_request WHERE status='VERIFYING'"""
                ).fetchone(),
                "verifying deletion count",
            )
            verifying = verifying_row["n"]
            DELETION_VERIFYING.set(verifying)
            # Docs 15.5 MMSTombstoneLedgerFailure: lag = how long the oldest VERIFYING request
            # has been waiting on the WORM ledger; success never lags (same-transaction write).
            TOMBSTONE_LEDGER_LAG.set(float(verifying_row["lag"]))
            if not verifying:
                return 0
            rows = conn.execute(
                """SELECT r.id, r.customer_id, r.approved_by
                   FROM memory_deletion_request r
                   JOIN customer_deletion_barrier b ON b.customer_id=r.customer_id AND b.status='ACTIVE'
                   WHERE r.status='VERIFYING' AND r.updated_at<now()-(%s || ' seconds')::interval
                   ORDER BY r.updated_at LIMIT 10""",
                (self.settings.deletion_retry_seconds,),
            ).fetchall()
        completed = 0
        for row in rows:
            try:
                with db.transaction() as conn:
                    self.deletion_service.repository.delete_customer_data(
                        conn,
                        row["customer_id"],
                        "mms-lifecycle",
                        "",
                        existing_request_id=row["id"],
                        approved_by=row["approved_by"],
                        ledger_writer=self.deletion_service.ledger.write,
                    )
            except Exception as exc:
                DELETION_RETRY.labels(result="failed").inc()
                logger.warning(
                    "deletion_verifying_retry_failed",
                    extra={"request_id": str(row["id"]), "error": exc.__class__.__name__},
                )
                continue
            completed += 1
            DELETION_RETRY.labels(result="completed").inc()
        if rows:
            with db.connection() as conn:
                remaining = require_row(
                    conn.execute(
                        "SELECT count(*) AS n FROM memory_deletion_request WHERE status='VERIFYING'"
                    ).fetchone(),
                    "verifying deletion count",
                )["n"]
            DELETION_VERIFYING.set(remaining)
        return completed

    def _retention_consent_deletions(self) -> int:
        """Doc 2.6 (P0-1) — MEMORY_RETENTION judgment point on the lifecycle/retention scan.

        An explicitly DENIED/WITHDRAWN (or expired) retention consent means the legal basis
        for holding memories is gone: skip retention and move the customer into the deletion
        queue (create/reuse a deletion request + barrier; execution stays review-gated like
        every other deletion). A merely missing snapshot only alerts via
        ``mms_consent_check_total{result=unavailable}`` — the write path already fails
        closed, and auto-barriering every customer during a consent-sync outage would turn
        a sync lag into a full-service outage.
        """
        db = get_db()
        with db.connection() as conn:
            rows = conn.execute(
                """SELECT DISTINCT m.customer_id, k.status, k.version
                   FROM customer_memory m
                   LEFT JOIN customer_memory_consent k
                     ON k.customer_id=m.customer_id AND k.purpose='MEMORY_RETENTION'
                   WHERE m.lifecycle_status IN ('ACTIVE','CONFLICTED')
                     AND (k.status IN ('DENIED','WITHDRAWN')
                          OR (k.status='GRANTED' AND k.expires_at IS NOT NULL AND k.expires_at<=now()))"""
            ).fetchall()
        if not rows:
            return 0
        service = self.deletion_service
        created = 0
        for row in rows:
            with db.transaction() as conn:
                current = conn.execute(
                    "SELECT status FROM customer_memory_consent WHERE customer_id=%s AND purpose='MEMORY_RETENTION' FOR UPDATE",
                    (row["customer_id"],),
                ).fetchone()
                if current is None or current["status"] == "GRANTED":
                    # Re-granted between the scan and this transaction — do not delete.
                    continue
                source = "CONSENT_WITHDRAWN" if current["status"] == "WITHDRAWN" else "SYSTEM"
                try:
                    service.request(
                        conn,
                        row["customer_id"],
                        requested_by="mms-lifecycle",
                        idempotency_key=f"retention-consent:{row['customer_id']}:{row['version']}",
                        request_source=source,
                    )
                    created += 1
                except BarrierError:
                    # Deletion already requested or completed for this customer.
                    continue
        return created

    def run_forever(self) -> None:
        # Doc 13.1: at most one lifecycle scheduler runs scans; a standby polls the
        # advisory lock instead of racing VERIFYING gauges and deletion retries.
        guard = SchedulerLock("mms:scheduler:lifecycle")
        last_recon = 0.0
        try:
            while True:
                if guard.try_acquire():
                    try:
                        self.run_once()
                        # Doc 18.3/18.4: reconciliation metrics need a scheduled producer;
                        # the lifecycle scheduler owns the single-instance duty.
                        if time.monotonic() - last_recon >= 300:
                            from .recon import ReconciliationService

                            try:
                                recon = ReconciliationService()
                                for recon_type in ("JOB", "VECTOR", "RECALL", "BUSINESS"):
                                    recon.run(recon_type)
                            except Exception:
                                # Provider/DB hiccups must not kill the lifecycle scheduler.
                                logging.getLogger(__name__).exception("scheduled_reconciliation_failed")
                            last_recon = time.monotonic()
                    finally:
                        guard.release()
                time.sleep(self.settings.lifecycle_poll_seconds)
        finally:
            guard.release()


def main() -> None:
    bootstrap_worker(get_settings())
    LifecycleService().run_forever()


if __name__ == "__main__":
    main()
