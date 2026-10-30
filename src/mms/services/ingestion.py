from __future__ import annotations

import time
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from ..config import get_settings
from ..models import CandidateInput
from ..observability.bootstrap import bootstrap_worker
from ..utils.backoff import jittered
from ..utils.db import DbConnection, get_db, require_row
from ..utils.embedding import provider_from_settings
from ..utils.errors import BarrierError, CandidateRejectedError
from ..utils.metrics import INBOX_DUPLICATES, INBOX_STALE, INGESTION_JOBS, INGESTION_OLDEST_AGE
from .repository import MemoryRepository
from .service import MemoryService


class IngestionService:
    def __init__(self, memory_service: MemoryService | None = None):
        self.settings = get_settings()
        self.memory_service = memory_service or MemoryService(
            MemoryRepository(),
            provider_from_settings(self.settings),
        )
        self.worker_id = f"ingestion-{uuid4()}"

    def accept(
        self,
        conn: DbConnection,
        candidate: CandidateInput,
        *,
        consumer_group: str,
        event_id: UUID,
        topic: str,
        partition: int,
        offset: int,
    ) -> dict[str, Any]:
        repository = self.memory_service.repository
        repository.lock_barrier(conn, candidate.customer_id)
        barrier = repository.barrier_snapshot(conn, candidate.customer_id)
        generation = 0 if barrier is None else int(barrier["generation"])
        if barrier is not None and candidate.observed_at < barrier["cutover_at"]:
            # Doc 8.3.1/14.5: a Kafka event that predates the deletion cutover must not be
            # resurrected even after the customer re-consents (barrier CANCELLED).
            raise BarrierError("STALE_GENERATION")
        carrier_created_at = require_row(conn.execute("SELECT now() AS ts").fetchone(), "carrier clock")["ts"]
        repository.verify_write_barrier(conn, candidate.customer_id, generation, carrier_created_at)
        inbox = conn.execute(
            """INSERT INTO memory_inbox_event (consumer_group,event_id,topic,partition,"offset",status,attempts)
               VALUES (%s,%s,%s,%s,%s,'PROCESSING',1)
               ON CONFLICT (consumer_group,event_id) DO NOTHING RETURNING event_id""",
            (consumer_group, event_id, topic, partition, offset),
        ).fetchone()
        if inbox is None:
            existing = require_row(
                conn.execute(
                    "SELECT * FROM memory_inbox_event WHERE consumer_group=%s AND event_id=%s FOR UPDATE",
                    (consumer_group, event_id),
                ).fetchone(),
                "inbox event",
            )
            if existing["status"] == "COMPLETED":
                INBOX_DUPLICATES.labels(result="completed").inc()
                return {"status": "COMPLETED", "duplicate": True}
            cutoff = require_row(
                conn.execute(
                    "SELECT now()-(%s || ' seconds')::interval AS cutoff", (self.settings.ingestion_lease_seconds,)
                ).fetchone(),
                "ingestion lease cutoff",
            )["cutoff"]
            if existing["status"] == "PROCESSING" and existing["updated_at"] > cutoff:
                INBOX_DUPLICATES.labels(result="processing").inc()
                return {"status": "PROCESSING", "duplicate": True}
            conn.execute(
                "UPDATE memory_inbox_event SET status='PROCESSING', attempts=attempts+1, last_error_code=NULL, updated_at=now() "
                "WHERE consumer_group=%s AND event_id=%s",
                (consumer_group, event_id),
            )
        payload = {
            "candidate": candidate.model_dump(mode="json"),
            "inbox": {
                "consumer_group": consumer_group,
                "event_id": str(event_id),
                "topic": topic,
                "partition": partition,
                "offset": offset,
            },
        }
        job = conn.execute(
            """INSERT INTO memory_ingestion_job
                 (id,customer_id,source_system,source_event_id,event_type,payload_ref,status,barrier_generation)
               VALUES (%s,%s,%s,%s,'memory.candidate.v1',%s,'RECEIVED',%s)
               ON CONFLICT (customer_id,source_system,source_event_id) DO UPDATE SET
                 payload_ref=EXCLUDED.payload_ref,status='RECEIVED',available_at=now(),locked_by=NULL,locked_at=NULL,
                 lease_expires_at=NULL,last_error_code=NULL,barrier_generation=EXCLUDED.barrier_generation,updated_at=now()
               WHERE memory_ingestion_job.status IN ('RECEIVED','RETRY','DEAD_LETTER','CANCELLED')
               RETURNING id""",
            (
                uuid4(),
                candidate.customer_id,
                candidate.source_system,
                candidate.source_event_id,
                Jsonb(payload),
                generation,
            ),
        ).fetchone()
        if job is None:
            job = require_row(
                conn.execute(
                    "SELECT id,status FROM memory_ingestion_job WHERE customer_id=%s AND source_system=%s AND source_event_id=%s",
                    (candidate.customer_id, candidate.source_system, candidate.source_event_id),
                ).fetchone(),
                "ingestion job",
            )
            if job["status"] == "COMPLETED":
                conn.execute(
                    "UPDATE memory_inbox_event SET status='COMPLETED',processed_at=now(),updated_at=now() "
                    "WHERE consumer_group=%s AND event_id=%s",
                    (consumer_group, event_id),
                )
        INGESTION_JOBS.labels(status="RECEIVED").inc()
        return {"job_id": job["id"], "status": job.get("status", "RECEIVED"), "duplicate": False}

    def claim(self, conn: DbConnection) -> dict[str, Any] | None:
        job = conn.execute(
            """SELECT * FROM memory_ingestion_job
               WHERE (status IN ('RECEIVED','RETRY') AND available_at<=now())
                  OR (status='PROCESSING' AND lease_expires_at<=now())
               ORDER BY available_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1"""
        ).fetchone()
        if not job:
            return None
        return dict(
            require_row(
                conn.execute(
                    """UPDATE memory_ingestion_job SET status='PROCESSING',locked_by=%s,locked_at=now(),
                 lease_expires_at=now()+(%s || ' seconds')::interval,attempts=attempts+1,updated_at=now()
               WHERE id=%s RETURNING *""",
                    (self.worker_id, self.settings.ingestion_lease_seconds, job["id"]),
                ).fetchone(),
                "ingestion claim",
            )
        )

    def process_one(self) -> bool:
        db = get_db()
        with db.transaction() as conn:
            job = self.claim(conn)
        if not job:
            return False
        inbox = job["payload_ref"]["inbox"]
        try:
            candidate = CandidateInput.model_validate(job["payload_ref"]["candidate"])
            # Candidate discovery (embedding provider I/O) stays outside the write transaction
            # and its advisory locks (doc 4.12).
            prepared = self.memory_service.prepare_candidate(candidate)
            with db.transaction() as conn:
                decision = self.memory_service.process_candidate(
                    conn,
                    candidate,
                    prepared=prepared,
                    carrier_created_at=job["created_at"],
                    carrier_barrier_generation=int(job["barrier_generation"]),
                )
                owned = conn.execute(
                    "UPDATE memory_ingestion_job SET status='COMPLETED',locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=now() "
                    "WHERE id=%s AND locked_by=%s AND status='PROCESSING' AND lease_expires_at>now() RETURNING id",
                    (job["id"], self.worker_id),
                ).fetchone()
                if owned is None:
                    # Lease was lost/reassigned; roll back so the new owner processes it.
                    raise RuntimeError("INGESTION_LEASE_LOST")
                conn.execute(
                    "UPDATE memory_inbox_event SET status='COMPLETED',processed_at=now(),last_error_code=NULL,updated_at=now() "
                    "WHERE consumer_group=%s AND event_id=%s",
                    (inbox["consumer_group"], inbox["event_id"]),
                )
            INGESTION_JOBS.labels(status="COMPLETED").inc()
            return decision is not None
        except (BarrierError, CandidateRejectedError) as exc:
            # Permanent failures (docs 1.3#7/2.6): a deletion barrier means no data may be
            # written for this customer (not even audit rows), and a candidate-level
            # rejection can never succeed on retry. Terminate the job with the reason
            # code instead of burning retries into the dead letter queue.
            with db.transaction() as conn:
                self._terminate_permanent(conn, job, exc)
            return True
        except Exception as exc:
            with db.transaction() as conn:
                self._retry_or_dead(conn, job, exc)
            return True

    def _terminate_permanent(
        self, conn: DbConnection, job: dict[str, Any], exc: BarrierError | CandidateRejectedError
    ) -> None:
        code = exc.code or exc.__class__.__name__
        conn.execute(
            """UPDATE memory_ingestion_job SET status='CANCELLED',last_error_code=%s,
                 locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=now()
               WHERE id=%s AND status='PROCESSING' AND locked_by=%s""",
            (code, job["id"], self.worker_id),
        )
        inbox = job["payload_ref"]["inbox"]
        conn.execute(
            "UPDATE memory_inbox_event SET status='COMPLETED',processed_at=now(),last_error_code=%s,updated_at=now() "
            "WHERE consumer_group=%s AND event_id=%s",
            (code, inbox["consumer_group"], inbox["event_id"]),
        )
        INGESTION_JOBS.labels(status="CANCELLED").inc()

    def _retry_or_dead(self, conn: DbConnection, job: dict[str, Any], exc: Exception) -> None:
        dead = int(job["attempts"]) >= self.settings.ingestion_max_attempts
        status = "DEAD_LETTER" if dead else "RETRY"
        # Doc 10.7: exponential backoff carries jitter so retries do not run in lockstep.
        delay = jittered(min(300, 2 ** max(0, int(job["attempts"]) - 1)))
        conn.execute(
            """UPDATE memory_ingestion_job SET status=%s,available_at=now()+(%s || ' seconds')::interval,
                 locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,last_error_code=%s,updated_at=now()
               WHERE id=%s AND status='PROCESSING' AND locked_by=%s""",
            (status, delay, exc.__class__.__name__, job["id"], self.worker_id),
        )
        inbox = job["payload_ref"]["inbox"]
        conn.execute(
            "UPDATE memory_inbox_event SET status=%s,last_error_code=%s,updated_at=now() WHERE consumer_group=%s AND event_id=%s",
            ("FAILED" if dead else "PROCESSING", exc.__class__.__name__, inbox["consumer_group"], inbox["event_id"]),
        )
        INGESTION_JOBS.labels(status=status).inc()

    def replay(self, conn: DbConnection, job_id: UUID) -> None:
        job = conn.execute("SELECT * FROM memory_ingestion_job WHERE id=%s FOR UPDATE", (job_id,)).fetchone()
        if not job:
            raise ValueError(f"ingestion job not found: {job_id}")
        conn.execute(
            "UPDATE memory_ingestion_job SET status='RECEIVED',attempts=0,available_at=now(),locked_by=NULL,locked_at=NULL,"
            "lease_expires_at=NULL,last_error_code=NULL,updated_at=now() WHERE id=%s",
            (job_id,),
        )
        inbox = job["payload_ref"]["inbox"]
        conn.execute(
            "UPDATE memory_inbox_event SET status='PROCESSING',attempts=attempts+1,last_error_code=NULL,updated_at=now() "
            "WHERE consumer_group=%s AND event_id=%s",
            (inbox["consumer_group"], inbox["event_id"]),
        )

    def _refresh_queue_signals(self, last_refresh: float) -> float:
        # Doc 10.3.1: inbox rows left in PROCESSING have no live worker behind them once the
        # retry backoff cap plus one lease has passed, and queued-job age is the ingestion
        # freshness SLO. Both must be exported by this process, not only by the API.
        now = time.monotonic()
        if now - last_refresh < 5.0:
            return last_refresh
        with get_db().connection() as conn:
            stale = require_row(
                conn.execute(
                    "SELECT count(*) AS n FROM memory_inbox_event"
                    " WHERE status='PROCESSING' AND updated_at<now()-(%s || ' seconds')::interval",
                    (self.settings.inbox_stale_seconds,),
                ).fetchone(),
                "stale inbox count",
            )["n"]
            oldest = require_row(
                conn.execute(
                    "SELECT EXTRACT(EPOCH FROM now()-min(created_at)) AS age FROM memory_ingestion_job"
                    " WHERE status IN ('RECEIVED','RETRY') AND available_at<=now()"
                ).fetchone(),
                "oldest ingestion job",
            )["age"]
        INBOX_STALE.set(int(stale or 0))
        INGESTION_OLDEST_AGE.set(max(0.0, float(oldest or 0.0)))
        return now

    def run_forever(self) -> None:
        last_refresh = 0.0
        while True:
            processed = self.process_one()
            last_refresh = self._refresh_queue_signals(last_refresh)
            if not processed:
                time.sleep(self.settings.worker_poll_seconds)


def main() -> None:
    bootstrap_worker(get_settings())
    IngestionService().run_forever()


if __name__ == "__main__":
    main()
