from __future__ import annotations

import json
import logging
import time
from typing import Any, Protocol
from uuid import UUID, uuid4

import httpx
from jsonschema import Draft202012Validator

from ..config import get_settings
from ..observability.bootstrap import bootstrap_worker
from ..utils.backoff import jittered
from ..utils.db import DbConnection, get_db, require_row
from ..utils.errors import BarrierError, MmsError
from ..utils.metrics import (
    BARRIER_POST_COMMIT_WRITES,
    OUTBOX_FAILED,
    OUTBOX_OLDEST_AGE,
    OUTBOX_PENDING,
    OUTBOX_PUBLISHED,
)
from ..utils.outbound import auth_headers, require_secure_endpoint
from .repository import MemoryRepository

logger = logging.getLogger(__name__)


class OutboxTransport(Protocol):
    def publish(self, event: dict[str, Any]) -> None: ...


class LocalOutboxTransport:
    def publish(self, event: dict[str, Any]) -> None:
        logger.info("published event_id=%s event_type=%s", event["event_id"], event["event_type"])


class HttpOutboxTransport:
    def __init__(self, endpoint: str, *, token: str | None = None, environment: str = "development"):
        self.endpoint = require_secure_endpoint(endpoint, setting="OUTBOX_ENDPOINT", environment=environment)
        self.headers = auth_headers(token)

    def publish(self, event: dict[str, Any]) -> None:
        response = httpx.post(self.endpoint, json=event, timeout=10, headers=self.headers)
        response.raise_for_status()


class OutboxPublisher:
    def __init__(self, transport: OutboxTransport | None = None):
        self.settings = get_settings()
        self.transport = transport or (
            HttpOutboxTransport(
                self.settings.outbox_endpoint,
                token=self.settings.outbound_token,
                environment=self.settings.deployment_environment,
            )
            if self.settings.outbox_endpoint
            else LocalOutboxTransport()
        )
        self.worker_id = f"outbox-{uuid4()}"
        schema = json.loads((self.settings.root / "contracts" / "event-envelope.schema.json").read_text())
        self.validator = Draft202012Validator(schema)

    def claim(self, conn: DbConnection) -> dict[str, Any] | None:
        row = conn.execute(
            """SELECT * FROM memory_outbox
               WHERE status IN ('PENDING','RETRY') AND available_at<=now()
                 AND (locked_at IS NULL OR locked_at<now()-(%s || ' seconds')::interval)
               ORDER BY available_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1""",
            (self.settings.outbox_lease_seconds,),
        ).fetchone()
        if not row:
            return None
        return dict(
            require_row(
                conn.execute(
                    "UPDATE memory_outbox SET locked_by=%s,locked_at=now(),publish_attempts=publish_attempts+1 WHERE id=%s RETURNING *",
                    (self.worker_id, row["id"]),
                ).fetchone(),
                "outbox claim",
            )
        )

    def event_for(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = dict(row["payload_json"])
        customer_ref = row["partition_key"] or payload.pop("customer_ref", "")
        event = {
            "event_id": str(row["event_id"]),
            "event_type": row["event_type"],
            "event_version": 1,
            "occurred_at": row["created_at"].isoformat(),
            "customer_ref": customer_ref,
            "source_system": payload.pop("source_system", "mms"),
            "trace_id": payload.pop("trace_id", str(row["event_id"])),
            "payload": payload,
            "barrier_generation": row["barrier_generation"],
        }
        self.validator.validate(event)
        return event

    def _barrier_rejection(self, conn: DbConnection, row: dict[str, Any]) -> str | None:
        """Doc 6.7.1: publishing is a write to the outside world and must pass the same
        barrier gate as candidate receipt and decision commits (DLQ replay included).
        Deletion-domain events are exempt — the tombstone itself has to go out."""
        if row["event_type"].startswith("customer.deletion."):
            return None
        identity = conn.execute(
            "SELECT customer_id FROM customer_identity WHERE customer_ref=%s", (row["partition_key"],)
        ).fetchone()
        if identity is None:
            # Doc 6.7.3: unknown customer_ref is never guessed at; hold it for operators.
            return "CUSTOMER_REF_UNKNOWN"
        try:
            MemoryRepository().verify_write_barrier(
                conn, str(identity["customer_id"]), int(row["barrier_generation"]), row["created_at"]
            )
        except BarrierError as exc:
            if exc.barrier_status == "STALE_GENERATION":
                return "STALE_GENERATION"
            # Docs 15.5 MMSBarrierPostCommitWrite: the row is itself a committed write, so an
            # active barrier with a carrier newer than cutover means a write landed after the
            # barrier was raised even though the in-transaction check passed.
            cutover = conn.execute(
                "SELECT cutover_at FROM customer_deletion_barrier WHERE customer_id=%s",
                (str(identity["customer_id"]),),
            ).fetchone()
            if cutover and row["created_at"] > cutover["cutover_at"]:
                BARRIER_POST_COMMIT_WRITES.inc()
            return "BARRIER_BLOCKED"
        return None

    def _defer(self, row: dict[str, Any], reason: str, *, retry: bool) -> None:
        delay = self.settings.outbox_barrier_retry_seconds if retry else 0
        with get_db().transaction() as conn:
            conn.execute(
                "UPDATE memory_outbox SET status=%s,available_at=now()+(%s || ' seconds')::interval,"
                "locked_by=NULL,locked_at=NULL,last_error=%s WHERE id=%s AND locked_by=%s",
                ("RETRY" if retry else "FAILED", delay, reason, row["id"], self.worker_id),
            )

    def process_one(self) -> bool:
        db = get_db()
        with db.transaction() as conn:
            row = self.claim(conn)
        if not row:
            self._refresh_metrics()
            return False
        try:
            with db.transaction() as conn:
                rejection = self._barrier_rejection(conn, row)
        except Exception as exc:
            # Fail closed: an unverifiable barrier never publishes (doc 6.7.1).
            logger.warning(
                "outbox_barrier_check_failed",
                extra={"event_id": str(row["event_id"]), "error": exc.__class__.__name__},
            )
            self._defer(row, "BARRIER_CHECK_FAILED", retry=True)
            OUTBOX_PUBLISHED.labels(result="barrier_check_failed").inc()
            self._refresh_metrics()
            return True
        if rejection:
            self._defer(row, rejection, retry=rejection == "BARRIER_BLOCKED")
            OUTBOX_PUBLISHED.labels(result=rejection.lower()).inc()
            self._refresh_metrics()
            return True
        try:
            self.transport.publish(self.event_for(row))
            with db.transaction() as conn:
                marked = conn.execute(
                    "UPDATE memory_outbox SET status='PUBLISHED',published_at=now(),locked_by=NULL,locked_at=NULL,last_error=NULL "
                    "WHERE id=%s AND locked_by=%s",
                    (row["id"], self.worker_id),
                ).rowcount
            if marked:
                OUTBOX_PUBLISHED.labels(result="published").inc()
            else:
                # The row vanished while the event was in flight (a deletion raced the
                # publish): the event is out, but it must not count as a success.
                logger.warning(
                    "outbox_row_vanished_during_publish",
                    extra={"event_id": str(row["event_id"]), "outbox_id": str(row["id"])},
                )
                OUTBOX_PUBLISHED.labels(result="row_gone").inc()
        except Exception as exc:
            with db.transaction() as conn:
                failed = int(row["publish_attempts"]) >= self.settings.outbox_max_attempts
                delay = jittered(min(300, 2 ** max(0, int(row["publish_attempts"]) - 1)))
                conn.execute(
                    "UPDATE memory_outbox SET status=%s,available_at=now()+(%s || ' seconds')::interval,"
                    "locked_by=NULL,locked_at=NULL,last_error=%s WHERE id=%s AND locked_by=%s",
                    ("FAILED" if failed else "RETRY", delay, exc.__class__.__name__, row["id"], self.worker_id),
                )
            OUTBOX_PUBLISHED.labels(result="failed" if failed else "retry").inc()
        self._refresh_metrics()
        return True

    def replay(self, conn: DbConnection, outbox_id: UUID) -> None:
        row = conn.execute("SELECT * FROM memory_outbox WHERE id=%s FOR UPDATE", (outbox_id,)).fetchone()
        if not row or row["status"] not in ("FAILED", "RETRY"):
            raise ValueError(f"replayable outbox row not found: {outbox_id}")
        # Doc 6.7.1: DLQ replay must pass the barrier gate before the row re-enters the queue.
        rejection = self._barrier_rejection(conn, row)
        if rejection:
            raise MmsError(
                f"outbox replay refused: {rejection}",
                code=rejection,
                status_code=410 if rejection == "STALE_GENERATION" else 409,
            )
        conn.execute(
            "UPDATE memory_outbox SET status='PENDING',available_at=now(),locked_by=NULL,locked_at=NULL,last_error=NULL "
            "WHERE id=%s",
            (outbox_id,),
        )

    def _refresh_metrics(self) -> None:
        with get_db().connection() as conn:
            row = require_row(
                conn.execute(
                    """SELECT count(*) FILTER (WHERE status IN ('PENDING','RETRY')) AS pending,
                              count(*) FILTER (WHERE status='FAILED') AS failed,
                              extract(epoch from (now()-min(created_at)
                                FILTER (WHERE status IN ('PENDING','RETRY')))) AS oldest_age
                       FROM memory_outbox"""
                ).fetchone(),
                "outbox counts",
            )
        OUTBOX_PENDING.set(row["pending"])
        OUTBOX_OLDEST_AGE.set(row["oldest_age"] or 0.0)
        # Doc 10.7: FAILED is terminal and invisible to the pending gauge, so it needs its own
        # series or a dead letter only ever exists in a log line.
        OUTBOX_FAILED.set(row["failed"])

    def run_forever(self) -> None:
        while True:
            if not self.process_one():
                time.sleep(self.settings.worker_poll_seconds)


def main() -> None:
    bootstrap_worker(get_settings())
    OutboxPublisher().run_forever()


if __name__ == "__main__":
    main()
