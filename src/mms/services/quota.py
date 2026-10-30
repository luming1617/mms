from __future__ import annotations

from ..config import get_settings
from ..utils.db import DbConnection, require_row
from ..utils.errors import MmsError
from ..utils.metrics import QUOTA_REJECTED

_BUCKET_SETTINGS = {
    "WRITE": "quota_write_per_day",
    "EXTRACTION": "quota_extraction_per_day",
    "EMBEDDING": "quota_embedding_per_day",
}

# Rows that still hold customer content; superseded/expired/deleted history is not storage.
_STORED_STATUSES = ("ACTIVE", "CONFLICTED", "RESOLVED")


class QuotaService:
    """Doc 12.6: daily per-customer budgets for writes, extractions, embeddings and storage.

    Counters live in PostgreSQL rather than in a process so that every replica shares one
    budget, and a rejection is a client-visible 429 instead of a silent drop.
    """

    def __init__(self) -> None:
        self.settings = get_settings()

    def limit(self, bucket: str) -> int:
        return int(getattr(self.settings, _BUCKET_SETTINGS[bucket]))

    def _increment(self, conn: DbConnection, customer_id: str, bucket: str, count: int) -> int:
        row = conn.execute(
            """INSERT INTO customer_quota_usage (customer_id,bucket,day,used)
               VALUES (%s,%s,current_date,%s)
               ON CONFLICT (customer_id,bucket,day) DO UPDATE
                 SET used=customer_quota_usage.used+EXCLUDED.used, updated_at=now()
               RETURNING used""",
            (customer_id, bucket, count),
        ).fetchone()
        return int(require_row(row, "quota usage")["used"])

    def _used(self, conn: DbConnection, customer_id: str, bucket: str) -> int:
        row = conn.execute(
            "SELECT used FROM customer_quota_usage WHERE customer_id=%s AND bucket=%s AND day=current_date",
            (customer_id, bucket),
        ).fetchone()
        return int(row["used"]) if row else 0

    def consume(self, conn: DbConnection, customer_id: str, bucket: str) -> None:
        """Count one unit and reject the request once the daily budget is over."""
        limit = self.limit(bucket)
        if limit <= 0:
            return
        if self._increment(conn, customer_id, bucket, 1) > limit:
            self._reject(bucket)

    def check(self, conn: DbConnection, customer_id: str, bucket: str) -> None:
        limit = self.limit(bucket)
        if limit > 0 and self._used(conn, customer_id, bucket) >= limit:
            self._reject(bucket)

    def record(self, conn: DbConnection, customer_id: str, bucket: str, count: int = 1) -> None:
        """Count usage without gating: background work must not fail a client request."""
        if self.limit(bucket) > 0:
            self._increment(conn, customer_id, bucket, count)

    def check_storage(self, conn: DbConnection, customer_id: str) -> None:
        limit = int(self.settings.quota_active_memories)
        if limit <= 0:
            return
        row = conn.execute(
            """SELECT count(*) AS n FROM customer_memory
               WHERE customer_id=%s AND lifecycle_status = ANY(%s::text[])""",
            (customer_id, list(_STORED_STATUSES)),
        ).fetchone()
        if int(require_row(row, "stored memory count")["n"]) >= limit:
            self._reject("STORAGE")

    @staticmethod
    def _reject(bucket: str) -> None:
        QUOTA_REJECTED.labels(bucket=bucket).inc()
        raise MmsError(
            f"daily {bucket.lower()} quota exceeded for this customer",
            code="QUOTA_EXCEEDED",
            status_code=429,
        )
