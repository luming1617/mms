from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..config import get_settings

type DbRow = dict[str, Any]
type DbConnection = Connection[DbRow]


def require_row(row: DbRow | None, context: str = "query") -> DbRow:
    if row is None:
        raise LookupError(f"{context} returned no row")
    return row


class Database:
    def __init__(self, dsn: str | None = None):
        self.pool: ConnectionPool[DbConnection] = ConnectionPool(
            dsn or get_settings().database_url,
            min_size=1,
            max_size=8,
            kwargs={"row_factory": dict_row},
            open=True,
        )
        self.replica_pool: ConnectionPool[DbConnection] | None = None

    @contextmanager
    def connection(self) -> Iterator[DbConnection]:
        with self.pool.connection() as conn:
            yield conn

    @contextmanager
    def transaction(self) -> Iterator[DbConnection]:
        with self.pool.connection() as conn, conn.transaction():
            try:
                yield conn
            except psycopg.errors.RaiseException as exc:
                # Doc 18.3: schema guard rejections are observable. Trigger messages start
                # with the constraint name ("MEMORY_VIEWS_INCOMPLETE: ...").
                prefix = str(exc).split(":", 1)[0].strip() or "UNKNOWN"
                from .metrics import DDL_GUARD_REJECTED

                DDL_GUARD_REJECTED.labels(constraint=prefix).inc()
                raise

    @contextmanager
    def scrubber_transaction(self) -> Iterator[DbConnection]:
        """Doc 6.7.5: audit scrubbing runs on an independent mms_audit_scrubber
        connection, never on the deletion transaction's connection."""
        settings = get_settings()
        if settings.scrubber_database_url:
            with psycopg.connect(settings.scrubber_database_url, row_factory=dict_row) as conn, conn.transaction():
                yield conn
            return
        with self.pool.connection() as conn, conn.transaction():
            # Dev/test fallback: escalate a pooled connection for this transaction only. This
            # only works when the connecting role is a member of mms_audit_scrubber (docs 6.7.5);
            # say so explicitly instead of leaking a bare InsufficientPrivilege from deep inside
            # the deletion transaction.
            try:
                conn.execute("SET LOCAL ROLE mms_audit_scrubber")
            except psycopg.errors.InsufficientPrivilege as exc:
                raise RuntimeError(
                    "audit scrub needs MMS_SCRUBBER_DATABASE_URL (or a login role that is a member "
                    "of mms_audit_scrubber); the current account cannot assume the scrubber role"
                ) from exc
            yield conn

    @contextmanager
    def deletion_transaction(self) -> Iterator[DbConnection]:
        """Doc 8.3.3: the tombstone retention purge escalates to ``mms_deletion_worker``.

        That role is the only account with DELETE on ``deletion_tombstone``; the lifecycle
        scan's own role is deliberately denied so that no ordinary convergence process can drop a
        deletion proof. Same split as ``scrubber_transaction``: production points
        MMS_DELETION_DATABASE_URL at that login, and without it the transaction escalates
        with SET LOCAL ROLE, which only works when the connecting role is a member.
        """
        settings = get_settings()
        if settings.deletion_database_url:
            with psycopg.connect(settings.deletion_database_url, row_factory=dict_row) as conn, conn.transaction():
                yield conn
            return
        with self.pool.connection() as conn, conn.transaction():
            try:
                conn.execute("SET LOCAL ROLE mms_deletion_worker")
            except psycopg.errors.InsufficientPrivilege as exc:
                raise RuntimeError(
                    "tombstone purge needs MMS_DELETION_DATABASE_URL (or a login role that is a member "
                    "of mms_deletion_worker); the current account cannot assume the deletion role"
                ) from exc
            yield conn

    @contextmanager
    def read_connection(self, customer_id: str) -> Iterator[DbConnection]:
        with self.read_connection_meta(customer_id) as (conn, _):
            yield conn

    @contextmanager
    def read_connection_meta(self, customer_id: str) -> Iterator[tuple[DbConnection, bool]]:
        """Yield the connection plus whether it is the replica. Retrieval needs the flag: doc
        11.2 rule 1 keeps both channels on one pool, which only matters when a replica is used."""
        settings = get_settings()
        if not settings.read_replica_enabled:
            with self.connection() as conn:
                yield conn, False
            return
        with self.pool.connection() as primary:
            recent = primary.execute(
                "SELECT 1 FROM customer_memory WHERE customer_id=%s AND updated_at>now()-(%s || ' seconds')::interval LIMIT 1",
                (customer_id, settings.read_replica_write_hold_seconds),
            ).fetchone()
        if recent:
            with self.connection() as conn:
                yield conn, False
            return
        if self.replica_pool is None:
            replica_url = settings.read_replica_database_url
            if not replica_url:
                raise ValueError("MMS_READ_REPLICA_DATABASE_URL is required when the read replica is enabled")
            self.replica_pool = ConnectionPool(
                replica_url,
                min_size=0,
                max_size=4,
                timeout=2,
                kwargs={"row_factory": dict_row, "connect_timeout": 2},
                open=True,
            )
        with self.replica_pool.connection() as conn:
            yield conn, True

    def replica_health(self) -> bool | None:
        if not get_settings().read_replica_enabled:
            return None
        try:
            with self.read_connection("__health__") as conn:
                conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    def close_replica(self) -> None:
        if self.replica_pool is not None:
            self.replica_pool.close()
            self.replica_pool = None

    def close(self) -> None:
        self.close_replica()
        self.pool.close()


_db: Database | None = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
    return _db


class ConvergenceLock:
    """Doc 13.1: periodic convergence duties run single-instance/standby; a session-level
    advisory lock on a dedicated connection keeps a standby from double-executing scans."""

    def __init__(self, name: str):
        self.name = name
        self.conn: DbConnection | None = None

    def try_acquire(self) -> bool:
        if self.conn is not None:
            return True
        conn = psycopg.connect(get_settings().database_url, row_factory=dict_row, autocommit=True)
        row = conn.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS locked", (self.name,)).fetchone()
        if row is not None and row["locked"]:
            self.conn = conn
            return True
        conn.close()
        return False

    def release(self) -> None:
        conn = self.conn
        self.conn = None
        if conn is None:
            return
        try:
            conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (self.name,))
        finally:
            conn.close()
