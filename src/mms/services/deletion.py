from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

import httpx
from psycopg.types.json import Jsonb

from ..config import get_settings
from ..utils.db import DbConnection
from ..utils.errors import BarrierError
from ..utils.hashing import sha256_canonical
from ..utils.outbound import auth_headers, require_secure_endpoint
from .repository import MemoryRepository


class DeletionLedger(Protocol):
    def write(self, key: str, payload: dict[str, Any]) -> None: ...


class LocalWormLedger:
    def __init__(self, directory: Path):
        self.directory = directory

    def write(self, key: str, payload: dict[str, Any]) -> None:
        # Doc 6.8: the object key is fixed per (request, scope_hash) and retries must
        # overwrite idempotently, so write a temp object and replace atomically.
        path = self.directory / key
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.flush()
        tmp.replace(path)


class HttpDeletionLedger:
    def __init__(self, endpoint: str, *, token: str | None = None, environment: str = "development"):
        self.endpoint = require_secure_endpoint(endpoint, setting="DELETION_LEDGER_ENDPOINT", environment=environment)
        self.headers = auth_headers(token)

    def write(self, key: str, payload: dict[str, Any]) -> None:
        response = httpx.put(f"{self.endpoint.rstrip('/')}/{key}", json=payload, timeout=10, headers=self.headers)
        response.raise_for_status()


class DeletionService:
    def __init__(self, repository: MemoryRepository | None = None, ledger: DeletionLedger | None = None):
        self.settings = get_settings()
        self.repository = repository or MemoryRepository()
        self.ledger = ledger or (
            HttpDeletionLedger(
                self.settings.deletion_ledger_endpoint,
                token=self.settings.outbound_token,
                environment=self.settings.deployment_environment,
            )
            if self.settings.deletion_ledger_endpoint
            else LocalWormLedger(self.settings.deletion_ledger_dir)
        )

    def request(
        self,
        conn: DbConnection,
        customer_id: str,
        requested_by: str,
        idempotency_key: str,
        *,
        request_source: str = "USER",
    ) -> dict[str, Any]:
        self.repository.lock_barrier(conn, customer_id)
        existing = conn.execute(
            "SELECT id,status FROM memory_deletion_request WHERE customer_id=%s AND request_idempotency_key=%s FOR UPDATE",
            (customer_id, idempotency_key),
        ).fetchone()
        if existing:
            return {"request_id": existing["id"], "status": existing["status"]}
        barrier = self.repository.barrier_snapshot(conn, customer_id)
        if barrier and barrier["status"] in {"ACTIVE", "COOLING_DOWN", "COMPLETED"}:
            raise BarrierError(barrier["status"])
        generation = 1 if barrier is None else int(barrier["generation"]) + 1
        request_id = uuid4()
        scope = {"all": True}
        conn.execute(
            """INSERT INTO memory_deletion_request
                 (id,customer_id,request_idempotency_key,request_source,requested_by,scope,scope_hash,status,cutover_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,'RECEIVED',now())""",
            (
                request_id,
                customer_id,
                idempotency_key,
                request_source,
                requested_by,
                Jsonb(scope),
                sha256_canonical(scope),
            ),
        )
        conn.execute(
            """INSERT INTO customer_deletion_barrier (customer_id,request_id,status,requested_at,cutover_at,generation)
               VALUES (%s,%s,'ACTIVE',now(),now(),%s)
               ON CONFLICT (customer_id) DO UPDATE SET request_id=EXCLUDED.request_id,status='ACTIVE',
                 requested_at=now(),cutover_at=now(),generation=EXCLUDED.generation,completed_at=NULL,updated_at=now()""",
            (customer_id, request_id, generation),
        )
        self.repository.insert_outbox(
            conn,
            request_id,
            "customer.deletion.barrier.changed.v1",
            {"customer_ref": self.repository.ensure_identity(conn, customer_id), "generation": generation},
            f"barrier:{customer_id}:{generation}",
            generation,
        )
        self.repository.invalidate_barrier_cache(customer_id)
        self.repository.notify_barrier_changed(conn, customer_id)
        task_id = uuid4()
        conn.execute(
            """INSERT INTO memory_review_task
                 (id,customer_id,task_type,deletion_request_id,status,risk_level,due_at,suggested_action,created_by)
               VALUES (%s,%s,'DELETION',%s,'PENDING','HIGH',now()+interval '30 days','APPROVE',%s)""",
            (task_id, customer_id, request_id, requested_by),
        )
        return {"request_id": request_id, "review_id": task_id, "status": "RECEIVED"}

    def request_consent_withdrawal(
        self, conn: DbConnection, customer_id: str, *, purpose: str, version: int, source: str = "consent-service"
    ) -> dict[str, Any]:
        """Doc 2.6 (P0-1): a WITHDRAWN consent is treated as a deletion request — create or
        reuse the deletion barrier (request_source=CONSENT_WITHDRAWN, tombstone
        reason_code=CONSENT_WITHDRAWN) and go through the same reviewed end-to-end deletion
        as 6.7. Merely stopping new writes while old memories stay recallable is forbidden.
        The idempotency key binds to (customer, purpose, consent version) so a replayed
        consent event never duplicates the request.
        """
        return self.request(
            conn,
            customer_id,
            requested_by=source,
            idempotency_key=f"consent-withdrawn:{customer_id}:{purpose}:{version}",
            request_source="CONSENT_WITHDRAWN",
        )

    def execute(self, conn: DbConnection, customer_id: str, request_id: UUID, reviewer_id: str) -> dict[str, Any]:
        return self.repository.delete_customer_data(
            conn,
            customer_id,
            "customer",
            "",
            existing_request_id=request_id,
            approved_by=reviewer_id,
            ledger_writer=self.ledger.write,
        )

    def get(self, conn: DbConnection, customer_id: str, request_id: UUID) -> dict[str, Any]:
        row = conn.execute(
            "SELECT id,customer_id,status,scope,proof,created_at,approved_by,approved_at,completed_at "
            "FROM memory_deletion_request WHERE id=%s AND customer_id=%s",
            (request_id, customer_id),
        ).fetchone()
        if not row:
            from ..utils.errors import MmsError

            raise MmsError("deletion request not found", code="NOT_FOUND", status_code=404)
        return dict(row)
