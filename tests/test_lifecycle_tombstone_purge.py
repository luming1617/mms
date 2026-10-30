import os
from uuid import uuid4

import pytest
from psycopg.errors import RaiseException

DSN = os.environ.get("MMS_TEST_DATABASE_URL")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not DSN, reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests"),
]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, process, service

from mms.services.lifecycle import LifecycleService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.hashing import utcnow


def seed_active_memory(customer):
    svc, provider = service()
    with get_db().transaction() as conn:
        decision = svc.process_candidate(conn, candidate(customer, f"seed-{uuid4()}"))
    assert decision.outcome == "ADD"
    process(EmbeddingWorker(provider))
    return decision.new_memory_id


def test_expired_memories_commit_even_when_the_tombstone_purge_fails(monkeypatch):
    customer = customer_id()
    grant_consent(customer)
    memory_id = seed_active_memory(customer)
    with get_db().transaction() as conn:
        conn.execute("UPDATE customer_memory SET expires_at=now()-interval '1 day' WHERE id=%s", (memory_id,))

    def purge_denied(self):
        raise RuntimeError("INSUFFICIENT_PRIVILEGE: deletion_tombstone")

    # Docs 8.3.3: the purge runs on the deletion account, after the scan committed. Before the
    # split it ran inside the scan as mms_worker (which holds no DELETE grant), so a single
    # privilege error rolled back every expiry, conflict-timeout and deletion-retry duty.
    monkeypatch.setattr(LifecycleService, "_purge_expired_tombstones", purge_denied)
    with pytest.raises(RuntimeError):
        LifecycleService().run_once()

    with get_db().connection() as conn:
        status = conn.execute("SELECT lifecycle_status FROM customer_memory WHERE id=%s", (memory_id,)).fetchone()
    assert status["lifecycle_status"] == "EXPIRED"
    # The expired memory keeps its two READY embedding-job rows (8.3.5 cancels only
    # PENDING/RUNNING/RETRY), and recon's ready_job_missing_vector repair re-pends them. That
    # churn is a separate defect; drop the rows so this test leaves no global state behind.
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM memory_embedding_job WHERE customer_id=%s", (customer,))


def test_tombstone_purge_only_drops_expired_rows():
    customer = customer_id()
    grant_consent(customer)
    seed_active_memory(customer)
    delete(customer, f"purge-{uuid4()}")
    with get_db().connection() as conn:
        row = conn.execute(
            "SELECT tombstone_id, retain_until FROM deletion_tombstone WHERE customer_id=%s", (customer,)
        ).fetchone()
    assert row is not None and row["retain_until"] > utcnow()

    assert LifecycleService()._purge_expired_tombstones() == 0
    with get_db().connection() as conn, pytest.raises(RaiseException) as guard:
        conn.execute("DELETE FROM deletion_tombstone WHERE tombstone_id=%s", (row["tombstone_id"],))
    assert "TOMBSTONE_RETENTION_NOT_EXPIRED" in str(guard.value)

    with get_db().transaction() as conn:
        conn.execute(
            """UPDATE deletion_tombstone
                  SET completed_at=now()-interval '800 days',
                      ledger_written_at=now()-interval '801 days',
                      retain_until=now()-interval '1 day'
                WHERE customer_id=%s""",
            (customer,),
        )
    assert LifecycleService()._purge_expired_tombstones() >= 1
    with get_db().connection() as conn:
        remaining = conn.execute(
            "SELECT count(*) AS n FROM deletion_tombstone WHERE customer_id=%s", (customer,)
        ).fetchone()
    assert remaining["n"] == 0
