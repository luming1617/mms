import os
from datetime import UTC, datetime, timedelta

import pytest

DSN = os.environ.get("MMS_TEST_DATABASE_URL")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not DSN, reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests"),
]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, process, service

from mms.services.recon import ReconciliationService
from mms.services.translation import TranslationService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db


def test_recon_rebuilds_missing_canonical_view_and_requeues_vector():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "recon-repair"))
    process(EmbeddingWorker(provider))
    with get_db().transaction() as conn:
        conn.execute(
            "UPDATE memory_search_document SET status='STALE' WHERE customer_id=%s AND memory_id=%s AND view_kind='CANONICAL'",
            (customer, added.new_memory_id),
        )
        conn.execute(
            "DELETE FROM memory_embedding WHERE customer_id=%s AND memory_id=%s AND view_kind='CANONICAL'",
            (customer, added.new_memory_id),
        )
    stats = ReconciliationService().run("VECTOR")
    assert stats["details"]["repaired_views"]["canonical"] >= 1
    with get_db().connection() as conn:
        row = conn.execute(
            "SELECT status FROM memory_search_document WHERE customer_id=%s AND memory_id=%s AND view_kind='CANONICAL'",
            (customer, added.new_memory_id),
        ).fetchone()
    assert row["status"] == "READY"
    process(EmbeddingWorker(provider))
    with get_db().connection() as conn:
        vector = conn.execute(
            "SELECT status FROM memory_embedding WHERE customer_id=%s AND memory_id=%s AND view_kind='CANONICAL'",
            (customer, added.new_memory_id),
        ).fetchone()
    assert vector["status"] == "READY"
    delete(customer, "delete-" + customer)


def test_embedding_worker_skips_provider_for_closed_memory():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "worker-closed"))
    with get_db().transaction() as conn:
        conn.execute(
            "UPDATE customer_memory SET lifecycle_status='EXPIRED',current_flag=false WHERE id=%s AND customer_id=%s",
            (added.new_memory_id, customer),
        )
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    with get_db().connection() as conn:
        rows = conn.execute(
            "SELECT status,last_error_code FROM memory_embedding_job WHERE customer_id=%s", (customer,)
        ).fetchall()
    assert rows and all(row["status"] == "CANCELLED" for row in rows)
    assert {row["last_error_code"] for row in rows} == {"STALE_GENERATION"}
    delete(customer, "delete-" + customer)


def test_translation_pre_provider_gate_blocks_closed_memory():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "translation-gate"))
    process(EmbeddingWorker(provider))
    service_ = TranslationService()
    with get_db().connection() as conn:
        memory = conn.execute(
            "SELECT id,customer_id,content_hash FROM customer_memory WHERE id=%s AND customer_id=%s",
            (added.new_memory_id, customer),
        ).fetchone()
        job = {
            "customer_id": customer,
            "memory_id": memory["id"],
            "content_hash": memory["content_hash"],
            "barrier_generation": 0,
            "created_at": datetime.now(UTC),
        }
        assert service_._pre_provider_block(conn, job) is None
    with get_db().transaction() as conn:
        conn.execute(
            "UPDATE customer_memory SET lifecycle_status='SUPERSEDED',current_flag=false WHERE id=%s AND customer_id=%s",
            (added.new_memory_id, customer),
        )
    with get_db().connection() as conn:
        assert service_._pre_provider_block(conn, job) == "STALE_GENERATION"
    delete(customer, "delete-" + customer)


def test_lifecycle_convergence_produces_recon(monkeypatch):
    from mms.services.lifecycle import LifecycleService

    calls = []

    class FakeGuard:
        def try_acquire(self):
            return True

        def release(self):
            return None

    class FakeRecon:
        def run(self, recon_type):
            calls.append(recon_type)

    monkeypatch.setattr("mms.services.lifecycle.ConvergenceLock", lambda name: FakeGuard())
    monkeypatch.setattr("mms.services.recon.ReconciliationService", FakeRecon)
    monkeypatch.setattr("mms.services.lifecycle.time.sleep", lambda seconds: (_ for _ in ()).throw(SystemExit))

    with pytest.raises(SystemExit):
        LifecycleService().run_forever()
    assert calls == ["JOB", "VECTOR", "RECALL", "BUSINESS"]


def test_ddl_guard_rejection_is_counted():
    import psycopg

    from mms.utils.metrics import DDL_GUARD_REJECTED

    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "ddl-guard"))
    before = DDL_GUARD_REJECTED.labels(constraint="MEMORY_ORIGINAL_IMMUTABLE")._value.get()
    with pytest.raises(psycopg.errors.RaiseException), get_db().transaction() as conn:
        conn.execute(
            "UPDATE customer_memory SET content=content || 'x' WHERE id=%s AND customer_id=%s",
            (added.new_memory_id, customer),
        )
    after = DDL_GUARD_REJECTED.labels(constraint="MEMORY_ORIGINAL_IMMUTABLE")._value.get()
    assert after == before + 1
    delete(customer, "delete-" + customer)


def test_ingestion_rejects_event_older_than_deletion_cutover():
    from uuid import uuid4

    from mms.services.ingestion import IngestionService
    from mms.utils.errors import BarrierError

    customer = customer_id()
    grant_consent(customer)
    old = candidate(customer, "stale-replay")
    old = old.model_copy(update={"observed_at": datetime.now(UTC) - timedelta(hours=1)})
    with get_db().transaction() as conn:
        conn.execute(
            """INSERT INTO customer_deletion_barrier
                 (customer_id,request_id,status,requested_at,cutover_at,generation)
               VALUES (%s,%s,'CANCELLED',now(),now(),2)""",
            (customer, uuid4()),
        )
    with pytest.raises(BarrierError), get_db().transaction() as conn:
        IngestionService().accept(
            conn,
            old,
            consumer_group="replay-test",
            event_id=uuid4(),
            topic="memory-candidates",
            partition=0,
            offset=1,
        )
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM customer_deletion_barrier WHERE customer_id=%s", (customer,))


def test_partial_admin_headers_are_rejected():
    from fastapi.testclient import TestClient

    from mms.api.main import app

    http = TestClient(app)
    customer = customer_id()
    headers = {
        "X-Customer-ID": customer,
        "X-Service-Token": __import__("mms.config", fromlist=["get_settings"]).get_settings().service_token,
        "X-Reviewer-ID": "partial-admin",
    }
    response = http.post(
        "/api/v1/memories/query",
        headers=headers,
        json={"query": "hello", "purpose": "AUDIT", "language": "en-US"},
    )
    assert response.status_code == 401


def test_worker_pre_provider_reports_active_and_stale_barrier():
    from uuid import uuid4

    from mms.services.worker import EmbeddingWorker

    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "worker-barrier"))
    worker = EmbeddingWorker(provider)
    job = {
        "customer_id": customer,
        "memory_id": added.new_memory_id,
        "content_hash": __import__("mms.utils.hashing", fromlist=["sha256_text"]).sha256_text("以后请用中文沟通"),
        "barrier_generation": 0,
        "created_at": datetime.now(UTC),
    }
    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO customer_deletion_barrier (customer_id,request_id,status,requested_at,cutover_at,generation) "
            "VALUES (%s,%s,'ACTIVE',now(),now(),1)",
            (customer, uuid4()),
        )
    with get_db().connection() as conn:
        assert worker._pre_provider_block(conn, job) == "BARRIER_ACTIVE"
    with get_db().transaction() as conn:
        conn.execute(
            "UPDATE customer_deletion_barrier SET status='CANCELLED',generation=2,cutover_at=now() WHERE customer_id=%s",
            (customer,),
        )
    with get_db().connection() as conn:
        assert worker._pre_provider_block(conn, job) == "STALE_GENERATION"
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM customer_deletion_barrier WHERE customer_id=%s", (customer,))
    delete(customer, "delete-" + customer)


def test_lifecycle_convergence_survives_recon_failure(monkeypatch):
    from mms.services.lifecycle import LifecycleService

    class FakeGuard:
        def try_acquire(self):
            return True

        def release(self):
            return None

    class FailingRecon:
        def run(self, recon_type):
            raise RuntimeError("provider down")

    monkeypatch.setattr("mms.services.lifecycle.ConvergenceLock", lambda name: FakeGuard())
    monkeypatch.setattr("mms.services.recon.ReconciliationService", FailingRecon)
    monkeypatch.setattr("mms.services.lifecycle.time.sleep", lambda seconds: (_ for _ in ()).throw(SystemExit))

    with pytest.raises(SystemExit):
        LifecycleService().run_forever()


def test_worker_cross_border_consent_blocks_unregistered_provider():
    from mms.services.worker import EmbeddingWorker

    customer = customer_id()
    grant_consent(customer)
    worker = EmbeddingWorker()
    with get_db().connection() as conn:
        blocked = worker._cross_border_consent_block(
            conn, {"embedding_model": "unregistered-model", "embedding_version": 1, "customer_id": customer}
        )
    assert blocked in {"CROSS_BORDER_CONSENT_DENIED", "CROSS_BORDER_CONSENT_UNAVAILABLE"}
