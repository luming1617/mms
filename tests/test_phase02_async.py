import os
from uuid import uuid4

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MMS_TEST_DATABASE_URL"),
        reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests",
    ),
]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, service

from mms.services.deletion import DeletionService
from mms.services.ingestion import IngestionService
from mms.services.outbox import OutboxPublisher
from mms.services.recon import ReconciliationService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.errors import MmsError


def test_inbox_completed_is_the_only_skip_and_replay_is_idempotent():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    ingestion = IngestionService(svc)
    event_id = uuid4()
    item = candidate(customer, "async-event")
    with get_db().transaction() as conn:
        accepted = ingestion.accept(
            conn, item, consumer_group="test", event_id=event_id, topic="candidates", partition=0, offset=1
        )
    assert accepted["status"] == "RECEIVED"
    with get_db().transaction() as conn:
        duplicate = ingestion.accept(
            conn, item, consumer_group="test", event_id=event_id, topic="candidates", partition=0, offset=1
        )
    assert duplicate["duplicate"] and duplicate["status"] == "PROCESSING"
    assert ingestion.process_one()
    for offset in range(2, 22):
        with get_db().transaction() as conn:
            repeated = ingestion.accept(
                conn, item, consumer_group="test", event_id=event_id, topic="candidates", partition=0, offset=offset
            )
        assert repeated["status"] == "COMPLETED" and repeated["duplicate"]
    with get_db().connection() as conn:
        counts = conn.execute(
            "SELECT (SELECT count(*) FROM memory_decision WHERE customer_id=%s) AS decisions,"
            "(SELECT count(*) FROM memory_ingestion_job WHERE customer_id=%s AND status='COMPLETED') AS jobs,"
            "(SELECT count(*) FROM memory_inbox_event WHERE consumer_group='test' AND event_id=%s AND status='COMPLETED') AS inbox",
            (customer, customer, event_id),
        ).fetchone()
    assert counts == {"decisions": 1, "jobs": 1, "inbox": 1}
    delete(customer, "delete-" + customer)


class RecordingTransport:
    def __init__(self):
        self.events = []

    def publish(self, event):
        self.events.append(event)


class FailingTransport:
    def publish(self, event):
        raise RuntimeError("broker unavailable")


def create_outbox(customer):
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "outbox-event"))
    with get_db().connection() as conn:
        return conn.execute(
            "SELECT * FROM memory_outbox WHERE aggregate_id IN (SELECT id FROM customer_memory WHERE customer_id=%s)",
            (customer,),
        ).fetchone()


def test_outbox_v2_publish_failure_and_replay():
    customer = customer_id()
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM memory_outbox")
    row = create_outbox(customer)
    transport = RecordingTransport()
    publisher = OutboxPublisher(transport)
    assert publisher.process_one()
    assert transport.events[0]["source_system"] == "canary" or transport.events[0]["source_system"] == "integration"
    assert transport.events[0]["customer_ref"] == row["partition_key"]
    with get_db().connection() as conn:
        assert (
            conn.execute("SELECT status FROM memory_outbox WHERE id=%s", (row["id"],)).fetchone()["status"]
            == "PUBLISHED"
        )

    failing = OutboxPublisher(FailingTransport())
    failing.settings.outbox_max_attempts = 1
    with get_db().transaction() as conn:
        replayed = conn.execute(
            "UPDATE memory_outbox SET status='RETRY',publish_attempts=0,locked_by=NULL,locked_at=NULL WHERE id=%s RETURNING id",
            (row["id"],),
        ).fetchone()
        assert replayed
    assert failing.process_one()
    with get_db().connection() as conn:
        assert (
            conn.execute("SELECT status FROM memory_outbox WHERE id=%s", (row["id"],)).fetchone()["status"] == "FAILED"
        )
    with get_db().transaction() as conn:
        failing.replay(conn, row["id"])
    successful = OutboxPublisher(RecordingTransport())
    assert successful.process_one()
    with get_db().connection() as conn:
        assert (
            conn.execute("SELECT status FROM memory_outbox WHERE id=%s", (row["id"],)).fetchone()["status"]
            == "PUBLISHED"
        )
    delete(customer, "delete-" + customer)


def test_outbox_publish_and_replay_are_gated_by_deletion_barrier():
    customer = customer_id()
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM memory_outbox")
    row = create_outbox(customer)
    with get_db().transaction() as conn:
        DeletionService().request(conn, customer, "customer", "delete-" + customer)

    transport = RecordingTransport()
    publisher = OutboxPublisher(transport)
    assert publisher.process_one()
    assert transport.events == []
    with get_db().connection() as conn:
        stored = conn.execute("SELECT status,last_error FROM memory_outbox WHERE id=%s", (row["id"],)).fetchone()
    assert stored["status"] == "RETRY" and stored["last_error"] == "BARRIER_BLOCKED"
    with get_db().transaction() as conn, pytest.raises(MmsError) as excinfo:
        publisher.replay(conn, row["id"])
    assert excinfo.value.code == "BARRIER_BLOCKED"

    # Cleanup runs with a fresh idempotency key so the reviewed request above stays pending.
    delete(customer, "delete-cleanup-" + customer)
    while publisher.process_one():
        pass
    event_types = [event["event_type"] for event in transport.events]
    # Deletion-domain events are exempt so the tombstone/barrier transition still flows.
    assert "customer.deletion.barrier.changed.v1" in event_types
    assert "memory.created.v1" not in event_types


def test_job_vector_recall_and_business_reconciliation():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "recon-event"))
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    recon = ReconciliationService()
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM customer_memory_current WHERE customer_id=%s", (customer,))
    business = recon.run("BUSINESS")
    assert business["fixed_count"] == 1
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM memory_embedding WHERE customer_id=%s AND view_kind='ORIGINAL'", (customer,))
    job = recon.run("JOB")
    assert job["details"]["ready_job_missing_vector"] == 1
    while worker.process_one():
        pass
    recall = recon.run("RECALL")
    assert recall["status"] == "COMPLETED" and recall["mismatch_count"] == 0
    with get_db().transaction() as conn:
        conn.execute("UPDATE customer_memory SET lifecycle_status='SUPERSEDED' WHERE id=%s", (added.new_memory_id,))
    vector = recon.run("VECTOR")
    assert vector["fixed_count"] >= 2
    with get_db().connection() as conn:
        assert (
            conn.execute(
                "SELECT count(*) AS n FROM memory_embedding WHERE customer_id=%s AND status='READY'", (customer,)
            ).fetchone()["n"]
            == 0
        )
        assert (
            conn.execute(
                "SELECT count(*) AS n FROM recon_run WHERE recon_type IN ('JOB','VECTOR','RECALL','BUSINESS')"
            ).fetchone()["n"]
            >= 4
        )
    delete(customer, "delete-" + customer)
