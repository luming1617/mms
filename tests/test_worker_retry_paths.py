import os
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MMS_TEST_DATABASE_URL"),
        reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests",
    ),
]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, service

from mms.services.ingestion import IngestionService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db


class FailingProvider:
    dimension = 1024

    def embed_documents(self, texts, model):
        raise ValueError("provider down")

    def embed_query(self, text, model):
        raise ValueError("provider down")

    def health(self):
        return False


def test_embedding_retry_dead_letter_and_invalid_vectors():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "retry-event"))
        conn.execute(
            "UPDATE memory_embedding_job SET available_at=now()+interval '1 hour' "
            "WHERE customer_id<>%s AND status IN ('PENDING','RETRY')",
            (customer,),
        )
    worker = EmbeddingWorker(FailingProvider(), worker_id="failing-worker")
    original_max_attempts = worker.settings.embedding_max_attempts
    while worker.process_one():
        pass
    with get_db().connection() as conn:
        statuses = [
            row["status"]
            for row in conn.execute(
                "SELECT status FROM memory_embedding_job WHERE customer_id=%s", (customer,)
            ).fetchall()
        ]
    assert statuses and set(statuses) == {"RETRY"}

    worker.settings.embedding_max_attempts = 1
    with get_db().transaction() as conn:
        conn.execute(
            "UPDATE memory_embedding_job SET status='PENDING', attempts=1, available_at=now() WHERE customer_id=%s",
            (customer,),
        )
    assert worker.process_one()
    with get_db().connection() as conn:
        statuses = [
            row["status"]
            for row in conn.execute(
                "SELECT status FROM memory_embedding_job WHERE customer_id=%s", (customer,)
            ).fetchall()
        ]
    assert "DEAD_LETTER" in statuses
    worker.settings.embedding_max_attempts = original_max_attempts

    validator = EmbeddingWorker(provider, worker_id="validator")
    for invalid in (
        [0.0] * 3,
        [float("nan")] * validator.settings.embedding_dimension,
        [0.0] * validator.settings.embedding_dimension,
    ):
        with pytest.raises(ValueError):
            validator._validate_vector(invalid)
    delete(customer, "delete-" + customer)


def test_embedding_view_not_ready_is_cancelled():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "view-not-ready"))
        conn.execute(
            "UPDATE memory_embedding_job SET available_at=now()+interval '1 hour' "
            "WHERE customer_id<>%s AND status IN ('PENDING','RETRY')",
            (customer,),
        )
        job = conn.execute(
            "SELECT * FROM memory_embedding_job WHERE customer_id=%s ORDER BY created_at LIMIT 1",
            (customer,),
        ).fetchone()
        conn.execute(
            "UPDATE memory_embedding_job SET available_at=now()+interval '1 hour' WHERE customer_id=%s",
            (customer,),
        )
        conn.execute(
            "UPDATE memory_embedding_job SET view_text_hash=%s, available_at=now() WHERE id=%s",
            ("0" * 64, job["id"]),
        )
    worker = EmbeddingWorker(provider, worker_id="no-view-worker")
    assert worker.process_one()
    with get_db().connection() as conn:
        cancelled = conn.execute(
            "SELECT status, last_error_code FROM memory_embedding_job WHERE id=%s", (job["id"],)
        ).fetchone()
    assert cancelled == {"status": "CANCELLED", "last_error_code": "VIEW_NOT_READY"}
    delete(customer, "delete-" + customer)


def test_ingestion_retry_replay_and_dead_letter():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    ingestion = IngestionService(svc)
    event_id = uuid4()
    with get_db().transaction() as conn:
        accepted = ingestion.accept(
            conn,
            candidate(customer, "ingestion-retry"),
            consumer_group="retry-test",
            event_id=event_id,
            topic="candidates",
            partition=0,
            offset=1,
        )
        conn.execute(
            "UPDATE memory_ingestion_job SET available_at=now()+interval '1 hour' "
            "WHERE customer_id<>%s AND status IN ('RECEIVED','RETRY')",
            (customer,),
        )
    job_id = accepted["job_id"]

    # Permanent candidate-level failure: an unknown fact key terminates the job with the
    # reason code instead of burning retries into the dead letter queue (docs 4.11).
    with get_db().transaction() as conn:
        row = conn.execute("SELECT payload_ref FROM memory_ingestion_job WHERE id=%s", (job_id,)).fetchone()
        payload = row["payload_ref"]
        payload["candidate"]["fact_key"] = "unknown.fact"
        conn.execute("UPDATE memory_ingestion_job SET payload_ref=%s WHERE id=%s", (Jsonb(payload), job_id))
    assert ingestion.process_one()
    with get_db().connection() as conn:
        job = conn.execute("SELECT status, last_error_code FROM memory_ingestion_job WHERE id=%s", (job_id,)).fetchone()
        inbox = conn.execute(
            "SELECT status, last_error_code FROM memory_inbox_event WHERE consumer_group='retry-test' AND event_id=%s",
            (event_id,),
        ).fetchone()
    assert job == {"status": "CANCELLED", "last_error_code": "UNKNOWN_FACT_KEY"}
    assert inbox == {"status": "COMPLETED", "last_error_code": "UNKNOWN_FACT_KEY"}
    delete(customer, "delete-cleanup-" + customer)

    # Transient failure: a malformed payload retries, dead-letters after max attempts,
    # and remains replayable from the DLQ.
    transient_customer = customer_id()
    grant_consent(transient_customer)
    transient_event = uuid4()
    with get_db().transaction() as conn:
        transient = ingestion.accept(
            conn,
            candidate(transient_customer, "ingestion-transient"),
            consumer_group="retry-test",
            event_id=transient_event,
            topic="candidates",
            partition=0,
            offset=2,
        )
        row = conn.execute(
            "SELECT payload_ref FROM memory_ingestion_job WHERE id=%s", (transient["job_id"],)
        ).fetchone()
        payload = row["payload_ref"]
        payload["candidate"]["observed_at"] = "not-a-timestamp"
        conn.execute(
            "UPDATE memory_ingestion_job SET payload_ref=%s WHERE id=%s", (Jsonb(payload), transient["job_id"])
        )
    assert ingestion.process_one()
    with get_db().connection() as conn:
        job = conn.execute(
            "SELECT status, last_error_code FROM memory_ingestion_job WHERE id=%s", (transient["job_id"],)
        ).fetchone()
    assert job == {"status": "RETRY", "last_error_code": "ValidationError"}
    with get_db().transaction() as conn:
        ingestion.replay(conn, transient["job_id"])
    with get_db().connection() as conn:
        assert (
            conn.execute("SELECT status FROM memory_ingestion_job WHERE id=%s", (transient["job_id"],)).fetchone()[
                "status"
            ]
            == "RECEIVED"
        )
    original_ingestion_attempts = ingestion.settings.ingestion_max_attempts
    ingestion.settings.ingestion_max_attempts = 1
    with get_db().transaction() as conn:
        conn.execute(
            "UPDATE memory_ingestion_job SET attempts=1, available_at=now() WHERE id=%s", (transient["job_id"],)
        )
    assert ingestion.process_one()
    with get_db().connection() as conn:
        job = conn.execute("SELECT status FROM memory_ingestion_job WHERE id=%s", (transient["job_id"],)).fetchone()
        inbox = conn.execute(
            "SELECT status FROM memory_inbox_event WHERE consumer_group='retry-test' AND event_id=%s",
            (transient_event,),
        ).fetchone()
    assert job["status"] == "DEAD_LETTER" and inbox["status"] == "FAILED"
    ingestion.settings.ingestion_max_attempts = original_ingestion_attempts
    with get_db().transaction() as conn, pytest.raises(ValueError):
        ingestion.replay(conn, uuid4())
    delete(transient_customer, "delete-cleanup-" + transient_customer)
