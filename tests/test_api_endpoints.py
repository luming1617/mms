import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MMS_TEST_DATABASE_URL"),
        reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests",
    ),
]

from tests.test_phase01_integration import candidate, customer_id, delete, service

from mms.api.main import app
from mms.config import get_settings
from mms.services.review import ReviewService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.errors import BarrierError

SERVICE_TOKEN = get_settings().service_token
INTERNAL = {"X-Service-Token": SERVICE_TOKEN}
ADMIN = {
    "X-Admin-Token": get_settings().admin_token,
    "X-Reviewer-ID": "api-test-admin",
    "X-Admin-Reason-Code": "TEST_AUDIT",
    "X-Admin-Ticket": "test-suite",
}
NOW = datetime.now(UTC).isoformat()


def client() -> TestClient:
    return TestClient(app)


def grant_api_consent(http: TestClient, customer: str, purposes=("MEMORY_WRITE", "MEMORY_READ")) -> None:
    for purpose in purposes:
        response = http.post(
            f"/internal/customers/{customer}/memory-consent",
            headers=INTERNAL,
            json={"purpose": purpose, "status": "GRANTED", "version": 1, "effective_at": NOW},
        )
        assert response.status_code == 200


def test_health_auth_and_validation_contracts():
    http = client()
    assert http.get("/health/live").json() == {"status": "live"}
    ready = http.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json()["schema"] == "initialized"
    assert http.get("/health/deps").status_code == 200
    assert http.get("/metrics").status_code == 200

    missing_identity = http.post("/api/v1/memories/query", json={"query": "hello"})
    assert missing_identity.status_code == 401
    assert missing_identity.json()["code"] == "AUTH_FAILED"

    bad_internal = http.get("/internal/customers/any/retrieval-capacity", headers={"X-Service-Token": "wrong"})
    assert bad_internal.status_code == 401

    review_needs_internal = http.get("/api/v1/review-queue", headers={"X-Customer-ID": "cust"})
    assert review_needs_internal.status_code == 401

    invalid_body = http.post("/api/v1/memories/query", headers={**INTERNAL, "X-Customer-ID": "cust"}, json={})
    assert invalid_body.status_code == 422

    as_of_without_history_purpose = http.post(
        "/api/v1/memories/query",
        headers={**INTERNAL, "X-Customer-ID": "cust"},
        json={"query": "hello", "as_of": NOW},
    )
    assert as_of_without_history_purpose.status_code == 422
    assert as_of_without_history_purpose.json()["code"] == "VALIDATION_FAILED"

    unknown_job = http.get(f"/internal/ingestion/jobs/{uuid4()}", headers=INTERNAL)
    assert unknown_job.status_code == 404

    unknown_review = http.post(
        f"/api/v1/review-queue/{uuid4()}/decision",
        headers={**INTERNAL, "X-Customer-ID": "cust", "X-Reviewer-ID": "reviewer"},
        json={"decision": "APPROVE", "decision_version": 1, "reason_code": "TEST"},
    )
    assert unknown_review.status_code == 404


def test_consent_ingestion_reconciliation_and_capacity_api():
    http = client()
    customer = "api-" + customer_id()
    grant_api_consent(http, customer, ("MEMORY_WRITE", "MEMORY_READ", "SENSITIVE_MEMORY"))
    consents = http.get(f"/internal/customers/{customer}/memory-consent", headers=INTERNAL)
    assert consents.status_code == 200
    assert {item["purpose"] for item in consents.json()["consents"]} >= {"MEMORY_WRITE", "MEMORY_READ"}

    payload = {
        "consumer_group": "api-test",
        "event_id": str(uuid4()),
        "topic": "memory-candidates",
        "partition": 0,
        "offset": 1,
        "candidate": candidate(customer, "api-ingestion").model_dump(mode="json", by_alias=True),
    }
    accepted = http.post("/internal/ingestion/events", headers=INTERNAL, json=payload)
    assert accepted.status_code == 202
    job_id = accepted.json()["job_id"]
    assert accepted.json()["duplicate"] is False
    job = http.get(f"/internal/ingestion/jobs/{job_id}", headers=INTERNAL)
    assert job.status_code == 200 and job.json()["status"] == "RECEIVED"
    duplicate = http.post("/internal/ingestion/events", headers=INTERNAL, json=payload)
    assert duplicate.status_code == 202 and duplicate.json()["duplicate"] is True

    assert http.post("/internal/view-cost/run", headers=INTERNAL).status_code == 202
    recon = http.post("/internal/reconciliation/JOB", headers=INTERNAL)
    assert recon.status_code == 202 and recon.json()["recon_type"] == "JOB"
    capacity = http.get(f"/internal/customers/{customer}/retrieval-capacity", headers=INTERNAL)
    assert capacity.status_code == 200 and capacity.json()["customer_id"] == customer
    delete(customer, "delete-" + customer)


def test_candidate_query_feedback_and_usage_api():
    http = client()
    customer = "api-" + customer_id()
    grant_api_consent(http, customer)

    extracted = http.post(
        "/api/v1/extractions",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json={"source_event_id": "api-extract-" + customer, "content": "以后请用中文沟通"},
    )
    assert extracted.status_code == 202
    assert extracted.json()["decisions"][0]["outcome"] == "ADD"

    mismatch = candidate("other-customer", "api-mismatch").model_dump(mode="json", by_alias=True)
    rejected = http.post("/api/v1/memories/candidates", headers={**INTERNAL, "X-Customer-ID": customer}, json=mismatch)
    assert rejected.status_code == 403 and rejected.json()["code"] == "CUSTOMER_MISMATCH"

    correction = candidate(customer, "api-correction", value="en-US", language="en-US", content="Please use English")
    corrected = http.post(
        "/api/v1/memories/candidates",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json=correction.model_dump(mode="json", by_alias=True),
    )
    assert corrected.status_code == 202 and corrected.json()["outcome"] == "SUPERSEDE"
    memory_id = corrected.json()["new_memory_id"]

    _, provider = service()
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass

    queried = http.post(
        "/api/v1/memories/query",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json={"query": "Please use English", "language": "en-US", "trace_id": "api-trace-" + customer},
    )
    assert queried.status_code == 200
    assert queried.json()["memories"][0]["memory_id"] == memory_id

    feedback = http.post(
        f"/api/v1/memories/{memory_id}/feedback",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json={"idempotency_key": "api-feedback-" + customer, "feedback_type": "HELPFUL"},
    )
    assert feedback.status_code == 202 and feedback.json()["duplicate"] is False
    repeated = http.post(
        f"/api/v1/memories/{memory_id}/feedback",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json={"idempotency_key": "api-feedback-" + customer, "feedback_type": "HELPFUL"},
    )
    assert repeated.status_code == 202 and repeated.json()["duplicate"] is True

    invalid_usage = http.post(
        "/api/v1/memory-usage",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json={"request_id": "api-trace-" + customer, "used_memory_ids": [memory_id], "answer_hash": "bad"},
    )
    assert invalid_usage.status_code == 422
    adopted = http.post(
        "/api/v1/memory-usage",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json={
            "request_id": "api-trace-" + customer,
            "used_memory_ids": [memory_id],
            "answer_hash": "d" * 64,
            "model_version": "api-test",
        },
    )
    assert adopted.status_code == 202 and adopted.json()["used"] is True
    delete(customer, "delete-" + customer)


def test_review_and_deletion_request_api():
    http = client()
    customer = "api-" + customer_id()
    grant_api_consent(http, customer)
    svc, _ = service()
    from mms.utils.db import get_db

    with get_db().transaction() as conn:
        pending = svc.process_candidate(conn, candidate(customer, "api-review").model_copy(update={"confidence": 0.0}))
    assert pending.outcome == "NEEDS_VERIFICATION"
    queue = http.get(
        "/api/v1/review-queue",
        headers={**INTERNAL, "X-Customer-ID": customer},
    )
    assert queue.status_code == 200 and len(queue.json()["items"]) == 1
    review_id = queue.json()["items"][0]["id"]

    no_reviewer = http.post(
        f"/api/v1/review-queue/{review_id}/decision",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json={"decision": "APPROVE", "decision_version": 1, "reason_code": "VERIFIED"},
    )
    assert no_reviewer.status_code == 401

    approved = http.post(
        f"/api/v1/review-queue/{review_id}/decision",
        headers={**INTERNAL, "X-Customer-ID": customer, "X-Reviewer-ID": "api-reviewer"},
        json={"decision": "APPROVE", "decision_version": 1, "reason_code": "VERIFIED", "note": "checked"},
    )
    assert approved.status_code == 200 and approved.json()["status"] == "APPROVED"

    invalid_scope = http.post(
        "/api/v1/deletion-requests",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json={"idempotency_key": "api-delete-" + customer, "scope": {"partial": True}},
    )
    assert invalid_scope.status_code == 422

    deletion = http.post(
        "/api/v1/deletion-requests",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json={"idempotency_key": "api-delete-" + customer},
    )
    assert deletion.status_code == 202
    request_id = deletion.json()["request_id"]
    deletion_review_id = deletion.json()["review_id"]
    status = http.get(f"/api/v1/deletion-requests/{request_id}", headers={**INTERNAL, "X-Customer-ID": customer})
    assert status.status_code == 200 and status.json()["status"] == "RECEIVED"

    unpaired = http.post(
        f"/api/v1/review-queue/{deletion_review_id}/decision",
        headers={**INTERNAL, "X-Customer-ID": customer, "X-Reviewer-ID": "privacy-reviewer"},
        json={"decision": "APPROVE", "decision_version": 1, "reason_code": "ERASURE_APPROVED"},
    )
    # Doc 12.6: erasure is the highest-risk review, so a single signature is refused.
    assert unpaired.status_code == 403 and unpaired.json()["code"] == "FOUR_EYES_REQUIRED"

    completed = http.post(
        f"/api/v1/review-queue/{deletion_review_id}/decision",
        headers={**INTERNAL, "X-Customer-ID": customer, "X-Reviewer-ID": "privacy-reviewer"},
        json={
            "decision": "APPROVE",
            "decision_version": 1,
            "reason_code": "ERASURE_APPROVED",
            "co_reviewer_id": "privacy-second-reviewer",
        },
    )
    assert completed.status_code == 200 and completed.json()["deletion"]["status"] == "COMPLETED"
    final = http.get(f"/api/v1/deletion-requests/{request_id}", headers={**INTERNAL, "X-Customer-ID": customer})
    assert final.status_code == 200 and final.json()["status"] == "COMPLETED"
    blocked = http.post(
        "/api/v1/memories/query",
        headers={**INTERNAL, "X-Customer-ID": customer},
        json={"query": "anything"},
    )
    assert blocked.status_code == 410 and blocked.json()["code"] == "CUSTOMER_DELETED"


def test_audit_purpose_requires_internal_identity():
    http = client()
    customer = customer_id()
    response = http.post(
        "/api/v1/memories/query",
        headers={"X-Customer-ID": customer},
        json={"query": "hello", "purpose": "AUDIT", "language": "en-US"},
    )
    # Without the service identity no purpose (AUDIT included) is reachable at all.
    assert response.status_code == 401 and response.json()["code"] == "AUTH_FAILED"
    grant_api_consent(http, customer, purposes=("MEMORY_READ",))
    internal = http.post(
        "/api/v1/memories/query",
        headers={"X-Customer-ID": customer, **INTERNAL},
        json={"query": "hello", "purpose": "AUDIT", "language": "en-US"},
    )
    assert internal.status_code == 401 and internal.json()["code"] == "ADMIN_AUTH_REQUIRED"
    audited = http.post(
        "/api/v1/memories/query",
        headers={"X-Customer-ID": customer, **INTERNAL, **ADMIN},
        json={"query": "hello", "purpose": "AUDIT", "language": "en-US"},
    )
    assert audited.status_code == 200
    delete(customer, "audit-purpose-cleanup")


def test_review_queue_degrades_to_empty_without_memory_read_consent():
    http = client()
    customer = customer_id()
    grant_api_consent(http, customer, purposes=("MEMORY_WRITE",))
    no_consent = http.get("/api/v1/review-queue", headers={"X-Customer-ID": customer, **INTERNAL})
    assert no_consent.status_code == 200
    assert no_consent.json()["consent_unavailable"] is True and no_consent.json()["items"] == []
    grant_api_consent(http, customer, purposes=("MEMORY_READ",))
    allowed = http.get("/api/v1/review-queue", headers={"X-Customer-ID": customer, **INTERNAL})
    assert allowed.status_code == 200 and allowed.json()["items"] == []
    delete(customer, "review-queue-cleanup")


def test_consent_withdrawal_deletes_and_regrant_reopens_customer():
    http = client()
    customer = customer_id()
    grant_api_consent(http, customer, purposes=("MEMORY_WRITE", "MEMORY_READ", "MEMORY_RETENTION"))
    svc, _ = service()
    review = ReviewService(svc.repository, svc)
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "withdraw-base"))

    withdrawn = http.post(
        f"/internal/customers/{customer}/memory-consent",
        headers=INTERNAL,
        json={"purpose": "MEMORY_RETENTION", "status": "WITHDRAWN", "version": 2, "effective_at": NOW},
    )
    assert withdrawn.status_code == 200
    with get_db().connection() as conn:
        request = conn.execute(
            "SELECT request_source,status FROM memory_deletion_request WHERE customer_id=%s", (customer,)
        ).fetchone()
        barrier = conn.execute(
            "SELECT status FROM customer_deletion_barrier WHERE customer_id=%s", (customer,)
        ).fetchone()
    assert request["request_source"] == "CONSENT_WITHDRAWN"
    assert barrier["status"] == "ACTIVE"

    with get_db().transaction() as conn:
        task = review.queue(conn, customer)[0]
        approved = review.decide(
            conn,
            task["id"],
            customer_id=customer,
            reviewer_id="privacy-reviewer",
            co_reviewer_id="privacy-second-reviewer",
            decision_version=1,
            approve=True,
            reason_code="GDPR_APPROVED",
            note=None,
        )
        assert approved["deletion"]["status"] == "COMPLETED"
    with get_db().transaction() as conn, pytest.raises(BarrierError):
        svc.process_candidate(conn, candidate(customer, "blocked-after-delete"))

    regrant = http.post(
        f"/internal/customers/{customer}/memory-consent",
        headers=INTERNAL,
        json={"purpose": "MEMORY_WRITE", "status": "GRANTED", "version": 3, "effective_at": NOW},
    )
    assert regrant.status_code == 200
    with get_db().connection() as conn:
        barrier = conn.execute(
            "SELECT status,generation FROM customer_deletion_barrier WHERE customer_id=%s", (customer,)
        ).fetchone()
    assert barrier["status"] == "CANCELLED" and int(barrier["generation"]) >= 2
    with get_db().transaction() as conn:
        assert svc.process_candidate(conn, candidate(customer, "allowed-after-regrant")).outcome == "ADD"
    delete(customer, "regrant-cleanup")
