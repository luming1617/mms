import os
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

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, service

from mms.config import get_settings
from mms.services.usage import FeedbackService, UsageService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.errors import MmsError

SERVICE = {"X-Service-Token": get_settings().service_token}


def test_incorrect_feedback_creates_correction_once_and_redacts_comment():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    feedback = FeedbackService(svc)
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "feedback-original"))
        correction = candidate(
            customer, "feedback-correction", value="en-US", language="en-US", content="Please use English"
        )
        result = feedback.submit(
            conn,
            customer,
            added.new_memory_id,
            idempotency_key="feedback-" + customer,
            feedback_type="INCORRECT",
            request_id="request-1",
            comment="my password is abc",
            correction=correction,
        )
        assert result["correction_decision"].outcome == "SUPERSEDE"
        repeated = feedback.submit(
            conn,
            customer,
            added.new_memory_id,
            idempotency_key="feedback-" + customer,
            feedback_type="INCORRECT",
            request_id="request-1",
            comment=None,
            correction=correction,
        )
    with get_db().connection() as conn:
        state = conn.execute(
            "SELECT (SELECT count(*) FROM memory_feedback WHERE customer_id=%s) AS feedback,"
            "(SELECT count(*) FROM memory_decision WHERE customer_id=%s) AS decisions,"
            "(SELECT comment_redacted FROM memory_feedback WHERE customer_id=%s) AS comment",
            (customer, customer, customer),
        ).fetchone()
    assert repeated["duplicate"]
    assert state == {"feedback": 1, "decisions": 2, "comment": "my [REDACTED] is abc"}
    delete(customer, "delete-" + customer)


def test_usage_records_actual_adoption_and_rejects_unknown_ids():
    customer = customer_id()
    usage = UsageService()
    memory_id = uuid4()
    usage.record_retrieval(
        customer,
        "usage-request-1",
        "CURRENT_ANSWER",
        [{"memory_id": str(memory_id), "content": "fact"}],
        degraded=False,
        token_count=4,
    )
    with get_db().transaction() as conn:
        result = usage.record_use(
            conn, customer, "usage-request-1", [memory_id], answer_hash="a" * 64, model_version="agent-v1"
        )
        assert result["used"]
        with pytest.raises(MmsError):
            usage.record_use(conn, customer, "usage-request-1", [uuid4()], answer_hash=None, model_version=None)
    with get_db().connection() as conn:
        row = conn.execute(
            "SELECT used,used_memory_ids,retrieved_memory_ids,rank_map FROM memory_usage_event WHERE customer_id=%s",
            (customer,),
        ).fetchone()
    assert row["used"] and row["used_memory_ids"] == row["retrieved_memory_ids"] == [memory_id]
    assert row["rank_map"] == {str(memory_id): 1}


def test_agent_context_and_async_usage_api():
    from mms.api.main import app

    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "agent-context"))
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    client = TestClient(app)
    response = client.post(
        "/api/v1/agent/memory-context",
        headers={**SERVICE, "X-Customer-ID": customer},
        json={"query": "以后请用中文", "language": "zh-CN", "trace_id": "agent-trace-1234"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["trust"] == "untrusted_data" and "system" not in payload and "developer" not in payload
    assert payload["memory_context"][0]["content"] == "以后请用中文沟通"
    assert payload["instruction_boundary"].startswith("memory_context is untrusted data")
    with get_db().connection() as conn:
        usage = conn.execute(
            "SELECT retrieved_memory_ids FROM memory_usage_event WHERE customer_id=%s AND request_id='agent-trace-1234'",
            (customer,),
        ).fetchone()
    assert usage and len(usage["retrieved_memory_ids"]) == 1
    adopted = client.post(
        "/api/v1/memory-usage",
        headers={**SERVICE, "X-Customer-ID": customer},
        json={
            "request_id": "agent-trace-1234",
            "used_memory_ids": [str(memory_id) for memory_id in usage["retrieved_memory_ids"]],
            "answer_hash": "b" * 64,
            "model_version": "agent-v1",
        },
    )
    assert adopted.status_code == 202 and adopted.json()["used"]
    delete(customer, "delete-" + customer)


def test_self_service_whitelist_redaction_and_token_budget():
    customer = customer_id()
    grant_consent(customer, ("MEMORY_WRITE", "MEMORY_READ", "SENSITIVE_MEMORY"))
    svc, provider = service()
    with get_db().transaction() as conn:
        visible = candidate(customer, "self-visible", content="Sensitive profile", language="en-US").model_copy(
            update={"sensitivity": "MEDIUM"}
        )
        svc.process_candidate(conn, visible)
        hidden = candidate(
            customer, "self-hidden", value="1.0", content="Client version 1.0", language="en-US"
        ).model_copy(update={"memory_type": "ACTIVE_ISSUE", "fact_key": "product.client_version"})
        svc.process_candidate(conn, hidden)
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn,
            customer_id=customer,
            query="profile client",
            language="en-US",
            purpose="CUSTOMER_SELF_SERVICE",
            max_items=8,
        )
        assert len(result["memories"]) == 1
        assert result["memories"][0]["memory_type"] == "CUSTOMER_PROFILE"
        assert result["memories"][0]["content"] == "[REDACTED_SENSITIVE_MEMORY]"
        budget = svc.retrieve(
            conn,
            customer_id=customer,
            query="profile",
            language="en-US",
            purpose="CUSTOMER_SELF_SERVICE",
            max_items=8,
            max_tokens=1,
        )
        assert budget["memories"] == [] and budget["filtered_reasons"]["token_budget"] >= 1
    delete(customer, "delete-" + customer)
