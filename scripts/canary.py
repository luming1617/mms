from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from uuid import uuid4

import httpx

BASE_URL = os.environ.get("MMS_CANARY_BASE_URL", "http://127.0.0.1:8000")
SERVICE_TOKEN = os.environ.get("MMS_SERVICE_TOKEN", "dev-token")


def main() -> None:
    customer_id = "canary-" + str(uuid4())
    # The customer-facing endpoints run behind the gateway identity today, so the gateway
    # credential has to accompany X-Customer-ID (docs 12.1).
    internal_headers = {"X-Service-Token": SERVICE_TOKEN}
    headers = {"X-Customer-ID": customer_id, **internal_headers}
    with httpx.Client(base_url=BASE_URL, timeout=15) as client:
        client.get("/health/ready").raise_for_status()
        for purpose in ("MEMORY_WRITE", "MEMORY_READ"):
            response = client.post(
                f"/internal/customers/{customer_id}/memory-consent",
                headers=internal_headers,
                json={
                    "purpose": purpose,
                    "status": "GRANTED",
                    "version": 1,
                    "effective_at": datetime.now(UTC).isoformat(),
                },
            )
            response.raise_for_status()
        response = client.post(
            "/api/v1/extractions",
            headers=headers,
            json={
                "source_system": "canary",
                "source_event_id": "extract-" + customer_id,
                "content": "以后请用中文和我沟通",
            },
        )
        response.raise_for_status()
        assert response.json()["decisions"][0]["outcome"] == "ADD"
        wait_for_ready(client, headers, customer_id, "以后请用中文")

        # Cross-customer isolation: a second customer must never see the first one's memory.
        other_customer = "canary-other-" + str(uuid4())
        other_headers = {"X-Customer-ID": other_customer, **internal_headers}
        for purpose in ("MEMORY_WRITE", "MEMORY_READ"):
            response = client.post(
                f"/internal/customers/{other_customer}/memory-consent",
                headers=internal_headers,
                json={
                    "purpose": purpose,
                    "status": "GRANTED",
                    "version": 1,
                    "effective_at": datetime.now(UTC).isoformat(),
                },
            )
            response.raise_for_status()
        response = client.post(
            "/api/v1/memories/candidates",
            headers=other_headers,
            json={
                "customer_id": other_customer,
                "source_system": "canary",
                "source_event_id": "other-" + other_customer,
                "source_refs": [{"ref_type": "message", "message_id": "canary-msg-other"}],
                "memory_type": "CUSTOMER_PROFILE",
                "fact_key": "communication.language",
                "object": {"value": "en-US"},
                "content": "Please use English",
                "content_language": "en-US",
                "language_confidence": 0.99,
                "source_kind": "USER_EXPLICIT",
                "observed_at": datetime.now(UTC).isoformat(),
            },
        )
        response.raise_for_status()
        wait_for_ready(client, other_headers, other_customer, "Please use English")
        main_view = client.post(
            "/api/v1/memories/query",
            headers=headers,
            json={"query": "语言", "fact_key": "communication.language", "max_items": 8},
        )
        main_view.raise_for_status()
        assert all(item["content"] != "Please use English" for item in main_view.json()["memories"])
        other_view = client.post(
            "/api/v1/memories/query",
            headers=other_headers,
            json={"query": "English", "fact_key": "communication.language", "max_items": 8},
        )
        other_view.raise_for_status()
        assert any(item["content"] == "Please use English" for item in other_view.json()["memories"])

        event_id = uuid4()
        ingestion_payload = {
            "consumer_group": "canary",
            "event_id": str(event_id),
            "topic": "memory-candidates",
            "partition": 0,
            "offset": 1,
            "candidate": {
                "customer_id": customer_id,
                "source_system": "canary",
                "source_event_id": "async-" + customer_id,
                "memory_type": "CUSTOMER_PROFILE",
                "fact_key": "communication.language",
                "object": {"value": "zh-CN"},
                "content": "以后请用中文",
                "content_language": "zh-CN",
                "language_confidence": 0.99,
                "source_kind": "USER_EXPLICIT",
                "observed_at": datetime.now(UTC).isoformat(),
            },
        }
        response = client.post("/internal/ingestion/events", headers=internal_headers, json=ingestion_payload)
        response.raise_for_status()
        job_id = response.json()["job_id"]
        wait_for_ingestion(client, internal_headers, job_id)
        repeated_event = client.post("/internal/ingestion/events", headers=internal_headers, json=ingestion_payload)
        repeated_event.raise_for_status()
        assert repeated_event.json()["duplicate"] and repeated_event.json()["status"] == "COMPLETED"

        hybrid = client.post(
            "/api/v1/memories/query",
            headers=headers,
            json={"query": "以后请用中文", "language": "zh-CN", "max_items": 8},
        )
        hybrid.raise_for_status()
        assert hybrid.json()["lexical_used"] and hybrid.json()["lexical_mode"] == "trgm"
        assert hybrid.json()["memories"][0]["content"] == "以后请用中文"

        memory_id = hybrid.json()["memories"][0]["memory_id"]
        trace_id = "agent-" + customer_id
        agent = client.post(
            "/api/v1/agent/memory-context",
            headers=headers,
            json={"query": "以后请用中文", "language": "zh-CN", "trace_id": trace_id},
        )
        agent.raise_for_status()
        assert agent.json()["trust"] == "untrusted_data"
        feedback = client.post(
            f"/api/v1/memories/{memory_id}/feedback",
            headers=headers,
            json={"idempotency_key": "feedback-" + customer_id, "feedback_type": "HELPFUL"},
        )
        feedback.raise_for_status()
        adopted = client.post(
            "/api/v1/memory-usage",
            headers=headers,
            json={
                "request_id": trace_id,
                "used_memory_ids": [memory_id],
                "answer_hash": "c" * 64,
                "model_version": "phase3-canary",
            },
        )
        adopted.raise_for_status()
        assert adopted.json()["used"]
        capacity = client.get(f"/internal/customers/{customer_id}/retrieval-capacity", headers=internal_headers)
        capacity.raise_for_status()
        assert capacity.json()["ready_vectors"] >= 2 and not capacity.json()["exceeded"]
        view_cost = client.post("/internal/view-cost/run", headers=internal_headers)
        view_cost.raise_for_status()

        response = client.post(
            "/api/v1/memories/candidates",
            headers=headers,
            json={
                "customer_id": customer_id,
                "source_system": "canary",
                "source_event_id": "supersede-" + customer_id,
                "source_refs": [{"ref_type": "message", "message_id": "canary-msg-supersede"}],
                "memory_type": "CUSTOMER_PROFILE",
                "fact_key": "communication.language",
                "object": {"value": "en-US"},
                "content": "Please use English",
                "content_language": "en-US",
                "language_confidence": 0.99,
                "source_kind": "USER_EXPLICIT",
                "explicit_correction": True,
                "observed_at": datetime.now(UTC).isoformat(),
            },
        )
        response.raise_for_status()
        assert response.json()["outcome"] == "SUPERSEDE"
        wait_for_ready(client, headers, customer_id, "Please use English")

        response = client.post("/internal/reconciliation/JOB", headers=internal_headers)
        response.raise_for_status()
        deletion = client.post(
            "/api/v1/deletion-requests",
            headers=headers,
            json={"idempotency_key": "delete-" + customer_id, "scope": {"all": True}},
        )
        deletion.raise_for_status()
        deletion_payload = deletion.json()
        assert deletion_payload["status"] == "RECEIVED"
        review_headers = {**headers, **internal_headers, "X-Reviewer-ID": "canary-privacy-reviewer"}
        approved = client.post(
            f"/api/v1/review-queue/{deletion_payload['review_id']}/decision",
            headers=review_headers,
            json={
                "decision": "APPROVE",
                "decision_version": 1,
                "reason_code": "CANARY_APPROVED",
                # docs 12.6: erasure needs four eyes, so the canary signs as two actors.
                "co_reviewer_id": "canary-privacy-second",
            },
        )
        approved.raise_for_status()
        assert approved.json()["deletion"]["status"] == "COMPLETED"
        repeated = client.post(
            "/api/v1/deletion-requests",
            headers=headers,
            json={"idempotency_key": "delete-" + customer_id, "scope": {"all": True}},
        )
        repeated.raise_for_status()
        assert repeated.json()["request_id"] == deletion_payload["request_id"]
        assert repeated.json()["status"] == "COMPLETED"
        blocked = client.post(
            "/api/v1/memories/query",
            headers=headers,
            json={"query": "English", "fact_key": "communication.language"},
        )
        assert blocked.status_code == 410 and blocked.json()["code"] == "CUSTOMER_DELETED"
        survived = client.post(
            "/api/v1/memories/query",
            headers=other_headers,
            json={"query": "English", "fact_key": "communication.language", "max_items": 8},
        )
        survived.raise_for_status()
        assert any(item["content"] == "Please use English" for item in survived.json()["memories"])
        _assert_deleted_everywhere(customer_id)
    print(f"Phase 3 canary passed: {customer_id}")


def _assert_deleted_everywhere(customer_id: str) -> None:
    """Doc 18.7: the canary greps every memory-domain table for the deleted customer."""
    import psycopg
    from psycopg import sql

    dsn = os.environ.get("MMS_DATABASE_URL")
    if not dsn:
        print("skip post-delete table grep: MMS_DATABASE_URL not set", file=os.sys.stderr)
        return
    tables = (
        "customer_memory",
        "memory_evidence",
        "memory_embedding",
        "memory_search_document",
        "memory_embedding_job",
        "memory_translation_job",
        "memory_candidate",
        "memory_ingestion_job",
        "customer_memory_current",
        "memory_decision",
    )
    with psycopg.connect(dsn) as conn:
        leftovers = {}
        for table in tables:
            count = conn.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE customer_id=%s").format(sql.Identifier(table)), (customer_id,)
            ).fetchone()[0]
            if count:
                leftovers[table] = count
        tombstones = conn.execute(
            "SELECT count(*) FROM deletion_tombstone WHERE customer_id=%s", (customer_id,)
        ).fetchone()[0]
    assert not leftovers, f"deleted customer still has rows: {leftovers}"
    assert tombstones >= 1, "deletion tombstone missing"


def wait_for_ingestion(client: httpx.Client, headers: dict[str, str], job_id: str) -> None:
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        response = client.get(f"/internal/ingestion/jobs/{job_id}", headers=headers)
        response.raise_for_status()
        if response.json()["status"] == "COMPLETED":
            return
        if response.json()["status"] == "DEAD_LETTER":
            raise RuntimeError(f"ingestion failed: {response.json()}")
        time.sleep(0.2)
    raise TimeoutError(f"ingestion did not complete: {job_id}")


def wait_for_ready(client: httpx.Client, headers: dict[str, str], customer_id: str, expected_content: str) -> None:
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        response = client.post(
            "/api/v1/memories/query",
            headers=headers,
            json={"query": expected_content, "fact_key": "communication.language", "max_items": 8},
        )
        response.raise_for_status()
        result = response.json()
        if (
            result["memories"]
            and result["memories"][0]["content"] == expected_content
            and not result["semantic_index_pending"]
        ):
            return
        time.sleep(0.2)
    raise TimeoutError(f"embedding did not become READY for {customer_id}")


if __name__ == "__main__":
    main()
