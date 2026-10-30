import os
from datetime import UTC, datetime, timedelta
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

from mms.api.main import app
from mms.config import get_settings
from mms.services.recon import ReconciliationService
from mms.services.repository import MemoryRepository
from mms.services.service import MemoryService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.embedding import DeterministicHashEmbeddingProvider
from mms.utils.language import detect_script, query_language_confidence
from mms.utils.query_cache import clear_query_cache

SETTINGS = get_settings()
ADMIN = {
    "X-Admin-Token": SETTINGS.admin_token,
    "X-Reviewer-ID": "admin-reviewer",
    "X-Admin-Reason-Code": "TEST",
    "X-Admin-Ticket": "T-100",
}


def drain(worker: EmbeddingWorker) -> None:
    while worker.process_one():
        pass


class CountingProvider(DeterministicHashEmbeddingProvider):
    def __init__(self):
        super().__init__()
        self.query_calls = 0
        self.batch_sizes = []

    def embed_query(self, text, model):
        self.query_calls += 1
        return super().embed_query(text, model)

    def embed_documents(self, texts, model):
        self.batch_sizes.append(len(texts))
        return super().embed_documents(texts, model)


def test_query_cache_reuses_vectors_per_language():
    customer = customer_id()
    grant_consent(customer)
    provider = CountingProvider()
    svc = MemoryService(MemoryRepository(), provider)
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "cache-event"))
    drain(EmbeddingWorker(provider))
    clear_query_cache()
    provider.query_calls = 0
    with get_db().connection() as conn:
        svc.retrieve(
            conn, customer_id=customer, query="以后请用中文", language="zh-CN", purpose="CURRENT_ANSWER", max_items=8
        )
        svc.retrieve(
            conn, customer_id=customer, query="以后请用中文", language="zh-CN", purpose="CURRENT_ANSWER", max_items=8
        )
        assert provider.query_calls == 1
        svc.retrieve(
            conn, customer_id=customer, query="以后请用中文", language="ja-JP", purpose="CURRENT_ANSWER", max_items=8
        )
        assert provider.query_calls == 2
    delete(customer, "delete-" + customer)


def test_embedding_worker_embeds_batch_in_one_provider_call():
    customer = customer_id()
    grant_consent(customer)
    provider = CountingProvider()
    svc = MemoryService(MemoryRepository(), provider)
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "batch-1"))
    with get_db().transaction() as conn:
        conn.execute(
            "UPDATE memory_embedding_job SET available_at=now()+interval '1 hour' WHERE customer_id<>%s",
            (customer,),
        )
    worker = EmbeddingWorker(provider, worker_id="batch-worker")
    provider.batch_sizes.clear()
    assert worker.process_batch()
    assert provider.batch_sizes and provider.batch_sizes[0] == 2  # ORIGINAL + CANONICAL
    drain(worker)
    delete(customer, "delete-" + customer)


def test_vector_recon_enqueues_missing_embeddings():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "recon-enqueue"))
    drain(EmbeddingWorker(provider))
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM memory_embedding WHERE customer_id=%s AND view_kind='CANONICAL'", (customer,))
    result = ReconciliationService().run("VECTOR")
    assert result["details"]["enqueued_missing_vectors"] >= 1
    with get_db().connection() as conn:
        pending = conn.execute(
            "SELECT count(*) AS n FROM memory_embedding_job WHERE customer_id=%s "
            "AND view_kind='CANONICAL' AND status='PENDING'",
            (customer,),
        ).fetchone()["n"]
    assert pending == 1
    drain(EmbeddingWorker(provider))
    with get_db().connection() as conn:
        ready = conn.execute(
            "SELECT count(*) AS n FROM memory_embedding WHERE customer_id=%s AND memory_id=%s "
            "AND view_kind='CANONICAL' AND status='READY'",
            (customer, added.new_memory_id),
        ).fetchone()["n"]
    assert ready == 1
    delete(customer, "delete-" + customer)


def test_history_debug_supplements_superseded_version():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    base = datetime.now(UTC) - timedelta(hours=2)
    v1 = candidate(customer, "history-v1").model_copy(update={"observed_at": base, "valid_from": base})
    with get_db().transaction() as conn:
        first = svc.process_candidate(conn, v1)
    assert first.outcome == "ADD"
    drain(EmbeddingWorker(provider))
    v2 = candidate(customer, "history-v2", value="en-US", language="en-US", content="Please use English").model_copy(
        update={"observed_at": base + timedelta(hours=1), "valid_from": base + timedelta(hours=1)}
    )
    with get_db().transaction() as conn:
        second = svc.process_candidate(conn, v2)
    assert second.outcome == "SUPERSEDE"
    drain(EmbeddingWorker(provider))
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn,
            customer_id=customer,
            query="以后请用中文沟通",
            language="zh-CN",
            purpose="HISTORY_DEBUG",
            max_items=8,
            as_of=base + timedelta(minutes=30),
        )
    ids = {item["memory_id"] for item in result["memories"]}
    assert str(first.new_memory_id) in ids
    assert all(item["use_policy"] == "history_only" for item in result["memories"])
    delete(customer, "delete-" + customer)


def test_low_confidence_language_disables_translated_views():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "lang-low"))
    drain(EmbeddingWorker(provider))
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn, customer_id=customer, query="以后请用中文", language="es-ES", purpose="CURRENT_ANSWER", max_items=8
        )
    assert result["degraded"] and "language_low_confidence" in {result["degraded_reason"]}
    assert result["lexical_skip_reason"] == "language_not_configured"
    delete(customer, "delete-" + customer)


def test_language_confidence_unit():
    assert detect_script("以后请用中文") == "HAN"
    assert query_language_confidence("以后请用中文", "zh-CN") == 1.0
    assert query_language_confidence("以后请用中文", "es-ES") == 0.0
    assert query_language_confidence("Please use English", "en-US") == 1.0
    assert query_language_confidence("...", "en-US") == 0.5


def grant_agent(agent: str, customer: str) -> None:
    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO agent_customer_grant (actor_id,customer_id,granted_by) VALUES (%s,%s,'integration-test') "
            "ON CONFLICT (actor_id,customer_id) DO UPDATE SET revoked_at=NULL",
            (agent, customer),
        )


def test_customer_memory_list_requires_agent_grant_and_audits():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "grant-check"))
    http = TestClient(app)
    agent = "agent-" + uuid4().hex[:8]
    headers = {"X-Service-Token": SETTINGS.service_token, "X-Agent-ID": agent}

    missing_agent = http.get(f"/api/v1/customers/{customer}/memories")
    assert missing_agent.status_code == 401 and missing_agent.json()["code"] == "AUTH_FAILED"

    denied = http.get(f"/api/v1/customers/{customer}/memories", headers=headers)
    assert denied.status_code == 403 and denied.json()["code"] == "CUSTOMER_MISMATCH"
    with get_db().connection() as conn:
        audit = conn.execute(
            "SELECT actor_role,action,reason_code,scope FROM admin_access_audit WHERE actor_id=%s",
            (agent,),
        ).fetchone()
    # Docs 12.1/12.4: the denied attempt is audited too, and the audit carries only the
    # keyed customer hash - never the raw id or memory content.
    assert audit["actor_role"] == "AGENT" and audit["action"] == "READ_MEMORY"
    assert audit["scope"] == {"endpoint": "list_customer_memories", "result": "denied"}

    grant_agent(agent, customer)
    allowed = http.get(f"/api/v1/customers/{customer}/memories", headers=headers)
    assert allowed.status_code == 200 and len(allowed.json()["items"]) == 1
    assert "content" not in allowed.json()["items"][0]
    with get_db().connection() as conn:
        results = conn.execute(
            """SELECT scope->>'result' AS result FROM admin_access_audit WHERE actor_id=%s ORDER BY created_at""",
            (agent,),
        ).fetchall()
    assert [row["result"] for row in results] == ["denied", "granted"]
    delete(customer, "delete-" + customer)


def test_customer_memory_list_pagination():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "page-1"))
    with get_db().transaction() as conn:
        svc.process_candidate(
            conn, candidate(customer, "page-2", value="en-US", language="en-US", content="Please use English")
        )
    drain(EmbeddingWorker(provider))
    http = TestClient(app)
    agent = "agent-" + uuid4().hex[:8]
    grant_agent(agent, customer)
    list_headers = {"X-Service-Token": SETTINGS.service_token, "X-Agent-ID": agent}
    page1 = http.get(f"/api/v1/customers/{customer}/memories?limit=1", headers=list_headers)
    assert page1.status_code == 200
    body1 = page1.json()
    assert len(body1["items"]) == 1 and body1["next_cursor"]
    page2 = http.get(
        f"/api/v1/customers/{customer}/memories?limit=1&cursor={body1['next_cursor']}",
        headers=list_headers,
    )
    assert page2.status_code == 200
    assert page2.json()["items"][0]["memory_id"] != body1["items"][0]["memory_id"]
    delete(customer, "delete-" + customer)


def test_admin_policy_fact_key_and_rebuild_endpoints():
    http = TestClient(app)
    assert http.get("/api/v1/admin/policies", headers={"X-Customer-ID": "c"}).status_code == 401
    name = "policy-" + uuid4().hex[:8]
    created = http.post(
        "/api/v1/admin/policies",
        headers={**ADMIN, "X-Reviewer-ID": "policy-author"},
        json={
            "action": "CREATE",
            "name": name,
            "version": "1",
            "memory_type": "CUSTOMER_PROFILE",
            "config": {"review_sla_days": 30},
        },
    )
    assert created.status_code == 200 and created.json()["status"] == "DRAFT", created.text
    four_eyes = http.post(
        "/api/v1/admin/policies",
        headers={**ADMIN, "X-Reviewer-ID": "policy-author"},
        json={"action": "ACTIVATE", "name": name, "version": "1"},
    )
    assert four_eyes.status_code == 403
    activated = http.post(
        "/api/v1/admin/policies",
        headers=ADMIN,
        json={"action": "ACTIVATE", "name": name, "version": "1"},
    )
    assert activated.status_code == 200 and activated.json()["status"] == "ACTIVE", activated.text
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM memory_policy WHERE name=%s", (name,))
    fact_keys = http.get("/api/v1/admin/fact-keys", headers=ADMIN)
    assert fact_keys.status_code == 200 and fact_keys.json()["items"]
    rebuild = http.post("/api/v1/admin/embeddings/rebuild", headers=ADMIN, json={"customer_id": "unknown-customer"})
    assert rebuild.status_code == 202 and rebuild.json()["enqueued"] == 0


def test_admin_lineage_and_decision_replay():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        decision = svc.process_candidate(conn, candidate(customer, "lineage-event"))
    drain(EmbeddingWorker(provider))
    http = TestClient(app)
    lineage = http.get(f"/api/v1/admin/memories/{decision.new_memory_id}/lineage", headers=ADMIN)
    assert lineage.status_code == 200, lineage.text
    body = lineage.json()
    assert body["memory"]["id"] == str(decision.new_memory_id)
    assert body["evidence"] and body["versions"] and body["embeddings"]
    replay = http.post("/api/v1/admin/decisions/replay", headers=ADMIN, json={"decision_id": str(decision.decision_id)})
    assert replay.status_code == 200, replay.text
    payload = replay.json()
    assert payload["recorded"]["outcome"] == "ADD"
    assert "outcome" in payload["drift"]
    with get_db().connection() as conn:
        audited = conn.execute(
            "SELECT count(*) AS n FROM admin_access_audit WHERE action IN ('READ_MEMORY','REPLAY') AND reason_code='TEST'"
        ).fetchone()["n"]
    assert audited >= 2
    delete(customer, "delete-" + customer)


def test_identity_ref_and_idempotency_header():
    customer = customer_id()
    http = TestClient(app)
    missing = http.get(
        f"/internal/customers/{customer}/identity-ref", headers={"X-Service-Token": SETTINGS.service_token}
    )
    assert missing.status_code == 404 and missing.json()["code"] == "CUSTOMER_REF_UNKNOWN"
    provisioned = http.post(
        f"/internal/customers/{customer}/identity-ref", headers={"X-Service-Token": SETTINGS.service_token}
    )
    assert provisioned.status_code == 200
    fetched = http.get(
        f"/internal/customers/{customer}/identity-ref", headers={"X-Service-Token": SETTINGS.service_token}
    )
    assert fetched.json()["customer_ref"] == provisioned.json()["customer_ref"]

    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "idem-event"))
    drain(EmbeddingWorker(provider))
    headers = {
        "X-Customer-ID": customer,
        "X-Service-Token": SETTINGS.service_token,
        "Idempotency-Key": "feedback-key-" + uuid4().hex[:8],
    }
    first = http.post(
        f"/api/v1/memories/{added.new_memory_id}/feedback", headers=headers, json={"feedback_type": "HELPFUL"}
    )
    assert first.status_code == 202, first.text
    duplicate = http.post(
        f"/api/v1/memories/{added.new_memory_id}/feedback", headers=headers, json={"feedback_type": "HELPFUL"}
    )
    assert duplicate.status_code == 202 and duplicate.json()["duplicate"] is True
    delete(customer, "delete-" + customer)


def test_dlq_replay_accepts_embedding_dead_letter():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "dlq-event"))
    with get_db().transaction() as conn:
        job = conn.execute(
            "SELECT id FROM memory_embedding_job WHERE customer_id=%s ORDER BY created_at LIMIT 1", (customer,)
        ).fetchone()
        conn.execute("UPDATE memory_embedding_job SET status='DEAD_LETTER', attempts=5 WHERE id=%s", (job["id"],))
    http = TestClient(app)
    replay = http.post(
        "/api/v1/admin/dlq/replay",
        headers={"X-Service-Token": SETTINGS.service_token},
        json={"job_type": "EMBEDDING", "id": str(job["id"])},
    )
    assert replay.status_code == 202, replay.text
    with get_db().connection() as conn:
        row = conn.execute("SELECT status, attempts FROM memory_embedding_job WHERE id=%s", (job["id"],)).fetchone()
    assert row == {"status": "PENDING", "attempts": 0}
    drain(EmbeddingWorker(provider))
    delete(customer, "delete-" + customer)


def test_guard_middlewares_reject_oversize_and_rate_limit():
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route
    from starlette.testclient import TestClient as StarletteClient

    from mms.api.guards import MaxBodySizeMiddleware, RateLimitMiddleware

    async def ok(request):
        return JSONResponse({"ok": True})

    guarded = Starlette(routes=[Route("/x", ok, methods=["POST"])])
    guarded.add_middleware(MaxBodySizeMiddleware, max_bytes=16)
    large = StarletteClient(guarded).post("/x", content=b"x" * 64)
    assert large.status_code == 413 and large.json()["code"] == "REQUEST_TOO_LARGE"

    limited = Starlette(routes=[Route("/x", ok, methods=["POST"])])
    limited.add_middleware(RateLimitMiddleware, enabled=True, rate=0.001, burst=1)
    client = StarletteClient(limited)
    assert client.post("/x", headers={"X-Customer-ID": "ratelimit-cust"}).status_code == 200
    blocked = client.post("/x", headers={"X-Customer-ID": "ratelimit-cust"})
    assert blocked.status_code == 429 and blocked.headers["Retry-After"]
    assert client.post("/x", headers={"X-Customer-ID": "ratelimit-other"}).status_code == 200
