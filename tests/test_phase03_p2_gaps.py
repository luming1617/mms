import asyncio
import os
import time
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MMS_TEST_DATABASE_URL"),
        reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests",
    ),
]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent, service

from mms.api import main as api_main
from mms.api.main import app
from mms.config import get_settings
from mms.services.ingestion import IngestionService
from mms.services.outbox import OutboxPublisher
from mms.services.quota import QuotaService
from mms.services.recon import ReconciliationService
from mms.services.repository import MemoryRepository
from mms.services.retriever import PostgresExactRetriever
from mms.services.service import MemoryService
from mms.services.worker import EmbeddingWorker
from mms.utils.barrier_listener import start_barrier_listener, stop_barrier_listener
from mms.utils.db import get_db
from mms.utils.embedding import DeterministicHashEmbeddingProvider, HttpEmbeddingProvider, vector_literal
from mms.utils.errors import MmsError
from mms.utils.metrics import INBOX_STALE, INGESTION_OLDEST_AGE, OUTBOX_FAILED, OUTBOX_PUBLISHED, QUOTA_REJECTED
from mms.utils.tokens import estimate_tokens


def drain(worker) -> None:
    while worker.process_one():
        pass


class CountingProvider(DeterministicHashEmbeddingProvider):
    def __init__(self):
        super().__init__()
        self.batch_sizes = []

    def embed_documents(self, texts, model):
        self.batch_sizes.append(len(texts))
        return super().embed_documents(texts, model)


def test_estimate_tokens_unit():
    assert estimate_tokens("") == 0
    assert estimate_tokens("以后请用中文沟通") == 8
    assert estimate_tokens("Please use English") == 5
    assert estimate_tokens("abc") == 1


def test_token_budget_uses_estimated_tokens():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "budget-p2"))
    drain(EmbeddingWorker(provider))
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn,
            customer_id=customer,
            query="以后请用中文沟通",
            language="zh-CN",
            purpose="CURRENT_ANSWER",
            max_items=8,
            max_tokens=5,
        )
    assert result["memories"] == []
    assert result["filtered_reasons"]["token_budget"] >= 1
    delete(customer, "delete-" + customer)


def test_semantic_candidates_require_live_projection():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "cand-p2"))
    drain(EmbeddingWorker(provider))
    repository = MemoryRepository()
    vector = provider.embed_query("以后请用中文沟通", "BAAI/bge-m3")
    with get_db().connection() as conn:
        found = repository.semantic_candidates(conn, customer, vector, 0.0, set(), "zh-CN")
        assert found
    with get_db().transaction() as conn:
        conn.execute("UPDATE memory_search_document SET status='DELETED' WHERE customer_id=%s", (customer,))
    with get_db().connection() as conn:
        assert repository.semantic_candidates(conn, customer, vector, 0.0, set(), "zh-CN") == []
    delete(customer, "delete-" + customer)


def test_status_set_validation():
    repository = MemoryRepository()
    with get_db().connection() as conn:
        with pytest.raises(ValueError):
            repository.retrieve(
                conn,
                "any",
                "q",
                None,
                "zh-CN",
                "CURRENT_ANSWER",
                5,
                status_override=["SUPERSEDED"],
            )
        with pytest.raises(ValueError):
            repository.retrieve(conn, "any", "q", None, "zh-CN", "NOT_A_PURPOSE", 5)


def test_worker_validates_against_registry_spec():
    worker = EmbeddingWorker(DeterministicHashEmbeddingProvider())
    unnormalized = {"dimension": 4, "config": {"normalize": False}}
    worker._validate_vector([0.0, 0.0, 0.0, 0.0], unnormalized)
    with pytest.raises(ValueError):
        worker._validate_vector([0.0, 0.0, 0.0], unnormalized)
    wide_range = {"dimension": 4, "config": {"normalize": True, "norm_l2_range": [0.0, 2.0]}}
    worker._validate_vector([0.0, 0.0, 0.0, 0.0], wide_range)
    with pytest.raises(ValueError):
        worker._validate_vector([0.0, 0.0, 0.0, 0.0], {"dimension": 4, "config": {"normalize": True}})


def test_worker_reads_back_already_written_vector():
    customer = customer_id()
    grant_consent(customer)
    svc, embedding = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "readback-p2"))
    with get_db().transaction() as conn:
        jobs = conn.execute(
            "SELECT j.*, d.content FROM memory_embedding_job j "
            "JOIN memory_search_document d ON d.customer_id=j.customer_id AND d.memory_id=j.memory_id "
            "AND d.view_kind=j.view_kind AND d.view_language=j.view_language "
            "WHERE j.customer_id=%s",
            (customer,),
        ).fetchall()
        for job in jobs:
            vector = embedding.embed_documents([job["content"]], job["embedding_model"])[0]
            conn.execute(
                """INSERT INTO memory_embedding
                     (memory_id, customer_id, embedding_model, embedding_version, embedding_dim, content_hash,
                      view_kind, view_language, view_text_hash, embedding, status, generated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::vector,'READY',now())""",
                (
                    job["memory_id"],
                    job["customer_id"],
                    job["embedding_model"],
                    job["embedding_version"],
                    embedding.dimension,
                    job["content_hash"],
                    job["view_kind"],
                    job["view_language"],
                    job["view_text_hash"],
                    vector_literal(vector),
                ),
            )
    counting = CountingProvider()
    worker = EmbeddingWorker(counting, worker_id="readback-worker")
    drain(worker)
    assert counting.batch_sizes == []
    with get_db().connection() as conn:
        statuses = {
            row["status"]
            for row in conn.execute(
                "SELECT status FROM memory_embedding_job WHERE customer_id=%s", (customer,)
            ).fetchall()
        }
    assert statuses == {"READY"}
    delete(customer, "delete-" + customer)


def test_source_summary_and_filtered_reasons_shape():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "summary-p2"))
    drain(EmbeddingWorker(provider))
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn,
            customer_id=customer,
            query="以后请用中文沟通",
            language="zh-CN",
            purpose="CURRENT_ANSWER",
            max_items=8,
        )
    assert result["memories"][0]["source_summary"] == "integration:msg-summary-p2"
    assert {"expired", "conflicted", "duplicates", "token_budget"} <= set(result["filtered_reasons"])
    delete(customer, "delete-" + customer)


def test_http_embedding_provider_applies_e5_prefixes(monkeypatch):
    captured = []

    class FakeResponse:
        def __init__(self, count):
            self.count = count

        def raise_for_status(self):
            return None

        def json(self):
            return {"embeddings": [[0.0] * 4 for _ in range(self.count)]}

    def fake_post(url, json, timeout, headers=None):
        captured.append(json)
        return FakeResponse(len(json["inputs"]))

    monkeypatch.setattr(httpx, "post", fake_post)
    provider = HttpEmbeddingProvider("http://embedding.local")
    provider.embed_documents(["hello"], "intfloat/multilingual-e5-large")
    provider.embed_query("hello", "intfloat/multilingual-e5-large")
    provider.embed_documents(["hello"], "BAAI/bge-m3")
    assert captured[0]["inputs"] == ["passage: hello"]
    assert captured[1]["inputs"] == ["query: hello"]
    assert captured[2]["inputs"] == ["hello"]


def test_barrier_notify_reaches_listener():
    received = []
    start_barrier_listener(received.append)
    try:
        probe = "listen-" + uuid4().hex
        with get_db().transaction() as conn:
            conn.execute("SELECT pg_notify('mms_barrier_changed', %s)", (probe,))
        deadline = time.time() + 5
        while probe not in received and time.time() < deadline:
            time.sleep(0.05)
        assert probe in received
    finally:
        stop_barrier_listener()


def test_memory_retriever_adapter():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "retriever-p2"))
    drain(EmbeddingWorker(provider))
    retriever = PostgresExactRetriever(svc)
    vector = provider.embed_query("以后请用中文沟通", "BAAI/bge-m3")
    hits = asyncio.run(
        retriever.search(
            "以后请用中文沟通",
            vector,
            customer_id=customer,
            query_language="zh-CN",
            purpose="CURRENT_ANSWER",
            status_set=("ACTIVE",),
            as_of=None,
            top_k=5,
            filters={},
        )
    )
    assert hits and hits[0]["memory_id"] == str(added.new_memory_id)
    assert hits[0]["matched_view_kind"] in {"ORIGINAL", "CANONICAL"}
    assert asyncio.run(retriever.health())["status"] == "ok"
    with pytest.raises(ValueError):
        asyncio.run(
            retriever.search(
                "q",
                vector,
                customer_id=customer,
                query_language="zh-CN",
                purpose="CUSTOMER_SELF_SERVICE",
                status_set=("SUPERSEDED",),
                as_of=None,
                top_k=5,
                filters={},
            )
        )
    delete(customer, "delete-" + customer)


ADMIN = {
    "X-Admin-Token": get_settings().admin_token,
    "X-Reviewer-ID": "p2-operator",
    "X-Admin-Reason-Code": "P2_FIX_VERIFICATION",
    "X-Admin-Ticket": "T-P2",
}


def test_dlq_replay_is_audited():
    """Doc 10.7: replaying a dead letter is an operator action and must leave an audit trail."""
    http = TestClient(app)
    customer = customer_id()
    job_id = uuid4()
    inbox_event_id = uuid4()
    with get_db().transaction() as conn:
        conn.execute(
            """INSERT INTO memory_ingestion_job (id,customer_id,source_system,source_event_id,event_type,payload_ref,status)
               VALUES (%s,%s,'p2-dlq',%s,'CANDIDATE',%s::jsonb,'DEAD_LETTER')""",
            (
                job_id,
                customer,
                "dlq-" + customer,
                Jsonb({"inbox": {"consumer_group": "p2-dlq", "event_id": str(inbox_event_id)}}),
            ),
        )
    replayed = http.post(
        "/api/v1/admin/dlq/replay",
        headers={**ADMIN, "X-Admin-Role": "OPERATOR"},
        json={"job_type": "INGESTION", "id": str(job_id)},
    )
    assert replayed.status_code == 202, replayed.text
    with get_db().connection() as conn:
        audit = conn.execute(
            "SELECT actor_id,actor_role,reason_code,scope->>'plane' AS plane,scope->>'job_id' AS job_id"
            " FROM admin_access_audit WHERE action='REPLAY' AND scope->>'job_id'=%s",
            (str(job_id),),
        ).fetchone()
        status = conn.execute("SELECT status FROM memory_ingestion_job WHERE id=%s", (job_id,)).fetchone()["status"]
    assert audit == {
        "actor_id": "p2-operator",
        "actor_role": "OPERATOR",
        "reason_code": "P2_FIX_VERIFICATION",
        "plane": "DLQ",
        "job_id": str(job_id),
    }
    assert status == "RECEIVED"
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM memory_ingestion_job WHERE id=%s", (job_id,))
    delete(customer, "delete-" + customer)


def test_admin_role_is_validated_and_gates_the_policy_plane():
    """Doc 12.6: the admin plane distinguishes roles; only policy-capable roles change policy."""
    http = TestClient(app)
    name = "p2-role-" + uuid4().hex[:8]
    body = {
        "action": "CREATE",
        "name": name,
        "version": "1",
        "memory_type": "CUSTOMER_PROFILE",
        "config": {"review_sla_days": 30},
    }

    unknown = http.post("/api/v1/admin/policies", headers={**ADMIN, "X-Admin-Role": "SUPERUSER"}, json=body)
    assert unknown.status_code == 403 and unknown.json()["code"] == "ADMIN_ROLE_INVALID"

    operator = http.post("/api/v1/admin/policies", headers={**ADMIN, "X-Admin-Role": "OPERATOR"}, json=body)
    assert operator.status_code == 403 and operator.json()["code"] == "ADMIN_ROLE_REQUIRED"

    allowed = http.post("/api/v1/admin/policies", headers={**ADMIN, "X-Admin-Role": "POLICY"}, json=body)
    assert allowed.status_code == 200, allowed.text
    with get_db().connection() as conn:
        audit = conn.execute(
            "SELECT actor_role FROM admin_access_audit WHERE action='POLICY_CHANGE' AND scope->>'name'=%s",
            (name,),
        ).fetchone()
    assert audit["actor_role"] == "POLICY"


def test_ingestion_exports_stale_inbox_and_queue_age_signals():
    """Doc 10.3.1: PROCESSING inbox rows past the lease and queued-job age must be measurable."""
    customer = customer_id()
    with get_db().transaction() as conn:
        conn.execute(
            'INSERT INTO memory_inbox_event (consumer_group,event_id,topic,partition,"offset",updated_at)'
            " VALUES ('p2-signal',%s,'memory-candidates',0,1,now()-interval '2 hours')",
            (uuid4(),),
        )
        conn.execute(
            "INSERT INTO memory_ingestion_job (id,customer_id,source_system,source_event_id,event_type,status,created_at)"
            " VALUES (%s,%s,'p2-signal',%s,'CANDIDATE','RECEIVED',now()-interval '12 minutes')",
            (uuid4(), customer, "signal-" + customer),
        )
    stale_before = INBOX_STALE._value.get()
    age_before = INGESTION_OLDEST_AGE._value.get()
    IngestionService()._refresh_queue_signals(0.0)
    assert INBOX_STALE._value.get() >= 1 and INBOX_STALE._value.get() > stale_before
    assert INGESTION_OLDEST_AGE._value.get() >= 700 and INGESTION_OLDEST_AGE._value.get() > age_before
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM memory_ingestion_job WHERE customer_id=%s", (customer,))
        conn.execute("DELETE FROM memory_inbox_event WHERE consumer_group='p2-signal'")
    delete(customer, "delete-" + customer)


def test_quota_blocks_writes_once_the_daily_budget_is_spent(monkeypatch):
    """Doc 12.6: per-customer budgets are enforced in the database and answered with 429."""
    http = TestClient(app)
    internal = {"X-Service-Token": get_settings().service_token}
    customer = customer_id()
    grant_consent(customer)
    quota = QuotaService()
    monkeypatch.setattr(quota, "limit", lambda bucket: 1)
    monkeypatch.setattr(api_main, "quota_service", quota)
    monkeypatch.setattr(api_main.service, "quota", quota)

    first = http.post(
        "/api/v1/memories/candidates",
        headers={**internal, "X-Customer-ID": customer},
        json=candidate(customer, "quota-1").model_dump(mode="json", by_alias=True),
    )
    assert first.status_code == 202, first.text
    second = http.post(
        "/api/v1/memories/candidates",
        headers={**internal, "X-Customer-ID": customer},
        json=candidate(customer, "quota-2").model_dump(mode="json", by_alias=True),
    )
    assert second.status_code == 429 and second.json()["code"] == "QUOTA_EXCEEDED"
    assert QUOTA_REJECTED.labels(bucket="WRITE")._value.get() >= 1
    with get_db().connection() as conn:
        used = conn.execute(
            "SELECT bucket,used FROM customer_quota_usage WHERE customer_id=%s ORDER BY bucket", (customer,)
        ).fetchall()
    assert {row["bucket"] for row in used} == {"WRITE", "EMBEDDING"}
    delete(customer, "delete-" + customer)


def test_quota_storage_bucket_counts_live_memories(monkeypatch):
    quota = QuotaService()
    monkeypatch.setattr(quota.settings, "quota_active_memories", 1)
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "storage-1"))
        with pytest.raises(MmsError) as excinfo:
            quota.check_storage(conn, customer)
    assert excinfo.value.status_code == 429 and excinfo.value.code == "QUOTA_EXCEEDED"
    delete(customer, "delete-" + customer)


def test_high_impact_fact_requires_confirmation():
    """Doc 11.5: a HIGH-sensitivity fact is evidence, not an assertion - the caller must confirm."""
    customer = customer_id()
    grant_consent(customer, ("MEMORY_WRITE", "MEMORY_READ", "SENSITIVE_MEMORY"))
    svc, _ = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "high-impact"))
    with get_db().transaction() as conn:
        conn.execute("UPDATE customer_memory SET sensitivity='HIGH' WHERE id=%s", (added.new_memory_id,))
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn,
            customer_id=customer,
            query="以后请用中文沟通",
            language="zh-CN",
            purpose="CURRENT_ANSWER",
            max_items=8,
            fact_key="communication.language",
        )
    assert result["requires_confirmation"] is True
    assert str(added.new_memory_id) in result["high_impact_memory_ids"]
    delete(customer, "delete-" + customer)


class RecordingReranker:
    def __init__(self):
        self.calls = 0

    def rerank(self, query, documents):
        self.calls += 1
        return {str(document["memory_id"]): 1.0 for document in documents}


def test_rerank_is_gated_by_purpose_and_cross_border_consent():
    """Docs 11.2/12.2: rerank is opt-in per purpose and an external reranker needs its own consent."""
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "rerank-gate"))
    drain(EmbeddingWorker(provider))
    reranker = RecordingReranker()
    gated = MemoryService(MemoryRepository(), provider, reranker=reranker)

    with get_db().connection() as conn:
        self_service = gated.retrieve(
            conn,
            customer_id=customer,
            query="以后请用中文沟通",
            language="zh-CN",
            purpose="CUSTOMER_SELF_SERVICE",
            max_items=8,
        )
    assert self_service["memories"] and not self_service["rerank_used"] and reranker.calls == 0

    with get_db().connection() as conn:
        no_consent = gated.retrieve(
            conn,
            customer_id=customer,
            query="以后请用中文沟通",
            language="zh-CN",
            purpose="CURRENT_ANSWER",
            max_items=8,
        )
    assert not no_consent["rerank_used"] and reranker.calls == 0

    grant_consent(customer, ("CROSS_BORDER_RERANK",))
    with get_db().connection() as conn:
        consented = gated.retrieve(
            conn,
            customer_id=customer,
            query="以后请用中文沟通",
            language="zh-CN",
            purpose="CURRENT_ANSWER",
            max_items=8,
        )
    assert consented["rerank_used"] is True and reranker.calls == 1
    delete(customer, "delete-" + customer)


def test_business_recon_surfaces_dangling_delete_pending():
    """Doc 6.7: a DELETE_PENDING memory with no live barrier is the window left by the
    independent pending-marking transaction; recon reports it, and only repairs the direction
    that a completed barrier proves."""
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "dangling-delete"))
    with get_db().transaction() as conn:
        conn.execute("UPDATE customer_memory SET lifecycle_status='DELETE_PENDING' WHERE id=%s", (added.new_memory_id,))
    dangling = ReconciliationService().run("BUSINESS")
    assert dangling["details"]["delete_pending_without_barrier"] == 1
    assert dangling["status"] == "PARTIAL"

    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO customer_deletion_barrier (customer_id,request_id,status,requested_at,cutover_at,completed_at,generation)"
            " VALUES (%s,%s,'COMPLETED',now(),now(),now(),1)",
            (customer, uuid4()),
        )
    repaired = ReconciliationService().run("BUSINESS")
    assert repaired["details"]["delete_pending_after_completed_barrier"] == 1
    assert repaired["details"].get("delete_pending_without_barrier", 0) == 0
    delete(customer, "delete-" + customer)


def test_recon_reports_dead_letter_stock_and_memory_provenance_gaps():
    """Doc 10.8: recon output covers terminal job stock and evidence/decision coverage."""
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "recon-stock"))
        job_id = uuid4()
        conn.execute(
            "INSERT INTO memory_embedding_job (id,customer_id,memory_id,operation,content_hash,embedding_model,"
            "embedding_version,view_kind,view_language,view_text_hash,generation,status,attempts) "
            "VALUES (%s,%s,%s,'UPSERT',%s,'BAAI/bge-m3',1,'CANONICAL','und',%s,1,'DEAD_LETTER',5)",
            (job_id, customer, added.new_memory_id, "0" * 64, "1" * 64),
        )
    result = ReconciliationService().run("JOB")
    assert result["details"]["dead_letter_stock"] >= 1
    assert result["status"] == "PARTIAL"
    business = ReconciliationService().run("BUSINESS")
    assert business["details"].get("memory_without_evidence", 0) == 0
    assert business["details"].get("memory_without_decision", 0) == 0
    with get_db().transaction() as conn:
        conn.execute("DELETE FROM memory_embedding_job WHERE id=%s", (job_id,))
    delete(customer, "delete-" + customer)


def test_recall_canary_samples_per_language_against_the_active_model():
    """Doc 10.8: the canary covers every language with READY vectors and uses the ACTIVE model."""
    zh_customer = customer_id()
    en_customer = customer_id()
    for customer, event, language, content in (
        (zh_customer, "canary-zh", "zh-CN", "以后请用中文沟通"),
        (en_customer, "canary-en", "en-US", "Please use English"),
    ):
        grant_consent(customer)
        svc, provider = service()
        with get_db().transaction() as conn:
            svc.process_candidate(conn, candidate(customer, event, language=language, content=content, value=language))
        drain(EmbeddingWorker(provider))
    result = ReconciliationService().run("RECALL")
    canary = result["details"]["semantic_canary"]
    assert canary["model"] == get_settings().embedding_model
    assert {"und", "zh-CN", "en-US"} <= set(canary["per_language"])
    assert all(entry["scanned"] >= 1 for entry in canary["per_language"].values())
    assert all(entry["misses"] == 0 for entry in canary["per_language"].values())
    assert result["status"] == "COMPLETED"
    for customer in (zh_customer, en_customer):
        delete(customer, "delete-" + customer)


def test_correction_uses_the_fast_priority_band_and_backfill_the_slow_one():
    """Doc 10.4: corrections are re-embedded first; rebuild/backfill only uses spare capacity."""
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "prio-default"))
    with get_db().transaction() as conn:
        svc.process_candidate(
            conn, candidate(customer, "prio-correction").model_copy(update={"explicit_correction": True})
        )
    with get_db().connection() as conn:
        priorities = {
            int(row["priority"])
            for row in conn.execute("SELECT priority FROM memory_embedding_job WHERE customer_id=%s", (customer,))
        }
    assert 10 in priorities

    with get_db().transaction() as conn:
        conn.execute("DELETE FROM memory_embedding WHERE customer_id=%s", (customer,))
        conn.execute("DELETE FROM memory_embedding_job WHERE customer_id=%s", (customer,))
        assert MemoryRepository().enqueue_missing_embeddings(conn, customer_id=customer) >= 1
    with get_db().connection() as conn:
        backfill = {
            int(row["priority"])
            for row in conn.execute("SELECT priority FROM memory_embedding_job WHERE customer_id=%s", (customer,))
        }
    assert backfill == {500}
    delete(customer, "delete-" + customer)


def test_outbox_failed_rows_are_visible_on_a_gauge():
    """Doc 10.7: FAILED is terminal and outside the pending gauge, so it needs its own series."""
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "outbox-failed"))
    with get_db().transaction() as conn:
        conn.execute(
            "UPDATE memory_outbox SET status='FAILED',last_error='TEST' WHERE aggregate_id=%s",
            (added.new_memory_id,),
        )
    OutboxPublisher()._refresh_metrics()
    assert OUTBOX_FAILED._value.get() >= 1


def test_publish_of_a_vanished_row_is_not_counted_as_success():
    """Doc 6.7.1/10.7: a deletion that races the publish must not look like a delivered event."""
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "outbox-race"))

    class DeletingTransport:
        def __init__(self):
            self.deleted_event_id = None

        def publish(self, event):
            with get_db().transaction() as conn:
                conn.execute("DELETE FROM memory_outbox WHERE event_id=%s", (event["event_id"],))
            self.deleted_event_id = event["event_id"]

    transport = DeletingTransport()
    publisher = OutboxPublisher(transport=transport)
    before = OUTBOX_PUBLISHED.labels(result="row_gone")._value.get()
    for _ in range(50):
        if not publisher.process_one() or transport.deleted_event_id:
            break
    assert transport.deleted_event_id is not None
    assert OUTBOX_PUBLISHED.labels(result="row_gone")._value.get() >= before + 1


def test_ingestion_rejects_topics_outside_the_allowlist():
    """Doc 10.3: the topic is gateway metadata on the HTTP carrier, not free client input."""
    http = TestClient(app)
    customer = customer_id()
    payload = {
        "consumer_group": "p3-topic",
        "event_id": str(uuid4()),
        "topic": "mms.memory.lifecycle.v1",
        "partition": 0,
        "offset": 1,
        "candidate": candidate(customer, "topic-gate").model_dump(mode="json", by_alias=True),
    }
    rejected = http.post(
        "/internal/ingestion/events",
        headers={"X-Service-Token": get_settings().service_token},
        json=payload,
    )
    assert rejected.status_code == 422 and rejected.json()["code"] == "VALIDATION_FAILED"


def test_deletion_tombstone_records_the_kms_key(monkeypatch):
    """Doc 12.5: the tombstone names the key the crypto-shred step has to destroy."""
    monkeypatch.setattr(get_settings(), "kms_key_id", "kms-test-key")
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "kms"))
    result = delete(customer, "delete-" + customer)
    with get_db().connection() as conn:
        stored = conn.execute(
            "SELECT kms_key_id FROM deletion_tombstone WHERE deletion_request_id=%s", (result["request_id"],)
        ).fetchone()["kms_key_id"]
    assert stored == "kms-test-key"
