import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

DSN = os.environ.get("MMS_TEST_DATABASE_URL")
if DSN:
    os.environ["MMS_DATABASE_URL"] = DSN

from mms.models import CandidateInput
from mms.services.repository import MemoryRepository
from mms.services.service import MemoryService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.embedding import DeterministicHashEmbeddingProvider
from mms.utils.errors import BarrierError

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not DSN, reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests"),
]


def customer_id():
    return "test-" + str(uuid4())


def grant_consent(customer, purposes=("MEMORY_WRITE", "MEMORY_READ")):
    with get_db().transaction() as conn:
        for purpose in purposes:
            conn.execute(
                "INSERT INTO customer_memory_consent (customer_id,purpose,status,version,effective_at,source) "
                "VALUES (%s,%s,'GRANTED',1,now(),'integration-test')",
                (customer, purpose),
            )


def candidate(customer, event, *, value="zh-CN", language="zh-CN", content="以后请用中文沟通"):
    return CandidateInput(
        customer_id=customer,
        source_system="integration",
        source_event_id=event,
        source_refs=[{"ref_type": "message", "conversation_id": "conv-test", "message_id": f"msg-{event}"}],
        memory_type="CUSTOMER_PROFILE",
        fact_key="communication.language",
        object={"value": value},
        content=content,
        content_language=language,
        source_kind="USER_EXPLICIT",
        observed_at=datetime.now(UTC),
        language_confidence=0.99,
        explicit_correction=True,
    )


def service():
    provider = DeterministicHashEmbeddingProvider()
    return MemoryService(MemoryRepository(), provider), provider


def process(worker):
    while worker.process_one():
        pass


def delete(customer, key):
    with get_db().transaction() as conn:
        return MemoryRepository().delete_customer_data(conn, customer, "test", key)


def test_full_canary_and_idempotent_delete():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, "add"))
    assert added.outcome == "ADD"
    process(EmbeddingWorker(provider))
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
    assert len(result["memories"]) == 1 and not result["semantic_index_pending"]
    with get_db().transaction() as conn:
        reinforced = svc.process_candidate(
            conn, candidate(customer, "reinforce", language="en-US", content="Please use Chinese")
        )
    assert reinforced.outcome == "REINFORCE"
    process(EmbeddingWorker(provider))
    with get_db().transaction() as conn:
        superseded = svc.process_candidate(
            conn, candidate(customer, "supersede", value="en-US", language="en-US", content="Please use English")
        )
    assert superseded.outcome == "SUPERSEDE"
    process(EmbeddingWorker(provider))
    first = delete(customer, "delete-" + customer)
    second = delete(customer, "delete-" + customer)
    assert first["request_id"] == second["request_id"] and second["status"] == "COMPLETED"
    with get_db().connection() as conn, pytest.raises(BarrierError):
        svc.retrieve(conn, customer_id=customer, query="中文", language="zh-CN", purpose="CURRENT_ANSWER", max_items=8)


def test_concurrent_single_value_has_one_current_version():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = service()
    candidates = [candidate(customer, f"event-{index}") for index in range(16)]

    def submit(item):
        with get_db().transaction() as conn:
            return svc.process_candidate(conn, item).outcome

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(submit, candidates))
    assert outcomes.count("ADD") == 1
    with get_db().connection() as conn:
        counts = conn.execute(
            "SELECT count(*) AS active, count(DISTINCT semantic_hash) AS versions FROM customer_memory "
            "WHERE customer_id=%s AND lifecycle_status='ACTIVE' AND current_flag",
            (customer,),
        ).fetchone()
    assert counts == {"active": 1, "versions": 1}
    delete(customer, "delete-" + customer)


def test_expired_lease_is_reclaimed_and_old_worker_is_fenced():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        svc.process_candidate(conn, candidate(customer, "add"))
    first = EmbeddingWorker(provider, worker_id="first")
    second = EmbeddingWorker(provider, worker_id="second")
    with get_db().transaction() as conn:
        job = first.claim(conn)
        conn.execute(
            "UPDATE memory_embedding_job SET available_at=now()+interval '1 hour' WHERE id<>%s AND customer_id=%s",
            (job["id"], customer),
        )
        conn.execute(
            "UPDATE memory_embedding_job SET lease_expires_at=now()-interval '1 second' WHERE id=%s", (job["id"],)
        )
    with get_db().transaction() as conn:
        reclaimed = second.claim(conn)
    assert reclaimed["id"] == job["id"] and reclaimed["locked_by"] == "second" and reclaimed["attempts"] == 2
    with get_db().transaction() as conn:
        assert not first._final_write(conn, job, provider.embed_query("stale", "BAAI/bge-m3"))
    with get_db().connection() as conn:
        assert (
            conn.execute("SELECT count(*) AS n FROM memory_embedding WHERE customer_id=%s", (customer,)).fetchone()["n"]
            == 0
        )
    with get_db().transaction() as conn:
        document = conn.execute(
            "SELECT content FROM memory_search_document WHERE customer_id=%s AND memory_id=%s AND view_kind=%s AND view_language=%s",
            (customer, reclaimed["memory_id"], reclaimed["view_kind"], reclaimed["view_language"]),
        ).fetchone()
        vector = provider.embed_documents([document["content"]], reclaimed["embedding_model"])[0]
        assert second._final_write(conn, reclaimed, vector)
    process(second)
    delete(customer, "delete-" + customer)


def test_temporal_non_overlap_adds_monotonic_version():
    customer = customer_id()
    grant_consent(customer, ("MEMORY_WRITE", "MEMORY_READ", "SENSITIVE_MEMORY"))
    svc, _ = service()
    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO fact_key_registry (fact_key,version,cardinality,allowed_source_kinds,sensitivity,auto_write,requires_authority,validator,mutual_exclusion,semantic_template,normalization_version,status,effective_from) "
            "VALUES ('issue.test_temporal',1,'TEMPORAL',ARRAY['SYSTEM_OF_RECORD'],'LOW',true,false,'{}','[]',NULL,'norm-test','ACTIVE',now()) "
            "ON CONFLICT (fact_key,version) DO NOTHING"
        )
    start = datetime.now(UTC) - timedelta(days=30)

    def temporal(event, value, valid_from, valid_to):
        return CandidateInput(
            customer_id=customer,
            source_system="ticketing",
            source_event_id=event,
            memory_type="ACTIVE_ISSUE",
            fact_key="issue.test_temporal",
            source_refs=[{"ref_type": "ticket", "ticket_id": f"T-{event}"}],
            object={"value": value},
            content=f"Issue status is {value}",
            content_language="en-US",
            source_kind="SYSTEM_OF_RECORD",
            observed_at=valid_from,
            valid_from=valid_from,
            valid_to=valid_to,
            language_confidence=0.99,
            sensitivity="LOW",
        )

    with get_db().transaction() as conn:
        assert svc.process_candidate(conn, temporal("t1", "open", start, start + timedelta(days=5))).outcome == "ADD"
        added = svc.process_candidate(
            conn, temporal("t2", "closed", start + timedelta(days=10), start + timedelta(days=15))
        )
    assert added.outcome == "ADD_TEMPORAL_VERSION"
    with get_db().connection() as conn:
        versions = conn.execute(
            "SELECT version_no FROM customer_memory WHERE customer_id=%s ORDER BY version_no", (customer,)
        ).fetchall()
    assert [row["version_no"] for row in versions] == [1, 2]
    delete(customer, "delete-" + customer)
