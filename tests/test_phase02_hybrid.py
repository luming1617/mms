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
from tests.test_phase02_review import conflict_candidate

from mms.config import get_settings
from mms.services.repository import MemoryRepository
from mms.services.service import MemoryService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.embedding import DeterministicHashEmbeddingProvider


def add_ready_memory(customer, event, *, language, content, value):
    svc, provider = service()
    with get_db().transaction() as conn:
        added = svc.process_candidate(conn, candidate(customer, event, language=language, content=content, value=value))
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    return svc, added


def test_english_fts_and_parallel_rrf():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = add_ready_memory(customer, "fts", language="en-US", content="Please use English", value="en-US")
    settings = get_settings()
    settings.retrieval_parallel_lexical = True
    try:
        with get_db().connection() as conn:
            result = svc.retrieve(
                conn,
                customer_id=customer,
                query="Please use English",
                language="en-US",
                purpose="CURRENT_ANSWER",
                max_items=8,
            )
    finally:
        settings.retrieval_parallel_lexical = False
    assert result["lexical_used"] and result["lexical_mode"] == "fts"
    assert result["memories"][0]["score"] == pytest.approx(2 / 61)
    delete(customer, "delete-" + customer)


def test_chinese_trgm_and_lexical_skip_reasons():
    customer = customer_id()
    grant_consent(customer)
    svc, _ = add_ready_memory(customer, "trgm", language="zh-CN", content="以后请用中文", value="zh-CN")
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn, customer_id=customer, query="以后请用中文", language="zh-CN", purpose="CURRENT_ANSWER", max_items=8
        )
        assert result["lexical_used"] and result["lexical_mode"] == "trgm"
        unsupported = svc.retrieve(
            conn, customer_id=customer, query="以后请用中文", language="es-ES", purpose="CURRENT_ANSWER", max_items=8
        )
        assert not unsupported["lexical_used"] and unsupported["lexical_skip_reason"] == "language_not_configured"
        short = svc.retrieve(
            conn, customer_id=customer, query="hi", language="en-US", purpose="CURRENT_ANSWER", max_items=8
        )
        assert short["lexical_skip_reason"] == "query_too_short"
    delete(customer, "delete-" + customer)


class FailingEmbeddingProvider:
    def embed_query(self, text, model):
        raise RuntimeError("embedding unavailable")

    def embed_documents(self, texts, model):
        raise RuntimeError("embedding unavailable")


def test_lexical_fallback_and_reranker_failure_preserve_results():
    customer = customer_id()
    grant_consent(customer)
    _, _ = add_ready_memory(customer, "fallback", language="en-US", content="Please use English", value="en-US")
    repository = MemoryRepository()
    svc = MemoryService(repository, FailingEmbeddingProvider())
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn,
            customer_id=customer,
            query="Please use English",
            language="en-US",
            purpose="CURRENT_ANSWER",
            max_items=8,
        )
    assert result["degraded"] and result["degraded_reason"] == "embedding_provider_unavailable"
    assert result["memories"] and result["lexical_mode"] == "fts"

    class FailingReranker:
        def rerank(self, query, documents):
            raise RuntimeError("reranker unavailable")

    # Doc 12.2: a reranker that is not declared self-hosted is a cross-border channel, so the
    # failing-reranker fallback path needs its own consent first.
    grant_consent(customer, ("CROSS_BORDER_RERANK",))
    svc = MemoryService(repository, DeterministicHashEmbeddingProvider(), reranker=FailingReranker())
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn,
            customer_id=customer,
            query="Please use English",
            language="en-US",
            purpose="CURRENT_ANSWER",
            max_items=8,
        )
    assert result["memories"] and not result["rerank_used"]
    assert result["degraded_reason"] == "reranker_unavailable"
    delete(customer, "delete-" + customer)


def test_rerank_downweights_low_quality_translation():
    svc, _ = service()
    first = {
        "memory_id": uuid4(),
        "content": "a",
        "matched_content": "a",
        "matched_view_kind": "ORIGINAL",
        "translation_quality": None,
        "rrf_score": 0.02,
    }
    second = {
        "memory_id": uuid4(),
        "content": "b",
        "matched_content": "b",
        "matched_view_kind": "TRANSLATED",
        "translation_quality": 0.5,
        "rrf_score": 0.02,
    }

    class ScoringReranker:
        def rerank(self, query, documents):
            return {str(first["memory_id"]): 10.0, str(second["memory_id"]): 18.0}

    svc.reranker = ScoringReranker()
    rows = svc._rerank("query", [second, first])
    assert rows[0]["memory_id"] == first["memory_id"] and rows[1]["score"] == 9.0


def test_conflicted_memory_uses_separate_confirmation_channel():
    customer = customer_id()
    grant_consent(customer)
    svc, provider = service()
    with get_db().transaction() as conn:
        conn.execute(
            "INSERT INTO fact_key_registry (fact_key,version,cardinality,allowed_source_kinds,sensitivity,auto_write,requires_authority,validator,mutual_exclusion,semantic_template,normalization_version,status,effective_from) "
            "VALUES ('profile.conflict',1,'SINGLE_VALUE',ARRAY['USER_EXPLICIT'],'LOW',true,true,'{}','[{\"group\":\"value\",\"values\":[\"*\"]}]',NULL,'norm-test','ACTIVE',now()) "
            "ON CONFLICT (fact_key,version) DO NOTHING"
        )
        first = svc.process_candidate(conn, conflict_candidate(customer, "hybrid-conflict-first", "zh-CN"))
    worker = EmbeddingWorker(provider)
    while worker.process_one():
        pass
    with get_db().transaction() as conn:
        conflicted = svc.process_candidate(conn, conflict_candidate(customer, "hybrid-conflict-second", "en-US"))
        assert conflicted.outcome == "CONFLICT"
    with get_db().connection() as conn:
        result = svc.retrieve(
            conn,
            customer_id=customer,
            query="Customer selected zh-CN",
            language="en-US",
            purpose="CURRENT_ANSWER",
            max_items=8,
        )
    assert result["memories"] == [] and len(result["conflicted_memories"]) == 1
    assert result["requires_confirmation"] and result["conflicted_memories"][0]["use_policy"] == "context_only"
    assert result["conflicted_memories"][0]["memory_id"] == str(first.new_memory_id)
    delete(customer, "delete-" + customer)
