import os

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MMS_TEST_DATABASE_URL"),
        reason="set MMS_TEST_DATABASE_URL for PostgreSQL integration tests",
    ),
]

from tests.test_phase01_integration import candidate, customer_id, delete, grant_consent

from mms.config import get_settings
from mms.services.repository import MemoryRepository
from mms.services.service import MemoryService
from mms.services.translation import DeterministicTranslationProvider, TranslationService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.embedding import DeterministicHashEmbeddingProvider


def translation_service(provider=None):
    settings = get_settings()
    settings.translation_target_languages = ["en-US"]
    settings.translation_provider = "deterministic"
    settings.translation_max_attempts = 2
    repository = MemoryRepository()
    return repository, TranslationService(repository, provider or DeterministicTranslationProvider())


def reset_translation_settings():
    settings = get_settings()
    settings.translation_target_languages = []
    settings.translation_provider = None


def test_translated_view_recall_preserves_original_and_provenance():
    customer = customer_id()
    grant_consent(customer)
    repository, translation = translation_service()
    embedding = DeterministicHashEmbeddingProvider()
    svc = MemoryService(repository, embedding, translation)
    try:
        with get_db().transaction() as conn:
            added = svc.process_candidate(conn, candidate(customer, "translate-add", content="以后请用中文"))
        assert added.outcome == "ADD"
        assert translation.process_one()
        worker = EmbeddingWorker(embedding)
        while worker.process_one():
            pass
        with get_db().connection() as conn:
            result = svc.retrieve(
                conn,
                customer_id=customer,
                query="Please use Chinese",
                language="en-US",
                purpose="CURRENT_ANSWER",
                max_items=8,
            )
            assert result["memories"][0]["matched_view_kind"] == "TRANSLATED"
            assert result["memories"][0]["content"] == "以后请用中文"
            view = conn.execute(
                """SELECT d.*,p.view_text_hash AS original_hash FROM memory_search_document d
                   JOIN memory_search_document p ON p.memory_id=d.memory_id AND p.view_kind='ORIGINAL' AND p.view_language=%s
                   WHERE d.memory_id=%s AND d.view_kind='TRANSLATED'""",
                ("zh-CN", added.new_memory_id),
            ).fetchone()
            assert view["source_view_hash"] == view["original_hash"] and view["translation_quality"] == 0.99
    finally:
        reset_translation_settings()
    delete(customer, "delete-" + customer)


class FailingTranslationProvider:
    model = "failing-translator"
    version = "1"

    def translate(self, text, source_language, target_language):
        raise RuntimeError("translation unavailable")

    def health(self):
        return False


def test_translation_failure_degrades_without_blocking_original_retrieval():
    customer = customer_id()
    grant_consent(customer)
    repository, translation = translation_service(FailingTranslationProvider())
    translation.settings.translation_max_attempts = 1
    embedding = DeterministicHashEmbeddingProvider()
    svc = MemoryService(repository, embedding, translation)
    try:
        with get_db().transaction() as conn:
            svc.process_candidate(conn, candidate(customer, "translate-failure", content="以后请用中文"))
        assert translation.process_one()
        worker = EmbeddingWorker(embedding)
        while worker.process_one():
            pass
        with get_db().connection() as conn:
            assert (
                conn.execute("SELECT status FROM memory_translation_job WHERE customer_id=%s", (customer,)).fetchone()[
                    "status"
                ]
                == "DEAD_LETTER"
            )
            result = svc.retrieve(
                conn,
                customer_id=customer,
                query="以后请用中文",
                language="en-US",
                purpose="CURRENT_ANSWER",
                max_items=8,
            )
            assert result["degraded"] and result["degraded_reason"] == "translation_unavailable"
            assert result["memories"][0]["content"] == "以后请用中文"
    finally:
        reset_translation_settings()
    delete(customer, "delete-" + customer)
