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

from mms.config import Settings, get_settings
from mms.services.repository import MemoryRepository
from mms.services.service import MemoryService
from mms.services.translation import DeterministicTranslationProvider, TranslationService
from mms.services.usage import UsageService
from mms.services.view_cost import ViewCostService
from mms.services.worker import EmbeddingWorker
from mms.utils.db import get_db
from mms.utils.embedding import DeterministicHashEmbeddingProvider


def test_analyzer_configuration_is_disjoint_and_complete():
    settings = Settings()
    assert settings.lexical_mode_map["zh-CN"] == "trgm"
    assert settings.lexical_mode_map["en-US"] == "fts"
    with pytest.raises(ValueError, match="disjoint"):
        Settings(
            lexical_enabled_languages=["zh-CN", "en-US"],
            ts_config_map={"zh-CN": "simple", "en-US": "english"},
            trgm_fallback_languages=["zh-CN"],
        )
    with pytest.raises(ValueError, match="coverage mismatch"):
        Settings(
            lexical_enabled_languages=["zh-CN", "en-US", "es-ES"],
            ts_config_map={"en-US": "english"},
            trgm_fallback_languages=["zh-CN"],
            lexical_mode_map={"zh-CN": "trgm", "en-US": "fts", "es-ES": "fts"},
        )


def test_view_cost_evicts_only_low_value_translations():
    customer = customer_id()
    grant_consent(customer)
    settings = get_settings()
    settings.translation_target_languages = ["en-US", "es-ES", "fr-FR", "de-DE"]
    settings.translation_language_priority = ["en-US", "es-ES", "fr-FR", "de-DE"]
    settings.translation_provider = "deterministic"
    repository = MemoryRepository()
    translation = TranslationService(repository, DeterministicTranslationProvider())
    embedding = DeterministicHashEmbeddingProvider()
    svc = MemoryService(repository, embedding, translation)
    try:
        with get_db().transaction() as conn:
            added = svc.process_candidate(conn, candidate(customer, "view-cost-add", content="以后请用中文"))
        while translation.process_one():
            pass
        worker = EmbeddingWorker(embedding)
        while worker.process_one():
            pass
        with get_db().transaction() as conn:
            reinforced = svc.process_candidate(
                conn, candidate(customer, "view-cost-original-ja", language="ja-JP", content="中国語でお願いします")
            )
            assert reinforced.outcome == "REINFORCE"
        while worker.process_one():
            pass
        UsageService().record_retrieval(
            customer,
            "view-hot-usage-1",
            "CURRENT_ANSWER",
            [
                {
                    "memory_id": str(added.new_memory_id),
                    "content": "以后请用中文",
                    "matched_view_kind": "TRANSLATED",
                    "matched_view_language": "en-US",
                }
            ],
            degraded=False,
            token_count=6,
        )
        result = ViewCostService().run_once()
        assert result["evicted_count"] == 1 and result["evicted"][0]["language"] != "en-US"
        with get_db().connection() as conn:
            views = conn.execute(
                "SELECT view_kind,view_language FROM memory_search_document WHERE memory_id=%s AND status='READY'",
                (added.new_memory_id,),
            ).fetchall()
            vector_count = conn.execute(
                "SELECT count(*) AS n FROM memory_embedding WHERE memory_id=%s AND status='READY'",
                (added.new_memory_id,),
            ).fetchone()["n"]
        identities = {(row["view_kind"], row["view_language"]) for row in views}
        assert len(views) == vector_count == 6
        assert ("CANONICAL", "und") in identities
        assert ("ORIGINAL", "zh-CN") in identities and ("ORIGINAL", "ja-JP") in identities
        assert ("TRANSLATED", "en-US") in identities
    finally:
        settings.translation_target_languages = []
        settings.translation_language_priority = []
        settings.translation_provider = None
    delete(customer, "delete-" + customer)
