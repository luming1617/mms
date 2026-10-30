import logging

import httpx
import pytest
from pydantic import ValidationError

from mms.config import Settings
from mms.services.translation import DeterministicTranslationProvider, HttpTranslationProvider
from mms.utils.embedding import DeterministicHashEmbeddingProvider, HttpEmbeddingProvider
from mms.utils.reranker import HttpReranker

pytestmark = pytest.mark.unit


def settings(**overrides) -> Settings:
    return Settings(_env_file=None, **overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"translation_target_languages": ["en-US"]},
        {
            "translation_target_languages": ["en-US", "zh-CN", "es-ES", "fr-FR", "de-DE"],
            "translation_provider": "deterministic",
            "translation_max_target_languages": 4,
        },
        {"translation_provider": "http"},
        {
            "ts_config_map": {"zh-CN": "simple"},
            "trgm_fallback_languages": ["zh-CN"],
            "lexical_enabled_languages": ["zh-CN"],
        },
        {"lexical_enabled_languages": ["zh-CN", "en-US", "es-ES"]},
        {"lexical_enabled_languages": ["en-US"]},
        {"lexical_mode_map": {"en-US": "trgm"}},
        {"lexical_mode_map": {"en-US": "bm25"}},
        {"embedding_provider": "http"},
        {"reranker_enabled": True},
        {"max_views_per_memory": 1},
        {"barrier_cache_ttl_seconds": 5.5},
        {"tombstone_retention_days": 60},
        {
            "translation_target_languages": ["en-US"],
            "translation_provider": "http",
            "translation_provider_url": "http://127.0.0.1:9",
            "translation_model": "mt",
        },
        {"read_replica_enabled": True},
    ],
)
def test_invalid_cross_configuration_is_rejected(overrides):
    with pytest.raises(ValidationError):
        settings(**overrides)


def test_view_budget_squeeze_warns_but_still_starts(caplog):
    with caplog.at_level(logging.WARNING, logger="mms.config"):
        configured = settings(
            translation_target_languages=["en-US", "zh-CN", "es-ES", "fr-FR"],
            translation_provider="deterministic",
            max_views_per_memory=5,
        )
    assert len(configured.translation_target_languages) == 4
    assert any("squeeze" in record.getMessage() for record in caplog.records)


def test_csv_and_json_environment_values_are_parsed():
    configured = settings(translation_target_languages="en-US, zh-CN", translation_provider="deterministic")
    assert configured.translation_target_languages == ["en-US", "zh-CN"]
    configured = settings(ts_config_map='{"en-US": "english"}')
    assert configured.ts_config_map == {"en-US": "english"}


def test_valid_http_configuration_is_accepted():
    configured = settings(
        embedding_provider="http",
        embedding_provider_url="http://embedding.local",
        translation_provider="http",
        translation_provider_url="http://translation.local",
        reranker_enabled=True,
        reranker_url="http://reranker.local",
        read_replica_enabled=True,
        read_replica_database_url="postgresql://replica/mms",
    )
    assert configured.embedding_provider_url == "http://embedding.local"


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("POST", "http://test")
            raise httpx.HTTPStatusError("boom", request=request, response=self)

    def json(self):
        return self.payload


def test_http_embedding_provider_contract(monkeypatch):
    calls = []

    def fake_post(url, json, timeout, headers=None):
        calls.append((url, json, timeout))
        return FakeResponse({"embeddings": [[0.1, 0.2], [0.3, 0.4]]})

    monkeypatch.setattr("httpx.post", fake_post)
    provider = HttpEmbeddingProvider("http://embedding.local/", timeout=2)
    assert provider.embed_documents(["a", "b"], "BAAI/bge-m3") == [[0.1, 0.2], [0.3, 0.4]]
    assert provider.embed_query("a", "BAAI/bge-m3") == [0.1, 0.2]
    assert calls[0][0] == "http://embedding.local/embed"
    assert calls[0][1]["inputs"] == ["a", "b"]


def test_http_embedding_health_true_and_false(monkeypatch):
    monkeypatch.setattr("httpx.get", lambda url, timeout, headers=None: FakeResponse({}, 200))
    assert HttpEmbeddingProvider("http://embedding.local").health() is True
    monkeypatch.setattr("httpx.get", lambda url, timeout, headers=None: FakeResponse({}, 503))
    assert HttpEmbeddingProvider("http://embedding.local").health() is False
    monkeypatch.setattr("httpx.get", lambda url, timeout, headers=None: (_ for _ in ()).throw(httpx.HTTPError("down")))
    assert HttpEmbeddingProvider("http://embedding.local").health() is False


def test_http_reranker_contract(monkeypatch):
    monkeypatch.setattr(
        "httpx.post",
        lambda url, json, timeout, headers=None: FakeResponse({"results": [{"memory_id": "m-1", "score": 0.9}]}),
    )
    scores = HttpReranker("http://reranker.local", timeout_ms=50).rerank("q", [{"memory_id": "m-1"}])
    assert scores == {"m-1": 0.9}


def test_translation_providers_contract(monkeypatch):
    monkeypatch.setattr(
        "httpx.post",
        lambda url, json, timeout, headers=None: FakeResponse({"translated_text": "Hello", "quality": 0.88}),
    )
    result = HttpTranslationProvider("http://translation.local/").translate("你好", "zh-CN", "en-US")
    assert (result.content, result.model, result.version, result.quality) == ("Hello", "http-translator", "1", 0.88)
    monkeypatch.setattr("httpx.get", lambda url, timeout, headers=None: FakeResponse({}, 200))
    provider = HttpTranslationProvider("http://translation.local")
    assert provider.health() is True
    assert DeterministicTranslationProvider().health() is True
    fallback = DeterministicTranslationProvider().translate("unknown", "zh-CN", "en-US")
    assert fallback.content == "[en-US] unknown" and fallback.quality == 0.75


def test_deterministic_embedding_provider_contract():
    provider = DeterministicHashEmbeddingProvider()
    assert provider.health() is True
    assert provider.embed_documents([], "model") == []
