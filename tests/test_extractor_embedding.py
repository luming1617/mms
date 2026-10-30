import pytest

from mms.utils.embedding import DeterministicHashEmbeddingProvider

pytestmark = pytest.mark.unit

from mms.services.extractor import RuleBasedExtractor


def test_rule_based_extractor_language_preference():
    candidates = RuleBasedExtractor().extract(
        customer_id="cust", source_system="test", source_event_id="evt", content="以后请用中文和我沟通"
    )
    assert len(candidates) == 1
    assert candidates[0].fact_key == "communication.language"
    assert candidates[0].object_json == {"value": "zh-CN"}
    assert candidates[0].content_language == "zh-CN"


def test_deterministic_embedding_normalized_and_repeatable():
    provider = DeterministicHashEmbeddingProvider()
    first = provider.embed_query("communication.language = zh-CN", "model")
    second = provider.embed_query("communication.language = zh-CN", "model")
    assert first == second
    assert len(first) == 1024
    assert abs(sum(v * v for v in first) - 1.0) < 1e-8
