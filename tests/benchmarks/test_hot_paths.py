"""Function-level performance baselines for hot paths.

These tests are marked slow and excluded from the default pytest profile.
Run them through scripts/quality_gate.py --profile nightly or directly with:

    pytest tests/benchmarks -m benchmark --benchmark-json=benchmark.json
"""

import pytest
from tests.test_rules import decide, make_candidate

from mms.utils.embedding import DeterministicHashEmbeddingProvider
from mms.utils.hashing import normalize_language, sha256_canonical

pytestmark = [pytest.mark.benchmark, pytest.mark.slow]

PAYLOAD = {
    "customer_id": "cust-benchmark",
    "fact_key": "communication.language",
    "object": {"value": "zh-CN"},
    "source_refs": [{"message_id": "m-1", "conversation_id": "c-1"}],
    "tags": ["profile", "language", "preference"],
}


def test_benchmark_canonical_hash(benchmark) -> None:
    assert benchmark(sha256_canonical, PAYLOAD)


def test_benchmark_language_normalization(benchmark) -> None:
    assert benchmark(normalize_language, "zh_CN") == "zh-CN"


def test_benchmark_deterministic_embedding(benchmark) -> None:
    provider = DeterministicHashEmbeddingProvider()
    vector = benchmark(provider.embed_query, "以后请用中文和我沟通", "BAAI/bge-m3")
    assert len(vector) == 1024


def test_benchmark_decision_matrix(benchmark) -> None:
    candidate, breakdown, registry, policy = make_candidate()
    decision = benchmark(decide, candidate, breakdown, registry, policy)
    assert decision.outcome == "ADD"
