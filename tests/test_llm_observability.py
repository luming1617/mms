import pytest
from prometheus_client import REGISTRY

from mms.config import Settings
from mms.observability import llm as llm_obs
from mms.utils.metrics import QUALITY_SCORE_AVG

pytestmark = pytest.mark.unit


def _sample_value(name: str, labels: dict[str, str]) -> float | None:
    return REGISTRY.get_sample_value(name, labels)


def test_redact_text_masks_emails_phones_and_tokens():
    text = "contact john.doe@example.com or +8613812345678, Authorization: Bearer abc.def.123 " + "x" * 600
    masked = llm_obs.redact_text(text)
    assert "john.doe@example.com" not in masked
    assert "+8613812345678" not in masked
    assert "abc.def.123" not in masked
    assert "[EMAIL]" in masked
    assert len(masked) == 512


def test_redact_payload_is_recursive():
    payload = {
        "query": "reach me at user@example.com",
        "nested": [{"body": "call +1 415 555 0132"}],
        "count": 3,
    }
    masked = llm_obs.redact_payload(payload)
    assert "user@example.com" not in masked["query"]
    assert "+1 415 555 0132" not in masked["nested"][0]["body"]
    assert masked["count"] == 3


def test_mask_langfuse_data_redacts_observation_payloads():
    masked = llm_obs._mask_langfuse_data(data={"input": "mail me at a@b.com"})
    assert "a@b.com" not in masked["input"]


def test_setup_langfuse_disabled_without_keys(monkeypatch):
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("LANGFUSE_SECRET_KEY", raising=False)
    assert llm_obs.setup_langfuse(Settings(langfuse_public_key=None, langfuse_secret_key=None)) is None
    assert not llm_obs.langfuse_enabled()


def test_setup_langfuse_reads_standard_env_fallback(monkeypatch):
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-lf-test")
    # dead local endpoint: never touch the real Langfuse cloud from tests
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://127.0.0.1:1")
    client = llm_obs.setup_langfuse(Settings(langfuse_public_key=None, langfuse_secret_key=None))
    assert client is not None
    assert llm_obs.langfuse_enabled()
    llm_obs.shutdown_langfuse()
    assert not llm_obs.langfuse_enabled()


def test_llm_observation_noop_without_client():
    with llm_obs.llm_observation("memory-retrieval", input={"query": "hi"}) as observation:
        assert observation is None


def test_model_call_records_success_metrics_and_tokens():
    from mms.utils.embedding import DeterministicHashEmbeddingProvider

    provider = DeterministicHashEmbeddingProvider(dimension=64)
    vectors = provider.embed_documents(["hello world", "second text"], "test-model")
    assert len(vectors) == 2
    labels = {"provider": "deterministic", "model": "test-model", "operation": "embedding", "status": "success"}
    assert _sample_value("mms_llm_requests_total", labels) == 1.0
    assert (
        _sample_value(
            "mms_llm_request_duration_seconds_count",
            {"provider": "deterministic", "model": "test-model", "operation": "embedding"},
        )
        == 1.0
    )
    assert (
        _sample_value(
            "mms_llm_tokens_total", {"provider": "deterministic", "model": "test-model", "direction": "input"}
        )
        or 0
    ) >= 5


def test_model_call_records_error_status(monkeypatch):
    before = (
        _sample_value(
            "mms_llm_requests_total", {"provider": "http", "model": "m", "operation": "rerank", "status": "error"}
        )
        or 0.0
    )
    with pytest.raises(RuntimeError), llm_obs.model_call("http", "m", "rerank", input_texts=["q"]):
        raise RuntimeError("provider down")
    after = (
        _sample_value(
            "mms_llm_requests_total", {"provider": "http", "model": "m", "operation": "rerank", "status": "error"}
        )
        or 0.0
    )
    assert after - before == 1.0


def test_retrieval_observation_records_rag_metrics():
    with llm_obs.retrieval_observation("some query", "CURRENT_ANSWER", user_id="c1", session_id=None, mode="semantic"):
        pass
    llm_obs.record_retrieval_results(3)
    assert (_sample_value("mms_rag_retrieval_duration_seconds_count", {"mode": "semantic"}) or 0) >= 1.0
    assert (_sample_value("mms_rag_retrieval_results_count", {}) or 0) >= 1.0


def test_publish_feedback_score_noop_without_client():
    llm_obs.publish_feedback_score("trace-x", "HELPFUL", "great answer")
    assert _sample_value("mms_rag_quality_score_avg", {"evaluator": "user_feedback"}) is None


def test_publish_feedback_score_publishes_to_langfuse(monkeypatch):
    calls: list[dict] = []

    class FakeClient:
        def create_score(self, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(llm_obs, "_client", FakeClient())
    llm_obs.record_quality_score("user_feedback", 0.0)
    llm_obs.publish_feedback_score("trace-x", "OUTDATED", "reach me at user@example.com")
    assert len(calls) == 1
    assert calls[0]["trace_id"] == "trace-x"
    assert calls[0]["value"] == 0.0
    assert "user@example.com" not in (calls[0]["comment"] or "")
    assert QUALITY_SCORE_AVG.labels(evaluator="user_feedback")._value.get() == 0.0
    monkeypatch.setattr(llm_obs, "_client", None)


def test_estimate_tokens():
    assert llm_obs.estimate_tokens(["abcd", "efgh"]) == 2
