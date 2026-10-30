"""Langfuse integration: LLM/model observability layered onto the existing OTel pipeline (docs/3).

Mode A (Python SDK) per docs/3 §2.1: the Langfuse SpanProcessor is attached to the *existing*
TracerProvider (§4.1/§4.2 — never a second global provider), third-party instrumentation scopes
(fastapi/httpx/psycopg) are blocked from Langfuse export (§4.4), and observation
input/output/metadata pass a redaction mask before export (§10.1)."""

from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from opentelemetry import trace

from ..utils.metrics import (
    LLM_LATENCY,
    LLM_REQUESTS,
    LLM_TOKENS,
    QUALITY_SCORE_AVG,
    RAG_RETRIEVAL_DURATION,
    RAG_RETRIEVAL_RESULTS,
)

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import TracerProvider

    from ..config import Settings

logger = logging.getLogger(__name__)

_client: Any = None

# docs/3 §4.4: keep Langfuse focused on LLM/业务 spans; HTTP/DB plumbing stays in the
# existing trace backend only. Blocking by instrumentation scope leaves the business
# observations (created under the langfuse tracer) as the trace roots.
BLOCKED_OTEL_SCOPES = [
    "opentelemetry.instrumentation.psycopg",
    "opentelemetry.instrumentation.httpx",
    # FastAPI request spans are emitted under the scope name "fastapi" (verified via Tempo),
    # background-task spans under "opentelemetry.instrumentation.fastapi", and ASGI middleware
    # under "opentelemetry.instrumentation.asgi".
    "fastapi",
    "opentelemetry.instrumentation.fastapi",
    "opentelemetry.instrumentation.asgi",
    # The manual otel spans from the API duplicate the Langfuse root observations
    # (memory-retrieval already covers retrieval.query; docs/3 §5.2 避免重复埋点).
    # They stay visible in the existing trace backend (Tempo).
    "mms.api",
]

_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d[\d\s-]{7,18}\d)(?!\d)")
_BEARER_RE = re.compile(r"(?i)bearer\s+[\w.-]+")

_MAX_TEXT_LENGTH = 512


def redact_text(text: str, max_length: int = _MAX_TEXT_LENGTH) -> str:
    """Mask emails/phones/tokens and truncate: field-level PII minimization (docs/3 §10.1)."""
    masked = _BEARER_RE.sub("bearer [REDACTED]", _PHONE_RE.sub("[PHONE]", _EMAIL_RE.sub("[EMAIL]", text)))
    return masked[:max_length]


def redact_payload(data: Any) -> Any:
    """Recursively redact strings in payloads recorded as observation input/output/metadata."""
    if isinstance(data, str):
        return redact_text(data)
    if isinstance(data, dict):
        return {key: redact_payload(value) for key, value in data.items()}
    if isinstance(data, (list, tuple)):
        return [redact_payload(item) for item in data]
    return data


def _mask_langfuse_data(*, data: Any, **kwargs: dict[str, Any]) -> Any:
    # MaskFunction hook: applied by the SDK before observations leave the process.
    return redact_payload(data)


def setup_langfuse(settings: Settings, tracer_provider: TracerProvider | None = None) -> Any:
    global _client
    public_key = settings.langfuse_public_key or os.getenv("LANGFUSE_PUBLIC_KEY")
    secret_key = settings.langfuse_secret_key or os.getenv("LANGFUSE_SECRET_KEY")
    if not public_key or not secret_key:
        return None
    base_url = settings.langfuse_base_url or os.getenv("LANGFUSE_BASE_URL")
    from langfuse import Langfuse

    client = Langfuse(
        public_key=public_key,
        secret_key=secret_key,
        base_url=base_url,
        environment=settings.deployment_environment,
        release=settings.service_version,
        sample_rate=settings.langfuse_sample_ratio,
        mask=_mask_langfuse_data,
        blocked_instrumentation_scopes=BLOCKED_OTEL_SCOPES,
        tracer_provider=tracer_provider,
    )
    _client = client
    logger.info(
        "langfuse initialized: base_url=%s sample_ratio=%s shared_tracer_provider=%s",
        base_url,
        settings.langfuse_sample_ratio,
        tracer_provider is not None,
    )
    return client


def langfuse_enabled() -> bool:
    return _client is not None


def shutdown_langfuse() -> None:
    global _client
    if _client is None:
        return
    try:
        _client.flush()
        _client.shutdown()
    finally:
        _client = None


def current_trace_id() -> str | None:
    span_context = trace.get_current_span().get_span_context()
    if span_context is not None and span_context.is_valid:
        return format(span_context.trace_id, "032x")
    return None


@contextmanager
def llm_observation(
    name: str,
    *,
    as_type: str = "span",
    input: Any = None,
    model: str | None = None,
    usage_details: dict[str, int] | None = None,
    metadata: Any = None,
    trace_context: dict[str, str] | None = None,
) -> Iterator[Any]:
    """Business observation (docs/3 §5): no-op context when Langfuse is disabled, so business
    code never branches on observability availability."""
    if _client is None:
        yield None
        return
    with _client.start_as_current_observation(
        name=name,
        as_type=as_type,
        input=input,
        model=model,
        usage_details=usage_details,
        metadata=metadata,
        trace_context=trace_context,
    ) as observation:
        yield observation


@contextmanager
def llm_attributes(
    *,
    user_id: str | None = None,
    session_id: str | None = None,
    metadata: dict[str, str] | None = None,
    tags: list[str] | None = None,
    version: str | None = None,
) -> Iterator[None]:
    """Attribute propagation to all observations within the scope (docs/3 §4.3)."""
    if _client is None:
        yield
        return
    from langfuse import propagate_attributes

    with propagate_attributes(
        user_id=user_id,
        session_id=session_id,
        metadata=metadata,
        tags=tags,
        version=version,
    ):
        yield


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(text) for text in texts) // 4


@contextmanager
def model_call(
    provider: str,
    model: str,
    operation: str,
    *,
    input_texts: list[str] | None = None,
    as_type: str = "span",
) -> Iterator[Any]:
    """One model/provider call: Prometheus metrics (docs/3 §6.1/§6.2) + Langfuse observation
    with usage details in one place, so every model touchpoint is instrumented identically."""
    started = time.perf_counter()
    usage: dict[str, int] | None = {"input": estimate_tokens(input_texts)} if input_texts else None
    try:
        with llm_observation(
            f"{operation}.{provider}",
            as_type=as_type,
            model=model,
            usage_details=usage,
            metadata={"provider": provider},
        ) as observation:
            yield observation
    except Exception:
        LLM_REQUESTS.labels(provider=provider, model=model, operation=operation, status="error").inc()
        raise
    else:
        LLM_REQUESTS.labels(provider=provider, model=model, operation=operation, status="success").inc()
    finally:
        LLM_LATENCY.labels(provider=provider, model=model, operation=operation).observe(time.perf_counter() - started)
        if input_texts:
            LLM_TOKENS.labels(provider=provider, model=model, direction="input").inc(estimate_tokens(input_texts))


@contextmanager
def retrieval_observation(
    query: str,
    purpose: str,
    *,
    user_id: str | None,
    session_id: str | None,
    mode: str,
    trace_id: str | None = None,
) -> Iterator[Any]:
    """Root retrieval observation + RAG duration/results metrics (docs/3 §5.2/§6.1).

    When the caller supplies a trace id (32-char hex), the Langfuse trace adopts it so the
    business trace id returned to the caller and the Langfuse trace id are the same value —
    later feedback scores then correlate without any extra lookup (docs/3 §8)."""
    started = time.perf_counter()
    trace_context = {"trace_id": trace_id} if trace_id and _is_hex_trace_id(trace_id) else None
    try:
        with (
            llm_attributes(user_id=user_id, session_id=session_id, tags=["retrieval"]),
            llm_observation(
                "memory-retrieval",
                input={"query": redact_text(query), "purpose": purpose, "mode": mode},
                trace_context=trace_context,
            ) as observation,
        ):
            yield observation
    finally:
        RAG_RETRIEVAL_DURATION.labels(mode=mode).observe(time.perf_counter() - started)


def _is_hex_trace_id(value: str) -> bool:
    return len(value) == 32 and all(char in "0123456789abcdef" for char in value.lower())


def record_retrieval_results(count: int) -> None:
    RAG_RETRIEVAL_RESULTS.observe(count)


_quality_state: dict[str, tuple[float, int]] = {}


def record_quality_score(evaluator: str, value: float) -> None:
    """Aggregate quality scores as low-cardinality Prometheus gauges (docs/3 §9.2);
    individual scores live in Langfuse, never as per-trace series."""
    total, count = _quality_state.get(evaluator, (0.0, 0))
    _quality_state[evaluator] = (total + value, count + 1)
    QUALITY_SCORE_AVG.labels(evaluator=evaluator).set((total + value) / (count + 1))


def publish_feedback_score(trace_id: str | None, feedback_type: str, comment: str | None) -> None:
    """User feedback -> Langfuse score on the original retrieval trace (docs/3 §7.2 User
    Feedback + §8 result correlation). Non-fatal: observability must not break the request."""
    if _client is None or not trace_id:
        return
    value = 1.0 if feedback_type.upper() == "HELPFUL" else 0.0
    record_quality_score("user_feedback", value)
    try:
        _client.create_score(
            trace_id=trace_id,
            name="user-feedback",
            value=value,
            data_type="NUMERIC",
            comment=redact_text(comment or "") or None,
        )
    except Exception:
        logger.warning("failed to publish feedback score", exc_info=True)
