"""TracerProvider bootstrap: OTLP export, sampling, resource identity and graceful shutdown (docs/2 §3.2/§3.3/§11.1)."""

from __future__ import annotations

import atexit
import logging
import os
from collections.abc import Sequence
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExportResult
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

from ..utils.metrics import OTEL_EXPORT_RESULTS

if TYPE_CHECKING:
    from ..config import Settings

logger = logging.getLogger(__name__)

_provider: TracerProvider | None = None


def resolve_traces_endpoint(settings: Settings) -> str | None:
    if settings.otel_endpoint:
        return settings.otel_endpoint
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
    if endpoint:
        return endpoint
    base = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if base:
        suffix = "/v1/traces" if not base.endswith("/v1/traces") else ""
        return f"{base.rstrip('/')}{suffix}"
    return None


def setup_tracing(settings: Settings) -> TracerProvider | None:
    """Configure the global TracerProvider once per process; subsequent calls are no-ops so
    repeated app startups (tests, reload) never recreate export threads (docs/2 §7.2)."""
    global _provider
    if _provider is not None:
        return _provider
    endpoint = resolve_traces_endpoint(settings)
    if not endpoint:
        return None
    resource = Resource.create(
        {
            "service.name": settings.service_name,
            "service.version": settings.service_version,
            "deployment.environment.name": settings.deployment_environment,
        }
    )
    sampler = ParentBased(TraceIdRatioBased(settings.otel_sample_ratio))
    provider = TracerProvider(resource=resource, sampler=sampler)
    exporter = _CountingSpanExporter(endpoint=endpoint, timeout=5)
    processor = BatchSpanProcessor(exporter, max_export_batch_size=512)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    _provider = provider
    _instrument_libraries()
    atexit.register(shutdown_tracing, timeout=3)
    logger.info("tracing initialized: endpoint=%s sample_ratio=%s", endpoint, settings.otel_sample_ratio)
    return provider


class _CountingSpanExporter(OTLPSpanExporter):
    """Count export outcomes so "collector down / traces lost" is itself observable (docs/2 §8.2)."""

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        result = super().export(spans)
        name = getattr(result, "name", str(result))
        OTEL_EXPORT_RESULTS.labels(telemetry="traces", result=name).inc()
        return result


def _instrument_libraries() -> None:
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    HTTPXClientInstrumentor().instrument()
    try:
        from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor

        PsycopgInstrumentor().instrument()
    except Exception:  # pragma: no cover - psycopg instrumentation is best-effort
        logger.debug("psycopg instrumentation unavailable", exc_info=True)


def shutdown_tracing(timeout: float = 5.0) -> None:
    global _provider
    if _provider is None:
        return
    try:
        _provider.force_flush(timeout_millis=int(timeout * 1000))
    finally:
        _provider.shutdown()
        _provider = None


def record_error(span: trace.Span, exc: BaseException) -> None:
    """Record exception and mark the span ERROR (docs/2 §3.3: business 4xx stays unset)."""
    span.record_exception(exc)
    span.set_status(trace.Status(trace.StatusCode.ERROR, exc.__class__.__name__))
