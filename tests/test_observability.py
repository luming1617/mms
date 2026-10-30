import asyncio
import contextlib
import json
import logging
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from prometheus_client import REGISTRY

from mms.config import Settings
from mms.observability.attributes import sanitize_attributes
from mms.observability.bootstrap import bootstrap_worker
from mms.observability.logging_config import JsonFormatter
from mms.observability.request_context import (
    HttpMetricsMiddleware,
    RequestContextMiddleware,
    current_request_id,
    request_id_var,
)
from mms.observability.tracing import setup_tracing, shutdown_tracing

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def tracing_provider():
    """A processor-less SDK provider so formatter/span tests see real trace context without
    any network export; installed once per process (OTel forbids re-setting)."""
    with contextlib.suppress(Exception):  # already set by another test in this process
        trace.set_tracer_provider(TracerProvider())
    yield


def _sample_value(name: str, labels: dict[str, str] | None = None) -> float | None:
    return REGISTRY.get_sample_value(name, labels or {})


def test_sanitize_attributes_drops_sensitive_and_high_cardinality_keys():
    attributes: dict[str, Any] = {
        "mms.purpose": "MEMORY_READ",
        "mms.retrieval.results": 3,
        "mms.rerank.used": True,
        "customer_id": "cust-123",
        "query": "如何优化向量检索",
        "request_id": "550e8400",
        "mms.degraded_reason": "x" * 300,
    }
    sanitized = sanitize_attributes(attributes)
    assert sanitized["mms.purpose"] == "MEMORY_READ"
    assert sanitized["mms.retrieval.results"] == 3
    assert sanitized["mms.rerank.used"] is True
    assert len(sanitized["mms.degraded_reason"]) == 128
    assert "customer_id" not in sanitized
    assert "query" not in sanitized
    assert "request_id" not in sanitized


def test_json_formatter_emits_structured_payload_with_trace_context():
    formatter = JsonFormatter("mms-test", "1.2.3")
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("test.span") as span:
        record = logging.LogRecord(
            name="mms.test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="retrieval backend timeout",
            args=(),
            exc_info=(TimeoutError, TimeoutError("upstream"), None),
        )
        record.__dict__["event"] = "retrieval.failed"
        payload = json.loads(formatter.format(record))
    context = span.get_span_context()
    assert payload["level"] == "ERROR"
    assert payload["service.name"] == "mms-test"
    assert payload["service.version"] == "1.2.3"
    assert payload["event"] == "retrieval.failed"
    assert payload["message"] == "retrieval backend timeout"
    assert payload["trace_id"] == format(context.trace_id, "032x")
    assert payload["span_id"] == format(context.span_id, "016x")
    assert payload["error.type"] == "TimeoutError"


def test_json_formatter_includes_request_id_and_redacts_sensitive_extra():
    formatter = JsonFormatter("mms-test", "1.2.3")
    token = request_id_var.set("req-abc-123")
    try:
        record = logging.LogRecord(
            name="mms.test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="document loaded",
            args=(),
            exc_info=None,
        )
        record.__dict__["operation"] = "document.load"
        record.__dict__["token"] = "super-secret"
        payload = json.loads(formatter.format(record))
    finally:
        request_id_var.reset(token)
    assert payload["request_id"] == "req-abc-123"
    assert payload["operation"] == "document.load"
    assert payload["token"] == "[REDACTED]"


def _context_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(HttpMetricsMiddleware)
    app.add_middleware(RequestContextMiddleware)

    @app.get("/echo")
    def echo():
        return {"request_id": current_request_id()}

    @app.get("/items/{item_id}")
    def read_item(item_id: int):
        return {"item_id": item_id}

    @app.get("/fail")
    def fail():
        raise RuntimeError("boom")

    return app


def test_request_id_middleware_echoes_and_generates():
    client = TestClient(_context_app())
    echoed = client.get("/echo", headers={"X-Request-ID": "gateway-42"})
    assert echoed.status_code == 200
    assert echoed.headers["x-request-id"] == "gateway-42"
    assert echoed.json()["request_id"] == "gateway-42"
    generated = client.get("/echo")
    assert generated.status_code == 200
    assert generated.headers["x-request-id"]


def test_http_metrics_use_route_templates_not_concrete_paths():
    client = TestClient(_context_app())
    assert client.get("/items/41").status_code == 200
    assert client.get("/items/42").status_code == 200
    labels = {"method": "GET", "route": "/items/{item_id}", "purpose": "other", "code": "200"}
    assert _sample_value("mms_api_requests_total", labels) == 2.0
    assert (
        _sample_value(
            "mms_api_requests_total", {"method": "GET", "route": "/items/41", "purpose": "other", "code": "200"}
        )
        is None
    )
    assert _sample_value("mms_api_latency_seconds_count", {"route": "/items/{item_id}", "path": "default"}) == 2.0


def test_http_metrics_record_unhandled_exception_as_5xx():
    client = TestClient(_context_app(), raise_server_exceptions=False)
    assert client.get("/fail").status_code == 500
    labels = {"method": "GET", "route": "/fail", "purpose": "other", "code": "500"}
    assert _sample_value("mms_api_requests_total", labels) == 1.0


@pytest.mark.asyncio
async def test_event_loop_lag_monitor_records_observations():
    from mms.observability.runtime import start_runtime_monitor, stop_runtime_monitor

    before = _sample_value("mms_event_loop_lag_seconds_count") or 0.0
    monitor = start_runtime_monitor(None)
    try:
        await asyncio.sleep(2.2)
    finally:
        stop_runtime_monitor()
        monitor.cancel()
    after = _sample_value("mms_event_loop_lag_seconds_count") or 0.0
    assert after - before >= 2


def test_setup_tracing_disabled_without_endpoint():
    settings = Settings(otel_endpoint=None)
    assert setup_tracing(settings) is None


def test_setup_tracing_configures_provider_once():
    from mms.observability import tracing as tracing_module

    settings = Settings(otel_endpoint="http://127.0.0.1:59999/v1/traces", otel_sample_ratio=0.5)
    provider = setup_tracing(settings)
    assert provider is not None
    assert setup_tracing(settings) is provider
    shutdown_tracing(timeout=2)
    assert tracing_module._provider is None
    # after shutdown a fresh setup is possible again (process restart semantics)
    assert setup_tracing(settings) is not None
    shutdown_tracing(timeout=2)


def test_otel_export_results_counter_counts_failures():
    # A dead OTLP endpoint must show up as a telemetry-pipeline metric, not silence (docs/2 §8.2).
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    from mms.observability import tracing as tracing_module

    exporter = tracing_module._CountingSpanExporter(endpoint="http://127.0.0.1:59999/v1/traces", timeout=2)
    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(exporter))
    tracer = provider.get_tracer("verify")
    with tracer.start_as_current_span("verify.span"):
        pass
    provider.force_flush(timeout_millis=5000)
    provider.shutdown()
    assert (_sample_value("mms_otel_export_results_total", {"telemetry": "traces", "result": "FAILURE"}) or 0) >= 1.0


def test_worker_bootstrap_starts_metrics_server():
    port = 9199
    settings = Settings(otel_endpoint=None, metrics_export_port=port)
    bootstrap_worker(settings)
    response = httpx.get(f"http://127.0.0.1:{port}/metrics", timeout=5)
    assert response.status_code == 200
    assert "mms_api_requests_total" in response.text or "python_gc" in response.text
