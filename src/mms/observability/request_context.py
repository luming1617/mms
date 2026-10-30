"""Request-id propagation and golden-signal HTTP metrics with route templates (docs/2 §4.4/§5.2/§4.1)."""

from __future__ import annotations

import time
import uuid
from contextvars import ContextVar

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ..utils.metrics import API_LATENCY, API_REQUESTS, HTTP_IN_FLIGHT

request_id_var: ContextVar[str | None] = ContextVar("mms_request_id", default=None)
# Doc 18.4: the latency tier is a property of the request (rerank / as_of / HISTORY_DEBUG /
# replica fallback), not a process-wide switch.
latency_tier_var: ContextVar[str | None] = ContextVar("mms_latency_tier", default=None)

REQUEST_ID_HEADER = b"x-request-id"


def current_request_id() -> str | None:
    return request_id_var.get()


def set_latency_tier(tier: str) -> None:
    latency_tier_var.set(tier)


class RequestContextMiddleware:
    """Echoes/derives X-Request-ID into the contextvar and the response so business logs,
    gateway logs and external systems can be correlated independently of the trace (docs/2 §5.2)."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request_id = Headers(scope=scope).get("x-request-id") or uuid.uuid4().hex
        token = request_id_var.set(request_id)

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((REQUEST_ID_HEADER, request_id.encode("ascii")))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            request_id_var.reset(token)


def _route_template(scope: Scope) -> str:
    route = scope.get("route")
    template = getattr(route, "path", None)
    if isinstance(template, str) and template:
        return template
    return "unmatched"


def _labels(scope: Scope, status_code: int, extended_enabled: bool) -> dict[str, str]:
    template = _route_template(scope)
    method = scope.get("method", "")
    purpose = "query" if template in ("/api/v1/memories/query", "/api/v1/agent/memory-context") else "other"
    requested = latency_tier_var.get()
    tier = (
        "extended" if template.startswith("/api/v1/admin") or extended_enabled or requested == "extended" else "default"
    )
    return {
        "method": method,
        "route": template,
        "purpose": purpose,
        "code": str(status_code),
        "path": tier,
    }


class HttpMetricsMiddleware:
    """Traffic/Errors/Latency with template routes (never concrete ids) and in-flight gauge."""

    def __init__(self, app: ASGIApp, extended_enabled: bool = False) -> None:
        self.app = app
        self.extended_enabled = extended_enabled

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()
        HTTP_IN_FLIGHT.inc()
        status: dict[str, int] = {"code": 500}

        async def send_capture_status(message: Message) -> None:
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_capture_status)
        finally:
            HTTP_IN_FLIGHT.dec()
            labels = _labels(scope, status["code"], self.extended_enabled)
            API_LATENCY.labels(route=labels["route"], path=labels["path"]).observe(time.perf_counter() - started)
            API_REQUESTS.labels(
                method=labels["method"], route=labels["route"], purpose=labels["purpose"], code=labels["code"]
            ).inc()
