from __future__ import annotations

import json
import math
import threading
import time
from collections import OrderedDict
from uuid import uuid4

from starlette.types import ASGIApp, Receive, Scope, Send


def _header(scope: Scope, name: bytes) -> str | None:
    for key, value in scope["headers"]:
        if key == name:
            return bytes(value).decode("latin-1")
    return None


async def _reject(send: Send, status: int, code: str, detail: str, headers: dict[str, str] | None = None) -> None:
    body = json.dumps(
        {
            "type": "about:blank",
            "title": code,
            "status": status,
            "detail": detail,
            "code": code,
            "trace_id": uuid4().hex,
        }
    ).encode("utf-8")
    raw_headers = [
        (b"content-type", b"application/problem+json"),
        (b"content-length", str(len(body)).encode()),
    ]
    for key, value in (headers or {}).items():
        raw_headers.append((key.lower().encode(), value.encode()))
    await send({"type": "http.response.start", "status": status, "headers": raw_headers})
    await send({"type": "http.response.body", "body": body})


class MaxBodySizeMiddleware:
    """Doc 9.6: request body limit (default 256KB), enforced before the body is parsed."""

    def __init__(self, app: ASGIApp, max_bytes: int):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            declared = _header(scope, b"content-length")
            if declared is not None:
                try:
                    length = int(declared)
                except ValueError:
                    length = 0
                if length > self.max_bytes:
                    await _reject(send, 413, "REQUEST_TOO_LARGE", f"request body exceeds {self.max_bytes} bytes")
                    return
        await self.app(scope, receive, send)


class RateLimitMiddleware:
    """Doc 9.6: 429 + Retry-After. In-process token bucket; deployment-level gateway can
    replace it when multiple API replicas need a shared budget."""

    _SKIP_PREFIXES = ("/health", "/metrics", "/internal")

    def __init__(self, app: ASGIApp, *, enabled: bool, rate: float, burst: int, max_keys: int = 10_000):
        self.app = app
        self.enabled = enabled and rate > 0 and burst > 0
        self.rate = rate
        self.burst = burst
        self.max_keys = max_keys
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()
        self._lock = threading.Lock()

    def _take(self, key: str) -> tuple[bool, float]:
        now = time.monotonic()
        with self._lock:
            tokens, last = self._buckets.get(key, (float(self.burst), now))
            tokens = min(float(self.burst), tokens + (now - last) * self.rate)
            if tokens < 1.0:
                retry_after = (1.0 - tokens) / self.rate
                self._buckets[key] = (tokens, now)
                self._buckets.move_to_end(key)
                return False, retry_after
            self._buckets[key] = (tokens - 1.0, now)
            self._buckets.move_to_end(key)
            while len(self._buckets) > self.max_keys:
                self._buckets.popitem(last=False)
            return True, 0.0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if self.enabled and scope["type"] == "http" and not scope.get("path", "").startswith(self._SKIP_PREFIXES):
            key = _header(scope, b"x-customer-id") or (scope.get("client") or ("unknown", 0))[0]
            allowed, retry_after = self._take(str(key))
            if not allowed:
                await _reject(
                    send,
                    429,
                    "RATE_LIMITED",
                    "request rate limit exceeded",
                    {"Retry-After": str(max(1, math.ceil(retry_after)))},
                )
                return
        await self.app(scope, receive, send)
