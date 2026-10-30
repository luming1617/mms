"""Structured JSON logging with trace/span/request correlation (docs/2 §5)."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from opentelemetry import trace

from .request_context import request_id_var

if TYPE_CHECKING:
    from ..config import Settings

_SENSITIVE_KEYS = frozenset({"password", "token", "api_key", "authorization", "cookie", "secret", "api_key_hash"})

_RESERVED_RECORD_KEYS = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "message",
        "asctime",
        "color_message",
    }
)


def _trace_fields() -> dict[str, str]:
    span = trace.get_current_span()
    context = span.get_span_context()
    if context is not None and context.is_valid:
        return {"trace_id": format(context.trace_id, "032x"), "span_id": format(context.span_id, "016x")}
    return {}


class JsonFormatter(logging.Formatter):
    """Emits one JSON object per record: timestamp/level/service identity + trace_id/span_id
    (live trace context, never fixed values) + request_id (docs/2 §5.1/§5.2)."""

    def __init__(self, service_name: str, service_version: str):
        super().__init__()
        self._service_name = service_name
        self._service_version = service_version

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "service.name": self._service_name,
            "service.version": self._service_version,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(_trace_fields())
        request_id = request_id_var.get()
        if request_id:
            payload["request_id"] = request_id
        event = record.__dict__.get("event")
        if event:
            payload["event"] = str(event)
        for key, value in record.__dict__.items():
            if key in _RESERVED_RECORD_KEYS or key.startswith("_") or key == "otelTraceID" or key == "otelSpanID":
                continue
            name = key.lower()
            if name in _SENSITIVE_KEYS:
                payload[key] = "[REDACTED]"
            else:
                payload[key] = value
        if record.exc_info and record.exc_info[0] is not None:
            payload["error.type"] = record.exc_info[0].__name__
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(settings: Settings) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(settings.service_name, settings.service_version))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(settings.log_level.upper())
    # uvicorn's own config is disabled at run(); access logs stay muted to INFO-noise per docs/2 §5.3.
    for name, level in (("uvicorn", None), ("uvicorn.error", None), ("uvicorn.access", logging.WARNING)):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers = []
        if level is not None:
            uvicorn_logger.setLevel(level)
