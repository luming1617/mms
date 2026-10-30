"""Common bootstrap for background worker processes (docs/2 §7.2/§7.3)."""

from __future__ import annotations

import atexit
from typing import TYPE_CHECKING

from .llm import setup_langfuse, shutdown_langfuse
from .logging_config import configure_logging
from .runtime import start_metrics_server
from .tracing import setup_tracing, shutdown_tracing

if TYPE_CHECKING:
    from ..config import Settings


def bootstrap_worker(settings: Settings) -> None:
    """Logging + tracing + a dedicated metrics HTTP server, once per worker process.
    Workers must run their own entrypoints; the API's /metrics endpoint does not exist here."""
    configure_logging(settings)
    tracer_provider = setup_tracing(settings)
    setup_langfuse(settings, tracer_provider=tracer_provider)
    start_metrics_server(settings.metrics_export_port)
    atexit.register(shutdown_tracing, timeout=3)
    atexit.register(shutdown_langfuse)
