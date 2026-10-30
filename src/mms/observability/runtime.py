"""Runtime metrics: event loop lag, DB pool saturation and worker metric exposure (docs/2 §4.2/§7.1)."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from ..utils.metrics import DB_POOL_CONNECTIONS, EVENT_LOOP_LAG

if TYPE_CHECKING:
    from ..utils.db import Database

logger = logging.getLogger(__name__)

_LOOP_LAG_INTERVAL = 1.0
_POOL_INTERVAL = 5.0

_loop_lag_task: asyncio.Task | None = None
_metrics_server_started = False


async def _monitor_loop_lag(db: Database | None) -> None:
    loop = asyncio.get_running_loop()
    next_pool_check = loop.time()
    while True:
        before = loop.time()
        await asyncio.sleep(_LOOP_LAG_INTERVAL)
        lag = loop.time() - before - _LOOP_LAG_INTERVAL
        if lag > 0:
            EVENT_LOOP_LAG.observe(lag)
        now = loop.time()
        if db is not None and now >= next_pool_check:
            next_pool_check = now + _POOL_INTERVAL
            update_db_pool_gauges(db)


def update_db_pool_gauges(db: Database) -> None:
    pools = (("primary", db.pool), ("replica", db.replica_pool))
    for name, pool in pools:
        if pool is None:
            continue
        stats = pool.get_stats()
        DB_POOL_CONNECTIONS.labels(pool=name, state="open").set(stats.get("pool_size", 0))
        DB_POOL_CONNECTIONS.labels(pool=name, state="available").set(stats.get("pool_available", 0))
        DB_POOL_CONNECTIONS.labels(pool=name, state="waiting").set(stats.get("requests_waiting", 0))


def start_runtime_monitor(db: Database | None) -> asyncio.Task | None:
    """Background task for the async API process; safe no-op if already running."""
    global _loop_lag_task
    if _loop_lag_task is not None and not _loop_lag_task.done():
        return _loop_lag_task
    _loop_lag_task = asyncio.get_running_loop().create_task(_monitor_loop_lag(db))
    return _loop_lag_task


def stop_runtime_monitor() -> None:
    global _loop_lag_task
    if _loop_lag_task is not None:
        _loop_lag_task.cancel()
        _loop_lag_task = None


def start_metrics_server(port: int) -> None:
    """Expose prometheus_client's default registry for non-HTTP worker processes (docs/2 §7.2:
    worker metrics are invisible without an exporter; one server per process)."""
    global _metrics_server_started
    if port <= 0 or _metrics_server_started:
        return
    from prometheus_client import start_http_server

    start_http_server(port)
    _metrics_server_started = True
    logger.info("worker metrics server listening on port %s", port)
