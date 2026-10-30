from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Callable

import psycopg

from ..config import get_settings

logger = logging.getLogger(__name__)

_CHANNEL = "mms_barrier_changed"
_STOP = threading.Event()
_READY = threading.Event()
_THREAD: threading.Thread | None = None
_CONNECTION: psycopg.Connection | None = None
_LOCK = threading.Lock()


def _listen(on_change: Callable[[str], None]) -> None:
    global _CONNECTION
    settings = get_settings()
    while not _STOP.is_set():
        try:
            with psycopg.connect(settings.database_url, autocommit=True) as conn:
                with _LOCK:
                    _CONNECTION = conn
                conn.execute(f"LISTEN {_CHANNEL}")
                _READY.set()
                for notify in conn.notifies():
                    if _STOP.is_set():
                        break
                    on_change(notify.payload)
        except Exception:
            if _STOP.is_set():
                break
            logger.warning("barrier_listener_reconnect")
            _STOP.wait(1.0)
        finally:
            _READY.clear()
            with _LOCK:
                _CONNECTION = None


def start_barrier_listener(on_change: Callable[[str], None]) -> threading.Thread:
    """Doc 6.7.6: cross-process barrier cache invalidation via stdout-free Postgres NOTIFY.

    Every API replica keeps a LISTEN connection; barrier/consent transitions notify on
    commit, so a read cache is flushed before the 5s TTL fallback would expire it.
    """
    global _THREAD
    if _THREAD is not None and _THREAD.is_alive():
        return _THREAD
    _STOP.clear()
    _THREAD = threading.Thread(target=_listen, args=(on_change,), name="mms-barrier-listener", daemon=True)
    _THREAD.start()
    if not _READY.wait(timeout=5.0):
        logger.warning("barrier_listener_not_ready")
    return _THREAD


def stop_barrier_listener() -> None:
    global _THREAD
    _STOP.set()
    _READY.clear()
    with _LOCK:
        conn = _CONNECTION
    if conn is not None:
        with contextlib.suppress(Exception):
            conn.close()
    if _THREAD is not None:
        _THREAD.join(timeout=2.0)
        _THREAD = None
