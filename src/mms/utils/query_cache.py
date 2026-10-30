from __future__ import annotations

import threading
import time
import unicodedata
from collections import OrderedDict
from hashlib import sha256

from ..config import get_settings
from .embedding import EmbeddingProvider


class _VectorCache:
    def __init__(self) -> None:
        self._entries: OrderedDict[str, tuple[float, list[float]]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> list[float] | None:
        ttl = get_settings().query_cache_ttl_seconds
        if ttl <= 0:
            return None
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            stored_at, vector = entry
            if time.monotonic() - stored_at >= ttl:
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return vector

    def put(self, key: str, vector: list[float]) -> None:
        limit = get_settings().query_cache_max_entries
        with self._lock:
            self._entries[key] = (time.monotonic(), vector)
            self._entries.move_to_end(key)
            while len(self._entries) > limit:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


_CACHE = _VectorCache()


def normalize_query(text: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", text).split())


def query_cache_key(model: str, version: int | str, language: str, text: str) -> str:
    payload = f"{model}\x00{version}\x00{language}\x00{normalize_query(text)}"
    return sha256(payload.encode("utf-8")).hexdigest()


def embed_query_cached(
    provider: EmbeddingProvider,
    text: str,
    model: str,
    *,
    version: int | str,
    language: str,
) -> list[float]:
    """Doc 8.5: cache only the vector, keyed by sha256(model||version||language||query).

    The query itself never enters the cache, and the key carries no customer identity.
    """
    key = query_cache_key(model, version, language, text)
    cached = _CACHE.get(key)
    if cached is not None:
        return cached
    vector = provider.embed_query(text, model)
    _CACHE.put(key, vector)
    return vector


def clear_query_cache() -> None:
    _CACHE.clear()
