from __future__ import annotations

import hashlib
import math
import re
import time
from itertools import pairwise
from typing import TYPE_CHECKING, Protocol

import httpx

from ..observability.llm import model_call
from ..utils.metrics import EMBEDDING_PROVIDER_LATENCY, EMBEDDING_PROVIDER_REQUESTS
from .outbound import auth_headers

if TYPE_CHECKING:
    from ..config import Settings

TOKEN_RE = re.compile(r"[\w\-]+|[\u4e00-\u9fff]", re.UNICODE)


class EmbeddingProvider(Protocol):
    def embed_documents(self, texts: list[str], model: str) -> list[list[float]]: ...
    def embed_query(self, text: str, model: str) -> list[float]: ...
    def health(self) -> bool: ...


class DeterministicHashEmbeddingProvider:
    """Local deterministic 1024-d provider for Phase 1 single-instance development/tests.

    It is normalized and language-agnostic enough for contract tests. Production deployments
    should replace it with the registered self-hosted BGE-M3 provider without changing interfaces.
    """

    def __init__(self, dimension: int = 1024):
        self.dimension = dimension

    def _embed(self, text: str) -> list[float]:
        vector = [0.0] * self.dimension
        tokens = TOKEN_RE.findall(text.lower())
        features: list[str] = []
        features.extend(tokens)
        features.extend("".join(pair) for pair in pairwise(tokens))
        if not tokens:
            features = [text.lower()]
        for feature in features:
            digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=16).digest()
            index = int.from_bytes(digest[:4], "big") % self.dimension
            sign = 1.0 if digest[4] % 2 == 0 else -1.0
            weight = 1.0 + (digest[5] / 255.0)
            vector[index] += sign * weight
        norm = math.sqrt(sum(value * value for value in vector))
        if norm == 0 or not math.isfinite(norm):
            raise ValueError("zero or invalid embedding vector")
        return [value / norm for value in vector]

    def embed_documents(self, texts: list[str], model: str) -> list[list[float]]:
        with model_call("deterministic", model, "embedding", input_texts=texts, as_type="embedding"):
            return [self._embed(text) for text in texts]

    def embed_query(self, text: str, model: str) -> list[float]:
        with model_call("deterministic", model, "embedding", input_texts=[text], as_type="embedding"):
            return self._embed(text)

    def health(self) -> bool:
        return True


class HttpEmbeddingProvider:
    def __init__(self, base_url: str, timeout: float = 10.0, token: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.headers = auth_headers(token)

    @staticmethod
    def _with_prefix(text: str, model: str, kind: str) -> str:
        # Doc 8.3: model-specific query/passage prefixes belong inside the provider.
        if "e5" in model.lower():
            return ("query: " if kind == "query" else "passage: ") + text
        return text

    def _embed(self, texts: list[str], model: str, kind: str) -> list[list[float]]:
        inputs = [self._with_prefix(text, model, kind) for text in texts]
        with model_call("http", model, "embedding", input_texts=inputs, as_type="embedding") as observation:
            started = time.perf_counter()
            try:
                response = httpx.post(
                    f"{self.base_url}/embed",
                    json={"model": model, "inputs": inputs},
                    timeout=self.timeout,
                    headers=self.headers,
                )
                response.raise_for_status()
            except Exception:
                EMBEDDING_PROVIDER_REQUESTS.labels(status="error").inc()
                raise
            finally:
                EMBEDDING_PROVIDER_LATENCY.observe(time.perf_counter() - started)
            EMBEDDING_PROVIDER_REQUESTS.labels(status="success").inc()
            vectors = response.json()["embeddings"]
            result = [[float(value) for value in vector] for vector in vectors]
            if observation is not None:
                observation.update(output={"vector_count": len(result)})
            return result

    def embed_documents(self, texts: list[str], model: str) -> list[list[float]]:
        return self._embed(texts, model, "passage")

    def embed_query(self, text: str, model: str) -> list[float]:
        return self._embed([text], model, "query")[0]

    def health(self) -> bool:
        try:
            return httpx.get(f"{self.base_url}/health", timeout=3, headers=self.headers).status_code == 200
        except httpx.HTTPError:
            return False


def provider_from_settings(settings: Settings) -> EmbeddingProvider:
    if settings.embedding_provider == "http":
        if not settings.embedding_provider_url:
            raise ValueError("MMS_EMBEDDING_PROVIDER_URL is required for the http embedding provider")
        return HttpEmbeddingProvider(
            settings.embedding_provider_url,
            timeout=settings.embedding_timeout_ms / 1000,
            token=settings.outbound_token,
        )
    return DeterministicHashEmbeddingProvider(settings.embedding_dimension)


def vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(f"{value:.9g}" for value in vector) + "]"
