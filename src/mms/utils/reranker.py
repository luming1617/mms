from __future__ import annotations

from typing import Any, Protocol

import httpx

from ..observability.llm import model_call
from .outbound import auth_headers


class Reranker(Protocol):
    def rerank(self, query: str, documents: list[dict[str, Any]]) -> dict[str, float]: ...


class HttpReranker:
    def __init__(self, endpoint: str, timeout_ms: int = 80, token: str | None = None):
        self.endpoint = endpoint
        self.timeout = timeout_ms / 1000
        self.headers = auth_headers(token)

    def rerank(self, query: str, documents: list[dict[str, Any]]) -> dict[str, float]:
        with model_call("http", "reranker", "rerank", input_texts=[query]) as observation:
            response = httpx.post(
                self.endpoint,
                json={"query": query, "documents": documents},
                timeout=self.timeout,
                headers=self.headers,
            )
            response.raise_for_status()
            results = {str(item["memory_id"]): float(item["score"]) for item in response.json()["results"]}
            if observation is not None:
                observation.update(output={"candidates": len(documents), "reranked": len(results)})
            return results
