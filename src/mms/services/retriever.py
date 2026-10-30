from __future__ import annotations

from datetime import datetime
from typing import Literal, Protocol, TypedDict

from ..utils.db import get_db
from .service import MemoryService

RetrievalPurpose = Literal["CURRENT_ANSWER", "HISTORY_DEBUG", "CUSTOMER_SELF_SERVICE", "CANDIDATE_DISCOVERY"]


class MemoryHit(TypedDict):
    memory_id: str
    score: float
    matched_view_kind: str
    matched_view_language: str


class HealthStatus(TypedDict, total=False):
    status: str
    detail: str


class MemoryRetriever(Protocol):
    """Doc 8.13 interface. v2 ships PostgresExactRetriever; a future ANN implementation
    must satisfy the same protocol behind a new ADR."""

    async def search(
        self,
        query_text: str,
        query_vector: list[float],
        *,
        customer_id: str,
        query_language: str,
        purpose: RetrievalPurpose,
        status_set: tuple[str, ...],
        as_of: datetime | None,
        top_k: int,
        filters: dict,
    ) -> list[MemoryHit]: ...

    async def health(self) -> HealthStatus: ...


class PostgresExactRetriever:
    """Adapter over MemoryService: view selection, exact semantic/lexical recall and RRF
    stay in one place; purpose/status_set validation is enforced by the repository."""

    def __init__(self, service: MemoryService):
        self.service = service

    async def search(
        self,
        query_text: str,
        query_vector: list[float],
        *,
        customer_id: str,
        query_language: str,
        purpose: RetrievalPurpose,
        status_set: tuple[str, ...],
        as_of: datetime | None,
        top_k: int,
        filters: dict,
    ) -> list[MemoryHit]:
        with get_db().connection() as conn:
            result = self.service.retrieve(
                conn,
                customer_id=customer_id,
                query=query_text,
                language=query_language,
                purpose=purpose,
                max_items=top_k,
                fact_key=filters.get("fact_key"),
                entity_ref=filters.get("entity_ref"),
                as_of=as_of,
                include_types=filters.get("memory_types"),
                precomputed_vector=query_vector,
                status_override=list(status_set),
                # The adapter supplies the narrowed main-row status set; the conflict and
                # as_of channels are purpose-level and must stay enabled (doc 12.6/12.13).
                include_channels=True,
            )
        return [
            MemoryHit(
                memory_id=str(item["memory_id"]),
                score=float(item["score"]),
                matched_view_kind=item["matched_view_kind"],
                matched_view_language=item["matched_view_language"],
            )
            for item in result["memories"]
        ]

    async def health(self) -> HealthStatus:
        with get_db().connection() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok", "detail": "exact"}
