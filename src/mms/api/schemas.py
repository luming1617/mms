from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from ..models import CandidateInput


class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str = Field(min_length=1, max_length=2000)
    conversation_id: str | None = None
    purpose: Literal["CURRENT_ANSWER", "HISTORY_DEBUG", "AUDIT", "CUSTOMER_SELF_SERVICE"] = "CURRENT_ANSWER"
    language: str = "zh-CN"
    max_items: int = Field(default=8, ge=1, le=50)
    max_tokens: int = Field(default=1200, ge=1, le=8000)
    include_types: list[str] | None = None
    as_of: datetime | None = None
    trace_id: str | None = None
    fact_key: str | None = None
    entity_ref: str | None = None


class ExtractRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_system: str = "contact-center"
    source_event_id: str
    content: str = Field(min_length=1, max_length=20_000)
    observed_at: datetime | None = None


class ConsentSyncRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    purpose: Literal[
        "MEMORY_WRITE",
        "MEMORY_READ",
        "MEMORY_RETENTION",
        "SENSITIVE_MEMORY",
        "CROSS_BORDER_EMBEDDING",
        "CROSS_BORDER_TRANSLATION",
        "CROSS_BORDER_RERANK",
    ]
    status: Literal["GRANTED", "DENIED", "WITHDRAWN"]
    version: int = Field(gt=0)
    effective_at: datetime
    expires_at: datetime | None = None
    source: str = "consent-service"
    evidence_ref: str | None = None


class DeleteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    idempotency_key: str | None = Field(default=None, min_length=8, max_length=128)
    scope: dict[str, Any] = Field(default_factory=lambda: {"all": True})


class IngestionEventRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    consumer_group: str = Field(default="mms-ingestion", min_length=1, max_length=128)
    event_id: UUID
    topic: str = Field(min_length=1, max_length=128)
    partition: int = Field(ge=0)
    offset: int = Field(ge=0)
    candidate: CandidateInput


class ReplayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    job_type: Literal["INGESTION", "OUTBOX", "EMBEDDING", "TRANSLATION"]
    id: UUID


class ReviewDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: Literal["APPROVE", "REJECT"]
    decision_version: int = Field(gt=0)
    reason_code: str = Field(min_length=1, max_length=64)
    note: str | None = Field(default=None, max_length=2000)
    co_reviewer_id: str | None = Field(default=None, max_length=128)


class FeedbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    idempotency_key: str | None = Field(default=None, min_length=8, max_length=128)
    request_id: str | None = Field(default=None, max_length=128)
    feedback_type: Literal["INCORRECT", "OUTDATED", "IRRELEVANT", "HELPFUL", "PRIVACY", "OTHER"]
    comment: str | None = Field(default=None, max_length=2000)
    correction: CandidateInput | None = None


class DecisionReplayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision_id: UUID


class PolicyChangeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action: Literal["CREATE", "ACTIVATE", "ROLLBACK"]
    name: str = Field(min_length=1, max_length=128)
    version: str = Field(min_length=1, max_length=64)
    memory_type: str | None = None
    config: dict[str, Any] | None = None
    effective_from: datetime | None = None


class EmbeddingRebuildRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    customer_id: str | None = Field(default=None, max_length=128)
    memory_id: UUID | None = None


class UsageReportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(min_length=8, max_length=128)
    used_memory_ids: list[UUID] = Field(default_factory=list)
    answer_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    model_version: str | None = Field(default=None, max_length=128)
