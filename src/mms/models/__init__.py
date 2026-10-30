from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

MemoryType = Literal[
    "CUSTOMER_PROFILE",
    "SERVICE_HISTORY",
    "ACTIVE_ISSUE",
    "COMMITMENT",
    "EPHEMERAL_CONTEXT",
    "ACCOUNT_FACT",
]
SourceKind = Literal[
    "SYSTEM_OF_RECORD",
    "USER_EXPLICIT",
    "AGENT_CONFIRMED",
    "USER_IMPLICIT",
    "MODEL_INFERRED",
    "KNOWLEDGE_RETRIEVED",
    "ASSISTANT_GENERATED",
]
Sensitivity = Literal["LOW", "MEDIUM", "HIGH"]
Cardinality = Literal["SINGLE_VALUE", "MULTI_VALUE", "TEMPORAL"]
Outcome = Literal[
    "ADD",
    "SUPERSEDE",
    "ADD_TEMPORAL_VERSION",
    "NOOP",
    "REINFORCE",
    "CONFLICT",
    "RESOLVE",
    "EXPIRE",
    "IGNORE",
    "REJECTED",
    "NEEDS_VERIFICATION",
    "DELETE_REQUESTED",
]


class CandidateInput(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    candidate_id: UUID = Field(default_factory=uuid4)
    customer_id: str = Field(min_length=1, max_length=128)
    source_system: str = Field(min_length=1, max_length=64)
    source_event_id: str = Field(min_length=1, max_length=128)
    memory_type: MemoryType
    fact_key: str = Field(pattern=r"^[a-z0-9_.-]+$")
    subject: str | None = None
    predicate: str | None = None
    entity_type: str = ""
    entity_ref: str = ""
    value_slot: str = ""
    object_json: dict[str, Any] = Field(default_factory=dict, validation_alias="object")
    content: str = Field(min_length=1, max_length=4000)
    content_language: str = "und"
    language: str | None = None
    script: str | None = None
    language_confidence: float | None = Field(default=1.0, ge=0, le=1)
    source_kind: SourceKind
    source_refs: list[dict[str, Any]] = Field(default_factory=list)
    confidence: float | None = Field(default=None, ge=0, le=1)
    sensitivity: Sensitivity = "LOW"
    explicit_correction: bool = False
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    observed_at: datetime
    expires_at: datetime | None = None
    policy_version: str | None = None

    @field_validator("valid_to")
    @classmethod
    def valid_interval(cls, value, info):
        valid_from = info.data.get("valid_from")
        if value is not None and valid_from is not None and value <= valid_from:
            raise ValueError("valid_to must be after valid_from")
        return value


class NormalizedCandidate(CandidateInput):
    object_hash: str
    content_hash: str
    semantic_text: str
    semantic_hash: str
    normalization_version: str
    fact_key_registry_version: int
    cardinality: Cardinality
    value_score: int
    policy_version: str
    policy_config_hash: str
    candidate_fingerprint: str


class ExistingFact(BaseModel):
    memory_id: UUID
    version_no: int
    lifecycle_status: str
    object_json: dict[str, Any] = Field(default_factory=dict)
    object_hash: str
    semantic_hash: str
    content_hash: str
    content_language: str
    source_kind: SourceKind
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    evidence_languages: set[str] = Field(default_factory=set)


class Decision(BaseModel):
    decision_id: UUID = Field(default_factory=uuid4)
    candidate_id: UUID
    outcome: Outcome
    reason_codes: list[str]
    rule_trace: list[dict[str, Any]]
    primary_rule_id: str
    target_memory_id: UUID | None = None
    new_memory_id: UUID | None = None
    matched_memory_ids: list[UUID] = Field(default_factory=list)
    expected_versions: dict[str, Any] = Field(default_factory=dict)
    policy_version: str
    fact_key_registry_version: int | None = None
    engine_version: str
    semantic_hash: str
    normalization_version: str
    score_breakdown: dict[str, Any] = Field(default_factory=dict)
    context_snapshot: dict[str, Any] = Field(default_factory=dict)
    requires_review: bool = False
    embedding_action: Literal["UPSERT", "UPSERT_VIEWS", "DELETE", "UPSERT_NEW_AND_DELETE_OLD", "NOOP", "REBUILD"] = (
        "NOOP"
    )
    view_actions: list[dict[str, Any]] = Field(default_factory=list)
    index_generation: int = 0
