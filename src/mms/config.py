import logging
from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .utils.outbound import require_secure_endpoint

logger = logging.getLogger(__name__)

# Doc 13.2/15.4: visibility-path cache TTLs must stay within the 5s deletion/consent SLO;
# this constant is the shared source both the config validation and the SLO configure against.
VISIBILITY_SLO_SECONDS = 5.0


def project_root() -> Path:
    source_root = Path(__file__).resolve().parents[2]
    return source_root if (source_root / "schema" / "schema.sql").is_file() else Path.cwd()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MMS_", env_file=".env", extra="ignore")

    database_url: str = "postgresql://mms:mms@127.0.0.1:5433/mms"
    service_token: str = "dev-token"  # noqa: S105  # nosec B105 - dev-only default; production sets MMS_SERVICE_TOKEN
    admin_token: str = "dev-admin-token"  # noqa: S105  # nosec B105 - dev-only default; production sets MMS_ADMIN_TOKEN
    # Doc 12.6: roles the admin plane accepts, and the subset allowed to change policy.
    admin_roles: list[str] = Field(default_factory=lambda: ["ADMIN", "POLICY", "PRIVACY", "OPERATOR", "READONLY"])
    policy_admin_roles: list[str] = Field(default_factory=lambda: ["ADMIN", "POLICY"])
    embedding_provider: str = Field(default="deterministic", pattern="^(deterministic|http)$")
    embedding_provider_url: str | None = None
    embedding_model: str = "BAAI/bge-m3"
    embedding_version: int = 1
    embedding_dimension: int = 1024
    embedding_batch_size: int = 16
    embedding_lease_seconds: int = 60
    embedding_max_attempts: int = 8
    embedding_timeout_ms: int = 10_000
    worker_poll_seconds: float = 1.0
    ingestion_lease_seconds: int = 60
    # Doc 10.3: over HTTP the topic is gateway metadata, not client input; only these are
    # accepted so a caller cannot mint topics that later confuse replay/dedup forensics.
    ingestion_topics: list[str] = Field(default_factory=lambda: ["memory-candidates"])
    ingestion_max_attempts: int = 5
    # Doc 10.3.1: an inbox event left in PROCESSING longer than the retry backoff cap
    # (300s) plus one lease has no live worker behind it, so it must alert.
    inbox_stale_seconds: int = 360
    outbox_lease_seconds: int = 60
    outbox_max_attempts: int = 5
    # Doc 6.7.1: outbox events blocked by an active barrier are deferred, not dropped.
    outbox_barrier_retry_seconds: int = 30
    outbox_endpoint: str | None = None
    # Doc 12.3: shared service credential for outbound provider/transport calls (TLS only).
    outbound_token: str | None = None
    recon_batch_size: int = 100
    lifecycle_poll_seconds: float = 60.0
    inbox_retention_days: int = 7
    deletion_ledger_endpoint: str | None = None
    # /tmp is volatile: a WORM deletion proof must survive reboots. Deployments should
    # point MMS_DELETION_LEDGER_ENDPOINT at real WORM/object-lock storage (doc 6.8).
    deletion_ledger_dir: Path = Field(default_factory=lambda: project_root() / "var" / "mms-deletion-ledger")
    # Doc 6.7.5: the audit scrub must run on an independent mms_audit_scrubber connection.
    # Point this at credentials whose login role is mms_audit_scrubber; when unset the
    # scrubber transaction falls back to SET LOCAL ROLE on a pooled connection.
    scrubber_database_url: str | None = None
    # Doc 8.3.3: the tombstone retention purge runs on an independent mms_deletion_worker
    # connection - that role is the only account holding DELETE on deletion_tombstone. When
    # unset the purge transaction falls back to SET LOCAL ROLE (development/test only).
    deletion_database_url: str | None = None
    # Doc 6.7.1/6.8: cooling-down window after a completed deletion, tombstone retention
    # (must exceed the backup window), and the backup generation the tombstone voids.
    deletion_cooldown_days: int = 30
    tombstone_retention_days: int = 400
    backup_retention_days: int = 35
    backup_recovery_window_days: int = 30
    deletion_retry_seconds: int = 300
    deletion_backup_generation: str = "unmanaged"
    # Doc 12.5: when storage-at-rest uses a managed key, the tombstone records the key id so
    # the crypto-shred step knows which key to destroy. Unset means encryption is external.
    kms_key_id: str | None = None
    # Doc 7.4/7.9: keyed pseudonyms for audit hashes and Kafka customer_ref values.
    # Dev defaults only; production sets MMS_CUSTOMER_PEPPER / MMS_KAFKA_REF_KEY.
    customer_pepper: str = "dev-customer-pepper"
    kafka_ref_key: str = "dev-kafka-ref-key"
    kafka_ref_key_version: int = 1
    query_cache_ttl_seconds: int = 3600
    query_cache_max_entries: int = 4096
    barrier_cache_ttl_seconds: float = 5.0
    barrier_cache_max_entries: int = 4096
    exact_scan_soft_limit: int = 10_000
    read_replica_enabled: bool = False
    read_replica_database_url: str | None = None
    read_replica_write_hold_seconds: int = 5
    max_views_per_memory: int = 6
    translation_target_languages: list[str] = Field(default_factory=list)
    translation_provider: str | None = Field(default=None, pattern="^(deterministic|http)$")
    translation_provider_url: str | None = None
    translation_model: str | None = None
    translation_model_version: str | None = None
    translation_lease_seconds: int = 60
    translation_max_attempts: int = 5
    translation_timeout_ms: int = 1500
    translation_max_chars: int = 4000
    translation_batch_size: int = 16
    translation_quality_threshold: float = 0.75
    translation_language_priority: list[str] = Field(default_factory=list)
    translation_max_target_languages: int = 4
    translation_memory_types: list[str] = Field(
        default_factory=lambda: [
            "CUSTOMER_PROFILE",
            "SERVICE_HISTORY",
            "ACTIVE_ISSUE",
            "COMMITMENT",
            "ACCOUNT_FACT",
        ]
    )
    lexical_enabled_languages: list[str] = Field(default_factory=lambda: ["zh-CN", "en-US"])
    ts_config_map: dict[str, str] = Field(default_factory=lambda: {"en-US": "english"})
    trgm_fallback_languages: list[str] = Field(default_factory=lambda: ["zh-CN"])
    lexical_mode_map: dict[str, str] = Field(default_factory=lambda: {"zh-CN": "trgm", "en-US": "fts"})
    lexical_trgm_threshold: float = 0.3
    lexical_min_query_chars: int = 3
    retrieval_parallel_lexical: bool = True
    authority_sync_mode: str = Field(default="async", pattern="^async$")
    reranker_enabled: bool = False
    reranker_url: str | None = None
    reranker_timeout_ms: int = 80
    # Doc 11.2: rerank is an opt-in per purpose and only ever scores the first N candidates.
    rerank_enabled_purposes: list[str] = Field(default_factory=lambda: ["CURRENT_ANSWER", "HISTORY_DEBUG"])
    rerank_top_n: int = 20
    # Doc 12.2/12.5: an external reranker receives raw memory text, so it counts as a
    # cross-border channel until the deployment declares it self-hosted.
    reranker_provider: str = Field(default="external", pattern="^(self-hosted|external)$")
    otel_endpoint: str | None = None
    otel_sample_ratio: float = 1.0
    langfuse_public_key: str | None = None
    langfuse_secret_key: str | None = None
    langfuse_base_url: str | None = None
    langfuse_sample_ratio: float = 1.0
    service_name: str = "mms"
    service_version: str = "0.1.0"
    deployment_environment: str = "development"
    log_level: str = "INFO"
    metrics_export_port: int = 0
    bind_host: str = "0.0.0.0"  # noqa: S104  # nosec B104 - container listen address, override with MMS_BIND_HOST
    bind_port: int = 8000
    max_request_bytes: int = 262_144
    # Doc 12.6: per-customer quotas are enforced from DB counters (shared across replicas),
    # not from the in-process token bucket. 0 disables that bucket.
    quota_write_per_day: int = 2000
    quota_extraction_per_day: int = 2000
    quota_embedding_per_day: int = 20000
    quota_active_memories: int = 5000
    rate_limit_enabled: bool = True
    rate_limit_requests_per_second: float = 50.0
    rate_limit_burst: int = 100
    root: Path = Field(default_factory=project_root)

    @field_validator(
        "ingestion_topics",
        "admin_roles",
        "policy_admin_roles",
        "rerank_enabled_purposes",
        "translation_target_languages",
        "translation_language_priority",
        "translation_memory_types",
        "lexical_enabled_languages",
        "trgm_fallback_languages",
        mode="before",
    )
    @classmethod
    def parse_csv(cls, value):
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("ts_config_map", "lexical_mode_map", mode="before")
    @classmethod
    def parse_ts_map(cls, value):
        if isinstance(value, str):
            import json

            return json.loads(value)
        return value

    @model_validator(mode="after")
    def validate_cross_config(self):
        if self.translation_target_languages and not self.translation_provider:
            raise ValueError("TRANSLATION_TARGET_LANGUAGES requires MMS_TRANSLATION_PROVIDER")
        if self.max_views_per_memory < 2:
            raise ValueError("MAX_VIEWS_PER_MEMORY must be >= 2 (ORIGINAL + CANONICAL)")
        if len(self.translation_target_languages) + 2 > self.max_views_per_memory:
            # Doc 13.2: warning, not rejection — ORIGINAL+CANONICAL always reserve 2 slots;
            # the squeeze only caps translation fan-out.
            logger.warning(
                "translation targets squeeze the view budget: %d targets + 2 > MAX_VIEWS_PER_MEMORY=%d",
                len(self.translation_target_languages),
                self.max_views_per_memory,
            )
        if (
            self.translation_target_languages
            and self.translation_provider == "http"
            and not (self.translation_model and self.translation_model_version)
        ):
            raise ValueError("http translation targets require MMS_TRANSLATION_MODEL and MMS_TRANSLATION_MODEL_VERSION")
        if self.barrier_cache_ttl_seconds > VISIBILITY_SLO_SECONDS:
            raise ValueError("BARRIER_CACHE_TTL_SECONDS exceeds the 5s visibility SLO (docs 13.2/15.4)")
        if self.tombstone_retention_days <= self.backup_retention_days + self.backup_recovery_window_days:
            raise ValueError(
                "tombstone retention must exceed backup retention plus the longest restore window (docs 13.2/13.3)"
            )
        if len(set(self.translation_target_languages)) > self.translation_max_target_languages:
            raise ValueError("too many translation target languages")
        if self.translation_provider == "http" and not self.translation_provider_url:
            raise ValueError("http translation provider requires MMS_TRANSLATION_PROVIDER_URL")
        fts_languages = set(self.ts_config_map)
        trgm_languages = set(self.trgm_fallback_languages)
        if fts_languages & trgm_languages:
            raise ValueError("TS_CONFIG_MAP and TRGM_FALLBACK_LANGUAGES must be disjoint")
        missing = set(self.lexical_enabled_languages) - (fts_languages | trgm_languages)
        extra = (fts_languages | trgm_languages) - set(self.lexical_enabled_languages)
        if missing or extra:
            raise ValueError(f"lexical analyzer coverage mismatch: missing={sorted(missing)}, extra={sorted(extra)}")
        for language, mode in self.lexical_mode_map.items():
            if (mode == "fts" and language not in fts_languages) or (mode == "trgm" and language not in trgm_languages):
                raise ValueError(f"lexical mode/analyzer mismatch for {language}")
        if self.embedding_provider == "http" and not self.embedding_provider_url:
            raise ValueError("http embedding provider requires MMS_EMBEDDING_PROVIDER_URL")
        if self.reranker_enabled and not self.reranker_url:
            raise ValueError("reranker requires MMS_RERANKER_URL")
        for setting_name in (
            "outbox_endpoint",
            "deletion_ledger_endpoint",
            "embedding_provider_url",
            "reranker_url",
            "translation_provider_url",
        ):
            endpoint = getattr(self, setting_name)
            if endpoint:
                require_secure_endpoint(endpoint, setting=setting_name.upper(), environment=self.deployment_environment)
        if self.deployment_environment not in {"development", "test", "local"} and not self.deletion_ledger_endpoint:
            # Doc 8.4/20.1: the tombstone ledger must live outside the backup domain; the
            # local file ledger is a development fallback only.
            raise ValueError("DELETION_LEDGER_ENDPOINT is required outside development/test")
        if self.read_replica_enabled and not self.read_replica_database_url:
            raise ValueError("read replica requires MMS_READ_REPLICA_DATABASE_URL")
        invalid_modes = set(self.lexical_mode_map.values()) - {"fts", "trgm"}
        if invalid_modes:
            raise ValueError(f"unsupported lexical modes: {sorted(invalid_modes)}")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
