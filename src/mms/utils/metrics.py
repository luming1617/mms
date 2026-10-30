from prometheus_client import Counter, Gauge, Histogram

API_REQUESTS = Counter(
    "mms_api_requests_total", "MMS API requests by route template", ["method", "route", "purpose", "code"]
)
API_LATENCY = Histogram("mms_api_latency_seconds", "MMS API latency", ["route", "path"])
HTTP_IN_FLIGHT = Gauge("mms_http_in_flight", "In-flight HTTP requests")
EVENT_LOOP_LAG = Histogram(
    "mms_event_loop_lag_seconds",
    "Event loop scheduling lag (async blocking detector)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
DB_POOL_CONNECTIONS = Gauge("mms_db_pool_connections", "psycopg pool connections", ["pool", "state"])
OTEL_EXPORT_RESULTS = Counter("mms_otel_export_results_total", "OTel SDK export results", ["telemetry", "result"])
LLM_REQUESTS = Counter(
    "mms_llm_requests_total", "Model/provider requests", ["provider", "model", "operation", "status"]
)
LLM_LATENCY = Histogram("mms_llm_request_duration_seconds", "Model request latency", ["provider", "model", "operation"])
LLM_TOKENS = Counter("mms_llm_tokens_total", "Estimated token usage", ["provider", "model", "direction"])
RAG_RETRIEVAL_DURATION = Histogram("mms_rag_retrieval_duration_seconds", "End-to-end retrieval duration", ["mode"])
RAG_RETRIEVAL_RESULTS = Histogram(
    "mms_rag_retrieval_results",
    "Retrieved memory count per query",
    buckets=(0, 1, 2, 3, 5, 8, 12, 16, 20, 50),
)
QUALITY_SCORE_AVG = Gauge("mms_rag_quality_score_avg", "Aggregated quality score by evaluator", ["evaluator"])
DECISIONS = Counter("mms_decisions_total", "Memory decisions", ["outcome", "rule_id", "reason_code"])
CANDIDATES = Counter("mms_candidates_total", "Memory candidates", ["source", "memory_type", "status"])
EMBEDDING_JOBS = Counter("mms_embedding_jobs_total", "Embedding jobs", ["status", "model", "version"])
EMBEDDING_READY_AGE = Histogram("mms_embedding_ready_age_seconds", "Embedding readiness age", ["view_kind", "model"])
EMBEDDING_FAILURES = Counter("mms_embedding_failures_total", "Embedding failures", ["model", "version", "error"])
EMBEDDING_INVALID_VECTOR = Counter("mms_embedding_invalid_vector_total", "Invalid embedding vectors", ["reason"])
EMBEDDING_CROSS_BORDER_BLOCKED = Counter(
    "mms_embedding_cross_border_blocked_total",
    "Embedding provider calls skipped because CROSS_BORDER_EMBEDDING is not granted",
    ["stage"],
)
EMBEDDING_OLDEST_AGE = Gauge("mms_embedding_job_oldest_age_seconds", "Oldest pending embedding job", ["priority_band"])
VECTOR_SEARCH_LATENCY = Histogram("mms_vector_search_latency_seconds", "Vector search latency", ["path"])
RETRIEVAL_DEGRADED = Counter("mms_retrieval_degraded_total", "Retrieval degradation", ["reason"])
BARRIER_BLOCKED = Counter("mms_barrier_write_blocked_total", "Writes blocked by deletion barrier", ["status"])
BARRIER_FAILURES = Counter("mms_barrier_write_check_failures_total", "Barrier write check failures")
BARRIER_CACHE_MISS = Counter("mms_barrier_cache_miss_total", "Read-path barrier cache misses (primary read)")
BARRIER_CACHE_AGE = Gauge("mms_barrier_cache_age_seconds", "Age of the cached barrier snapshot used by a read")
RETRIEVAL_BLOCKED = Counter(
    "mms_retrieval_blocked_by_barrier_total", "Retrievals blocked by the deletion barrier", ["status"]
)
TOMBSTONE_LEDGER_FAILURES = Counter(
    "mms_deletion_tombstone_ledger_write_failures_total", "Deletion tombstone ledger write failures"
)
DELETION_RETRY = Counter("mms_deletion_retry_total", "Deletion requests retried from VERIFYING", ["result"])
DELETION_VERIFYING = Gauge(
    "mms_deletion_request_verifying", "Deletion requests waiting for the WORM ledger (barrier stays ACTIVE)"
)
CONSENT_CHECKS = Counter("mms_consent_check_total", "Consent checks", ["result"])
OUTBOX_PENDING = Gauge("mms_outbox_pending_total", "Pending outbox rows")
OUTBOX_FAILED = Gauge("mms_outbox_failed_total", "Outbox rows in the terminal FAILED state")
DDL_GUARD_REJECTED = Counter("mms_ddl_guard_rejected_total", "DDL guard rejections", ["constraint"])
CROSS_CUSTOMER_HITS = Counter("mms_cross_customer_hits_total", "Cross customer hits", ["purpose"])
INBOX_DUPLICATES = Counter("mms_inbox_duplicate_total", "Duplicate inbox events", ["result"])
INGESTION_JOBS = Counter("mms_ingestion_jobs_total", "Ingestion job transitions", ["status"])
INBOX_STALE = Gauge("mms_inbox_stale_total", "Inbox events still PROCESSING with no live worker behind them")
INGESTION_OLDEST_AGE = Gauge("mms_ingestion_job_oldest_age_seconds", "Oldest queued ingestion job age")
QUOTA_REJECTED = Counter("mms_quota_rejected_total", "Requests rejected by a per-customer quota", ["bucket"])
OUTBOX_PUBLISHED = Counter("mms_outbox_publish_total", "Outbox publish results", ["result"])
RECON_MISMATCHES = Counter("mms_recon_mismatches_total", "Reconciliation mismatches", ["recon_type"])
LIFECYCLE_TRANSITIONS = Counter("mms_lifecycle_transitions_total", "Lifecycle transitions", ["transition"])
CANDIDATE_EXPIRED_BY_SLA = Counter(
    "mms_candidate_expired_by_sla_total", "Candidates expired by the review SLA (docs/6 6.2)"
)
CONFLICT_TIMEOUT_EXPIRED = Counter(
    "mms_conflict_timeout_expired_total", "Conflicts auto-resolved after conflict_max_age_days (docs/6 6.2)"
)
TRANSLATION_JOBS = Counter("mms_translation_jobs_total", "Translation job transitions", ["status", "target_language"])
TRANSLATION_FAILURES = Counter("mms_translation_failures_total", "Translation failures", ["target_language", "error"])
RERANK_USED = Counter("mms_rerank_total", "Reranker outcomes", ["result"])
RETRIEVAL_POST_FILTER_LOSS = Counter(
    "mms_retrieval_post_filter_loss_total", "Candidates dropped after retrieval filters", ["reason"]
)
RETRIEVAL_ZERO_RESULTS = Counter("mms_retrieval_zero_results_total", "Queries that returned no memory", ["reason"])
RETRIEVAL_CANDIDATES = Histogram(
    "mms_retrieval_candidates",
    "Candidate rows before post-filtering",
    buckets=(0, 1, 2, 4, 8, 16, 32, 64, 128),
)
RETRIEVAL_FILTERED = Counter(
    "mms_retrieval_filtered_total", "Retrieval rows removed after candidate generation", ["reason"]
)
RETRIEVAL_TOKENS = Counter("mms_retrieval_tokens_total", "Estimated retrieval tokens", ["direction"])
FEEDBACK = Counter("mms_feedback_total", "Memory feedback", ["feedback_type"])
CORRECTIONS = Counter("mms_correction_total", "Correction candidate decisions", ["outcome"])
EXACT_SCAN_LARGE_CUSTOMER = Counter("mms_exact_scan_large_customer_total", "Customers above exact scan soft limit")
VECTORS_PER_CUSTOMER = Histogram(
    "mms_vectors_per_customer",
    "READY vectors per customer (docs 13.4 distribution; p50/p95/p99 via histogram_quantile)",
    buckets=(100, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 5000000),
)
VECTORS_PER_CUSTOMER_MAX = Gauge("mms_vectors_per_customer_max", "Largest per-customer READY vector count")
CUSTOMERS_OVER_EXACT_SOFT_LIMIT = Gauge(
    "mms_customers_over_exact_soft_limit", "Customers whose READY vector count exceeds the exact scan soft limit"
)
VIEW_COUNT = Histogram("mms_view_count_per_memory", "Ready views per memory")
VIEW_EVICTED = Counter("mms_view_evicted_total", "Evicted translated views", ["language"])
LANGUAGE_RETRIEVAL = Counter(
    "mms_language_retrieval_total", "Retrieval hits by language and channel", ["language", "channel"]
)
TRANSLATION_READY_AGE = Histogram("mms_translation_ready_age_seconds", "Translation readiness age", ["target_language"])
TRANSLATION_LATENCY = Histogram("mms_translation_latency_seconds", "Translation provider latency", ["target_language"])
RECALL_AT_10 = Gauge("mms_retrieval_recall_at_10", "Recall@10 by language and channel", ["language", "channel"])
RECALL_MRR = Gauge("mms_retrieval_mrr", "MRR by language and channel", ["language", "channel"])
RECALL_BASELINE = Gauge("mms_retrieval_recall_baseline", "Configured Recall@10 baseline", ["language", "channel"])
VIEW_BUDGET_EXHAUSTED = Counter("mms_view_budget_exhausted_total", "Translation view budget exhausted")
LEXICAL_SKIPPED = Counter("mms_lexical_skipped_total", "Lexical channel skipped", ["reason"])
RETRIEVAL_VIEWS_SCANNED = Histogram(
    "mms_retrieval_views_scanned",
    "View rows scanned by a retrieval channel",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000),
)
RETRIEVAL_DUPLICATE_VIEWS_COLLAPSED = Counter(
    "mms_retrieval_duplicate_views_collapsed_total", "Views collapsed by per-memory dedup"
)
RETRIEVAL_VIEW_HITS = Counter(
    "mms_retrieval_view_hits_total", "Retrieval hits by matched view", ["view_kind", "language"]
)
RERANK_LANGUAGE_MISMATCH = Counter(
    "mms_rerank_language_mismatch_total", "Rerank inputs whose matched view language differs from the query"
)
VIEW_MISSING = Gauge(
    "mms_view_missing_views", "Memories missing a required READY embedding view", ["view_kind", "language"]
)

# Docs 15.3 metric registry completion: every metric referenced by alert/SLO rules
# must exist here before the rules ship (contract check: scripts/metrics_contract.py).
OUTBOX_OLDEST_AGE = Gauge("mms_outbox_oldest_age_seconds", "Age of the oldest unpublished (PENDING/RETRY) outbox row")
EMBEDDING_PROVIDER_REQUESTS = Counter(
    "mms_embedding_provider_requests_total", "Embedding provider HTTP requests", ["status"]
)
EMBEDDING_PROVIDER_LATENCY = Histogram("mms_embedding_provider_latency_seconds", "Embedding provider HTTP latency")
EMBEDDING_LEASE_EXPIRED = Counter(
    "mms_embedding_job_lease_expired_total", "Embedding jobs reclaimed after lease expiry"
)
EXTRACTION_PARSE_FAILURES = Counter(
    "mms_extraction_parse_failures_total", "Extractor output parse failures", ["model", "language"]
)
DELETION_COMPLETION = Histogram("mms_deletion_completion_seconds", "Deletion request age at full completion", ["scope"])
BARRIER_EVENT_LAG = Histogram(
    "mms_barrier_event_lag_seconds", "Barrier event publish to API cache invalidation", ["topic"]
)
CONSENT_EVENT_LAG = Histogram(
    "mms_consent_event_lag_seconds", "Consent event publish to API cache invalidation", ["topic"]
)
# Historical naming exceptions (docs 15.3): these are state-snapshot gauges, not counters.
CURRENT_DUPLICATE = Gauge("mms_current_duplicate_total", "Fact slots with more than one ACTIVE current memory")
CURRENT_CONFLICTS = Gauge("mms_current_conflicts_total", "Memory conflicts currently open or in review")
CONSENT_VERSION_STALE = Counter(
    "mms_consent_version_stale_total", "Consent syncs that did not advance the stored version"
)
DECISION_REPLAY_MISMATCH = Counter("mms_decision_replay_mismatch_total", "Decision replays with outcome drift")
DECISION_RULE_UNCOVERED = Counter("mms_decision_rule_uncovered_total", "Decision matrix misses (no rule matched)")
AUDIT_APPEND_ONLY_VIOLATION = Counter(
    "mms_audit_append_only_violation_total", "Append-only audit guard violations", ["table"]
)
ADMIN_AUDIT_WRITE_FAILURES = Counter("mms_admin_audit_write_failures_total", "Admin access audit write failures")
TOMBSTONE_LEDGER_LAG = Gauge(
    "mms_deletion_tombstone_ledger_lag_seconds", "Age of the oldest deletion stuck in VERIFYING behind the WORM ledger"
)
KAFKA_CONSUMER_LAG = Gauge(
    "mms_kafka_consumer_lag_seconds", "Kafka consumer lag behind the topic retention window", ["topic", "group"]
)
KAFKA_TOPIC_RETENTION = Gauge("mms_kafka_topic_retention_seconds", "Kafka topic retention window", ["topic"])
KAFKA_TOMBSTONE_LAG = Histogram("mms_kafka_tombstone_lag_seconds", "Kafka tombstone publish lag")
RENORM_LAG_RATIO = Gauge(
    "mms_renorm_lag_ratio", "Share of ACTIVE memories behind the current registry normalization version"
)
BARRIER_POST_COMMIT_WRITES = Counter(
    "mms_barrier_post_commit_writes_total",
    "Writes committed after a deletion barrier cutover (must stay 0; docs 15.5 MMSBarrierPostCommitWrite)",
)
