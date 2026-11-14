-- MMS greenfield schema baseline.
-- Source of truth for a new database: extensions, roles, tables, partitions,
-- constraints, triggers and seed data. Apply once to an empty database.
-- Do not edit semantics without a new ADR.


-- Runtime roles (NOLOGIN). Password/login wiring belongs to environment bootstrap.
DO $$
DECLARE
  role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['mms_app','mms_worker','mms_deletion_worker','mms_audit_scrubber','mms_readonly'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN', role_name);
    END IF;
  END LOOP;
END $$;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- 事实键注册表
CREATE TABLE fact_key_registry (
  fact_key text NOT NULL,
  version integer NOT NULL,
  cardinality text NOT NULL CHECK (cardinality IN ('SINGLE_VALUE','MULTI_VALUE','TEMPORAL')),
  authority_source text,
  allowed_source_kinds text[] NOT NULL,
  sensitivity text NOT NULL CHECK (sensitivity IN ('LOW','MEDIUM','HIGH')),
  retention_days integer CHECK (retention_days IS NULL OR retention_days > 0),
  auto_write boolean NOT NULL DEFAULT false,
  requires_authority boolean NOT NULL DEFAULT false,
  normalizer text,
  validator jsonb NOT NULL DEFAULT '{}'::jsonb,
  mutual_exclusion jsonb NOT NULL DEFAULT '[]'::jsonb,
  semantic_template text,
  normalization_version text NOT NULL,
  status text NOT NULL CHECK (status IN ('DRAFT','ACTIVE','RETIRED')),
  effective_from timestamptz NOT NULL,
  PRIMARY KEY (fact_key, version),
  UNIQUE (fact_key, version, cardinality)
);

-- 政策版本
CREATE TABLE memory_policy (
  id uuid PRIMARY KEY,
  name text NOT NULL,
  version text NOT NULL,
  memory_type text CHECK (memory_type IN
    ('CUSTOMER_PROFILE','SERVICE_HISTORY','ACTIVE_ISSUE','COMMITMENT','EPHEMERAL_CONTEXT','ACCOUNT_FACT')),
  status text NOT NULL CHECK (status IN ('DRAFT','SHADOW','ACTIVE','RETIRED')),
  config jsonb NOT NULL,
  config_hash char(64) NOT NULL,
  effective_from timestamptz NOT NULL,
  created_by text NOT NULL,
  approved_by text,
  approved_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (name, version),
  CHECK (approved_by IS NULL OR approved_by <> created_by),
  CHECK (status <> 'ACTIVE' OR (approved_by IS NOT NULL AND approved_at IS NOT NULL))
);

-- 业务记忆
CREATE TABLE customer_memory (
  id uuid PRIMARY KEY,
  customer_id text NOT NULL,
  memory_type text NOT NULL CHECK (memory_type IN
    ('CUSTOMER_PROFILE','SERVICE_HISTORY','ACTIVE_ISSUE','COMMITMENT','EPHEMERAL_CONTEXT','ACCOUNT_FACT')),
  fact_key text NOT NULL,
  subject text,
  predicate text,
  cardinality text NOT NULL CHECK (cardinality IN ('SINGLE_VALUE','MULTI_VALUE','TEMPORAL')),
  entity_type text NOT NULL DEFAULT '',
  entity_ref text NOT NULL DEFAULT '',
  value_slot text NOT NULL DEFAULT '',
  object_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  object_hash char(64) NOT NULL,
  content text NOT NULL,
  content_language text NOT NULL,
  language text NOT NULL,
  script text,
  content_hash char(64) NOT NULL,
  semantic_text text NOT NULL,
  semantic_hash char(64) NOT NULL,
  normalization_version text NOT NULL,
  language_confidence real CHECK (language_confidence IS NULL OR language_confidence BETWEEN 0 AND 1),
  source_kind text NOT NULL CHECK (source_kind IN
    ('SYSTEM_OF_RECORD','USER_EXPLICIT','AGENT_CONFIRMED','USER_IMPLICIT','MODEL_INFERRED','KNOWLEDGE_RETRIEVED','ASSISTANT_GENERATED')),
  confidence real CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
  value_score smallint,
  sensitivity text NOT NULL DEFAULT 'LOW' CHECK (sensitivity IN ('LOW','MEDIUM','HIGH')),
  valid_from timestamptz,
  valid_to timestamptz,
  observed_at timestamptz NOT NULL,
  expires_at timestamptz,
  lifecycle_status text NOT NULL CHECK (lifecycle_status IN
    ('ACTIVE','CONFLICTED','RESOLVED','SUPERSEDED','EXPIRED','DELETE_PENDING','DELETED')),
  version_no integer NOT NULL DEFAULT 1 CHECK (version_no > 0),
  supersedes_memory_id uuid REFERENCES customer_memory(id),
  current_flag boolean NOT NULL DEFAULT true,
  policy_version text NOT NULL,
  fact_key_registry_version integer NOT NULL,
  extractor_version text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to > valid_from),
  CHECK (value_score IS NULL OR value_score BETWEEN 0 AND 100),
  CHECK (cardinality <> 'TEMPORAL' OR valid_from IS NOT NULL),
  FOREIGN KEY (fact_key, fact_key_registry_version, cardinality)
    REFERENCES fact_key_registry (fact_key, version, cardinality),
  CONSTRAINT uq_memory_version UNIQUE NULLS NOT DISTINCT
    (customer_id, fact_key, entity_type, entity_ref, value_slot, version_no)
);

CREATE INDEX ix_memory_owner_status ON customer_memory (customer_id, lifecycle_status);
CREATE INDEX ix_memory_fact_lookup ON customer_memory (customer_id, fact_key, current_flag);
CREATE INDEX ix_memory_entity ON customer_memory (customer_id, entity_type, entity_ref);
-- 过期/保留扫描覆盖 ACTIVE、CONFLICTED、RESOLVED、SUPERSEDED（后三类仍需 EXPIRE 收敛）
CREATE INDEX ix_memory_expiry ON customer_memory (expires_at)
  WHERE lifecycle_status IN ('ACTIVE','CONFLICTED','RESOLVED','SUPERSEDED')
    AND expires_at IS NOT NULL;
CREATE INDEX ix_memory_expiry_owner ON customer_memory (customer_id, expires_at)
  WHERE lifecycle_status IN ('ACTIVE','CONFLICTED','RESOLVED','SUPERSEDED')
    AND expires_at IS NOT NULL;
CREATE INDEX ix_memory_content_hash ON customer_memory (customer_id, content_hash);
CREATE INDEX ix_memory_semantic_hash ON customer_memory (customer_id, semantic_hash);
CREATE INDEX ix_memory_source ON customer_memory (source_kind, observed_at);
CREATE INDEX ix_memory_validity ON customer_memory (customer_id, valid_from, valid_to);

CREATE UNIQUE INDEX ux_memory_multi_value ON customer_memory
  (customer_id, fact_key, entity_type, entity_ref, value_slot, object_hash)
  WHERE current_flag AND cardinality = 'MULTI_VALUE'
    AND lifecycle_status IN ('ACTIVE','CONFLICTED');

ALTER TABLE customer_memory ADD CONSTRAINT ex_memory_temporal_overlap
  EXCLUDE USING gist (
    customer_id WITH =,
    fact_key WITH =,
    entity_type WITH =,
    entity_ref WITH =,
    value_slot WITH =,
    tstzrange(valid_from, valid_to, '[)') WITH &&
  ) WHERE (current_flag AND cardinality = 'TEMPORAL'
    AND lifecycle_status IN ('ACTIVE','CONFLICTED'));

-- SINGLE_VALUE 当前版本指针
-- 注意：主键包含 value_slot，不能单独证明 SINGLE_VALUE 唯一；
-- 唯一性由 ux_current_single_value + value_slot='' 的 CHECK 保证。
CREATE TABLE customer_memory_current (
  customer_id text NOT NULL,
  fact_key text NOT NULL,
  entity_type text NOT NULL DEFAULT '',
  entity_ref text NOT NULL DEFAULT '',
  cardinality text NOT NULL DEFAULT 'SINGLE_VALUE' CHECK (cardinality = 'SINGLE_VALUE'),
  value_slot text NOT NULL DEFAULT '' CHECK (value_slot = ''),
  memory_id uuid NOT NULL REFERENCES customer_memory(id) ON DELETE CASCADE,
  generation bigint NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (customer_id, fact_key, entity_type, entity_ref, value_slot),
  CHECK (cardinality <> 'SINGLE_VALUE' OR value_slot = '')
);
CREATE UNIQUE INDEX ux_current_single_value ON customer_memory_current
  (customer_id, fact_key, entity_type, entity_ref)
  WHERE cardinality = 'SINGLE_VALUE';

-- 授权/同意快照（P0-1；权威源在身份/授权服务）
CREATE TABLE customer_memory_consent (
  customer_id text NOT NULL,
  purpose text NOT NULL CHECK (purpose IN
    ('MEMORY_WRITE','MEMORY_READ','MEMORY_RETENTION','SENSITIVE_MEMORY',
     'CROSS_BORDER_EMBEDDING','CROSS_BORDER_TRANSLATION','CROSS_BORDER_RERANK')),
  status text NOT NULL CHECK (status IN ('GRANTED','DENIED','WITHDRAWN')),
  version bigint NOT NULL CHECK (version > 0),
  effective_at timestamptz NOT NULL,
  expires_at timestamptz,
  source text NOT NULL,
  evidence_ref text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (customer_id, purpose)
);
CREATE INDEX ix_consent_status ON customer_memory_consent (status, updated_at);

-- 客户假名映射：Kafka 事件只携带 customer_ref，不携带 raw customer_id
CREATE TABLE customer_identity (
  customer_id text NOT NULL,
  customer_ref char(64) NOT NULL,
  ref_key_version integer NOT NULL CHECK (ref_key_version > 0),
  status text NOT NULL CHECK (status IN ('ACTIVE','RETIRED')),
  is_current boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  retired_at timestamptz,
  PRIMARY KEY (customer_id, ref_key_version),
  UNIQUE (customer_ref),
  CHECK (status <> 'RETIRED' OR retired_at IS NOT NULL),
  CHECK (is_current = false OR status = 'ACTIVE')
);
-- 轮换期允许多行 ACTIVE（旧 ref 在验证窗口内仍可解析），但生产者只能有一个当前 ref
CREATE UNIQUE INDEX ux_identity_current ON customer_identity (customer_id) WHERE is_current;

-- >>> mms:upgrade
-- 本节（到 <<< mms:upgrade）是纯增量且必须幂等：scripts/init_db.py 会对已初始化
-- 的库重放全部 mms:upgrade 区，存量开发库不必重建即可拿到 P1 变更。
-- 坐席/服务主体到客户的授权集合（docs 12.1）：path 里的 customer_id 只是请求目标，
-- 不是授权凭据；没有有效授权行一律 403 CUSTOMER_MISMATCH，读取 fail-closed。
CREATE TABLE IF NOT EXISTS agent_customer_grant (
  actor_id text NOT NULL,
  customer_id text NOT NULL,
  granted_by text NOT NULL,
  granted_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz,
  PRIMARY KEY (actor_id, customer_id),
  CHECK (revoked_at IS NULL OR revoked_at > granted_at)
);
CREATE INDEX IF NOT EXISTS ix_agent_grant_customer ON agent_customer_grant (customer_id, revoked_at);

-- 每客户配额计数（docs 12.6）：按天累计，跨副本共享，所以记账在库里而不是进程内令牌桶。
CREATE TABLE IF NOT EXISTS customer_quota_usage (
  customer_id text NOT NULL,
  bucket text NOT NULL CHECK (bucket IN ('WRITE','EXTRACTION','EMBEDDING')),
  day date NOT NULL,
  used integer NOT NULL DEFAULT 0 CHECK (used >= 0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (customer_id, bucket, day)
);
CREATE INDEX IF NOT EXISTS ix_quota_usage_day ON customer_quota_usage (bucket, day);
-- <<< mms:upgrade

-- 语言视图向量投影（按客户 HASH 分区；精确检索，不建 HNSW）
CREATE TABLE memory_embedding (
  memory_id uuid NOT NULL REFERENCES customer_memory(id) ON DELETE CASCADE,
  customer_id text NOT NULL,
  embedding_model text NOT NULL,
  embedding_version integer NOT NULL,
  embedding_dim integer NOT NULL,
  -- content_hash 是所属 customer_memory.content_hash（所有视图共享）；view_text_hash 才是本视图文本的 hash
  content_hash char(64) NOT NULL,
  view_kind text NOT NULL CHECK (view_kind IN ('ORIGINAL','CANONICAL','TRANSLATED')),
  view_language text NOT NULL,
  view_text_hash char(64) NOT NULL,
  embedding vector(1024),
  status text NOT NULL CHECK (status IN ('READY','STALE','DELETED')),
  generated_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (customer_id, memory_id, embedding_model, embedding_version, view_kind, view_language),
  CHECK (embedding_dim = 1024),
  CHECK (view_kind <> 'CANONICAL' OR view_language = 'und'),
  CHECK (view_kind <> 'ORIGINAL' OR view_language <> 'und'),
  CHECK (view_kind <> 'TRANSLATED' OR (view_language <> 'und' AND view_language <> '')),
  CHECK (status <> 'READY' OR embedding IS NOT NULL),
  CHECK (status <> 'READY' OR generated_at IS NOT NULL)
) PARTITION BY HASH (customer_id);

-- 示例分区；生产按选定分区数（64/256）循环生成
CREATE TABLE memory_embedding_p0 PARTITION OF memory_embedding
  FOR VALUES WITH (MODULUS 64, REMAINDER 0);
-- ... p1 ... p63

-- 精确检索：按 customer_id 走 B-tree，再只对该客户的向量行计算距离；不建 HNSW
CREATE INDEX ix_embedding_customer ON memory_embedding
  (customer_id, status, embedding_model, embedding_version, view_kind, view_language);
CREATE INDEX ix_embedding_memory ON memory_embedding (memory_id);
-- 分区表上的索引会传播到子分区；在线创建/重建时逐个子分区 CONCURRENTLY 执行

-- Embedding 任务
CREATE TABLE memory_embedding_job (
  id uuid PRIMARY KEY,
  memory_id uuid NOT NULL REFERENCES customer_memory(id) ON DELETE CASCADE,
  customer_id text NOT NULL,
  operation text NOT NULL CHECK (operation IN ('UPSERT','DELETE','REBUILD')),
  embedding_model text NOT NULL,
  embedding_version integer NOT NULL,
  content_hash char(64) NOT NULL,
  view_kind text NOT NULL CHECK (view_kind IN ('ORIGINAL','CANONICAL','TRANSLATED')),
  view_language text NOT NULL,
  view_text_hash char(64) NOT NULL,
  generation bigint NOT NULL,
  barrier_generation bigint NOT NULL DEFAULT 0,
  status text NOT NULL CHECK (status IN ('PENDING','RUNNING','READY','RETRY','DEAD_LETTER','CANCELLED')),
  priority smallint NOT NULL DEFAULT 100 CHECK (priority >= 0),
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_by text,
  locked_at timestamptz,
  lease_expires_at timestamptz,
  last_error_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- 活动唯一键**不含 view_text_hash**：同一视图同时只允许一个活动 Job，
-- 否则新旧文本的两个 Job 会并发写同一 PK，后写者覆盖先写者（旧向量胜出的竞态）。
-- 文本变化用 generation 围栏：新 Job 递增 generation，Worker 写入前必须匹配视图当前 generation。
CREATE UNIQUE INDEX ux_embedding_job_active
  ON memory_embedding_job
     (memory_id, embedding_model, embedding_version, view_kind, view_language, operation)
  WHERE status IN ('PENDING', 'RUNNING', 'RETRY');
CREATE INDEX ix_embedding_job_lease ON memory_embedding_job (lease_expires_at)
  WHERE status = 'RUNNING';
CREATE INDEX ix_embedding_job_view ON memory_embedding_job
  (memory_id, view_kind, view_language)
  WHERE status IN ('PENDING','RUNNING','RETRY');
CREATE INDEX ix_embedding_job_ready ON memory_embedding_job (available_at, created_at)
  WHERE status IN ('PENDING','RETRY');
CREATE INDEX ix_embedding_job_claim ON memory_embedding_job (priority, available_at)
  WHERE status IN ('PENDING','RETRY');

-- 模型注册表
CREATE TABLE embedding_model_registry (
  model_id text NOT NULL,
  version integer NOT NULL CHECK (version > 0),
  provider text NOT NULL,
  dimension integer NOT NULL,
  distance_metric text NOT NULL DEFAULT 'cosine',
  languages text[] NOT NULL DEFAULT '{}',
  max_tokens integer,
  status text NOT NULL CHECK (status IN ('ACTIVE','SHADOW','RETIRED')),
  config jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz,
  PRIMARY KEY (model_id, version)
);

CREATE UNIQUE INDEX ux_embedding_model_single_active ON embedding_model_registry (status)
  WHERE status = 'ACTIVE';

-- 语言视图 + 词法投影；同一记忆可有 ORIGINAL/CANONICAL/多个 TRANSLATED 视图
CREATE TABLE memory_search_document (
  memory_id uuid NOT NULL REFERENCES customer_memory(id) ON DELETE CASCADE,
  customer_id text NOT NULL,
  view_kind text NOT NULL CHECK (view_kind IN ('ORIGINAL','CANONICAL','TRANSLATED')),
  view_language text NOT NULL,
  content text NOT NULL,
  source_evidence_id uuid,
  -- content_hash 是所属 customer_memory.content_hash；view_text_hash 是本视图 content 的 hash
  content_hash char(64) NOT NULL,
  view_text_hash char(64) NOT NULL,
  translation_model text,
  translation_version text,
  translation_quality real CHECK (translation_quality IS NULL OR translation_quality BETWEEN 0 AND 1),
  source_view_hash char(64),
  search_vector tsvector,
  analyzer_version text NOT NULL,
  status text NOT NULL DEFAULT 'READY' CHECK (status IN ('READY','STALE','DELETED')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (customer_id, memory_id, view_kind, view_language),
  CHECK (view_kind <> 'CANONICAL' OR view_language = 'und'),
  CHECK (view_kind <> 'ORIGINAL' OR view_language <> 'und'),
  CHECK (view_kind <> 'TRANSLATED' OR (view_language <> 'und' AND view_language <> '')),
  CHECK (view_kind <> 'TRANSLATED' OR (translation_model IS NOT NULL AND translation_version IS NOT NULL)),
  CHECK (view_kind <> 'TRANSLATED' OR source_view_hash IS NOT NULL),
  CHECK ((view_kind = 'ORIGINAL' AND source_evidence_id IS NOT NULL)
      OR (view_kind <> 'ORIGINAL' AND source_evidence_id IS NULL))
) PARTITION BY HASH (customer_id);

CREATE TABLE memory_search_document_p0 PARTITION OF memory_search_document
  FOR VALUES WITH (MODULUS 64, REMAINDER 0);
CREATE INDEX ix_search_doc_customer ON memory_search_document (customer_id, view_language, status);
CREATE INDEX ix_search_doc_p0_tsv ON memory_search_document_p0 USING gin (search_vector);
CREATE INDEX ix_search_doc_p0_trgm ON memory_search_document_p0 USING gin (content gin_trgm_ops);
-- p1..p63 必须逐个分区建同样的 GIN/trgm 索引：在分区父表上 CREATE INDEX ... CONCURRENTLY
-- 不被支持，只能逐子分区建好后由父表索引聚合；缺任一子分区索引都会退化成全分区顺序扫描。
-- 上线校验：pg_indexes 中每个子分区都必须存在 _tsv 与 _trgm 两条记录（见 15.2 门禁）。

-- 消费幂等：纯签收簿，行存在即重复；只有 RECEIVED/COMPLETED，无 attempts/租约字段，
-- 崩溃恢复完全由 Job 状态机 + 租约 + JOB 对账承担（《InboxEvent机制设计》2.2/2.3）
CREATE TABLE memory_inbox_event (
  consumer_group text NOT NULL,
  event_id uuid NOT NULL,
  topic text NOT NULL,
  partition integer NOT NULL,
  "offset" bigint NOT NULL,
  status text NOT NULL DEFAULT 'RECEIVED'
    CHECK (status IN ('RECEIVED','COMPLETED')),
  processed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, event_id),
  CHECK (status <> 'COMPLETED' OR processed_at IS NOT NULL)
);
-- COMPLETED 独立保留 7 天，由 Convergence 清理；清理按 updated_at 扫描
CREATE INDEX ix_inbox_completed ON memory_inbox_event (status, updated_at)
  WHERE status = 'COMPLETED';

-- 删除屏障
CREATE TABLE customer_deletion_barrier (
  customer_id text NOT NULL,
  request_id uuid NOT NULL,
  status text NOT NULL CHECK (status IN ('ACTIVE','COOLING_DOWN','COMPLETED','CANCELLED')),
  requested_at timestamptz NOT NULL,
  cutover_at timestamptz NOT NULL,
  completed_at timestamptz,
  generation bigint NOT NULL,
  scope_type text NOT NULL DEFAULT 'CUSTOMER' CHECK (scope_type = 'CUSTOMER'),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (customer_id)
);

-- 事务 Outbox
CREATE TABLE memory_outbox (
  id uuid PRIMARY KEY,
  aggregate_id uuid NOT NULL,
  event_type text NOT NULL,
  payload_json jsonb NOT NULL,
  idempotency_key text NOT NULL UNIQUE,
  event_id uuid NOT NULL,
  partition_key text NOT NULL DEFAULT '',
  aggregate_version bigint NOT NULL DEFAULT 0,
  barrier_generation bigint NOT NULL DEFAULT 0,
  status text NOT NULL CHECK (status IN ('PENDING','PUBLISHED','RETRY','FAILED')),
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_by text,
  locked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  publish_attempts integer NOT NULL DEFAULT 0,
  last_error text
);

CREATE INDEX ix_outbox_ready ON memory_outbox (available_at, created_at)
  WHERE status IN ('PENDING','RETRY');

-- 候选：原始正文不进入长期库；只保留脱敏最小内容/hash
CREATE TABLE memory_candidate (
  candidate_id uuid PRIMARY KEY,
  customer_id text NOT NULL,
  source_system text NOT NULL,
  source_event_id text NOT NULL,
  candidate_fingerprint char(64) NOT NULL,
  memory_type text NOT NULL CHECK (memory_type IN
    ('CUSTOMER_PROFILE','SERVICE_HISTORY','ACTIVE_ISSUE','COMMITMENT','EPHEMERAL_CONTEXT','ACCOUNT_FACT')),
  fact_key text NOT NULL,
  fact_key_registry_version integer,
  cardinality text CHECK (cardinality IN ('SINGLE_VALUE','MULTI_VALUE','TEMPORAL')),
  subject text,
  predicate text,
  entity_type text NOT NULL DEFAULT '',
  entity_ref text NOT NULL DEFAULT '',
  value_slot text NOT NULL DEFAULT '',
  object_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  object_hash char(64) NOT NULL,
  content_redacted text,
  content_language text,
  language text,
  script text,
  content_hash char(64),
  semantic_text text,
  semantic_hash char(64),
  normalization_version text,
  language_confidence real CHECK (language_confidence IS NULL OR language_confidence BETWEEN 0 AND 1),
  source_kind text NOT NULL CHECK (source_kind IN
    ('SYSTEM_OF_RECORD','USER_EXPLICIT','AGENT_CONFIRMED','USER_IMPLICIT','MODEL_INFERRED','KNOWLEDGE_RETRIEVED','ASSISTANT_GENERATED')),
  source_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
  confidence real CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
  value_score smallint CHECK (value_score IS NULL OR value_score BETWEEN 0 AND 100),
  sensitivity text NOT NULL DEFAULT 'LOW' CHECK (sensitivity IN ('LOW','MEDIUM','HIGH')),
  explicit_correction boolean NOT NULL DEFAULT false,
  valid_from timestamptz,
  valid_to timestamptz,
  observed_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  candidate_status text NOT NULL CHECK (candidate_status IN
    ('RECEIVED','VALIDATED','PENDING_REVIEW','PROCESSED','ACCEPTED','REJECTED','IGNORED','EXPIRED')),
  last_reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
  policy_version text NOT NULL,
  extractor_version text NOT NULL,
  barrier_generation bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to > valid_from),
  CHECK (candidate_status <> 'ACCEPTED' OR fact_key_registry_version IS NOT NULL),
  CHECK (candidate_status <> 'ACCEPTED' OR cardinality IS NOT NULL),
  CHECK (candidate_status <> 'ACCEPTED' OR content_hash IS NOT NULL),
  CHECK (candidate_status <> 'ACCEPTED' OR semantic_hash IS NOT NULL),
  CHECK (candidate_status <> 'ACCEPTED' OR semantic_text IS NOT NULL),
  CHECK (candidate_status <> 'ACCEPTED' OR normalization_version IS NOT NULL),
  UNIQUE (customer_id, source_system, source_event_id, candidate_fingerprint),
  FOREIGN KEY (fact_key, fact_key_registry_version, cardinality)
    REFERENCES fact_key_registry (fact_key, version, cardinality)
);

CREATE INDEX ix_candidate_customer ON memory_candidate (customer_id, created_at DESC);
CREATE INDEX ix_candidate_review ON memory_candidate (candidate_status, created_at)
  WHERE candidate_status IN ('RECEIVED','VALIDATED','PENDING_REVIEW');
CREATE INDEX ix_candidate_expiry ON memory_candidate (expires_at)
  WHERE candidate_status IN ('RECEIVED','VALIDATED','PENDING_REVIEW');

-- 摄取事件处理队列（Kafka/HTTP 去重后的异步入口）
CREATE TABLE memory_ingestion_job (
  id uuid PRIMARY KEY,
  customer_id text NOT NULL,
  source_system text NOT NULL,
  source_event_id text NOT NULL,
  event_type text NOT NULL CHECK (event_type IN ('memory.candidate.v1','memory.extraction.requested.v1')),
  payload_ref jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL CHECK (status IN
    ('RECEIVED','PROCESSING','COMPLETED','RETRY','DEAD_LETTER','CANCELLED')),
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_by text,
  locked_at timestamptz,
  last_error_code text,
  barrier_generation bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (customer_id, source_system, source_event_id)
);

CREATE INDEX ix_ingestion_job_ready ON memory_ingestion_job (available_at, created_at)
  WHERE status IN ('RECEIVED','RETRY');
CREATE INDEX ix_ingestion_job_customer ON memory_ingestion_job (customer_id, created_at DESC);

-- 证据：memory_id / candidate_id 至少一个；删除时先匿名化或删除证据
CREATE TABLE memory_evidence (
  id uuid PRIMARY KEY,
  customer_id text NOT NULL,
  memory_id uuid REFERENCES customer_memory(id) ON DELETE SET NULL,
  candidate_id uuid REFERENCES memory_candidate(candidate_id) ON DELETE SET NULL,
  anonymized boolean NOT NULL DEFAULT false,
  source_kind text NOT NULL CHECK (source_kind IN
    ('SYSTEM_OF_RECORD','USER_EXPLICIT','AGENT_CONFIRMED','USER_IMPLICIT','MODEL_INFERRED','KNOWLEDGE_RETRIEVED','ASSISTANT_GENERATED')),
  source_system text NOT NULL,
  source_event_id text,
  content_language text,
  conversation_id text,
  message_id text,
  ticket_id text,
  evidence_excerpt_redacted text,
  evidence_hash char(64) NOT NULL,
  observed_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (memory_id IS NOT NULL OR candidate_id IS NOT NULL OR anonymized),
  CHECK (anonymized OR source_event_id IS NOT NULL),
  UNIQUE (customer_id, source_system, source_event_id, evidence_hash)
);

CREATE INDEX ix_evidence_memory ON memory_evidence (memory_id);
CREATE INDEX ix_evidence_candidate ON memory_evidence (candidate_id);

-- 证据追加后不可改写；仅审计擦除角色可执行一次性脱敏，其他字段必须原样保留
CREATE OR REPLACE FUNCTION assert_evidence_immutable() RETURNS trigger AS $$
BEGIN
  -- FK 的 ON DELETE SET NULL 只能解除父行引用，不得改证据内容。
  IF (to_jsonb(NEW) - ARRAY['memory_id','candidate_id'])
       = (to_jsonb(OLD) - ARRAY['memory_id','candidate_id'])
     AND (NEW.memory_id IS NOT DISTINCT FROM OLD.memory_id OR NEW.memory_id IS NULL)
     AND (NEW.candidate_id IS NOT DISTINCT FROM OLD.candidate_id OR NEW.candidate_id IS NULL) THEN
    RETURN NEW;
  END IF;
  IF current_user = 'mms_audit_scrubber'
     AND OLD.anonymized = false AND NEW.anonymized = true
     AND NEW.memory_id IS NULL AND NEW.candidate_id IS NULL
     AND NEW.source_event_id IS NULL AND NEW.conversation_id IS NULL
     AND NEW.message_id IS NULL AND NEW.ticket_id IS NULL
     AND NEW.evidence_excerpt_redacted IS NULL
     AND (to_jsonb(NEW) - ARRAY['anonymized','memory_id','candidate_id',
           'source_event_id','conversation_id','message_id','ticket_id',
           'evidence_excerpt_redacted'])
         = (to_jsonb(OLD) - ARRAY['anonymized','memory_id','candidate_id',
           'source_event_id','conversation_id','message_id','ticket_id',
           'evidence_excerpt_redacted']) THEN
    RETURN NEW;
  END IF;
  RAISE exception 'EVIDENCE_IMMUTABLE: evidence_id=%', OLD.id;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_evidence_immutable
  BEFORE UPDATE ON memory_evidence
  FOR EACH ROW EXECUTE FUNCTION assert_evidence_immutable();

-- 决策审计：append-only；候选删除后保留 candidate_hash
CREATE TABLE memory_decision (
  decision_id uuid PRIMARY KEY,
  candidate_id uuid REFERENCES memory_candidate(candidate_id) ON DELETE SET NULL,
  customer_id text NOT NULL,
  customer_hash char(64) NOT NULL,
  outcome text NOT NULL CHECK (outcome IN
    ('ADD','SUPERSEDE','ADD_TEMPORAL_VERSION','NOOP','REINFORCE','CONFLICT','RESOLVE','EXPIRE','IGNORE','REJECTED','NEEDS_VERIFICATION','DELETE_REQUESTED')),
  reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
  rule_trace jsonb NOT NULL DEFAULT '[]'::jsonb,
  primary_rule_id text NOT NULL,
  candidate_hash char(64) NOT NULL,
  semantic_hash char(64) NOT NULL,
  normalization_version text NOT NULL,
  target_memory_id uuid REFERENCES customer_memory(id) ON DELETE SET NULL,
  new_memory_id uuid REFERENCES customer_memory(id) ON DELETE SET NULL,
  matched_memory_ids uuid[] NOT NULL DEFAULT '{}'::uuid[],
  expected_versions jsonb NOT NULL DEFAULT '{}'::jsonb,
  policy_version text NOT NULL,
  fact_key_registry_version integer,
  engine_version text NOT NULL,
  model_version text,
  score_breakdown jsonb NOT NULL DEFAULT '{}'::jsonb,
  context_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
  requires_review boolean NOT NULL DEFAULT false,
  attempt_no integer NOT NULL CHECK (attempt_no > 0),
  operator_id text,
  pii_scrubbed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (candidate_id, attempt_no),
  CHECK (pii_scrubbed_at IS NULL OR customer_id = 'scrubbed')
);

CREATE INDEX ix_decision_customer ON memory_decision (customer_id, created_at DESC);
CREATE INDEX ix_decision_outcome ON memory_decision (outcome, created_at DESC);
CREATE INDEX ix_decision_rule ON memory_decision (primary_rule_id, created_at DESC);
CREATE INDEX ix_decision_semantic ON memory_decision (customer_id, semantic_hash);

-- 冲突：同一事实同一时间只允许一个未解决冲突
CREATE TABLE memory_conflict (
  id uuid PRIMARY KEY,
  customer_id text NOT NULL,
  fact_key text NOT NULL,
  entity_type text NOT NULL DEFAULT '',
  entity_ref text NOT NULL DEFAULT '',
  value_slot text NOT NULL DEFAULT '',
  status text NOT NULL CHECK (status IN ('OPEN','IN_REVIEW','RESOLVED','DISMISSED')),
  risk_level text NOT NULL CHECK (risk_level IN ('LOW','MEDIUM','HIGH')),
  detected_reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
  current_memory_id uuid REFERENCES customer_memory(id) ON DELETE SET NULL,
  winning_memory_id uuid REFERENCES customer_memory(id) ON DELETE SET NULL,
  opened_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  resolution_reason_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status NOT IN ('RESOLVED','DISMISSED') OR resolved_at IS NOT NULL)
);

CREATE UNIQUE INDEX ux_conflict_open_fact ON memory_conflict
  (customer_id, fact_key, entity_type, entity_ref, value_slot)
  WHERE status IN ('OPEN','IN_REVIEW');
CREATE INDEX ix_conflict_queue ON memory_conflict (status, risk_level, opened_at);

CREATE TABLE memory_conflict_item (
  id uuid PRIMARY KEY,
  customer_id text NOT NULL,
  conflict_id uuid NOT NULL REFERENCES memory_conflict(id) ON DELETE CASCADE,
  memory_id uuid REFERENCES customer_memory(id) ON DELETE RESTRICT,
  candidate_id uuid REFERENCES memory_candidate(candidate_id) ON DELETE RESTRICT,
  evidence_id uuid REFERENCES memory_evidence(id) ON DELETE RESTRICT,
  value_hash char(64) NOT NULL,
  object_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  content_redacted text,
  source_kind text NOT NULL CHECK (source_kind IN
    ('SYSTEM_OF_RECORD','USER_EXPLICIT','AGENT_CONFIRMED','USER_IMPLICIT','MODEL_INFERRED','KNOWLEDGE_RETRIEVED','ASSISTANT_GENERATED')),
  observed_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (memory_id IS NOT NULL OR candidate_id IS NOT NULL)
);

CREATE INDEX ix_conflict_item_conflict ON memory_conflict_item (conflict_id);
CREATE INDEX ix_conflict_item_memory ON memory_conflict_item (memory_id);

-- 删除请求：幂等键唯一；高风险删除 4-eyes
CREATE TABLE memory_deletion_request (
  id uuid PRIMARY KEY,
  customer_id text NOT NULL,
  request_idempotency_key text NOT NULL,
  request_source text NOT NULL CHECK (request_source IN ('USER','COMPLIANCE','ADMIN','SYSTEM','CONSENT_WITHDRAWN','DELETE_REQUESTED')),
  requested_by text NOT NULL,
  legal_basis text,
  scope jsonb NOT NULL DEFAULT '{"all":true}'::jsonb,
  scope_hash char(64) NOT NULL,
  barrier_scope text NOT NULL DEFAULT 'CUSTOMER' CHECK (barrier_scope = 'CUSTOMER'),
  status text NOT NULL CHECK (status IN
    ('RECEIVED','APPROVED','IN_PROGRESS','VERIFYING','COMPLETED','REJECTED','CANCELLED')),
  approved_by text,
  approved_at timestamptz,
  cutover_at timestamptz NOT NULL,
  completed_at timestamptz,
  proof jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (customer_id, request_idempotency_key),
  CHECK (approved_by IS NULL OR approved_by <> requested_by),
  CHECK (status IN ('RECEIVED','REJECTED','CANCELLED') OR approved_by IS NOT NULL),
  CHECK (status <> 'COMPLETED' OR completed_at IS NOT NULL)
);

CREATE INDEX ix_deletion_request_status ON memory_deletion_request (status, created_at);

-- 删除明细：可重入、可验证；target_id/target_ref 至少一个
CREATE TABLE memory_deletion_item (
  id uuid PRIMARY KEY,
  deletion_request_id uuid NOT NULL REFERENCES memory_deletion_request(id) ON DELETE CASCADE,
  customer_id text NOT NULL,
  target_type text NOT NULL CHECK (target_type IN
    ('CUSTOMER_MEMORY','CUSTOMER_MEMORY_CURRENT','CONSENT','CUSTOMER_IDENTITY','AUDIT_SCRUB','EVIDENCE','DECISION','EMBEDDING','SEARCH_DOCUMENT','CANDIDATE','INGESTION_JOB','EMBEDDING_JOB','OUTBOX','INBOX','FEEDBACK','USAGE_EVENT','CONFLICT','REVIEW_TASK','KAFKA_TOMBSTONE','BACKUP_TOMBSTONE')),
  target_id uuid,
  target_ref text,
  status text NOT NULL CHECK (status IN
    ('PENDING','DELETED','ANONYMIZED','SKIPPED','FAILED','VERIFIED')),
  attempts integer NOT NULL DEFAULT 0,
  last_error_code text,
  verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (target_id IS NOT NULL OR target_ref IS NOT NULL),
  CHECK (status <> 'VERIFIED' OR verified_at IS NOT NULL)
);

CREATE UNIQUE INDEX ux_deletion_item_target ON memory_deletion_item
  (deletion_request_id, target_type,
   COALESCE(target_id, '00000000-0000-0000-0000-000000000000'::uuid),
   COALESCE(target_ref, ''));
CREATE INDEX ix_deletion_item_status ON memory_deletion_item (status, created_at);

-- 删除墓碑：比备份保留更久；备份恢复后据此重放删除，防止复活
CREATE TABLE deletion_tombstone (
  tombstone_id uuid PRIMARY KEY,
  deletion_request_id uuid NOT NULL,
  customer_id text NOT NULL,
  customer_ref char(64) NOT NULL,
  scope_hash char(64) NOT NULL,
  scope jsonb NOT NULL,
  reason_code text NOT NULL,
  completed_at timestamptz NOT NULL,
  backup_generation text NOT NULL,
  kms_key_id text,
  ledger_object_key text NOT NULL,
  ledger_written_at timestamptz NOT NULL,
  retain_until timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (deletion_request_id, scope_hash),
  CHECK (retain_until > completed_at),
  CHECK (ledger_written_at <= completed_at)
);
CREATE INDEX ix_tombstone_customer ON deletion_tombstone (customer_id, completed_at DESC);
CREATE INDEX ix_tombstone_retain ON deletion_tombstone (retain_until);

-- 墓碑到期清理守卫：本表对运行期角色 append-only，唯一例外是保留期清理；
-- 只允许删除 retain_until 已过的行，未到期删除一律拒绝。外部 WORM 账本是权威，不随本表清理。
CREATE OR REPLACE FUNCTION guard_tombstone_purge() RETURNS trigger AS $$
BEGIN
  IF OLD.retain_until > now() THEN
    RAISE exception 'TOMBSTONE_RETENTION_NOT_EXPIRED: tombstone=% retain_until=%',
      OLD.tombstone_id, OLD.retain_until;
  END IF;
  RETURN OLD;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tombstone_purge_guard
  BEFORE DELETE ON deletion_tombstone
  FOR EACH ROW EXECUTE FUNCTION guard_tombstone_purge();

-- 审核任务：candidate/conflict/deletion 三选一；部分唯一防止重复待办
CREATE TABLE memory_review_task (
  id uuid PRIMARY KEY,
  customer_id text NOT NULL,
  task_type text NOT NULL CHECK (task_type IN ('CANDIDATE','CONFLICT','DELETION')),
  candidate_id uuid REFERENCES memory_candidate(candidate_id) ON DELETE RESTRICT,
  conflict_id uuid REFERENCES memory_conflict(id) ON DELETE RESTRICT,
  deletion_request_id uuid REFERENCES memory_deletion_request(id) ON DELETE RESTRICT,
  status text NOT NULL CHECK (status IN
    ('PENDING','IN_REVIEW','APPROVED','REJECTED','EXPIRED','CANCELLED')),
  risk_level text NOT NULL CHECK (risk_level IN ('LOW','MEDIUM','HIGH')),
  priority smallint NOT NULL DEFAULT 100,
  due_at timestamptz,
  assigned_to text,
  suggested_action text,
  decision_version integer NOT NULL DEFAULT 1 CHECK (decision_version > 0),
  resolution_reason_code text,
  resolution_note_redacted text,
  created_by text NOT NULL,
  reviewed_by text,
  co_reviewer_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  CHECK (
    (task_type = 'CANDIDATE' AND candidate_id IS NOT NULL AND conflict_id IS NULL AND deletion_request_id IS NULL) OR
    (task_type = 'CONFLICT' AND conflict_id IS NOT NULL AND candidate_id IS NULL AND deletion_request_id IS NULL) OR
    (task_type = 'DELETION' AND deletion_request_id IS NOT NULL AND candidate_id IS NULL AND conflict_id IS NULL)
  ),
  CHECK (reviewed_by IS NULL OR reviewed_by <> created_by),
  CHECK (co_reviewer_id IS NULL OR co_reviewer_id <> reviewed_by),
  CHECK (status NOT IN ('APPROVED','REJECTED') OR reviewed_by IS NOT NULL),
  -- 5.4: high-risk resolutions need a co-reviewer; customer-wide deletion keeps its
  -- own approved_by 4-eyes CHECK on memory_deletion_request.
  CHECK (status <> 'APPROVED' OR risk_level <> 'HIGH' OR task_type = 'DELETION' OR co_reviewer_id IS NOT NULL)
);

CREATE UNIQUE INDEX ux_review_open_candidate ON memory_review_task (candidate_id)
  WHERE candidate_id IS NOT NULL AND status IN ('PENDING','IN_REVIEW');
CREATE UNIQUE INDEX ux_review_open_conflict ON memory_review_task (conflict_id)
  WHERE conflict_id IS NOT NULL AND status IN ('PENDING','IN_REVIEW');
CREATE UNIQUE INDEX ux_review_open_deletion ON memory_review_task (deletion_request_id)
  WHERE deletion_request_id IS NOT NULL AND status IN ('PENDING','IN_REVIEW');
CREATE UNIQUE INDEX ux_review_decision_version ON memory_review_task
  (task_type, COALESCE(candidate_id, conflict_id, deletion_request_id), decision_version);
CREATE INDEX ix_review_queue ON memory_review_task (status, due_at, priority);

-- 反馈：不自动提升可信度；错误反馈进入 correction 候选
CREATE TABLE memory_feedback (
  id uuid PRIMARY KEY,
  customer_id text NOT NULL,
  idempotency_key text NOT NULL,
  request_id text,
  memory_id uuid REFERENCES customer_memory(id) ON DELETE SET NULL,
  feedback_type text NOT NULL CHECK (feedback_type IN
    ('INCORRECT','OUTDATED','IRRELEVANT','HELPFUL','PRIVACY','OTHER')),
  reason_code text,
  actor_type text NOT NULL CHECK (actor_type IN ('CUSTOMER','AGENT','SYSTEM','REVIEWER')),
  actor_id text NOT NULL,
  comment_redacted text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (customer_id, idempotency_key)
);

CREATE INDEX ix_feedback_memory ON memory_feedback (memory_id, created_at DESC);

-- 使用事件：检索返回与实际采用；高写入量，按保留期清理/归档
CREATE TABLE memory_usage_event (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id text NOT NULL,
  request_id text NOT NULL,
  purpose text NOT NULL CHECK (purpose IN
    ('CURRENT_ANSWER','HISTORY_DEBUG','CUSTOMER_SELF_SERVICE','AUDIT','CANDIDATE_DISCOVERY')),
  retrieved_memory_ids uuid[] NOT NULL DEFAULT '{}'::uuid[],
  used_memory_ids uuid[] NOT NULL DEFAULT '{}'::uuid[],
  rank_map jsonb NOT NULL DEFAULT '{}'::jsonb,
  answer_hash char(64),
  model_version text,
  used boolean NOT NULL DEFAULT false,
  degraded boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (used = (cardinality(used_memory_ids) > 0))
);

CREATE INDEX ix_usage_customer ON memory_usage_event (customer_id, created_at DESC);
CREATE INDEX ix_usage_request ON memory_usage_event (request_id);

-- 对账批次（全局）
CREATE TABLE recon_run (
  id uuid PRIMARY KEY,
  recon_type text NOT NULL CHECK (recon_type IN ('JOB','VECTOR','RECALL','BUSINESS')),
  status text NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED','PARTIAL')),
  scope jsonb NOT NULL DEFAULT '{}'::jsonb,
  started_at timestamptz NOT NULL,
  completed_at timestamptz,
  scanned_count bigint NOT NULL DEFAULT 0,
  mismatch_count bigint NOT NULL DEFAULT 0,
  fixed_count bigint NOT NULL DEFAULT 0,
  error_count bigint NOT NULL DEFAULT 0,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_code text,
  triggered_by text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (completed_at IS NULL OR completed_at >= started_at)
);

CREATE INDEX ix_recon_run ON recon_run (recon_type, started_at DESC);

-- 管理员读取审计：只落 customer_hash，不落 raw customer_id/正文；append-only
CREATE TABLE admin_access_audit (
  audit_id uuid PRIMARY KEY,
  actor_id text NOT NULL,
  actor_role text NOT NULL,
  customer_hash char(64) NOT NULL,
  action text NOT NULL CHECK (action IN ('READ_MEMORY','SEARCH','EXPORT','DELETE','REPLAY','POLICY_CHANGE','REBUILD')),
  reason_code text NOT NULL,
  reason_text_redacted text,
  scope jsonb NOT NULL DEFAULT '{}'::jsonb,
  trace_id text NOT NULL,
  request_id text,
  pii_scrubbed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (pii_scrubbed_at IS NULL OR reason_text_redacted IS NULL)
);
CREATE INDEX ix_admin_audit_customer ON admin_access_audit (customer_hash, created_at DESC);
CREATE INDEX ix_admin_audit_actor ON admin_access_audit (actor_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- 7.13.1 P0-3 客户一致性约束（v2.4）
-- 目标：任何带 customer_id 的投影/指针/审计行都不能指向另一个客户的记忆。
-- 约束名按 PostgreSQL 默认命名。
-- 基线 PostgreSQL 16：支持 ON DELETE SET NULL (column_list)。
-- ---------------------------------------------------------------------------

ALTER TABLE customer_memory
  ADD CONSTRAINT uq_memory_id_customer UNIQUE (id, customer_id);
ALTER TABLE memory_candidate
  ADD CONSTRAINT uq_candidate_id_customer UNIQUE (candidate_id, customer_id);
ALTER TABLE memory_conflict
  ADD CONSTRAINT uq_conflict_id_customer UNIQUE (id, customer_id);
ALTER TABLE memory_deletion_request
  ADD CONSTRAINT uq_deletion_request_id_customer UNIQUE (id, customer_id);
ALTER TABLE memory_evidence
  ADD CONSTRAINT uq_evidence_id_customer UNIQUE (id, customer_id);

CREATE UNIQUE INDEX ux_fact_key_single_active ON fact_key_registry (fact_key)
  WHERE status = 'ACTIVE';
CREATE UNIQUE INDEX ux_policy_single_active_scope
  ON memory_policy (COALESCE(memory_type, '*')) WHERE status = 'ACTIVE';

ALTER TABLE customer_memory_current
  DROP CONSTRAINT customer_memory_current_memory_id_fkey;
ALTER TABLE customer_memory_current
  ADD CONSTRAINT fk_current_memory_customer
  FOREIGN KEY (memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id) ON DELETE CASCADE;

ALTER TABLE memory_embedding
  DROP CONSTRAINT memory_embedding_memory_id_fkey;
ALTER TABLE memory_embedding
  ADD CONSTRAINT fk_embedding_memory_customer
  FOREIGN KEY (memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id) ON DELETE CASCADE;

ALTER TABLE memory_search_document
  DROP CONSTRAINT memory_search_document_memory_id_fkey;
ALTER TABLE memory_search_document
  ADD CONSTRAINT fk_search_doc_memory_customer
  FOREIGN KEY (memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id) ON DELETE CASCADE;

ALTER TABLE memory_search_document
  ADD CONSTRAINT fk_search_doc_evidence_customer
  FOREIGN KEY (source_evidence_id, customer_id)
  REFERENCES memory_evidence (id, customer_id) ON DELETE RESTRICT;

ALTER TABLE memory_embedding_job
  DROP CONSTRAINT memory_embedding_job_memory_id_fkey;
ALTER TABLE memory_embedding_job
  ADD CONSTRAINT fk_embedding_job_memory_customer
  FOREIGN KEY (memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id) ON DELETE CASCADE;

ALTER TABLE memory_evidence
  DROP CONSTRAINT memory_evidence_memory_id_fkey;
ALTER TABLE memory_evidence
  ADD CONSTRAINT fk_evidence_memory_customer
  FOREIGN KEY (memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id)
  ON DELETE SET NULL (memory_id);

ALTER TABLE memory_evidence
  DROP CONSTRAINT memory_evidence_candidate_id_fkey;
ALTER TABLE memory_evidence
  ADD CONSTRAINT fk_evidence_candidate_customer
  FOREIGN KEY (candidate_id, customer_id)
  REFERENCES memory_candidate (candidate_id, customer_id)
  ON DELETE SET NULL (candidate_id);

ALTER TABLE memory_decision
  DROP CONSTRAINT memory_decision_candidate_id_fkey;
ALTER TABLE memory_decision
  ADD CONSTRAINT fk_decision_candidate_customer
  FOREIGN KEY (candidate_id, customer_id)
  REFERENCES memory_candidate (candidate_id, customer_id)
  ON DELETE SET NULL (candidate_id);

ALTER TABLE memory_decision
  DROP CONSTRAINT memory_decision_target_memory_id_fkey;
ALTER TABLE memory_decision
  ADD CONSTRAINT fk_decision_target_customer
  FOREIGN KEY (target_memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id)
  ON DELETE SET NULL (target_memory_id);

ALTER TABLE memory_decision
  DROP CONSTRAINT memory_decision_new_memory_id_fkey;
ALTER TABLE memory_decision
  ADD CONSTRAINT fk_decision_new_customer
  FOREIGN KEY (new_memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id)
  ON DELETE SET NULL (new_memory_id);

ALTER TABLE memory_conflict
  DROP CONSTRAINT memory_conflict_current_memory_id_fkey;
ALTER TABLE memory_conflict
  ADD CONSTRAINT fk_conflict_current_customer
  FOREIGN KEY (current_memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id)
  ON DELETE SET NULL (current_memory_id);

ALTER TABLE memory_conflict
  DROP CONSTRAINT memory_conflict_winning_memory_id_fkey;
ALTER TABLE memory_conflict
  ADD CONSTRAINT fk_conflict_winning_customer
  FOREIGN KEY (winning_memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id)
  ON DELETE SET NULL (winning_memory_id);

ALTER TABLE memory_conflict_item
  DROP CONSTRAINT memory_conflict_item_memory_id_fkey;
ALTER TABLE memory_conflict_item
  ADD CONSTRAINT fk_conflict_item_memory_customer
  FOREIGN KEY (memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id) ON DELETE RESTRICT;

ALTER TABLE memory_conflict_item
  DROP CONSTRAINT memory_conflict_item_candidate_id_fkey;
ALTER TABLE memory_conflict_item
  ADD CONSTRAINT fk_conflict_item_candidate_customer
  FOREIGN KEY (candidate_id, customer_id)
  REFERENCES memory_candidate (candidate_id, customer_id) ON DELETE RESTRICT;

ALTER TABLE memory_conflict_item
  DROP CONSTRAINT memory_conflict_item_evidence_id_fkey;
ALTER TABLE memory_conflict_item
  ADD CONSTRAINT fk_conflict_item_evidence_customer
  FOREIGN KEY (evidence_id, customer_id)
  REFERENCES memory_evidence (id, customer_id) ON DELETE RESTRICT;

ALTER TABLE memory_conflict_item
  DROP CONSTRAINT memory_conflict_item_conflict_id_fkey;
ALTER TABLE memory_conflict_item
  ADD CONSTRAINT fk_conflict_item_conflict_customer
  FOREIGN KEY (conflict_id, customer_id)
  REFERENCES memory_conflict (id, customer_id) ON DELETE CASCADE;

ALTER TABLE memory_review_task
  DROP CONSTRAINT memory_review_task_candidate_id_fkey;
ALTER TABLE memory_review_task
  ADD CONSTRAINT fk_review_candidate_customer
  FOREIGN KEY (candidate_id, customer_id)
  REFERENCES memory_candidate (candidate_id, customer_id) ON DELETE RESTRICT;

ALTER TABLE memory_review_task
  DROP CONSTRAINT memory_review_task_conflict_id_fkey;
ALTER TABLE memory_review_task
  ADD CONSTRAINT fk_review_conflict_customer
  FOREIGN KEY (conflict_id, customer_id)
  REFERENCES memory_conflict (id, customer_id) ON DELETE RESTRICT;

ALTER TABLE memory_review_task
  DROP CONSTRAINT memory_review_task_deletion_request_id_fkey;
ALTER TABLE memory_review_task
  ADD CONSTRAINT fk_review_deletion_customer
  FOREIGN KEY (deletion_request_id, customer_id)
  REFERENCES memory_deletion_request (id, customer_id) ON DELETE RESTRICT;

ALTER TABLE memory_feedback
  DROP CONSTRAINT memory_feedback_memory_id_fkey;
ALTER TABLE memory_feedback
  ADD CONSTRAINT fk_feedback_memory_customer
  FOREIGN KEY (memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id)
  ON DELETE SET NULL (memory_id);

ALTER TABLE customer_memory
  DROP CONSTRAINT customer_memory_supersedes_memory_id_fkey;
ALTER TABLE customer_memory
  ADD CONSTRAINT fk_memory_supersedes_customer
  FOREIGN KEY (supersedes_memory_id, customer_id)
  REFERENCES customer_memory (id, customer_id);

ALTER TABLE memory_deletion_item
  DROP CONSTRAINT memory_deletion_item_deletion_request_id_fkey;
ALTER TABLE memory_deletion_item
  ADD CONSTRAINT fk_deletion_item_request_customer
  FOREIGN KEY (deletion_request_id, customer_id)
  REFERENCES memory_deletion_request (id, customer_id) ON DELETE CASCADE;

-- 运行期角色由 schema 初始化创建（NOLOGIN），此处只声明权限基线：
-- mms_app（API）、mms_worker（决策/Embedding）、mms_deletion_worker（删除编排）、
-- mms_audit_scrubber（审计擦除）、mms_readonly（只读）

CREATE OR REPLACE FUNCTION enforce_memory_decision_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  scrub_keys text[] := ARRAY['customer_id','candidate_id','target_memory_id','new_memory_id',
    'matched_memory_ids','context_snapshot','score_breakdown','expected_versions','rule_trace','pii_scrubbed_at'];
  fk_keys text[] := ARRAY['candidate_id','target_memory_id','new_memory_id'];
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'MEMORY_DECISION_APPEND_ONLY';
  END IF;

  -- 复合外键 ON DELETE SET NULL 只允许把引用列置空
  IF (to_jsonb(NEW) - fk_keys) = (to_jsonb(OLD) - fk_keys)
     AND (NEW.candidate_id IS NULL OR NEW.candidate_id = OLD.candidate_id)
     AND (NEW.target_memory_id IS NULL OR NEW.target_memory_id = OLD.target_memory_id)
     AND (NEW.new_memory_id IS NULL OR NEW.new_memory_id = OLD.new_memory_id)
  THEN
    RETURN NEW;
  END IF;

  -- 删除擦除：仅 mms_audit_scrubber，且只改 allowlist 列
  IF current_user = 'mms_audit_scrubber'
     AND NEW.pii_scrubbed_at IS NOT NULL
     AND (to_jsonb(NEW) - scrub_keys) = (to_jsonb(OLD) - scrub_keys)
  THEN
    RETURN NEW;
  END IF;

  RAISE EXCEPTION 'MEMORY_DECISION_APPEND_ONLY';
END $$;

CREATE TRIGGER trg_memory_decision_append_only
  BEFORE UPDATE OR DELETE ON memory_decision
  FOR EACH ROW EXECUTE FUNCTION enforce_memory_decision_append_only();

CREATE OR REPLACE FUNCTION enforce_admin_audit_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE scrub_keys text[] := ARRAY['reason_text_redacted','pii_scrubbed_at'];
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'ADMIN_AUDIT_APPEND_ONLY';
  END IF;
  IF current_user = 'mms_audit_scrubber'
     AND NEW.pii_scrubbed_at IS NOT NULL
     AND (to_jsonb(NEW) - scrub_keys) = (to_jsonb(OLD) - scrub_keys)
  THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'ADMIN_AUDIT_APPEND_ONLY';
END $$;

CREATE TRIGGER trg_admin_audit_append_only
  BEFORE UPDATE OR DELETE ON admin_access_audit
  FOR EACH ROW EXECUTE FUNCTION enforce_admin_audit_append_only();

REVOKE UPDATE, DELETE ON memory_decision FROM PUBLIC;
REVOKE UPDATE, DELETE ON admin_access_audit FROM PUBLIC;
REVOKE UPDATE, DELETE ON memory_decision FROM mms_app, mms_worker, mms_deletion_worker, mms_readonly;
REVOKE UPDATE, DELETE ON admin_access_audit FROM mms_app, mms_worker, mms_deletion_worker, mms_readonly;
REVOKE UPDATE ON deletion_tombstone FROM mms_app, mms_worker, mms_deletion_worker, mms_readonly;
REVOKE DELETE ON deletion_tombstone FROM mms_app, mms_worker, mms_readonly;
GRANT SELECT, INSERT ON memory_decision TO mms_app, mms_worker;
GRANT SELECT, INSERT ON admin_access_audit TO mms_app;
GRANT SELECT, INSERT, UPDATE ON customer_identity TO mms_app, mms_worker;
GRANT UPDATE (anonymized, memory_id, candidate_id, source_event_id, conversation_id,
  message_id, ticket_id, evidence_excerpt_redacted) ON memory_evidence TO mms_audit_scrubber;
GRANT SELECT, INSERT, DELETE ON deletion_tombstone TO mms_deletion_worker;  -- DELETE 仅用于到期清理，未到期由 trg_tombstone_purge_guard 拒绝
GRANT SELECT ON memory_decision, admin_access_audit, deletion_tombstone TO mms_readonly;
GRANT SELECT ON memory_decision, admin_access_audit TO mms_audit_scrubber;
GRANT UPDATE (customer_id, candidate_id, target_memory_id, new_memory_id,
  matched_memory_ids, context_snapshot, score_breakdown, expected_versions, rule_trace, pii_scrubbed_at)
  ON memory_decision TO mms_audit_scrubber;
GRANT UPDATE (reason_text_redacted, pii_scrubbed_at)
  ON admin_access_audit TO mms_audit_scrubber;
GRANT SELECT, UPDATE (scope, details) ON recon_run TO mms_audit_scrubber;
GRANT SELECT, UPDATE (proof) ON memory_deletion_request TO mms_audit_scrubber;

ALTER TABLE memory_embedding
  ADD CONSTRAINT fk_embedding_model_registry
  FOREIGN KEY (embedding_model, embedding_version)
  REFERENCES embedding_model_registry (model_id, version);

ALTER TABLE memory_embedding_job
  ADD CONSTRAINT fk_embedding_job_model_registry
  FOREIGN KEY (embedding_model, embedding_version)
  REFERENCES embedding_model_registry (model_id, version);

CREATE OR REPLACE FUNCTION assert_embedding_model_registered()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM embedding_model_registry
   WHERE model_id = NEW.embedding_model AND version = NEW.embedding_version;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'EMBEDDING_MODEL_NOT_REGISTERED: %/%',
      NEW.embedding_model, NEW.embedding_version;
  END IF;
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION assert_embedding_dim_matches_registry()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE registered_dim integer;
BEGIN
  SELECT dimension INTO registered_dim
    FROM embedding_model_registry
   WHERE model_id = NEW.embedding_model AND version = NEW.embedding_version;
  IF registered_dim IS NULL THEN
    RAISE EXCEPTION 'EMBEDDING_MODEL_NOT_REGISTERED: %/%',
      NEW.embedding_model, NEW.embedding_version;
  END IF;
  IF NEW.embedding_dim <> registered_dim THEN
    RAISE EXCEPTION 'EMBEDDING_DIM_MISMATCH: expected %, got %',
      registered_dim, NEW.embedding_dim;
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER trg_embedding_registry
  BEFORE INSERT OR UPDATE ON memory_embedding
  FOR EACH ROW EXECUTE FUNCTION assert_embedding_model_registered();

CREATE TRIGGER trg_embedding_dim_registry
  BEFORE INSERT OR UPDATE ON memory_embedding
  FOR EACH ROW EXECUTE FUNCTION assert_embedding_dim_matches_registry();

CREATE TRIGGER trg_embedding_job_registry
  BEFORE INSERT OR UPDATE ON memory_embedding_job
  FOR EACH ROW EXECUTE FUNCTION assert_embedding_model_registered();

-- 种子：默认 BGE-M3 dense 1024；初始化时必须按实际权重 hash 补齐 config
-- 若已有其他 ACTIVE 模型，先降级/退役；不要直接插入第二个 ACTIVE
-- >>> mms:upgrade
-- 2026-10-30: deletion approval must invalidate projections in the same transaction as
-- the barrier/request. The deferred view-presence guard therefore skips deletion states.
CREATE OR REPLACE FUNCTION assert_memory_views_present() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.lifecycle_status IN ('DELETE_PENDING','DELETED') THEN
    RETURN NEW;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM memory_search_document d
                  WHERE d.memory_id = NEW.id AND d.customer_id = NEW.customer_id
                    AND d.view_kind = 'CANONICAL' AND d.view_language = 'und')
     OR NOT EXISTS (SELECT 1 FROM memory_search_document d
                  WHERE d.memory_id = NEW.id AND d.customer_id = NEW.customer_id
                    AND d.view_kind = 'ORIGINAL'
                    AND d.view_language = NEW.content_language) THEN
    RAISE exception 'MEMORY_VIEWS_INCOMPLETE: memory=%', NEW.id;
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;
-- <<< mms:upgrade

-- >>> mms:upgrade
-- 2026-10-30: rebuild is an audited admin action.
ALTER TABLE admin_access_audit DROP CONSTRAINT IF EXISTS admin_access_audit_action_check;
ALTER TABLE admin_access_audit ADD CONSTRAINT admin_access_audit_action_check
  CHECK (action IN ('READ_MEMORY','SEARCH','EXPORT','DELETE','REPLAY','POLICY_CHANGE','REBUILD'));
-- <<< mms:upgrade

-- >>> mms:upgrade
-- 2026-10-30: ADR-013 correction for databases initialized before co-reviewer columns.
ALTER TABLE memory_review_task ADD COLUMN IF NOT EXISTS co_reviewer_id text;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='chk_review_co_reviewer_distinct') THEN
    ALTER TABLE memory_review_task ADD CONSTRAINT chk_review_co_reviewer_distinct
      CHECK (co_reviewer_id IS NULL OR co_reviewer_id <> reviewed_by);
  END IF;
END $$;
-- <<< mms:upgrade

INSERT INTO embedding_model_registry
  (model_id, version, provider, dimension, distance_metric, languages,
   max_tokens, status, config, activated_at)
VALUES
  ('BAAI/bge-m3', 1, 'self-hosted', 1024, 'cosine',
   ARRAY['zh','en','es','fr','de','ja','ko'], 8192, 'ACTIVE',
   '{"normalize":true,"query_prefix":""}'::jsonb, now())
ON CONFLICT (model_id, version) DO NOTHING;

-- 1) 视图语言与 hash 必须与母表/证据一致（向量表）
CREATE OR REPLACE FUNCTION assert_embedding_view_invariant() RETURNS trigger AS $$
DECLARE
  m customer_memory%ROWTYPE;
BEGIN
  SELECT * INTO STRICT m FROM customer_memory
   WHERE id = NEW.memory_id AND customer_id = NEW.customer_id;

  IF NEW.content_hash <> m.content_hash THEN
    RAISE exception 'EMBEDDING_HASH_MISMATCH: memory=% view_hash=% memory_hash=%',
      NEW.memory_id, NEW.content_hash, m.content_hash;
  END IF;

  IF NEW.view_kind = 'ORIGINAL' THEN
    IF NEW.view_language <> m.content_language AND NOT EXISTS (
      SELECT 1 FROM memory_evidence e
       WHERE e.memory_id = m.id AND e.customer_id = m.customer_id
         AND e.content_language = NEW.view_language) THEN
      RAISE exception 'EMBEDDING_VIEW_LANGUAGE_UNEVIDENCED: memory=% language=%',
        NEW.memory_id, NEW.view_language;
    END IF;
  END IF;

  IF NEW.view_kind = 'TRANSLATED' THEN
    -- 译文视图必须存在同 hash 的检索文档，且 provenance 齐全
    IF NOT EXISTS (
      SELECT 1 FROM memory_search_document d
       WHERE d.customer_id = NEW.customer_id AND d.memory_id = NEW.memory_id
         AND d.view_kind = 'TRANSLATED' AND d.view_language = NEW.view_language
         AND d.view_text_hash = NEW.view_text_hash AND d.status = 'READY') THEN
      RAISE exception 'TRANSLATED_VIEW_DOCUMENT_MISSING: memory=% language=%',
        NEW.memory_id, NEW.view_language;
    END IF;
  END IF;

  -- READY 只允许在记忆处于已索引状态时存在（8.6 状态集口径）
  IF NEW.status = 'READY'
     AND m.lifecycle_status NOT IN ('ACTIVE','RESOLVED','CONFLICTED') THEN
    RAISE exception 'EMBEDDING_READY_ON_UNINDEXABLE_STATE: memory=% state=%',
      NEW.memory_id, m.lifecycle_status;
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

-- memory_embedding 是分区表：PG 不支持在分区表上建约束触发器/语句级触发器，
-- 因此用普通 BEFORE ROW 触发器（会自动传播到所有子分区），提交时兜底交给 8.10 对账
CREATE TRIGGER trg_embedding_view_invariant
  BEFORE INSERT OR UPDATE ON memory_embedding
  FOR EACH ROW EXECUTE FUNCTION assert_embedding_view_invariant();

-- 2) 检索文档同样校验（语言来源 + 原文 hash）
CREATE OR REPLACE FUNCTION assert_search_document_invariant() RETURNS trigger AS $$
DECLARE
  m customer_memory%ROWTYPE;
BEGIN
  SELECT * INTO STRICT m FROM customer_memory
   WHERE id = NEW.memory_id AND customer_id = NEW.customer_id;
  IF NEW.content_hash <> m.content_hash THEN
    RAISE exception 'SEARCH_DOC_HASH_MISMATCH: memory=%', NEW.memory_id;
  END IF;
  IF NEW.view_kind = 'ORIGINAL'
     AND NEW.view_language = m.content_language
     AND NEW.content IS DISTINCT FROM m.content THEN
    RAISE exception 'SEARCH_DOC_PRIMARY_ORIGINAL_MISMATCH: memory=%', NEW.memory_id;
  END IF;
  IF NEW.view_kind = 'ORIGINAL'
     AND NEW.view_language <> m.content_language
     AND NOT EXISTS (SELECT 1 FROM memory_evidence e
                      WHERE e.memory_id = m.id AND e.customer_id = m.customer_id
                        AND e.content_language = NEW.view_language) THEN
    RAISE exception 'SEARCH_DOC_LANGUAGE_UNEVIDENCED: memory=% language=%',
      NEW.memory_id, NEW.view_language;
  END IF;
  -- 主 ORIGINAL：证据只需归属同一记忆且语言一致；脱敏摘录允许短于原文，不要求等于 content
  IF NEW.view_kind = 'ORIGINAL'
     AND NEW.view_language = m.content_language
     AND NOT EXISTS (SELECT 1 FROM memory_evidence e
                      WHERE e.id = NEW.source_evidence_id
                        AND e.memory_id = m.id
                        AND e.customer_id = m.customer_id
                        AND e.content_language = NEW.view_language) THEN
    RAISE exception 'SEARCH_DOC_PRIMARY_EVIDENCE_MISMATCH: memory=% evidence=%',
      NEW.memory_id, NEW.source_evidence_id;
  END IF;
  -- 其他证据语言的 ORIGINAL：文本必须逐字节等于对应证据的脱敏摘录
  IF NEW.view_kind = 'ORIGINAL'
     AND NEW.view_language <> m.content_language
     AND NOT EXISTS (SELECT 1 FROM memory_evidence e
                      WHERE e.id = NEW.source_evidence_id
                        AND e.memory_id = m.id
                        AND e.customer_id = m.customer_id
                        AND e.content_language = NEW.view_language
                        AND e.evidence_excerpt_redacted IS NOT DISTINCT FROM NEW.content) THEN
    RAISE exception 'SEARCH_DOC_SECONDARY_ORIGINAL_MISMATCH: memory=% evidence=%',
      NEW.memory_id, NEW.source_evidence_id;
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_search_doc_invariant
  BEFORE INSERT OR UPDATE ON memory_search_document
  FOR EACH ROW EXECUTE FUNCTION assert_search_document_invariant();

-- 3) 新记忆提交时必须已有 CANONICAL + 主原文 ORIGINAL 两条文档
CREATE OR REPLACE FUNCTION assert_memory_views_present() RETURNS trigger AS $$
BEGIN
  -- 删除事务在同一事务内先置 DELETE_PENDING、再删投影；此状态不再要求视图存在
  IF TG_OP = 'UPDATE' AND NEW.lifecycle_status IN ('DELETE_PENDING','DELETED') THEN
    RETURN NEW;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM memory_search_document d
                  WHERE d.memory_id = NEW.id AND d.customer_id = NEW.customer_id
                    AND d.view_kind = 'CANONICAL' AND d.view_language = 'und')
     OR NOT EXISTS (SELECT 1 FROM memory_search_document d
                  WHERE d.memory_id = NEW.id AND d.customer_id = NEW.customer_id
                    AND d.view_kind = 'ORIGINAL'
                    AND d.view_language = NEW.content_language) THEN
    RAISE exception 'MEMORY_VIEWS_INCOMPLETE: memory=%', NEW.id;
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

-- customer_memory 是普通表，可以用约束触发器把“同事务内建视图”的检查推迟到提交时
CREATE CONSTRAINT TRIGGER trg_memory_views_present
  AFTER INSERT OR UPDATE ON customer_memory
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION assert_memory_views_present();

-- 4) current_flag 与 SINGLE_VALUE 指针必须一致（延迟到提交后校验）
CREATE OR REPLACE FUNCTION assert_current_flag_pointer() RETURNS trigger AS $$
DECLARE
  m customer_memory%ROWTYPE;
BEGIN
  SELECT * INTO STRICT m FROM customer_memory WHERE id = NEW.memory_id;
  IF m.cardinality = 'SINGLE_VALUE'
     AND (m.current_flag IS DISTINCT FROM (SELECT memory_id = NEW.memory_id
              FROM customer_memory_current
             WHERE customer_id = NEW.customer_id AND fact_key = NEW.fact_key
               AND entity_type = NEW.entity_type AND entity_ref = NEW.entity_ref)) THEN
    RAISE exception 'CURRENT_FLAG_POINTER_DRIFT: memory=%', NEW.memory_id;
  END IF;
  RETURN NULL;
END $$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_current_flag_pointer
  AFTER INSERT OR UPDATE ON customer_memory_current
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION assert_current_flag_pointer();
-- 同客户同事实键的 current_flag 由对账兜底（指针删除侧不会触发本触发器，见 8.10）

-- 5) 禁止引用 DRAFT 或不存在的 registry 版本；RETIRED 必须放行
--    每个 fact_key 只有一个 ACTIVE 版本（ux_fact_key_single_active），新版本激活后旧版本
--    必然 RETIRED，而已有记忆行仍引用旧版本；生命周期状态变更、过期、RENORMALIZE 等 UPDATE
--    不得因此被拒。触发器只在引用列变化时求值，ORM 全列 UPDATE 也不会误触发。
CREATE OR REPLACE FUNCTION assert_registry_active() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'UPDATE'
     AND NEW.fact_key IS NOT DISTINCT FROM OLD.fact_key
     AND NEW.fact_key_registry_version IS NOT DISTINCT FROM OLD.fact_key_registry_version THEN
    RETURN NEW;
  END IF;
  IF NEW.fact_key_registry_version IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM fact_key_registry r
     WHERE r.fact_key = NEW.fact_key AND r.version = NEW.fact_key_registry_version
       AND r.status IN ('ACTIVE','RETIRED')) THEN
    RAISE exception 'REGISTRY_VERSION_NOT_ACTIVE: fact_key=% version=% status_not_active',
      NEW.fact_key, NEW.fact_key_registry_version;
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_memory_registry_active
  BEFORE INSERT OR UPDATE OF fact_key, fact_key_registry_version ON customer_memory
  FOR EACH ROW EXECUTE FUNCTION assert_registry_active();

-- 6) 版本链必须同事实键、版本号单调递增，且不得跨客户
CREATE OR REPLACE FUNCTION assert_supersedes_chain() RETURNS trigger AS $$
DECLARE
  prev customer_memory%ROWTYPE;
BEGIN
  IF NEW.supersedes_memory_id IS NULL THEN
    RETURN NEW;
  END IF;
  SELECT * INTO STRICT prev FROM customer_memory WHERE id = NEW.supersedes_memory_id;
  IF prev.customer_id <> NEW.customer_id
     OR prev.fact_key <> NEW.fact_key
     OR prev.entity_type <> NEW.entity_type
     OR prev.entity_ref <> NEW.entity_ref
     OR prev.value_slot <> NEW.value_slot
     OR prev.version_no >= NEW.version_no THEN
    RAISE exception 'SUPERSEDES_CHAIN_INVALID: new=% prev=%', NEW.id, prev.id;
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_memory_supersedes
  BEFORE INSERT OR UPDATE OF supersedes_memory_id, version_no ON customer_memory
  FOR EACH ROW EXECUTE FUNCTION assert_supersedes_chain();

-- 7) 接纳后的原文证据不可变；生命周期与规范字段仍按各自规则更新
CREATE OR REPLACE FUNCTION assert_memory_original_immutable() RETURNS trigger AS $$
BEGIN
  IF NEW.content IS DISTINCT FROM OLD.content
     OR NEW.content_language IS DISTINCT FROM OLD.content_language
     OR NEW.content_hash IS DISTINCT FROM OLD.content_hash THEN
    RAISE exception 'MEMORY_ORIGINAL_IMMUTABLE: memory_id=%', OLD.id;
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_memory_original_immutable
  BEFORE UPDATE OF content, content_language, content_hash ON customer_memory
  FOR EACH ROW EXECUTE FUNCTION assert_memory_original_immutable();

-- hash 列格式可选加固（防止截断/大写/base64 混用）
CREATE DOMAIN sha256_hex AS char(64) CHECK (VALUE ~ '^[0-9a-f]{64}$');
-- 新建表直接使用 sha256_hex。
-- Complete the 64 HASH partitions shown only as p0 examples in the design baseline.
DO $$
BEGIN
  FOR i IN 0..63 LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS memory_embedding_p%s PARTITION OF memory_embedding FOR VALUES WITH (MODULUS 64, REMAINDER %s)', i, i);
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS memory_search_document_p%s PARTITION OF memory_search_document FOR VALUES WITH (MODULUS 64, REMAINDER %s)', i, i);
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS ix_search_doc_p%s_tsv ON memory_search_document_p%s USING gin (search_vector)', i, i);
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS ix_search_doc_p%s_trgm ON memory_search_document_p%s USING gin (content gin_trgm_ops)', i, i);
  END LOOP;
END $$;


-- ---------------------------------------------------------------------------
-- Seed: default policy and fact-key registry
-- ---------------------------------------------------------------------------

SELECT pg_advisory_xact_lock(hashtextextended('mms_seed:contract', 0));

INSERT INTO memory_policy
  (id, name, version, memory_type, status, config, config_hash, effective_from, created_by, approved_by, approved_at)
VALUES
  ('00000000-0000-0000-0000-000000000001', 'memory-policy-default', '1', NULL, 'ACTIVE',
   '{"auto_accept_threshold":70,"auto_write":true,"conflict_max_age_days":90,"customer_visible_types":["CUSTOMER_PROFILE"],"ignore_threshold":50,"language_confidence_min":0.8,"max_views_per_memory":6,"retention_days":365,"review_rules":{"below_auto_accept":true,"high_impact_requires_authority":true},"review_sla_days":30,"scoring_weights":{"actionability":15,"explicitness":20,"future_service_value":20,"persistence":25,"risk_penalty":-100,"source_trust":20},"semantic_match_threshold":0.82,"sensitive_policy":{"high_requires_consent":true,"medium_requires_consent":true},"translatable":true}'::jsonb, '90955c21bc0496e500f51085b48aeabe60583944bfb297cd8f9e19d7d7269eff', '2026-10-26T00:00:00Z', 'mms-system', 'mms-reviewer', now())
ON CONFLICT (name, version) DO NOTHING;

INSERT INTO fact_key_registry
  (fact_key, version, cardinality, authority_source, allowed_source_kinds, sensitivity,
   retention_days, auto_write, requires_authority, normalizer, validator, mutual_exclusion,
   semantic_template, normalization_version, status, effective_from)
VALUES
  ('communication.language', 1, 'SINGLE_VALUE', NULL,
   ARRAY['USER_EXPLICIT','AGENT_CONFIRMED'], 'LOW', NULL, true, false, 'language-v1',
   '{"type":"object","required":["value"],"properties":{"value":{"type":"string"}}}'::jsonb,
   '[{"group":"language","values":["zh-CN","en-US","es-ES","fr-FR","de-DE","ja-JP","ko-KR"]}]'::jsonb,
   'communication.language = {value}; valid_from = {valid_from}; valid_to = {valid_to}',
   'norm-2026-10-01', 'ACTIVE', '2026-10-26T00:00:00Z'),
  ('product.client_version', 1, 'SINGLE_VALUE', 'device-registry',
   ARRAY['SYSTEM_OF_RECORD','USER_EXPLICIT','AGENT_CONFIRMED'], 'LOW', 365, true, false, 'generic-v1',
   '{"type":"object","required":["value"],"properties":{"value":{"type":"string"}}}'::jsonb,
   '[{"group":"client_version","values":["*"]}]'::jsonb,
   'product.client_version = {value}; valid_from = {valid_from}; valid_to = {valid_to}',
   'norm-2026-10-01', 'ACTIVE', '2026-10-26T00:00:00Z'),
  ('issue.status', 1, 'TEMPORAL', 'ticketing-system',
   ARRAY['SYSTEM_OF_RECORD','AGENT_CONFIRMED'], 'MEDIUM', 365, false, true, 'generic-v1',
   '{"type":"object","required":["value"],"properties":{"value":{"type":"string"}}}'::jsonb,
   '[]'::jsonb,
   'issue.status = {value}; valid_from = {valid_from}; valid_to = {valid_to}',
   'norm-2026-10-01', 'ACTIVE', '2026-10-26T00:00:00Z')
ON CONFLICT (fact_key, version) DO NOTHING;


-- ---------------------------------------------------------------------------
-- Phase 1: single-value concurrency constraints
-- ---------------------------------------------------------------------------

ALTER TABLE customer_memory
  ADD CONSTRAINT ck_memory_single_value_slot
  CHECK (cardinality <> 'SINGLE_VALUE' OR value_slot = '');

CREATE UNIQUE INDEX ux_memory_single_value_active ON customer_memory
  (customer_id, fact_key, entity_type, entity_ref)
  WHERE current_flag AND cardinality = 'SINGLE_VALUE'
    AND lifecycle_status IN ('ACTIVE','CONFLICTED');


-- ---------------------------------------------------------------------------
-- Phase 2: ingestion/outbox lease indexes
-- ---------------------------------------------------------------------------

ALTER TABLE memory_ingestion_job
  ADD COLUMN lease_expires_at timestamptz;

CREATE INDEX ix_ingestion_job_lease ON memory_ingestion_job (lease_expires_at)
  WHERE status = 'PROCESSING';

CREATE INDEX ix_outbox_lease ON memory_outbox (locked_at)
  WHERE status IN ('PENDING','RETRY');


-- ---------------------------------------------------------------------------
-- Phase 2: translation job queue
-- ---------------------------------------------------------------------------

CREATE TABLE memory_translation_job (
  id uuid PRIMARY KEY,
  memory_id uuid NOT NULL,
  customer_id text NOT NULL,
  source_language text NOT NULL,
  target_language text NOT NULL,
  source_view_hash char(64) NOT NULL,
  content_hash char(64) NOT NULL,
  translation_model text NOT NULL,
  translation_version text NOT NULL,
  barrier_generation bigint NOT NULL DEFAULT 0,
  status text NOT NULL CHECK (status IN ('PENDING','RUNNING','READY','RETRY','DEAD_LETTER','CANCELLED')),
  priority smallint NOT NULL DEFAULT 120 CHECK (priority >= 0),
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_by text,
  locked_at timestamptz,
  lease_expires_at timestamptz,
  last_error_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (customer_id,memory_id,target_language,source_view_hash,translation_model,translation_version),
  FOREIGN KEY (memory_id,customer_id) REFERENCES customer_memory(id,customer_id) ON DELETE CASCADE
);
CREATE INDEX ix_translation_job_claim ON memory_translation_job (priority,available_at,created_at)
  WHERE status IN ('PENDING','RETRY');
CREATE INDEX ix_translation_job_lease ON memory_translation_job (lease_expires_at)
  WHERE status='RUNNING';
CREATE INDEX ix_translation_job_customer ON memory_translation_job (customer_id,target_language,status);


-- ---------------------------------------------------------------------------
-- Phase 3: usage idempotency and feedback indexes
-- ---------------------------------------------------------------------------

CREATE UNIQUE INDEX ux_usage_customer_request ON memory_usage_event (customer_id,request_id);
CREATE INDEX ix_feedback_customer_created ON memory_feedback (customer_id,created_at DESC);


-- ---------------------------------------------------------------------------
-- Phase 3: matched views on usage events
-- ---------------------------------------------------------------------------

ALTER TABLE memory_usage_event
  ADD COLUMN matched_views jsonb NOT NULL DEFAULT '{}'::jsonb;

-- >>> mms:upgrade
-- 2026-11-14: Inbox 收敛为《InboxEvent机制设计》目标形态（纯签收簿）：
-- 仅 RECEIVED/COMPLETED，删 attempts/last_error_code，清理索引改 COMPLETED 保留期扫描；
-- ingestion event_type 补枚举 CHECK。旧状态值一律映射 RECEIVED（失败恢复由 Job 侧承担）。
-- 顺序敏感：先删旧 CHECK（否则 RECEIVED 被旧枚举拒绝），迁移数据后再建新 CHECK。
ALTER TABLE memory_inbox_event DROP CONSTRAINT IF EXISTS memory_inbox_event_status_check;
UPDATE memory_inbox_event SET status = 'RECEIVED' WHERE status IN ('PROCESSING','FAILED');
ALTER TABLE memory_inbox_event ADD CONSTRAINT memory_inbox_event_status_check
  CHECK (status IN ('RECEIVED','COMPLETED'));
ALTER TABLE memory_inbox_event ALTER COLUMN status SET DEFAULT 'RECEIVED';
ALTER TABLE memory_inbox_event DROP COLUMN IF EXISTS attempts;
ALTER TABLE memory_inbox_event DROP COLUMN IF EXISTS last_error_code;
DROP INDEX IF EXISTS ix_inbox_stale;
CREATE INDEX IF NOT EXISTS ix_inbox_completed ON memory_inbox_event (status, updated_at)
  WHERE status = 'COMPLETED';
ALTER TABLE memory_ingestion_job DROP CONSTRAINT IF EXISTS memory_ingestion_job_event_type_check;
ALTER TABLE memory_ingestion_job ADD CONSTRAINT memory_ingestion_job_event_type_check
  CHECK (event_type IN ('memory.candidate.v1','memory.extraction.requested.v1'));
-- <<< mms:upgrade

-- ---------------------------------------------------------------------------
-- >>> mms:upgrade
-- 7.15 运行期账号权限基线（docs 12.3）
-- 角色由本文件开头以 NOLOGIN 建立，LOGIN/口令属于环境 bootstrap（scripts/init_db.py
-- 在 MMS_PROVISION_LOGIN_ROLES=1 时接管）。本节必须留在文件末尾：GRANT ... ON ALL
-- TABLES 只覆盖此前已存在的表和分区。
-- ---------------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO mms_app, mms_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mms_readonly;

-- 向量表只允许 Worker 写入：API 账号既不能建向量、也不能改写向量值，失效与擦除
-- 一律走物理 DELETE（docs 12.5 "删除后仍可召回" 的最强缓解）。
REVOKE INSERT, UPDATE ON memory_embedding FROM mms_app;

-- 审计与墓碑对运行期账号保持 append-only；上面的宽授权会覆盖 7.6 的 REVOKE，故重申。
REVOKE UPDATE, DELETE ON memory_decision FROM mms_app, mms_worker;
REVOKE UPDATE, DELETE ON admin_access_audit FROM mms_app, mms_worker;
REVOKE UPDATE ON deletion_tombstone FROM mms_app, mms_worker, mms_deletion_worker;
REVOKE DELETE ON deletion_tombstone FROM mms_app, mms_worker;
-- 跨境 rerank 通道需要独立 consent 目的（docs 12.2/12.5）：ADD CONSTRAINT 没有 IF NOT EXISTS，
-- 所以按约束名先删后建，保证本节可在存量库重放。
ALTER TABLE customer_memory_consent DROP CONSTRAINT IF EXISTS customer_memory_consent_purpose_check;
ALTER TABLE customer_memory_consent ADD CONSTRAINT customer_memory_consent_purpose_check CHECK (purpose IN
    ('MEMORY_WRITE','MEMORY_READ','MEMORY_RETENTION','SENSITIVE_MEMORY',
     'CROSS_BORDER_EMBEDDING','CROSS_BORDER_TRANSLATION','CROSS_BORDER_RERANK'));
-- <<< mms:upgrade
