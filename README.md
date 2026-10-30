# 智能客服记忆管理服务（MMS）

面向 C 端智能客服的独立长期记忆服务：从会话里抽取"关于这个客户的稳定事实"，经硬规则准入与规则表决策后写入 PostgreSQL，再按授权、生命周期与删除合规的要求，召回给对话 Agent、客服工作台和运营后台。

- 技术栈：Python 3.13 / FastAPI / PostgreSQL 16 + pgvector + pg_trgm / OTel + Tempo + Loki + Prometheus + Grafana + Alertmanager / Langfuse。
- 权威设计文档：`docs/记忆管理-技术方案.md`（v2.11，24 章）。本 README 只做功能导览与操作说明；口径或细节与文档冲突时，以该文档为准。

## 一、功能说明

### 1.1 能力清单

| 能力 | 做什么 | 实现位置 | 技术方案 |
|---|---|---|---|
| 会话→候选抽取 | 规则提取器从会话文本产出结构化候选（fact_key / value / 证据定位符 / 语言 / 置信度）；LLM 提取器仅留接口，尚未接线 | `src/mms/services/extractor.py` | §4 |
| 准入硬规则与评分 | 敏感门禁、证据定位符校验、注册表敏感级别、配额与限流、跨境外发门禁；不满足直接 `REJECTED`/`NEEDS_VERIFICATION` | `services/normalizer.py`、`services/quota.py` | §5 |
| 写入决策引擎 | 有序规则表 R001–R110（22 条）、首条命中、原因码归因；产出 `ADD`/`SUPERSEDE`/`REINFORCE`/`NOOP`/`CONFLICT`/`IGNORE` 等动作与可回放决策快照 | `services/rules.py:apply_decision_matrix` | §6 |
| 同事务落库 + 异步投影 | 决策、事实版本链、证据、ORIGINAL/CANONICAL 检索文档、Job、Outbox 在一个事务内提交；embedding 是事务外的投影 | `services/service.py`、`services/repository.py` | §7 §23 |
| 向量投影与就绪态 | Embedding Worker 租约消费 Job，写入就绪向量；检索只读就绪态，写入不被 provider 拖住 | `services/worker.py`、`utils/embedding.py` | §12.3 |
| 多语言 | 语义标准化用 CANONICAL 视图，跨语言可发现性用 TRANSLATED 视图，词法按语言分策略（fts / trgm）；超预算时按热度/优先级/质量淘汰译文视图 | `services/translation.py`、`services/view_cost.py`、`utils/language.py` | §9 |
| 检索 | `fact_key` 精确命中 + 单客户 exact 多视图语义 + 词法召回，RRF（k=60）融合，可选 rerank；每条降级都有原因码 | `services/service.py:MemoryService.retrieve` | §12 |
| 授权与撤回 | 7 个 purpose 的 consent 快照、写入与检索两侧的放行判定、版本单调、撤回复用删除屏障 | `api/main.py:sync_consent`、`services/service.py` | §3 |
| 删除与失效 | 客户级删除屏障（generation 递增）+ 同库失效 + 墓碑账本 + 备份窗口重放校验 | `services/deletion.py`、`scripts/deletion_replay.py` | §8 |
| 冲突识别与人工裁决 | 冲突入审核队列，批准/驳回带原因码与版本，强制 4-eyes（审核人≠创建人，HIGH 风险需第二人） | `services/review.py` | §10 |
| 生命周期与对账 | 候选处理/业务生命周期/索引就绪/审核待办四态分离，每个状态都有自动出口；JOB/VECTOR/RECALL/BUSINESS 四类对账 | `services/lifecycle.py`、`services/recon.py` | §11 §14 |
| 幂等摄取 | 事务 Inbox 记录 topic/partition/offset，重放走完整 consent/barrier 判定；DLQ 可重放 | `services/ingestion.py` | §14 |
| Agent 上下文与反馈 | 只返回不可信 `memory_context` + 指令边界声明；反馈幂等，`INCORRECT` 必须携带纠正候选；记录实际采用记忆与答案哈希 | `POST /api/v1/agent/memory-context`、`services/usage.py` | §13 |
| 身份与审计 | 服务令牌 + 网关注入的客户头 + 坐席授权表 + 管理面五头凭证；客服面读取无论允许或拒绝都先落审计（写失败 fail-closed） | `api/guards.py`、`services/admin.py` | §15 |
| 容量与成本治理 | 单客户 exact 扫描容量画像、向量名额告警与处置建议；`EXACT_SCAN_SOFT_LIMIT` 只告警，不自动切 ANN | `services/capacity.py` | §12 §16 |
| 线上观测 | Trace/Metrics/Logs/SLO/告警 + 模型可观测（Langfuse 模式 A） | `src/mms/observability/`、`deploy/` | §18 |
| 离线评测 | 代码质量六道门禁 + 模型质量 Golden 集 + 公开基准 + Judge 校准 | `scripts/quality_gate.py`、`evals/` | §17 |

### 1.2 对外接口

身份列的含义：`SVC` = `X-Service-Token`（网关/业务后端凭证）；`CID` = `X-Customer-ID`（网关注入的主体映射，请求体永远不能决定客户身份）；`AGENT` = `X-Agent-ID` 且该坐席必须在 `agent_customer_grant` 中有未撤销授权；`INT` = 内网凭证；`ADMIN` = `X-Admin-Token` + `X-Reviewer-ID` + `X-Admin-Reason-Code` + `X-Admin-Ticket` + `X-Admin-Role`。

| 分组 | 端点 | 身份 | 说明 |
|---|---|---|---|
| 写入 | `POST /api/v1/extractions` | SVC+CID | 会话文本进规则提取器，202 异步 |
| 写入 | `POST /api/v1/memories/candidates` | SVC+CID | 提交结构化候选，走完整决策闭环 |
| 写入 | `POST /api/v1/memories/{id}/feedback` | SVC+CID | 幂等反馈；写回 Langfuse Score |
| 写入 | `POST /api/v1/memory-usage` | SVC+CID | 上报实际采用的记忆、答案哈希与模型版本 |
| 写入 | `POST /internal/ingestion/events` | INT | 事务 Inbox + durable job，topic 白名单 |
| 检索 | `POST /api/v1/memories/query` | SVC+CID | 精确或混合检索；`as_of` 仅 `HISTORY_DEBUG`/`AUDIT` 可用 |
| 检索 | `POST /api/v1/agent/memory-context` | SVC+CID | 扁平化不可信记忆上下文 + 指令边界 |
| 客服面 | `GET /api/v1/customers/{id}/memories` | SVC+CID+AGENT | 坐席代表客户读取；内容经凭证掩码后截断 |
| 审核 | `GET /api/v1/review-queue`、`POST /api/v1/review-queue/{id}/decision` | SVC(+`X-Reviewer-ID`) | 冲突/候选/删除审核，4-eyes |
| 删除 | `POST /api/v1/deletion-requests`、`GET /api/v1/deletion-requests/{id}` | SVC+CID | 先 `RECEIVED`，审核通过才物理删除并写墓碑 |
| 授权 | `GET/POST /internal/customers/{id}/memory-consent` | INT | 同意快照同步与查询 |
| 身份 | `GET/POST /internal/customers/{id}/identity-ref` | INT | 身份标识解析与登记 |
| 容量 | `GET /internal/customers/{id}/retrieval-capacity` | INT | exact 扫描容量、分区与处置建议 |
| 治理 | `POST /internal/view-cost/run`、`POST /internal/reconciliation/{JOB\|VECTOR\|RECALL\|BUSINESS}`、`GET /internal/ingestion/jobs/{id}` | INT | 视图成本淘汰与对账 |
| 管理 | `GET/POST /api/v1/admin/policies`、`GET /api/v1/admin/fact-keys`、`POST /api/v1/admin/embeddings/rebuild`、`GET /api/v1/admin/memories/{id}/lineage`、`POST /api/v1/admin/decisions/replay`、`POST /api/v1/admin/dlq/replay` | ADMIN（dlq 可用 SVC） | 策略版本、血缘、重建与决策回放 |
| 运维 | `GET /health/live`、`/health/ready`、`/health/deps`、`GET /metrics` | 公开 | ready 校验可写库 + pgvector + schema 标记；deps 真实探测 provider/副本 |


## 二、架构设计

### 2.1 支撑实现的关键原则

完整 11 条见技术方案 §1.3，代码里真正被这四条约束住的是：

1. **业务状态与向量索引同库**——一致性只有一个边界，一条 trace 能走完提取→决策→落库→索引→召回→失效。
2. **先决策，再生成索引**——原始对话不得无治理地进向量表；embedding 是异步投影，检索只读就绪态。
3. **删除是最高优先级**——屏障一旦 ACTIVE，写入、检索、重放、后台任务全部以它为准，且必须覆盖备份窗口。
4. **授权是写入与检索的前置条件**——purpose 与判断点一一对应，默认失败关闭高风险变更；决策上下文快照化，可回放且结果一致。

### 2.2 组件与部署形态

```mermaid
flowchart LR
  CHAT["对话服务 / 业务后端"]
  CONSENT["授权服务"]
  FEED["事件投递方（Kafka 桥接为 HTTP）"]
  subgraph MMS["记忆管理服务（同一镜像，不同入口）"]
    API["FastAPI 应用"]
    W1["embedding worker"]
    W2["ingestion worker"]
    W3["outbox publisher"]
    W4["lifecycle（含对账触发）"]
    W5["translation worker"]
    W6["view-cost"]
    RC["recon（按需一次性执行）"]
  end
  PG[("PostgreSQL 16 + pgvector：状态 / 证据 / 投影 / Job / Outbox / 墓碑")]
  PROV["外部 Provider：Embedding、Translation、Rerank、WORM 账本、Outbox 消费端"]
  OBS["观测栈：Collector→Tempo、Promtail→Loki、Prometheus→Alertmanager+Grafana、Langfuse"]
  CHAT --> API
  CONSENT --> API
  FEED --> API
  API --> PG
  API --> PROV
  W1 --> PG
  W2 --> PG
  W4 --> PG
  W5 --> PG
  W6 --> PG
  RC --> PG
  W1 --> PROV
  W3 --> PROV
  W3 --> PG
  W5 --> PROV
  API --> OBS
  W1 --> OBS
  W4 --> OBS
```

要点：

- **进程只有 API 与 6 个常驻 worker**；`recon` 是一次性命令（`mms-recon`）与 `POST /internal/reconciliation/{type}`，lifecycle 每 300 秒顺带触发对账。
- **代码内没有 Kafka 客户端**。§14 的 Kafka 语义由投递方落实：外部把事件以 `POST /internal/ingestion/events` 送进来，MMS 用 `memory_inbox_event`（topic/partition/offset 唯一）做幂等，重复投递直接返回既有 job。
- **读副本只用于检索**；屏障、consent、写后读与所有写操作仍走主库，副本不可用即回落主库并记降级原因。

### 2.3 写入路径（一次候选提交发生了什么）

1. `QuotaService.consume` 记配额，`api` 层已做体积与令牌桶限制。
2. `MemoryService.prepare_candidate`：加载策略与事实键注册表 → 注册表敏感级别校验 → 回执门禁 → `normalize_candidate` 硬规则（缺证据定位符直接 `REJECTED/MISSING_EVIDENCE`；`entity_ref` 命中权威来源且非 SoR 时 `NEEDS_VERIFICATION/IDENTITY_UNVERIFIED`）→ 跨境外发门禁 → 发现式 embedding 与 `semantic_candidates` 预取。
3. `MemoryService.process_candidate`：会话级咨询锁 `lock_barrier` / `lock_fact` → consent 门禁（拒绝即终态 `REJECTED`）→ 落候选 → `find_exact` 取同键现存事实 → `apply_decision_matrix` 按 R001–R110 首条命中产出动作与原因码。
4. `MemoryService._persist_decision`（同一事务）：写 `customer_memory` 版本链与 `customer_memory_current` 指针、`memory_evidence`、`memory_search_document`（ORIGINAL + CANONICAL）、两个 `memory_embedding_job`、必要时 `memory_translation_job`、`memory_outbox`、`memory_decision` 快照。只允许 `ADD`/`SUPERSEDE` 移动 `SINGLE_VALUE` 指针；`SINGLE_VALUE` 叠加 R090 会撞唯一索引，因此该组合改出 `CONFLICT` 转人工。
5. Embedding Worker 用 `FOR UPDATE SKIP LOCKED` 租约取 Job，再次校验 consent 与屏障，批量调用 provider，`_final_write` 写 `memory_embedding` 就绪行并更新就绪状态。
6. Outbox Publisher 按信封 JSON Schema 发布事件；发布前重查屏障，屏障命中则 `BARRIER_BLOCKED` 并留痕。

### 2.4 检索路径

```mermaid
flowchart TD
  A["POST /memories/query"] --> B{"屏障 + consent 门禁"}
  B -->|不通过| Z["空结果 + 降级原因"]
  B -->|通过| C["purpose → 可见状态集合"]
  C --> D{"fact_key 精确？"}
  D -->|是| E["exact 索引取版本链"]
  D -->|否| F["单客户 exact 语义 top-K（就绪向量集合内）"]
  F --> G["词法通道：fts ts_rank_cd 或 trgm，可按语言并行"]
  E --> H["RRF k=60 融合 + 类型/来源排序"]
  G --> H
  H --> I{"启用 rerank 且跨境 rerank 已授权？"}
  I -->|是| J["外部 reranker"]
  I -->|否| K["保持融合顺序"]
  J --> L["冲突项只出上下文 + 历史 as_of 追加"]
  K --> L
  L --> M["响应：memories + conflicted_memories + 降级原因 + trace_id"]
```

- 语义通道是**单客户集合内的精确向量比较**，不是全局 HNSW。schema 里没有 HNSW/IVFFlat 索引；选择率算例见 §12.2。
- 降级都是有名字、有指标的：embedding 不可用、词法不可用、副本不可用、翻译不可用、语言低置信。检索侧向量通道关闭不计入服务降级（`_retrieval_query_vector`）。
- 查询向量有进程内 LRU（`utils/query_cache.py`），屏障变更通过 `LISTEN mms_barrier_changed` 主动失效（`utils/barrier_listener.py`）。

### 2.5 授权、删除与审核

- **consent**：`customer_memory_consent` 按 `(customer_id, purpose)` 一行，7 个 purpose 各自独立；版本必须严格更大才生效（等版本重放视为幂等，不算陈旧）。撤回 `MEMORY_WRITE`/`MEMORY_READ` 会生成删除请求（`request_source=CONSENT_WITHDRAWN`）；屏障再授权时把已完成的删除请求转 `CANCELLED` 并递增 generation。
- **删除**：`DeletionService.request` → 屏障 ACTIVE（generation+1）+ HIGH 风险审核任务 + outbox + `NOTIFY` → 审核通过后 `MemoryRepository.delete_customer_data`：置 `DELETE_PENDING`、删向量、由 `mms_audit_scrubber` 连接清洗 `memory_decision` 的 PII 列、物理删除事实与证据、经 WORM 账本写墓碑 → `COOLING_DOWN` → `COMPLETED`。墓碑清理走独立连接角色 `mms_deletion_worker`（`MMS_DELETION_DATABASE_URL`），已过期才允许物理清理，守卫触发器兜底。
- **备份窗口**：PITR 恢复出来的库可能复活已删数据，`scripts/deletion_replay.py` 读账本、在恢复库中解析匿名客户引用并重放同一删除事务（§8.4、§20.1 门禁项）。
- **4-eyes**：`ReviewService.decide` 拒绝 reviewer==creator；HIGH 风险还需不同的 `co_reviewer_id`。

### 2.6 数据模型

`schema/schema.sql` 建 30 张父表（`memory_embedding`、`memory_search_document` 另有 DEFAULT 子分区；两表按 `HASH(customer_id)` 分区，实际部署 64×2）。按用途分组：

| 分组 | 表 |
|---|---|
| 配置与契约 | `fact_key_registry`、`memory_policy`、`embedding_model_registry` |
| 事实与审计 | `customer_memory`（版本链）、`customer_memory_current`、`memory_evidence`、`memory_candidate`、`memory_decision`（追加写） |
| 检索投影 | `memory_search_document`（view_type=ORIGINAL/CANONICAL/TRANSLATED + `search_vector`）、`memory_embedding` |
| 队列与事件 | `memory_embedding_job`、`memory_translation_job`、`memory_ingestion_job`、`memory_inbox_event`、`memory_outbox` |
| 删除与屏障 | `customer_deletion_barrier`、`memory_deletion_request`、`memory_deletion_item`、`deletion_tombstone` |
| 授权与身份 | `customer_memory_consent`、`customer_identity`、`agent_customer_grant`、`customer_quota_usage` |
| 冲突与审核 | `memory_review_task`、`memory_conflict`、`memory_conflict_item` |
| 反馈与运维 | `memory_feedback`、`memory_usage_event`、`recon_run`、`admin_access_audit` |

索引与约束的取向：72 个索引里，B-tree `(customer_id, status, model)` 支撑 exact-first，每分区 `GIN(search_vector)` + `GIN (content gin_trgm_ops)`，租约与队列大量使用**部分索引**与**部分唯一索引**（如 `ux_memory_single_value_active` 保证单值活跃唯一）；扩展 `vector`、`pg_trgm`、`btree_gist`；12 个守卫触发器把不变量压在数据库里——证据与 ORIGINAL 视图不可变、`memory_decision` 与 `admin_access_audit` 追加写、墓碑未过期不得清理、embedding 的模型/维度/Job 必须命中注册表、检索文档与视图不变量、`supersedes` 版本链一致。

数据库角色（DDL 里是 NOLOGIN，`scripts/init_db.py` 可选开成登录账号）：`mms_app`（API）、`mms_worker`（worker/对账/生命周期）、`mms_readonly`（副本读）、`mms_audit_scrubber`（仅 PII 列的清洗 UPDATE）、`mms_deletion_worker`（仅墓碑相关 SELECT/INSERT/DELETE）。`mms_app` 对 `memory_embedding` 无 INSERT/UPDATE 权，对 `memory_decision` 与 `admin_access_audit` 无 UPDATE/DELETE 权——权限本身就是"向量是投影、决策是追加写"这条原则的落点。

### 2.7 运行形态

| 进程 | 模块入口 | console script | 度量端口 | 触发与互斥 |
|---|---|---|---|---|
| API | `python -m mms.api.main` | `mms-api` | 8000 `/metrics` | uvicorn，`MMS_BIND_HOST/PORT` |
| Embedding Worker | `python -m mms.services.worker` | `mms-worker` | 9101 | 轮询 1s，`SKIP LOCKED` 租约 |
| Ingestion Worker | `python -m mms.services.ingestion` | `mms-ingestion` | 9102 | 轮询 inbox job，`SKIP LOCKED` |
| Outbox Publisher | `python -m mms.services.outbox` | `mms-outbox` | 9103 | 轮询待发，发布前重查屏障 |
| Lifecycle | `python -m mms.services.lifecycle` | `mms-lifecycle` | 9104 | 60s 轮询 + `SchedulerLock`（会话级 `pg_try_advisory_lock`），每 300s 触发对账 |
| Translation | `python -m mms.services.translation` | `mms-translation` | 9105 | 消费 `memory_translation_job`，写完以优先级 120 排 embedding |
| View-cost | `python -m mms.services.view_cost` | `mms-view-cost` | 9106 | `SchedulerLock` 轮询，超名额时淘汰低价值 TRANSLATED |
| Reconciliation | `python -m mms.services.recon` | `mms-recon` | — | 一次性执行，输出 JSON |

多副本安全：队列靠 `SKIP LOCKED`，单例任务靠 advisory 锁；lifecycle 的墓碑清理与扫描已在 2026-10-30 拆到独立事务与独立角色，避免权限错配导致清理静默失败。

### 2.8 可观测体系

| 信号 | 链路 | 关键配置 |
|---|---|---|
| Trace | FastAPI/HTTPX/psycopg 自动插桩 + `retrieval.query`/`extraction.run`/`embedding.job` 业务 Span → OTLP HTTP → Collector(0.111.0) → Tempo | `MMS_OTEL_ENDPOINT`、`MMS_OTEL_SAMPLE_RATIO` |
| Metrics | API 与 6 个 worker 各自暴露 `/metrics`；Prometheus 抓 `api:8000` + 9101–9106 + collector 自身 | `MMS_METRICS_EXPORT_PORT`；规则在 `deploy/prometheus/{slo-rules,alerts}.yml` |
| Logs | 结构化 JSON（trace_id/span_id/request_id + 脱敏）→ stdout → Promtail → Loki；Loki 数据源可由 TraceID 跳转 Tempo | `MMS_LOG_LEVEL` |
| 模型可观测 | Langfuse 模式 A：SpanProcessor 挂在**现有** TracerProvider 上，`blocked_instrumentation_scopes` 屏蔽 fastapi/asgi/httpx/psycopg/mms.api，只上报业务与模型 Observation；observation 经 `mask` 脱敏 | `MMS_LANGFUSE_*`（兼容标准 `LANGFUSE_*`）、`MMS_LANGFUSE_SAMPLE_RATIO` |
| 展示与告警 | Grafana 预置 `MMS Observability - Golden Signals`（16 面板）；Alertmanager 收 Prometheus 告警 | `deploy/grafana/dashboards/` |

模型侧指标统一走 `model_call` 封装（embedding/translation/rerank）：`mms_llm_requests_total`、`mms_llm_request_duration_seconds`、`mms_llm_tokens_total`、`mms_rag_retrieval_duration_seconds`、`mms_rag_retrieval_results`、`mms_rag_quality_score_avg`。业务 `trace_id`（hex32）即 Langfuse trace id，`POST /memories/{id}/feedback` 按 `request_id` 把 `user-feedback` Score 写回同一条 Trace。

**指标契约是硬约束**：`deploy/prometheus/*.yml` 里引用的每个指标名都必须注册在 `src/mms/utils/metrics.py`，否则 PromQL 得到空序列、告警静默失效。`python scripts/metrics_contract.py` 做这条校验，已接进 CI 与本地门禁。


## 三、目录说明

```
.
├── src/mms/              服务实现（见 3.1）
├── schema/schema.sql     权威 DDL：30 张父表、角色、分区、索引、不变量触发器
├── contracts/            候选/决策/策略/事实键/事件信封 JSON Schema + OpenAPI 基线
├── tests/                pytest 单元/集成/API；benchmarks/ 性能基线，evaluation/ DeepEval 服务评测
├── evals/                离线模型质量评测（见 3.2）
├── loadtests/            Locust 负载脚本
├── scripts/              初始化、契约校验、质量门禁、金丝雀、恢复工具（见 4.6/4.8）
├── deploy/               Prometheus/SLO/告警、Grafana 面板与数据源、Loki/Promtail、Tempo、OTel Collector、Alertmanager
├── docker/               Dockerfile、Dockerfile.quality、docker-compose.yml（api + 6 worker + 观测栈 + Langfuse）
├── docs/                 技术方案 + 5 份专项设计 + 方案总结（见 3.3）
├── .github/workflows/    quality-gate.yml（PR）、nightly-quality.yml（02:00 CST）、llm-evaluation.yml（02:30 CST）
├── pyproject.toml        依赖、extras、console scripts、ruff/mypy/bandit/pytest 配置
├── uv.lock               锁定依赖（`uv sync --frozen`）
├── .env.example          51 个 MMS_* 键的样例（见 4.10：当前 SMB 目录项损坏，可读副本在 .env.example.new）
└── .gitleaks.toml        密钥扫描允许清单
```

### 3.1 `src/mms/`

| 目录 | 内容 |
|---|---|
| `api/` | `main.py` 全部路由与依赖装配；`guards.py` 请求体上限与按客户令牌桶；`schemas.py` 请求模型（多数 `extra="forbid"`） |
| `models/` | `CandidateInput`、`NormalizedCandidate`、`ExistingFact`、`Decision` 等 Pydantic 契约 |
| `services/` | `extractor` 规则抽取 · `normalizer` 归一与硬门 · `rules` 决策矩阵 · `service` 编排 · `repository` 全部 SQL · `retriever` 检索协议 · `worker`/`ingestion`/`outbox`/`lifecycle`/`translation`/`view_cost`/`recon` 后台进程 · `deletion` 删除与墓碑 · `review` 审核与 4-eyes · `admin` 管理面与审计 · `quota`/`capacity`/`usage` 配额与容量 · `quality` Recall 评估与阶段门禁 |
| `observability/` | `tracing` OTel 装配 · `llm` Langfuse 模式 A 与脱敏 · `logging_config` JSON 日志 · `request_context` request_id 贯穿 · `runtime`/`attributes`/`bootstrap` 运行时与属性 |
| `utils/` | `db` 连接池/副本/scrubber 与 deletion 事务/`SchedulerLock` · `embedding` Deterministic 与 Http provider · `reranker` HttpReranker · `language` 脚本与语言判定 · `hashing` 语义哈希与指纹 · `metrics` 指标注册表（契约来源） · `backoff`/`outbound` 重试与出站 TLS/token 强校验 · `query_cache` 查询向量 LRU · `barrier_listener` `LISTEN mms_barrier_changed` 失效 · `tokens`/`errors` |

### 3.2 `evals/`

```
evals/
├── datasets/            core_goldens / rag_cases / security_cases / regression_cases
├── benchmarks/          LongMemEval + LoCoMo：download_data → convert → samples → replay → reader/scoring
├── lifecycle/cases/     memory_write / conflict_update / expiry / deletion_isolation 四类 JSONL
├── metrics/             deterministic.py 契约指标，judge_metrics.py LLM-as-a-Judge
├── calibration/         Judge 校准（误通过率/误拒绝率）
└── harness/runner/recorder/judge/loader + test_*.py
```

`harness.py` 用 FastAPI `TestClient` 驱动真实服务并带起 worker/lifecycle，评的是被测应用的输出质量，不是 mock。原始基准数据不入库（`evals/benchmarks/data/` 已 gitignore），入库的是带 SHA256 manifest 的转换样本。

### 3.3 `docs/`

| 文件 | 作用 |
|---|---|
| `记忆管理-技术方案.md` | **权威方案**（v2.11，24 章）：架构、契约、DDL、上线门禁 |
| `记忆管理-方案总结.md` | 面向评审的归纳版：亮点、多语言挑战的场景→问题→行动→结果（基于 v2.8，滞后于正文） |
| `1-离线质量评测体系设计-代码.md` / `2-...-模型.md` | 代码质量六道门禁、模型质量评测体系设计 |
| `3-离线质量评测集设计-记忆管理.md` | Golden 集与公开基准的构建规则 |
| `4-线上可观测体系设计-代码.md` / `5-...-模型.md` | OTel/Prometheus/Loki/Grafana 链路与 Langfuse 模式 A |

## 四、使用方式

### 4.1 最快路径：docker compose

```bash
cp .env.example .env           # SMB 上 .env.example 读不出时改用 .env.example.new
docker compose -p mms -f docker/docker-compose.yml up -d --build
```

`postgres`（宿主 5433）→ `init-db`（建库、加载 `schema/schema.sql`、可选开通登录角色）→ `api`（8000）→ 6 个 worker → 观测栈一次拉起；`langfuse-*` 只在显式指定服务时启动。构建上下文是仓库根，必须在仓库根执行。

```bash
curl -s localhost:8000/health/ready | python -m json.tool
docker compose -p mms -f docker/docker-compose.yml logs -f api worker
```

### 4.2 本地裸跑（不打包）

```bash
uv sync --frozen --extra dev            # 或 pip install '.[dev]'
docker compose -p mms -f docker/docker-compose.yml up -d postgres
export MMS_DATABASE_URL=postgresql://mms:mms@127.0.0.1:5433/mms
PYTHONPATH=src python scripts/init_db.py

PYTHONPATH=src python -m mms.services.worker &
PYTHONPATH=src python -m mms.services.ingestion &
PYTHONPATH=src python -m mms.services.outbox &
PYTHONPATH=src python -m mms.services.lifecycle &
PYTHONPATH=src python -m mms.services.view_cost &
PYTHONPATH=src python -m mms.api.main      # uvicorn 0.0.0.0:8000
```

安装为包后可直接用 console scripts（`mms-api`、`mms-worker`、`mms-ingestion`、`mms-outbox`、`mms-lifecycle`、`mms-translation`、`mms-view-cost`、`mms-recon`）。`scripts/init_db.py` 只认 `MMS_DATABASE_URL`（无命令行参数）；`MMS_PROVISION_LOGIN_ROLES=1` 才把 DDL 里的 NOLOGIN 运行期角色开成登录账号并设密码（compose 的 `init-db` 已默认开启）。账号分离后 API 用 `mms_app` DSN，worker/对账/生命周期用 `mms_worker` DSN，另配 `MMS_DELETION_DATABASE_URL`（`mms_deletion_worker`）与 scrubber 凭证。

### 4.3 最小调用样例

```bash
SVC='X-Service-Token: dev-token'          # 对应 MMS_SERVICE_TOKEN
CID='X-Customer-ID: c-1001'
JSON='Content-Type: application/json'

# 1) 先给写入与读取授权（版本必须严格递增）
for P in MEMORY_WRITE MEMORY_READ; do
  curl -sX POST localhost:8000/internal/customers/c-1001/memory-consent \
    -H "$SVC" -H "$JSON" \
    -d "{\"purpose\":\"$P\",\"status\":\"GRANTED\",\"version\":1,\"effective_at\":\"2026-10-30T00:00:00Z\"}"
done

# 2) 会话文本进规则提取器
curl -sX POST localhost:8000/api/v1/extractions -H "$SVC" -H "$CID" -H "$JSON" \
  -d '{"source_event_id":"evt-1","content":"我对海鲜过敏，配送前请打电话确认。"}'

# 3) 等 embedding job 被 worker 消费（就绪态才可被语义召回），再检索
curl -sX POST localhost:8000/api/v1/memories/query -H "$SVC" -H "$CID" -H "$JSON" \
  -d '{"query":"配送前能联系我吗","purpose":"CURRENT_ANSWER","max_items":5}'

# 4) 给定 fact_key 的精确读
curl -sX POST localhost:8000/api/v1/memories/query -H "$SVC" -H "$CID" -H "$JSON" \
  -d '{"query":"过敏","purpose":"CURRENT_ANSWER","fact_key":"diet.allergy.seafood"}'
```

没有 GRANTED 的 consent 时写入与检索都会被门禁挡下并给出原因码；返回体里的 `trace_id` 可直接在 Tempo / Langfuse 里查同一条链路。

### 4.4 配置与 provider

所有键都是 `MMS_` 前缀，可用 `.env` 注入（`src/mms/config.py` 是唯一口径）。默认 provider 只够本地契约验证，**不能上生产**：

| 维度 | 本地默认 | 生产 |
|---|---|---|
| Embedding | `MMS_EMBEDDING_PROVIDER=deterministic`（正交哈希向量，无语义） | `=http` 指向自托管 BGE-M3；`MMS_EMBEDDING_PROVIDER_URL`、`MMS_EMBEDDING_MODEL`（默认 `BAAI/bge-m3`）、`MMS_EMBEDDING_VERSION`、`MMS_EMBEDDING_TIMEOUT_MS`；维度默认 1024，改动要走新 ADR |
| Translation | `MMS_TRANSLATION_PROVIDER` 为空，不产出 TRANSLATED 视图 | `=http` + `MMS_TRANSLATION_PROVIDER_URL`、`MMS_TRANSLATION_TARGET_LANGUAGES`、`MMS_TRANSLATION_MODEL`/`MMS_TRANSLATION_MODEL_VERSION` |
| Rerank | `MMS_RERANKER_ENABLED=false` | 配 `MMS_RERANKER_URL`，跨境 rerank 需 `CROSS_BORDER_RERANK` 授权 |
| Outbox | local 日志 transport | `MMS_OUTBOX_ENDPOINT` + 凭证 |
| WORM 账本 | `MMS_DELETION_LEDGER_DIR`（默认 `var/mms-deletion-ledger`） | `MMS_DELETION_LEDGER_ENDPOINT`（非 dev 环境必填，否则启动即失败） |

`validate_cross_config` 会在启动时 fail-fast，不是运行到一半才报错：http provider 缺 URL/model/version、`max_views_per_memory < 2`、屏障缓存 TTL 超过 5s 可见性 SLO、墓碑保留期不超过"备份保留 + 恢复窗口"、词法语言映射与 `ts_config` 不覆盖或互相冲突、reranker 缺 URL、非回环 http 出站地址、非 dev 缺删除账本 endpoint、配了副本却没 DSN——任一条直接拒绝启动。

### 4.5 测试

```bash
docker exec mms-postgres-1 psql -U mms -d mms -c "DROP DATABASE IF EXISTS mms_test WITH (FORCE)" -c "CREATE DATABASE mms_test OWNER mms"
docker exec -i mms-postgres-1 psql -U mms -d mms_test < schema/schema.sql
export MMS_TEST_DATABASE_URL=postgresql://mms:mms@127.0.0.1:5433/mms_test
PYTHONPATH=src pytest -q -p no:cacheprovider                    # 默认排除 slow 标记
```

标记：`unit`（不需数据库）/`integration`（需 `MMS_TEST_DATABASE_URL`）/`evaluation`/`benchmark`/`public_benchmark`/`slow`。只跑单元用 `pytest -m unit`。集成库不会自动迁移，schema 变更后需 `DROP DATABASE ... WITH (FORCE)` 重建再灌 `schema/schema.sql`。跑套件前确认 compose 的 worker 容器不会来抢同一库里的 job（先 `docker stop mms-worker-1 mms-ingestion-1 mms-outbox-1 mms-lifecycle-1 mms-translation-1 mms-view-cost-1`）。

### 4.6 质量门禁

```bash
python scripts/quality_gate.py --profile local     # ruff/format/mypy/单元/openapi/metrics-contract
python scripts/quality_gate.py --profile ci --require-new-code-coverage --diff-base origin/main
python scripts/quality_gate.py --profile nightly    # 再加 benchmark 与 mutation
python scripts/quality_gate.py --only metrics-contract
```

维度与判据见技术方案 §17；`--skip`/`--only` 可临时摘掉阶段。报告写 `quality/reports/{latest.json,latest.md,history.ndjson}`（不进版本库）。基线缺失时 `baseline` 阶段会 FAIL 并提示重建：

```bash
python scripts/quality_gate.py --update-baseline --accept-new    # 首次建基线或明确接受新增债务
python scripts/quality_gate.py --only llm-evaluation --update-eval-baseline
```

契约类检查可单独跑：`python scripts/openapi_contract.py`（运行时 OpenAPI 对比 `contracts/openapi-baseline.json`）与 `python scripts/metrics_contract.py`（告警/SLO 引用的指标必须已注册）。

### 4.7 离线评测

```bash
pytest evals -m evaluation                                  # 确定性契约指标，无需 LLM
MMS_EVAL_JUDGE_MODEL=gpt-4o pytest evals -m evaluation       # 启用 LLM-as-a-Judge
MMS_EVAL_JUDGE_MODEL=gpt-4o python evals/calibration/run_calibration.py
python evals/benchmarks/download_data.py                     # 公开基准原始数据（M 压力集需 --all）
python evals/benchmarks/convert.py --longmemeval-s 1 --longmemeval-oracle 1 --locomo-qas 2
pytest evals/test_benchmarks.py -m public_benchmark          # 3500+ 次真实写入，约 6 分钟
```

被测模型与评判模型严格分离：target 由 `MMS_*` 决定，judge 由 `MMS_EVAL_JUDGE_MODEL` 指定；未配 judge 时确定性契约照常执行、阶段标记 WARN，`MMS_EVAL_REQUIRE_JUDGE=1` 可强制。只有 `review_status=approved` 的 Golden 才进门禁，结果落 `quality/reports/eval-results.json` 与 `eval-history.ndjson`（run_id、commit、provider/rule set 版本、judge、数据集版本、分数与延迟）。DeepEval 的服务级评测在 `tests/evaluation/`：

```bash
export MMS_TEST_DATABASE_URL=postgresql://mms:mms@127.0.0.1:5433/mms_deepeval
MMS_DATABASE_URL=$MMS_TEST_DATABASE_URL python scripts/init_db.py
DEEPEVAL_TELEMETRY_OPT_OUT=YES DEEPEVAL_CACHE_FOLDER=/tmp/mms-deepeval-cache \
  deepeval test run tests/evaluation --display all
```

性能与负载：`pytest tests/benchmarks -m benchmark`（基线 `quality/performance-baseline.json`，稳定 runner 上加 `--update-performance-baseline`，`--enforce-performance` 才会因 >50% 回归阻断）；`locust -f loadtests/locustfile.py --host http://127.0.0.1:8000`（需 `.[load]`，并先准备同意与记忆数据）。

### 4.8 观测栈、金丝雀与恢复

```bash
# 单独拉起观测栈
docker compose -p mms -f docker/docker-compose.yml up -d otel-collector tempo loki promtail prometheus grafana alertmanager
# Langfuse 全栈（web/worker/clickhouse/minio/redis/postgres）
docker compose -p mms -f docker/docker-compose.yml up -d langfuse-web
```

入口：Grafana http://localhost:3000（匿名 Admin，admin 密码 `mms-grafana`）、Prometheus 9090、Alertmanager 9093（本地 profile 丢弃通知，生产接真实渠道）、Tempo 3200、Loki 3100、Langfuse 3001（`admin@mms.local` / `mms-langfuse-admin`，项目密钥 `LANGFUSE_INIT_*` 固定为 `pk/sk-lf-mms-dev-*`，**生产必换**）。

```bash
python scripts/canary.py                 # 真实走一遍主链路并留下观测证据
python scripts/phase3_gate.py --help     # 由 SLO/Recall JSON 生成一年期门禁记录（角色/环境/判据/证据）
python scripts/deletion_replay.py --help # PITR 恢复后按 WORM 账本重放删除，覆盖备份窗口
python scripts/mutation_check.py         # rules/normalizer/hashing 的变异得分（趋势指标，不阻断）
```