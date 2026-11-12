# 14. Kafka消息的处理

## 14.1 Topic 规划（Kafka 可选）

| Topic | 用途 | 是否必需 |
|---|---|---|
| `mms.conversation.completed.v1` | 会话完成事件 | 可选（也可 HTTP 摄取） |
| `mms.business.fact.changed.v1` | 订单/工单/CRM 可信变更 | 可选 |
| `mms.ingestion.requested.v1` | 触发记忆处理 | 可选 |
| `mms.memory.lifecycle.v1` | 过期、替代、解决等事件 | 可选 |
| `mms.deletion.requested.v1` | 最高优先级删除事件 | 建议 |
| `customer.consent.changed.v1` | 同意/撤回授权变更 | 建议 |
| `customer.deletion.barrier.changed.v1` | 删除屏障变更，用于 barrier cache 失效 | 建议 |
| `mms.review.required.v1` | 待人工审核通知 | 可选 |
| `mms.dlq.*` | 死信，按业务域隔离；含 `mms.dlq.customer-ref-unknown.v1` | 建议 |

**Topic 级配置（与 8.3.3 的删除证明同源）**：所有携带 `customer_ref` 的 topic 必须 `cleanup.policy=compact,delete`、`retention.ms=86400000`、`delete.retention.ms=86400000`（tombstone 不得早于数据段过期）、`min.compaction.lag.ms=3600000`、`segment.ms` 不得大于保留期；删除/consent/barrier 三类 topic 的 consumer group 必须监控 `mms_kafka_consumer_lag_seconds`，lag 超过 `retention.ms` 的 50% 触发 `kafka_deletion_replay`（见 8.3.3/18.5）。

**v2 不设置 `mms.index.sync.requested.v1`。** Embedding 由 `memory_embedding_job`（DB 队列）驱动；Kafka 只用于跨服务事件/摄取。若未来需要独立 Embedding 服务，再引入对应 topic。

## 14.2 事件 envelope

```json
{
  "event_id": "evt_01J...",
  "event_type": "conversation.completed",
  "event_version": 1,
  "occurred_at": "2026-10-25T09:00:00Z",
  "customer_ref": "9f2c...",
  "source_system": "contact-center",
  "trace_id": "4bf92f...",
  "payload": {
    "conversation_id": "conv_501",
    "message_ids": ["msg_1", "msg_2"]
  }
}
```

- **Kafka 不承载 raw `customer_id`**：`customer_ref = lower(hex(hmac_sha256(kafka_ref_key_vN, customer_id)))`，由 `customer_identity` 维护映射；HMAC key 在 KMS 托管、MMS 与上游按版本共享；客户首次入库时 MMS 生成并 upsert 映射，上游通过 `GET/POST /internal/customers/{customer_id}/identity-ref` 获取；轮换时新增 `ref_key_version` 行，旧 ref 在验证窗口内仍可解析；
- payload 只放 ID/引用，不放完整对话/正文；避免 Kafka 保留正文导致删除不可单条清除；
- schema 版本化（JSON Schema/Avro），CI 做兼容性检查；
- partition key：`customer_ref`（保证同一客户顺序）；consumer 通过 `customer_identity` 解析 `customer_id`，未知 ref 进入隔离 topic，不猜测；
- topic 配置 `cleanup.policy=compact,delete`、`retention.ms≤86400000`；删除时发布 tombstone；详见 8.3.3。

## 14.3 可靠处理模式

1. 业务事件进入 Kafka/HTTP；
2. Consumer 在同一个 PG 事务中 `INSERT INTO memory_ingestion_job ... ON CONFLICT ...`；插入成功后再提交 Kafka offset。`memory_ingestion_job` 是 durable queue，不依赖内存或 Kafka 重放；
3. Worker 构建候选，Decision Engine 读取现状并计算 outcome；事务提交前再次校验 `barrier_generation` 与 consent version；
4. 在同一 PostgreSQL 事务内更新业务记忆、证据、决策（含 `context_snapshot`）、current、Embedding Job、Outbox，并把 `memory_inbox_event` 置 `COMPLETED`；
5. Outbox Publisher 发送领域事件到 Kafka（如启用）；
6. Embedding Worker 从 DB Job 队列按 `priority, available_at` 取任务，生成向量，写同一 PG；
7. 成功后更新 Job/向量状态；`RUNNING` 超租约回 `RETRY`；
8. 未知结果先回读对账，不盲目重放；
9. 超过重试上限进入 `DEAD_LETTER`，告警 + 人工重放入口。

### 14.3.1 事务性 Inbox/Job 协议（P0-4）

**结论：`memory_ingestion_job` 是 durable queue，`memory_inbox_event` 是“已应用事件账本”；两者不能各做一半。**

- Consumer 收到事件后，先用 `customer_ref` 查 `customer_identity` 解析 `customer_id`；未知 ref 进 `mms.dlq.customer-ref-unknown.v1`，不得写 job；然后在同一 PG 事务中插入 job，插入成功后再提交 Kafka offset：
  ```sql
  INSERT INTO memory_ingestion_job
    (id, customer_id, source_system, source_event_id, event_type, status, barrier_generation)
  VALUES (:id, :customer_id, :source_system, :source_event_id, :event_type, 'RECEIVED', :barrier_generation)
  ON CONFLICT (customer_id, source_system, source_event_id)
  DO UPDATE SET status = 'RECEIVED', available_at = now(), updated_at = now()
  WHERE memory_ingestion_job.status IN ('RETRY','DEAD_LETTER');
  ```
- 同一事务还要插入/更新 `memory_inbox_event(status='PROCESSING')`，写入 `topic/partition/offset`；Worker 在业务事务内把它更新为 `COMPLETED`。若 inbox 已是 `COMPLETED` 直接 ack；`PROCESSING` 未超租约则 ack，超租约允许重试。
- Worker 处理 Job 时：若 inbox 已 `COMPLETED` 直接 ack；否则在同一事务内写业务记忆/证据/决策/current/Embedding Job/Outbox，并 upsert inbox 为 `COMPLETED`；业务事务回滚时 inbox 不得标记完成，Job 继续 `RETRY`。
- `PROCESSING` 超过租约（`updated_at + lease`）允许重试；`FAILED` 保留错误码，交给人工/DLQ。
- Inbox 主键仍是 `(consumer_group,event_id)`；`processed_at` 只在 `COMPLETED` 时写入。
- 指标：`mms_inbox_duplicate_total`、`mms_inbox_stale_total`、`mms_ingestion_job_oldest_age_seconds`；告警：重复决策 >0、`PROCESSING` 超租约 >5min。

**实现复核（2026-10-28）**

- 双账本已按本节落地：`IngestionService.process_one` 在同一事务内写业务结果并把 inbox upsert 为 `COMPLETED`；job 表按 `status='RECEIVED'` 领取，`RUNNING` 写 `lease_expires_at`（`src/mms/services/ingestion.py`）；
- 队列信号由摄取进程导出，不依赖 API 进程：`_refresh_queue_signals` 每 5s（`ingestion.py:IngestionService._refresh_queue_signals`）刷新 `mms_inbox_stale_total` 与 `mms_ingestion_job_oldest_age_seconds`，陈旧阈值 `MMS_INBOX_STALE_SECONDS=360`（`src/mms/config.py:Settings.inbox_stale_seconds`）；
- 摄取 topic 白名单：`MMS_INGESTION_TOPICS` 默认 `["memory-candidates"]`，未登记的 topic 直接 `422 VALIDATION_FAILED`（`src/mms/api/main.py:ingest_event`、`config.py:Settings.ingestion_topics`）；
- 告警：`MMSInboxStale`（>0 持续 5m）、`MMSIngestionQueueAge`（>300s 持续 10m），见 `deploy/prometheus/alerts.yml:MMSInboxStale`。

## 14.4 幂等键

- 摄取：`customer_id + source_system + source_event_id`；Job 重放用 upsert，inbox 只有 `COMPLETED` 才跳过；
- 候选指纹：`SHA-256(canonical_json({source_system, source_event_id, stable_candidate_ref, evidence_hash}))`；`stable_candidate_ref` 是源消息/工单证据 ID 加稳定 span/ordinal，输入中**不含** `object_hash`、`semantic_hash` 或 `normalization_version`，避免重标准化或抽取规则升级改变同一源事件的幂等身份；
- 业务操作：`memory_id + operation_type + target_version`；
- Embedding Job：`(memory_id, embedding_model, embedding_version, view_kind, view_language, operation)` 的活动状态部分唯一键；文本变化就地改写活动 Job（不并存两个），历史 `READY` 不阻塞同内容重嵌入；同一记忆的不同语言视图各自独立幂等；
- 删除请求：`customer_id + request_idempotency_key`；
- 审核决策：`review_id + decision_version`。

**不要假设 embedding provider 支持幂等键。** 超时/未知结果按 `memory_id + model + version + view_kind + view_language + view_text_hash` 回读确认，再决定是否重试。

**实现复核（2026-10-28）**

- `stable_candidate_ref` 的实现见 `src/mms/services/normalizer.py:stable_candidate_ref`：固定包含 `fact_key/entity_type/entity_ref/value_slot`，并在 `source_refs` 提供时附加 `evidence_locators`（`ref_type/ref_id/message_id/ticket_id/span/ordinal/index` 的稳定子集，按 canonical JSON 排序）；
- 指纹输入实测为 `{source_system, source_event_id, stable_candidate_ref}`（`normalizer.py:normalize_candidate（指纹）`）。**与本节公式的差异**：`evidence_hash` 不再单独参与，证据身份由 `stable_candidate_ref.evidence_locators` 表达——对摘录正文做 hash 会让“重抽取同一证据”改变幂等身份；该收编记入 ADR-014；
- `content_hash = sha256_text(candidate.content)` 仍写入候选，但只用于视图/embedding 失效判定，不参与指纹（`normalizer.py:normalize_candidate（content_hash）`）；
- fact slot 之所以留在 ref 里：上游并非都提供 span/ordinal，去掉 slot 会把同一条源消息里的两个不同事实合并成一个候选并被静默丢弃（`normalizer.py:_LOCATOR_KEYS` 与 `tests/test_hashing_normalizer.py` 的指纹回归）。

## 14.5 删除优先级与旧消息防回写

- 删除操作最高优先级；
- 每条消息/Job 携带 `barrier_generation`；
- `DELETE_PENDING/DELETED` 墓碑优先于旧写入；
- 删除后到达的旧事件拒绝执行或只补做清理；
- DLQ 重放也执行同样的状态校验，禁止跳过治理逻辑直接写向量；
- 删除期间 Embedding Worker 写前检查 barrier/状态/generation，失败则 `CANCELLED`。

## 14.6 故障处置表

| 故障 | 业务库状态 | 向量处置 | 恢复方式 |
|---|---|---|---|
| Embedding provider 超时 | 记忆已提交，Job `PENDING/RETRY` | 不写/回读确认 | 指数退避、上限、DLQ；检索走精确/词法 |
| PostgreSQL 不可用 | 不提交决策 | 不生成向量 | Kafka 重试/暂停消费 |
| Kafka 不可用 | 本地事务写 Outbox/Job | 不阻塞 DB 队列 | 恢复 Publisher 后续发 |
| 向量写入成功但回执丢失 | 记忆有效，Job 状态待确认 | 按 hash/ID 回读 | 对账确认后补状态 |
| 删除时 provider 不可用 | 立即 `DELETE_PENDING` + 向量置 DELETED | 不依赖 provider | 告警 + 删除队列重试 |
| 重复事件/并发更新 | 幂等命中或版本冲突 | 不应产生重复向量 | 事务重读与重新决策 |
| 模型持续输出非法格式 | 不落业务记忆 | 不写向量 | 有界重试、DLQ、模型告警 |
| 向量分区/B-tree 不可用或 exact 查询超时 | 业务记录有效 | 检索降级/重试 | 检查分区裁剪、B-tree、连接池与慢查询；必要时重建索引 |

## 14.7 重试、退避、DLQ

- 最大尝试次数：摄取/决策 5 次；Embedding 8 次；删除无限重试但告警（受合规 SLA）；
- 退避：指数 + jitter，起点 1s，上限 5min；
- `next_retry_at`/`available_at`；Worker 用 `FOR UPDATE SKIP LOCKED`；
- Embedding Job 按 `priority, available_at` 领取；普通 UPSERT=100，删除/纠正=10，REBUILD/回填=500；`RUNNING` 写 `lease_expires_at`，超租约回 `RETRY`（P0-6）；
- 毒消息隔离：连续失败进入 `DEAD_LETTER`，不能阻塞分区；
- DLQ 重放前校验业务状态/barrier；重放动作记录审计。

**实现复核（2026-10-28）**

- 优先级实际常量：`PRIORITY_CORRECTION=10`、`PRIORITY_DEFAULT=100`、`PRIORITY_TRANSLATED=120`、`PRIORITY_BACKFILL=500`（`src/mms/services/repository.py:PRIORITY_CORRECTION`）；用户显式纠正的候选入队即 10（`src/mms/services/service.py:MemoryService._persist_decision（纠错优先级）`），翻译视图 120、回填补建 500（`repository.py:MemoryRepository.enqueue_missing_embeddings`）；
- 退避实现：`jittered()` 在 `[0.5×base, base]` 上随机（`src/mms/utils/backoff.py`），单次上限 300s；outbox 的 base 是 `2^(attempts-1)`（`src/mms/services/outbox.py:OutboxPublisher.process_one`），embedding worker 是 `2^attempts`（`src/mms/services/worker.py:EmbeddingWorker._retry_or_dead`），因为领取时 attempts 已自增；
- Embedding 尝试上限 `MMS_EMBEDDING_MAX_ATTEMPTS=8`（`src/mms/config.py:Settings.embedding_max_attempts`）；`_retry_or_dead` 用 `attempts >= max` 判定 `DEAD_LETTER`，避免二次自增提前烧掉一次重试（`worker.py`）；
- Outbox 新增终态 `FAILED`：超过 `MMS_OUTBOX_MAX_ATTEMPTS=5`（`config.py:Settings.outbox_max_attempts`）不再回到 `PENDING`；发布成功但行已被并发删除时记 `mms_outbox_publish_total{result="row_gone"}` 并写告警日志，不当作成功（`outbox.py:OutboxPublisher.process_one`）。

## 14.8 对账与补偿

- **Job 对账**：PENDING/RETRY/RUNNING 超时、DEAD_LETTER；
- **向量对账**：ACTIVE/RESOLVED 无 READY、hash 不一致、状态不一致、孤儿；
- **召回对账**：每语言 canary，Recall@K；
- **业务对账**：ACTIVE 记忆必须有证据/决策/current/embedding（允许 PENDING 窗口）；
- 每次对账写 `recon_run`，有范围、发现数、修复数、错误、耗时；
- 对账任务限速、低峰运行；修复动作可重放/幂等。

**实现复核（2026-10-28）**

- `ReconciliationService.run(recon_type)` 由 mms-lifecycle 每 300s 依次跑 `JOB/VECTOR/RECALL/BUSINESS`，各写一行 `recon_run`（`src/mms/services/recon.py:ReconciliationService.run`）；RECALL 用短连接、在事务之外执行，避免 canary 的 provider 调用占住协调事务（`src/mms/services/recon.py:ReconciliationService.run`、`src/mms/services/recon.py:ReconciliationService._recall_recon`）；
- 召回对账按 `view_language` 分层抽样：每语言取 `max(1, recon_batch_size // 10)` 条，模型取 registry 当前 `ACTIVE` 版本（`recon.py:ReconciliationService._recall_recon`），避免“只抽主语言、翻译视图永远没核对”；
- 死信观测合并四类：embedding/translation/ingestion 的 `DEAD_LETTER` 与 outbox 的 `FAILED`（`recon.py:ReconciliationService._observe_dead_letters`），统一计入 `mms_recon_mismatches_total`；
- BUSINESS 新增四类差异：`memory_without_evidence`、`memory_without_decision`、`delete_pending_after_completed_barrier`（自动修复）、`delete_pending_without_barrier`（只观测：无屏障时无法判定该恢复还是该删，交人工）；
- 已知缺口：`delete_pending_without_barrier` 与 `memory_without_decision` 只告警不自动修。
