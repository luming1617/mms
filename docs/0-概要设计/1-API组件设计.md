# API（mms-api）组件设计

## 一、一句话认识 API

系统里所有"请求驱动、必须在线同步给出结果"的工作——检索记忆上下文、受理候选与摄取事件、接收反馈纠正、审核决策、受理删除请求、同步同意快照、管理面运维——都只经过 **API 进程**这一个在线入口。耗时且不能阻塞在线主流程的工作（文档向量、翻译、事件外发）不在请求里做完，而是在同一个 PostgreSQL 事务里落 Job/Outbox 记录，交给 Worker 异步执行；没有天然触发时机的收敛工作交给 Convergence。

---

## 二、为什么 API 只做"门面"

这是"去掉中间件、一个库一个事务边界"架构决策（见[《0-总体架构》](0-总体架构.md)）在在线侧的延续：

1. **在线预算必须闭合**：默认档 P95 ≤300ms，预算求和 `20 + 50 + max(96,50) + 30 = 196ms`（[《12-记忆检索》](../2-详细设计/12-记忆检索.md) 12.15）。提取、文档向量、翻译、事件外发任何一个放进请求都会击穿预算——它们全部异步化，热路径只保留"查询向量 + 召回 + 可选 rerank"。
2. **一个事务边界**：API 的同步写端点与 Worker 的异步消费**共用同一候选决策实现（同一写入主链）**——业务数据、Job、Outbox 同事务提交，在线/离线两条写路径行为一致，不会漂移出两套决策逻辑。
3. **无状态才能水平扩容**：配额、屏障、同意、业务状态全部在 PG；进程内只有可重建缓存与每副本独立的限流桶，任意副本可杀可加。
4. **合规闸必须前置在入口**：customer_id 只由网关注入；同意/屏障/审计在读写发生**之前**判定，fail-closed。入口集中，闸才集中。

一句话：**API 把"在线必须做的事"做薄，把"可以稍后做的事"全部落到同一个事务里的队列上**。

---

## 三、部署形态：无状态多副本

### 3.1 进程与账号

- 与 Worker/Convergence 同镜像、不同命令启动；非 root 运行（uid 65532），对外监听 8000 端口（业务与指标同端口）。
- 业务连接（`MMS_DATABASE_URL`）使用 `mms_app` 账号：业务表/视图表/Job 表/Outbox 可写，但**向量表 `memory_embedding` 的 INSERT/UPDATE 权限被显式回收**——API 永远算不了向量，只能登记 Job 等 Worker 写。
- 审计擦除连接（`MMS_SCRUBBER_DATABASE_URL`）使用独立的 `mms_audit_scrubber` 账号：审核批准删除时的审计擦除走独立连接（见[《15-安全与审计》](../2-详细设计/15-安全与审计.md)），不复用业务连接。
- 决策/审计表 append-only：`mms_app` 对 `memory_decision`/`admin_access_audit` 无 UPDATE/DELETE（DB 层权限回收 + 触发器二次拦截）。

### 3.2 进程内状态（全部可重建或副本独立）

| 状态 | 位置 | 多副本语义 |
|---|---|---|
| 查询向量缓存 | 进程内 | 每副本独立；key=`sha256(model‖version‖language‖NFKC(query))`，**只存向量不存原文、key 无客户标识**；TTL 3600s / 4096 条 |
| 屏障缓存 | 进程内 | 每副本独立，TTL 5s；`pg_notify` + 每副本 LISTEN 连接即时失效（5s 可见性 SLO） |
| 限流令牌桶 | 进程内 | 每副本独立预算；共享预算由网关卡或 DB 配额承担（见第六章） |
| 同意快照 | **无缓存** | 每次直查主库；fail-closed，无陈旧窗口 |
| 配额计数 | `customer_quota_usage` 表 | **DB 共享**，所有副本同一预算 |

### 3.3 连接池与读路由

- 主池 min 1 / max 8；副本池 min 0 / max 4 / 2s 连接超时（读副本开关默认关）。
- 读路由：近"写后主库保持窗"（默认 5s）内有写入的客户走主库（写后读一致）；副本异常回退主库并标 `replica_unavailable` 降级。
- **同意与屏障判定永远走主库**——副本只承载召回 SQL；副本上语义/词法强制串行同池（副本只有一条连接，两通道并行会产生新鲜度不一致，见[《12-记忆检索》](../2-详细设计/12-记忆检索.md) 12.15）。

### 3.4 启动与关闭

启动：初始化 tracing/Langfuse → Web 框架埋点（排除 health/metrics）→ 运行时监控（事件循环滞后/连接池饱和）→ 屏障 LISTEN 线程。关闭：反序停止并 flush 待上报批次。Web 服务器不自装日志配置，统一 JSON 日志；access log 关闭去噪。

---

## 四、请求处理共性骨架

所有端点共享同一套"**中间件闸 → 认证面 → 业务闸 → 服务实现 → 统一错误模型**"模式。认证见第五章，限流与配额见第六章。

### 4.1 中间件链

```
请求 → RateLimit（429+Retry-After）→ MaxBodySize（256KB，413）
     → RequestContext（X-Request-ID 回显/派生）→ HttpMetrics（黄金信号）→ 路由
```

- 体积闸在 body 解析**之前**按 Content-Length 拒绝；
- 被外层中间件拒绝的请求不进入路由（也不产生路由级指标）。

### 4.2 错误模型与稳定错误码

- RFC 9457 `application/problem+json`：`type/title/status/detail/code/trace_id`；
- 4xx 是业务结果（WARNING 日志），5xx 才是系统故障（ERROR）；
- `trace_id` 取当前 span，缺省回退 X-Request-ID。

稳定错误码枚举（完整契约见[《13-接口设计》](../4-接口与契约/13-接口设计.md) 13.6）：

| 错误码 | HTTP | 含义 |
|---|---|---|
| `AUTH_FAILED` | 401 | 凭证缺失或无效 |
| `CUSTOMER_MISMATCH` | 403 | body 携带的 customer_id 与网关身份不一致 |
| `ADMIN_REASON_REQUIRED` | 403 | 管理操作缺少理由码/工单号 |
| `CONSENT_REQUIRED` / `CONSENT_WITHDRAWN` | 403 | 同意缺失 / 已撤回（写路径） |
| `VALIDATION_FAILED` | 422 | 参数校验失败 |
| `RATE_LIMITED` | 429 | 限流拒绝（携带 Retry-After） |
| `QUOTA_EXCEEDED` | 429 | 配额超限 |
| `DELETE_IN_PROGRESS` | 409 | 删除屏障进行中 |
| `STALE_GENERATION` | 409 | 屏障代际过期（cutover 后旧写入） |
| `CONFLICT_PENDING` | 409 | 冲突未决 |
| `CUSTOMER_DELETED` | 410 | 客户已删除 |
| `NOT_FOUND` / `CUSTOMER_REF_UNKNOWN` | 404 | 资源不存在 / 假名未预置 |
| `CONSENT_UNAVAILABLE` | 503 | 同意快照不可用（fail-closed） |
| `AUDIT_WRITE_FAILED` | 503 | 审计写失败，拒绝操作 |
| `BARRIER_CHECK_FAILED` | 503 | 屏障校验异常，事务回滚 |
| `EMBEDDING_UNAVAILABLE` | 503 | 查询向量化不可用 |
| `INTERNAL` | 500 | 内部错误 |

> 读路径的同意拒绝不走错误码：返回空结果 + consent 标志，不泄露记忆是否存在（见第八章）。

---

## 五、接口鉴权

### 5.1 调用方分类

MMS 的业务调用方分两大类——**前台用户**与**后台运营**（客服坐席 + 管理维护），二者是两个独立部署的上游服务，经不同认证面接入；另有服务间内部调用与基础设施调用：

| 调用方 | 是谁 | 认证面 |
|---|---|---|
| 前台用户 | 用户前台（客服 Agent / 回答服务），代表终端用户发起检索、写入、反馈、删除 | 客户面 |
| 后台运营 · 客服坐席 | 客服工作台，查看被授权客户的记忆摘要 | 坐席面 |
| 后台运营 · 管理维护 | 运营后台（审核台 / 运营工作台）：审核、政策、重建、回放、审计读取、运维触发 | 管理面（运维触发端点走内部面） |
| 内部服务 | 上游同意权威系统、身份服务、事件投递方（权威业务系统/事件网关） | 内部面 |
| 基础设施 | 容器探针、监控采集 | 内网直连，无业务凭证 |

### 5.2 四个认证面（凭证不混用）

| 面 | 凭证 | 身份来源 | 服务的调用方与能力 |
|---|---|---|---|
| 客户面 | `X-Service-Token` + 网关注入 `X-Customer-ID` | 网关 JWT/身份服务映射 | 前台用户：检索/候选/提取/反馈/用量/删除 |
| 坐席面 | `X-Service-Token` + `X-Agent-ID` | `agent_customer_grant` 授权表交叉校验 | 后台运营 · 客服坐席：客户记忆摘要列表 |
| 管理面 | `X-Admin-Token` + `X-Reviewer-ID` + `X-Admin-Reason-Code` + `X-Admin-Ticket` + `X-Admin-Role` | 角色白名单（见 5.4） | 后台运营 · 管理维护：审核/政策/事实键/重建/lineage/决策回放/AUDIT 读取 |
| 内部面 | `X-Service-Token` | 服务身份 | 内部服务：摄取/同意同步/identity-ref；运维触发：手动对账/视图成本/容量 |

### 5.3 鉴权与授权规则

- **token 物理分离**：管理面 admin token 与客户面 service token 物理分离、不混用；政策变更再要求角色 ∈ 政策管理角色集（见 5.4）。
- **customer_id 只由网关注入**：body 重复携带 customer_id 且与网关身份不一致 → 403 `CUSTOMER_MISMATCH`；path 里的 customer_id 只是"请求目标"，不是授权凭据。
- **坐席授权交叉校验**：坐席读取客户摘要前校验坐席-客户授权关系，granted/denied 都先写审计；审计写失败 503 拒读。
- **管理操作留痕**：管理面访问客户数据必须携带理由码 + 工单号；`AUDIT`/`as_of` 读取必须管理面凭证，**读前同事务写审计，写失败 503 拒读**（fail-closed）。
- **DLQ 重放双通道**：内部面或管理面凭证均可，均同事务审计。
- **审核域走管理面**：审核台属运营后台，审核队列与审核决策使用管理面凭证（审核人身份 + 4-eyes）。

### 5.4 管理角色枚举

| 角色 | 定位 | 允许的操作 |
|---|---|---|
| `ADMIN` | 超级管理员 | 全部管理操作，含政策变更 |
| `POLICY` | 政策管理员 | 政策创建/激活/回滚 |
| `PRIVACY` | 隐私管理员 | 删除审核、同意与审计相关操作 |
| `OPERATOR` | 运维人员 | 重建/重放/对账等运维操作 |
| `READONLY` | 只读人员 | 管理域只读访问 |

- 管理面白名单默认：`ADMIN,POLICY,PRIVACY,OPERATOR,READONLY`；
- 政策变更角色集默认：`ADMIN,POLICY`（白名单的子集，启动期校验）。

---

## 六、接口限流与配额

### 6.1 两层预算闸的分工

| 闸 | 位置 | 预算范围 | 防什么 | 拒绝行为 |
|---|---|---|---|---|
| 限流 | 进程内令牌桶 | 每副本独立、按客户 | 瞬时突发打垮进程 | 429 `RATE_LIMITED` + Retry-After |
| 配额 | DB 计数表 | 所有副本共享、按客户 | 日级/存量业务预算超支 | 429 `QUOTA_EXCEEDED` |

进程桶只是最后防线；跨副本的共享速率预算由网关承担，共享业务预算由 DB 配额承担。

### 6.2 限流（进程内令牌桶）

- 按 `X-Customer-ID` 建桶（缺省回退 client IP）；默认 50 请求/秒、突发 100；
- **每副本独立预算**：总限流预算 = 单副本预算 × 副本数，扩容会放大总预算，容量规划必须计入；
- 跳过 `/health`、`/metrics`、`/internal`（内部面是服务间受控调用，预算由调用方与配额管理）；
- 在中间件层拒绝，不进入路由、不产生路由级指标。

### 6.3 配额（DB 共享预算）

配额桶枚举（计数落 `customer_quota_usage`，所有副本同一预算）：

| 桶 | 维度 | 默认上限 | 扣减/校验时机 |
|---|---|---|---|
| `WRITE` | 每客户每日 | 2000 | 写入决策时计数 |
| `EXTRACTION` | 每客户每日 | 2000 | 提取受理**先扣再调提取器** |
| `EMBEDDING` | 每客户每日 | 20000 | 决策事务内复查 |
| `ACTIVE_MEMORIES` | 存量记忆条数 | 5000 | 决策事务内复查 |

- **原子自增后判定**，超限 429 `QUOTA_EXCEEDED`；
- 提取端点先扣 `EXTRACTION` 再调提取器——配额耗尽的客户不能继续烧 provider；
- 存量按生命周期状态 `ACTIVE`/`CONFLICTED`/`RESOLVED` 计数；
- DB 共享意味着扩容不漂移预算，与限流的副本独立语义正好互补。

---

## 七、能力全景（接口矩阵）

各表"调用方"列按 5.1 分类；字段级完整契约见[《13-接口设计》](../4-接口与契约/13-接口设计.md)。

### 7.1 检索域（在线读）

检索用途（purpose）枚举：

| purpose | 含义 | `as_of` | 说明 |
|---|---|---|---|
| `CURRENT_ANSWER` | 当前回答（默认） | 不允许 | 事实通道只取 ACTIVE，CONFLICTED 走独立待核实通道 |
| `CUSTOMER_SELF_SERVICE` | 客户自助 | 不允许 | 仅 policy 白名单类型，中/高敏感脱敏 |
| `HISTORY_DEBUG` | 历史调试 | 允许 | 可召回 RESOLVED/SUPERSEDED/EXPIRED 等历史状态 |
| `AUDIT` | 审计 | 允许 | 必须管理面凭证，读前审计 |

> `CANDIDATE_DISCOVERY` 是决策引擎内部用途，不接受外部传入。purpose 由服务端强制映射状态集，调用方不能自选状态集。

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `POST /api/v1/memories/query` | 前台用户 | 语义+词法+精确过滤检索 | 客户面；purpose 服务端映射；`as_of` 仅 HISTORY_DEBUG/AUDIT |
| `POST /api/v1/agent/memory-context` | 前台用户（客服 Agent） | Agent 注入式上下文 | 客户面；扁平 `memory_context` + `instruction_boundary` + `trust=untrusted_data`；原检索信封下移 `retrieval` |
| `GET /api/v1/customers/{id}/memories` | 后台运营 · 客服坐席 | 坐席查看客户记忆摘要 | 坐席面；grant 交叉校验 + 审计前置；cursor 分页；脱敏摘要（160 字符） |

### 7.2 写入域（同步决策 / 异步摄取）

**设计口径：两类摄取统一入队、Ingestion Worker 统一消费，两条路径仅差提取方**——路径 A 由本系统提取会话原文，路径 B 由投递方提前提取候选。受理只做"过闸 + 落 Job"，202 + job_id 即返回；提取与决策由 Worker 异步完成，与同步写端点共用同一写入主链。`candidates`/`feedback`/`review` 三个用户在场的入口保留同步决策：即时反馈、审核批准与业务写入同事务原子；摄取队列 FIFO 无纠正优先级，纠正入队会排在批量摄取之后。详见[《Ingestion Job 设计》](../1-核心流程/1-Ingestion%20Job设计.md)第二十二节。

事件类型（event_type）枚举：

| event_type | 含义 | 提取方 |
|---|---|---|
| `memory.candidate.v1` | 候选事件（路径 B） | 投递方（上游已完成提取） |
| `memory.extraction.requested.v1` | 提取请求（路径 A） | 本系统（Worker 侧规则/LLM 提取器） |

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `POST /api/v1/memories/candidates` | 前台用户 | 单候选同步决策 | 客户面；与 Worker 共用同一写入主链；202 返回决策结果 |
| `POST /api/v1/extractions` | 前台用户 | 会话提取受理（异步） | 客户面；受理落 Job，202 + job_id；先扣 EXTRACTION 配额；Worker 取数→提取→逐候选决策 |
| `POST /internal/ingestion/events` | 内部服务（上游业务系统/事件网关） | 候选事件异步摄取 | 内部面；topic 白名单；Inbox+Job 双账本同事务；202 + job_id |
| `GET /internal/ingestion/jobs/{id}` | 前台用户 / 后台运营 · 管理维护 | 摄取任务状态查询 | 内部面；状态/attempts/last_error_code |

关键设计点：

- **会话级 Job**：路径 A 一行 Job 对应一次会话提取（一事件一 Job 的唯一键容纳不下 N 个候选）；候选级幂等由候选指纹 `(customer_id, source_system, source_event_id, candidate_fingerprint)` 承担，单候选失败不影响同批其他候选。路径 B 维持一事件一候选一 Job。
- **只传引用取数**：事件只携带会话/消息引用，Worker 以对会话存储的服务身份取数并校验归属；归属不符按安全事件处理，不提取。
- **失败分账**：提取失败属运行失败（可重试），候选拒绝属业务终态（不重试）。
- **Job 状态枚举**：`PENDING` / `RUNNING` / `READY` / `RETRY` / `DEAD_LETTER` / `CANCELLED`。
- **topic 白名单**：默认 `memory-candidates`（可配置）。

### 7.3 反馈与用量域（客户面）

反馈类型（feedback_type）枚举：

| feedback_type | 含义 | 约束 |
|---|---|---|
| `INCORRECT` | 内容错误 | 必须携带 correction，纠正走决策引擎 |
| `OUTDATED` | 内容过时 | — |
| `IRRELEVANT` | 不相关 | — |
| `HELPFUL` | 有帮助 | 不自动提升可信度 |
| `PRIVACY` | 隐私问题 | 转隐私处理流程 |
| `OTHER` | 其他 | — |

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `POST /api/v1/memories/{id}/feedback` | 前台用户 | 反馈/纠正 | Idempotency-Key 必需；INCORRECT 必须带 correction；纠正走决策引擎 |
| `POST /api/v1/memory-usage` | 前台用户 | 回报实际采用的记忆 | request_id 关联检索 trace；answer_hash 校验 |

### 7.4 审核域（管理面）

审核决策（decision）枚举：`APPROVE`（批准，立即执行写入或删除）/ `REJECT`（驳回，终态）。

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `GET /api/v1/review-queue` | 后台运营 · 管理维护（审核台） | 待审列表 | MEMORY_READ 同意闸（拒绝降级为空队列）；cursor 分页 |
| `POST /api/v1/review-queue/{id}/decision` | 后台运营 · 管理维护（审核人） | 审核决策 | `decision_version` 乐观锁；4-eyes（批准人≠提交人）；批准即执行（与业务写入同事务） |

### 7.5 删除域（客户面）

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `POST /api/v1/deletion-requests` | 前台用户（终端用户经前台发起） | 请求全量删除 | Idempotency-Key 必需；scope 仅 `{all:true}`；建屏障+审核任务 |
| `GET /api/v1/deletion-requests/{id}` | 前台用户 | 删除状态/证明 | `proof` 字段随状态返回 |

删除请求状态枚举：`RECEIVED` → `APPROVED` → `IN_PROGRESS` → `VERIFYING` → `COMPLETED`；分支终态 `REJECTED` / `CANCELLED`。

删除屏障状态枚举：`ACTIVE`（删除进行中）/ `COOLING_DOWN`（冷却收尾）/ `COMPLETED`（完成）/ `CANCELLED`（重新授权撤销）。

删除审核批准由后台运营 · 管理维护执行（见 7.4）；冷却收尾与墓碑清理归 Convergence（见第十一章）；撤回触发的删除同走此链路（见[《03-记忆授权与撤回》](../2-详细设计/03-记忆授权与撤回.md) 3.4）。

### 7.6 同意与身份域（内部面）

同意用途（purpose）枚举：

| purpose | 含义 |
|---|---|
| `MEMORY_WRITE` | 允许写入记忆 |
| `MEMORY_READ` | 允许读取/召回记忆 |
| `MEMORY_RETENTION` | 允许留存记忆 |
| `SENSITIVE_MEMORY` | 允许处理中/高敏感记忆 |
| `CROSS_BORDER_EMBEDDING` | 允许跨境向量化通道 |
| `CROSS_BORDER_TRANSLATION` | 允许跨境翻译通道 |
| `CROSS_BORDER_RERANK` | 允许跨境重排通道 |

同意状态枚举：`GRANTED`（已授权）/ `DENIED`（拒绝）/ `WITHDRAWN`（撤回）。

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `GET/POST /internal/customers/{id}/memory-consent` | 内部服务（同意权威系统） | 同意快照读取/同步 | 版本单调；GRANTED 翻屏障；WITHDRAWN 视同删除请求（见[《03-记忆授权与撤回》](../2-详细设计/03-记忆授权与撤回.md) 3.4） |
| `GET/POST /internal/customers/{id}/identity-ref` | 内部服务（身份服务/事件投递方） | `customer_ref` 读取/预置 | Kafka 假名（HMAC），幂等 |

### 7.7 管理域（管理面 + 同事务审计）

政策操作（action）枚举：`CREATE`（创建草稿）/ `ACTIVATE`（激活）/ `ROLLBACK`（回滚）。

死信队列类型（job_type）枚举：`INGESTION` / `OUTBOX` / `EMBEDDING` / `TRANSLATION`。

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `GET/POST /api/v1/admin/policies` | 后台运营 · 管理维护 | 政策列表/创建/激活/回滚 | 4-eyes（批准人≠创建人）；单 ACTIVE 约束 |
| `GET /api/v1/admin/fact-keys` | 后台运营 · 管理维护 | 事实键注册表 | 只读 |
| `POST /api/v1/admin/embeddings/rebuild` | 后台运营 · 管理维护 | 向量重建/回填 | 必须限定客户或记忆；**只登记 Job，Worker 执行** |
| `POST /api/v1/admin/decisions/replay` | 后台运营 · 管理维护 | 决策回放 | 钉住 policy/registry 版本重放规则矩阵 |
| `GET /api/v1/admin/memories/{id}/lineage` | 后台运营 · 管理维护 | 证据→决策→版本→向量→使用 | 读前审计 |
| `POST /api/v1/admin/dlq/replay` | 后台运营 · 管理维护 | 四类队列死信统一重放 | 审计同事务，写失败重放不生效 |

### 7.8 内部运维域（内部面）

对账类型（recon_type）枚举：

| recon_type | 含义 |
|---|---|
| `JOB` | 任务对账：超租约回收、死信回队 |
| `VECTOR` | 向量对账：缺向量回填、视图修复 |
| `BUSINESS` | 业务状态对账：状态与投影一致性 |
| `RECALL` | 召回体检：canary 采样复用检索 |

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `POST /internal/reconciliation/{type}` | 后台运营 · 管理维护（运维工作台） | 手动触发四类对账 | 执行体与 Convergence 同一实现 |
| `POST /internal/view-cost/run` | 后台运营 · 管理维护（运维工作台） | 手动触发视图成本回收 | 同上 |
| `GET /internal/customers/{id}/retrieval-capacity` | 后台运营 · 管理维护（运维工作台） | 单客户检索容量报告 | 只读 |

### 7.9 健康与指标

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `GET /health/live` | 基础设施（容器探针） | 进程存活 | 无依赖 |
| `GET /health/ready` | 基础设施（容器探针） | 硬依赖：主库可写 + pgvector + schema 初始化 | 任一缺失 503，Pod 摘流量 |
| `GET /health/deps` | 基础设施（运维诊断） | 软依赖：embedding/translation provider、DB、副本 | 不参与 readiness，防 provider 故障摘全站 |
| `GET /metrics` | 基础设施（监控采集） | Prometheus 指标 | 与业务同端口 8000（Worker 用独立 metrics 端口） |

---

## 八、安全与合规闸（fail-closed 清单）

| 闸 | 判定位置 | 失败/拒绝行为 |
|---|---|---|
| 客户身份 | 网关注入 `X-Customer-ID`；body 不一致即拒 | 401 `AUTH_FAILED` / 403 `CUSTOMER_MISMATCH` |
| 坐席授权 | `agent_customer_grant` 交叉校验，granted/denied 都先审计 | 403；审计写失败 503 拒读 |
| 管理面 | token + 角色白名单 + reason + ticket | 401/403；AUDIT 读前审计失败 503 |
| 屏障（读） | 主库快照 + 5s 缓存 + NOTIFY 失效 | `DELETE_IN_PROGRESS` 409 / `CUSTOMER_DELETED` 410 |
| 屏障（写） | advisory lock + 主库快照 + 代际/cutover 校验 | 拒绝或 `STALE_GENERATION`；校验异常回滚（`BARRIER_CHECK_FAILED` 503） |
| 同意（读） | `MEMORY_READ` 主库直查 | 空结果 + consent 标志，不泄露记忆是否存在 |
| 同意（写） | `MEMORY_WRITE`/`SENSITIVE_MEMORY` FOR UPDATE | 终态 REJECTED 决策，不进 DLQ |
| 跨境通道 | 注册表 provider 判定 + `CROSS_BORDER_EMBEDDING/RERANK` 同意 | 关闭对应通道（政策），不算降级 |
| 管理审计 | `admin_access_audit` 与业务变更同事务 | 写失败 503，操作不生效 |
| 配额 | DB 原子计数，先扣后判 | 429 `QUOTA_EXCEEDED` |
| 注入边界 | `instruction_boundary` 服务端常量，不可覆盖 | `trust=untrusted_data`，记忆只作证据不作指令 |

---

## 九、关键配置参数（默认值）

配置统一经环境变量注入（前缀 `MMS_`）；启动期交叉校验 fail-fast（如屏障缓存 TTL 不得超过 5s 可见性 SLO）：

| 参数 | 值 | 说明 |
|---|---|---|
| `bind_host` / `bind_port` | 0.0.0.0 / 8000 | 监听地址 |
| `max_request_bytes` | 262144（256KB） | 请求体上限，解析前拒绝 |
| `rate_limit_enabled` / `rate_limit_requests_per_second` / `rate_limit_burst` | true / 50 / 100 | 进程内令牌桶（每客户，见第六章） |
| `quota_write_per_day` / `quota_extraction_per_day` | 2000 / 2000 | 每客户每日写入/提取配额（DB 共享） |
| `quota_embedding_per_day` / `quota_active_memories` | 20000 / 5000 | 向量预算 / 存量记忆上限 |
| `query_cache_ttl_seconds` / `query_cache_max_entries` | 3600 / 4096 | 查询向量缓存（进程内） |
| `barrier_cache_ttl_seconds` / `barrier_cache_max_entries` | 5 / 4096 | 屏障缓存；TTL 超 5s 拒绝启动 |
| `read_replica_enabled` / `read_replica_write_hold_seconds` | false / 5 | 读副本开关与写后读主库保持窗 |
| `retrieval_parallel_lexical` | true | 语义/词法并行（副本上强制串行） |
| `reranker_enabled` / `rerank_enabled_purposes` / `rerank_top_n` / `reranker_timeout_ms` | false / CURRENT_ANSWER,HISTORY_DEBUG / 20 / 80 | rerank 开关与预算 |
| `reranker_provider` | external | 非 self-hosted 需 `CROSS_BORDER_RERANK` 同意 |
| `lexical_enabled_languages` / `lexical_min_query_chars` / `lexical_trgm_threshold` | zh-CN,en-US / 3 / 0.3 | 词法通道 |
| `embedding_timeout_ms` | 10000 | 查询向量 provider 超时 |
| `admin_roles` / `policy_admin_roles` | ADMIN,POLICY,PRIVACY,OPERATOR,READONLY / ADMIN,POLICY | 管理面角色白名单（见 5.4） |
| `ingestion_topics` | memory-candidates | 摄取 topic 白名单 |
| `authority_sync_mode` | async | 同意权威同步模式：异步快照同步 |

---

## 十、可观测性总览

**黄金信号**（按路由模板统计而非具体 id，禁止客户标签）：

| 指标 | 口径 |
|---|---|
| `mms_api_requests_total{method,route,purpose,code}` | `purpose=query` 仅覆盖两个查询路由 |
| `mms_api_latency_seconds{route,path}` | `path=default/extended` 分档；admin 路由恒 extended |
| `mms_http_in_flight` | 在途请求 |

**检索质量**：`mms_retrieval_degraded_total{reason}`（**每请求一次**，SLI 分子是降级请求数不是原因数）、`mms_retrieval_zero_results_total`、`mms_retrieval_candidates`、`mms_retrieval_view_hits_total{view_kind,language}`、`mms_lexical_skipped_total{reason}`、`mms_rerank_total{result}`。

**安全与合规**：`mms_cross_customer_hits_total`（P0 page）、`mms_consent_check_total{result}`、`mms_consent_version_stale_total`、`mms_barrier_cache_age_seconds`、`mms_quota_rejected_total{bucket}`、`mms_admin_audit_write_failures_total`。

**运行时**：`mms_event_loop_lag_seconds`、`mms_db_pool_connections{pool,state}`。

**SLO 与告警**：

| SLI | 目标 | 告警 |
|---|---|---|
| 查询可用性 | 99.9%/30d | `MMSAvailabilitySLOBreach` |
| 默认档延迟 | P95 ≤300ms / P99 ≤800ms | `MMSDefaultLatencySLOBreach` |
| 扩展档延迟 | P95 ≤500ms / P99 ≤1500ms | `MMSExtendedLatencySLOBreach` |
| 降级率 | <1%（告警 >2%） | `MMSDegradedRatioHigh` |
| 跨客户命中 | 0 | `MMSCrossCustomerHit`（page） |
| 配额 429 激增 | 5m ≤20 | `MMSQuotaRejections` |
| 同意快照缺失/版本落后 | 0 | `MMSConsentCheckUnavailable` / `MMSConsentVersionStale` |
| 审计写失败 | 0 | `MMSAuditGuardFailure`（page） |

**Trace/日志**：OTel Web 框架埋点 + Langfuse `retrieval_observation`（检索全链路）+ `X-Request-ID` 回显；业务 trace_id 同时是 Langfuse/OTel trace id，供后续 feedback 精确打分；日志只落 `customer_hash` 不落正文。

---

## 十一、API 与 Worker/Convergence 的边界

Worker 与 Convergence 的完整设计见[《2-Worker组件设计》](2-Worker组件设计.md)与[《3-Convergence组件设计》](3-Convergence组件设计.md)。

| 维度 | API（本文） | Worker | Convergence |
|---|---|---|---|
| 实例 | 无状态多副本 | 多实例并行（`SKIP LOCKED`） | 单实例（advisory lock 主备） |
| 触发 | 请求驱动 | 任务驱动（轮询队列） | 定时轮询全库 |
| DB 账号 | `mms_app`（向量表只读） | `mms_worker` | `mms_worker`（+deletion 提权） |
| 是否调外部 provider | 是：查询向量/rerank（受同意闸） | 是：文档向量/翻译/事件外发 | 基本不调（canary 例外） |
| 典型产物 | 在线响应、决策事务、Job/Outbox 登记 | READY 向量、译文视图、已发布事件 | 状态推进、回队、删除补偿 |
| 关系 | **把请求变成事务与任务** | **把任务消费掉** | **把卡住的任务放回队列 / 作废任务** |

衔接点：

- **同一事务边界**：API 决策事务内登记 embedding/translation Job 与 Outbox，Worker 随后消费——业务写成功 = 任务一定入队；
- **运维入口在 API，执行体在后台**：DLQ 重放、手动对账、视图成本触发、向量重建都是 API 端点，但只回队/登记，实际执行归 Worker/Convergence；
- **屏障广播**：API 写路径与 Convergence 收尾都发 `pg_notify('mms_barrier_changed')`，每个 API 副本的 LISTEN 线程即时失效本地缓存（5s TTL 兜底）；
- **删除接力**：API 受理（屏障 ACTIVE）→ 审核批准执行（同事务）→ Convergence 收尾（COOLING_DOWN→COMPLETED、墓碑清理）。

---