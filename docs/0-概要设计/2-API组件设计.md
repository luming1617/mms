# API（mms-api）组件设计

## 一、一句话认识 API

系统里所有"请求驱动、必须在线同步给出结果"的工作——检索记忆上下文、受理候选与摄取事件、接收反馈纠正、审核决策、受理删除请求、同步同意快照、管理面运维——都只经过 **API 进程**这一个在线入口。耗时且不能阻塞在线主流程的工作（文档向量、事件外发）不在请求里做完，而是在同一个 PostgreSQL 事务里落 Job/Outbox 记录，交给 Worker 异步执行；没有天然触发时机的收敛工作交给 Convergence。

---

## 二、为什么 API 只做"门面"

这是"去掉中间件、一个库一个事务边界"架构决策（见[《1-总体架构》](1-总体架构.md)）在在线侧的延续：

1. **在线预算必须闭合**：默认档 P95 ≤300ms，预算求和 `20 + 50 + max(96,50) + 30 = 196ms`（[《11-记忆检索》](../2-详细设计/11-记忆检索.md)）。提取、文档向量、事件外发任何一个放进请求都会击穿预算——它们全部异步化，热路径只保留"查询向量 + 召回 + 可选 rerank"。跨语言召回由多语言向量模型承担；可选的查询侧翻译兜底默认关闭。
2. **一个事务边界**：API 的同步写端点与 Worker 的异步消费**共用同一候选决策实现（同一写入主链）**——业务数据、Job、Outbox 同事务提交，在线/离线两条写路径行为一致，不会漂移出两套决策逻辑。**语义所有权在 MMS：结构化事实的规范句由 MMS 确定性模板生成并版本化，调用方只供原始断言与引证，跨 agent 判重口径统一。**
3. **无状态才能水平扩容**：配额、屏障、同意、业务状态全部在 PG；进程内只有可重建缓存与每副本独立的限流桶，任意副本可杀可加。
4. **合规闸必须前置在入口**：customer_id 随请求传入，必须校验在 API 密钥的授权范围内（终端用户身份不进入 MMS）；同意/屏障/审计在读写发生**之前**判定，fail-closed。入口集中，闸才集中。

一句话：**API 把"在线必须做的事"做薄，把"可以稍后做的事"全部落到同一个事务里的队列上**。

---

## 三、部署形态：无状态多副本

### 3.1 进程与账号

- 与 Worker/Convergence 同镜像、不同命令启动；非 root 运行（uid 65532），对外监听 8000 端口（业务与指标同端口）。
- 业务连接（`MMS_DATABASE_URL`）使用 `mms_app` 账号：业务表/视图表/Job 表/Outbox 可写，但**向量表 `memory_embedding` 的 INSERT/UPDATE 权限被显式回收**——API 永远算不了向量，只能登记 Job 等 Worker 写。
- 审计擦除连接（`MMS_SCRUBBER_DATABASE_URL`）使用独立的 `mms_audit_scrubber` 账号：审核批准删除时的审计擦除走独立连接（见[《14-安全与审计》](../2-详细设计/14-安全与审计.md)），不复用业务连接。
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
- 读路由：近"写后主库保持窗"（默认 5s）内有写入的客户走主库（写后读一致）；副本异常回退主库并标 `replica_unavailable` 降级。HA 备库仅用于故障转移，不承载读（读副本是另一可选开关，见总体架构 1.3）。
- **同意与屏障判定永远走主库**——副本只承载召回 SQL；副本上语义/词法强制串行同池（副本连接池小（max 4），串行避免两通道并行产生新鲜度不一致，见[《11-记忆检索》](../2-详细设计/11-记忆检索.md)）。

### 3.4 启动与关闭

启动：初始化 tracing/Langfuse → Web 框架埋点（排除 health/metrics）→ 运行时监控（事件循环滞后/连接池饱和）→ 屏障 LISTEN 线程。关闭：反序停止并 flush 待上报批次。Web 服务器不自装日志配置，统一 JSON 日志；access log 关闭去噪。

---

## 四、请求处理共性骨架

所有端点共享同一套"**中间件闸 → 认证面 → 业务闸 → 服务实现 → 统一错误模型**"模式。认证见第五章，限流与配额见第六章。

### 4.1 中间件链

```
请求 → RateLimit（429+Retry-After）→ MaxBodySize（256KB，413）
     → RequestContext（X-Trace-Id 回显/派生）→ HttpMetrics（黄金信号）→ 路由
```

- 体积闸在 body 解析**之前**按 Content-Length 拒绝；
- 被外层中间件拒绝的请求不进入路由（也不产生路由级指标）。

### 4.2 响应与错误模型（BaseResp，两面独立）

- 所有端点统一返回 `BaseResp<T>{code,message,data}`：成功 `code=SUCCESS`、data=业务数据；失败 data=null。**agent 服务面与管理面各自独立定义枚举（不同包），不共用**——完整契约见[《1.1-用户面-接口鉴权与响应》](../4-接口与契约/1.1-用户面-接口鉴权与响应.md)与[《2.1-管理面-接口鉴权与响应》](../4-接口与契约/2.1-管理面-接口鉴权与响应.md)。
- `trace_id` 只走响应头 `X-Trace-Id`（取当前 span，缺省派生），**不进响应体**。
- 4xx 是业务结果（WARNING 日志），5xx 才是系统故障（ERROR）。
- 同步决策（candidates / feedback 纠正 / review 批准）成功返回 **HTTP 200** + 决策结果；202 仅用于异步受理（extractions / ingestion events / deletion 受理）。

场景 → 响应码映射（码名与 HTTP 以两面契约枚举为准）：

| 场景 | 面 | code | HTTP |
|---|---|---|---|
| 凭证缺失或无效 | 两 | `AUTH_FAILED`（BaseCode） | 401 |
| 角色/能力位不覆盖端点 | 两 | `FORBIDDEN`（BaseCode） | 403 |
| customer_id 不在密钥授权范围 | agent | `CUSTOMER_MISMATCH` | 403 |
| 管理操作缺少理由码/工单号 | 管理 | `ADMIN_REASON_REQUIRED` | 400 |
| 同意缺失 / 已撤回（写路径） | agent | `CONSENT_REQUIRED` / `CONSENT_WITHDRAWN` | 403 |
| 参数校验失败 / 请求体超限 | 两 | `VALIDATION_FAILED` | 400 / 413 |
| 限流拒绝（携带 Retry-After） | 两 | `RATE_LIMITED` | 429 |
| 配额超限 | agent | `QUOTA_EXCEEDED` ※ | 429 |
| 删除屏障进行中 | 两 | `DELETE_IN_PROGRESS` | 409 |
| 屏障代际过期（cutover 后旧写入） | agent | `STALE_GENERATION` ※ | 409 |
| 冲突未决 | agent | `CONFLICT_PENDING` | 409 |
| 客户已删除 | 两 | `CUSTOMER_DELETED` | 410 |
| 资源不存在 / 假名未预置 | agent | `NOT_FOUND` / `CUSTOMER_REF_UNKNOWN` | 404 |
| 同意快照不可用（fail-closed） | agent | `CONSENT_UNAVAILABLE` | 503 |
| 审计写失败，拒绝操作 | 管理 | `AUDIT_WRITE_FAILED` | 503 |
| 屏障校验异常，事务回滚 | agent | `BARRIER_CHECK_FAILED` ※ | 503 |
| 内部错误 | 两 | `INTERNAL` | 500 |

> ※ `QUOTA_EXCEEDED` / `STALE_GENERATION` / `BARRIER_CHECK_FAILED` 为本设计补充的业务码，当前两面契约枚举未列，**须补入对应面 BizCode 后方可启用**（契约同步跟进项）。
> 读路径的同意拒绝不走错误码：`code=SUCCESS` + 空结果 + consent 标志，不泄露记忆是否存在（见第八章）。
> **查询向量化不可用不是错误**：超时/失败立即降级为 sparse 词法单通道并计数（需求 §5.1 硬要求，见第九章），不返回 503、不重试、不等待；契约中的 `EMBEDDING_UNAVAILABLE` 不适用于检索路径。
> **同意快照缺失不是终态拒绝**：新客户首会话可能早于同意快照同步到达——同步路径返回 403/503 由调用方重试；异步候选 RETRY 退避等待快照（TTL 24h★），明确 DENIED/WITHDRAWN 才终态 REJECTED。宁可错拒、不可错放，但错拒不烧数据（需求 §4.2，R9 澄清）。

---

## 五、接口鉴权

### 5.1 调用方分类

**MMS 不直接面向 C 端用户**，只有两个认证面（《0-需求与规模假设》§一）：**agent 服务面**与**管理面**，凭证完全独立、不混用。

| 调用方 | 是谁 | 认证面 |
|---|---|---|
| agent 主服务（可多个） | 独立部署的 agent 服务，代表客户发起检索、写入、反馈、删除 | agent 服务面 |
| 上游服务 | 权威业务系统/事件网关（事件投递）、同意权威系统（同意同步）、身份服务（identity-ref） | agent 服务面（服务身份密钥，与 agent 同面不同密钥） |
| 运营后台 | 审核台 / 运营工作台：审核、政策、重建、回放、审计读取、运维触发、客户记忆查看 | 管理面 |
| 基础设施 | 容器探针、监控采集 | 内网直连，无业务凭证 |

### 5.2 两个认证面（凭证不混用）

| 面 | 凭证 | 身份来源 | 服务的调用方与能力 |
|---|---|---|---|
| agent 服务面 | API 密钥（`X-API-Key`，一服务一把、可动态配置）+ 请求携带 `X-Customer-Id` | 密钥注册表：密钥 → 服务身份 + 客户授权范围 + 能力位 | agent 主服务：检索/候选/提取/反馈/用量/删除；上游服务：事件摄取/同意同步/identity-ref |
| 管理面 | 运营 JWT（登录服务签发，MMS 只校验）+ `X-Admin-Reason-Code` + `X-Admin-Ticket` | JWT 携带操作员账号与角色，角色白名单校验（见 5.4） | 运营后台：审核/政策/事实键/重建/lineage/决策回放/AUDIT 读取/运维触发/客户记忆查看 |

- **客户号范围校验**：`X-Customer-Id` 必须在该密钥的客户授权范围内，越界即 403 `CUSTOMER_MISMATCH`；终端用户身份不进入 MMS。
- **密钥能力位**：agent 密钥只开放业务读写能力；摄取/同意同步/identity-ref 能力仅授予对应上游服务的密钥，agent 密钥默认不具备。

### 5.3 鉴权与授权规则

- **凭证物理分离**：API 密钥与运营 JWT 物理分离、不混用；政策变更再要求角色 ∈ 政策管理角色集（见 5.4）。
- **customer_id 只做范围校验**：请求携带的 customer_id 不在密钥授权范围内 → 403 `CUSTOMER_MISMATCH`；path 里的 customer_id 只是"请求目标"，不是授权凭据。
- **管理操作留痕**：管理面访问客户数据必须携带理由码 + 工单号；`AUDIT`/`as_of` 读取必须管理面凭证，**读前同事务写审计，写失败 503 拒读**（fail-closed）。运营查看客户记忆摘要同样读前审计。
- **DLQ 重放走管理面**：同事务审计，写失败重放不生效。
- **审核域走管理面**：审核台属运营后台，审核队列与审核决策使用运营 JWT（审核人身份 + 4-eyes）。

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

- 按 `X-Customer-Id` 建桶（缺省回退 client IP）；默认 50 请求/秒、突发 100；
- **每副本独立预算**：总限流预算 = 单副本预算 × 副本数，扩容会放大总预算，容量规划必须计入；
- 跳过 `/health`、`/metrics`、`/internal`（`/internal` 为上游服务受控调用，预算由调用方与配额管理）；
- 在中间件层拒绝，不进入路由、不产生路由级指标。

### 6.3 配额（DB 共享预算）

配额桶枚举（计数落 `customer_quota_usage`，所有副本同一预算）：

| 桶 | 维度 | 默认上限 | 扣减/校验时机 |
|---|---|---|---|
| `WRITE` | 每客户每日 | 2000 | 写入决策时计数 |
| `EXTRACTION` | 每客户每日 | 2000 | 提取受理**先扣再调提取器** |
| `EMBEDDING` | 每客户每日 | 20000 | 决策事务内复查 |
| `ACTIVE_MEMORIES` | 存量记忆条数 | 1000 | 决策事务内复查（产品硬上限，需求 §4.1） |

- **原子自增后判定**，超限 429 `QUOTA_EXCEEDED`；
- 提取端点先扣 `EXTRACTION` 再调提取器——配额耗尽的客户不能继续烧 provider；
- 存量按**在用状态 `ACTIVE`/`CONFLICTED`** 计数（历史态 RESOLVED/SUPERSEDED/EXPIRED 不计，需求 §4.1"在用记忆数"）；
- **配额感知淘汰（以遗忘代拒写）**：存量 ≥95% 上限时，决策事务内先按保护级淘汰——最弱偏好（强度最低）→ 最老情景（归档语义，置 EXPIRED），**单值事实当前值永不淘汰**；淘汰后仍超限才 429 `QUOTA_EXCEEDED`。淘汰与新写同事务、配额即时释放；投影由生命周期 #11–13 作废；
- DB 共享意味着扩容不漂移预算，与限流的副本独立语义正好互补。

---

## 七、能力全景（接口矩阵）

各表"调用方"列按 5.1 分类；字段级完整契约见[《1.2-用户面-接口清单》](../4-接口与契约/1.2-用户面-接口清单.md)与[《2.2-管理面-接口清单》](../4-接口与契约/2.2-管理面-接口清单.md)。

### 7.1 检索域（在线读）

检索用途（purpose）枚举：

| purpose | 含义 | `as_of` | 说明 |
|---|---|---|---|
| `CURRENT_ANSWER` | 当前回答（默认） | 不允许 | 事实通道只取 ACTIVE；**CONFLICTED 不进入可注入上下文**（需求 §3.1：挂起不参与日常回答），响应仅带 `pending_verification_count` 条数提示、不带内容；待核实内容仅管理面可见（审核台 / AUDIT）。**命中高风险事实且 `next_verify_at` 到期时，以独立短事务登记回源核对 Job（活动唯一键免重）并刷新 +7 天★，受每日核对预算闸约束；登记失败只打点，不影响响应**（见 Convergence #17） |
| `HISTORY_DEBUG` | 历史调试 | 允许 | 可召回 RESOLVED/SUPERSEDED/EXPIRED 等历史状态；**历史通道走结构化过滤（时间窗必带，默认最近 365 天★ + fact_key + 状态）+ 词法匹配，不做向量扫描**（历史态不保留向量，扫描集有界） |
| `AUDIT` | 审计 | 允许 | 必须管理面凭证，读前审计 |

> `CANDIDATE_DISCOVERY` 是决策引擎内部用途，不接受外部传入。purpose 由服务端强制映射状态集，调用方不能自选状态集。MMS 不直接面向 C 端用户，不设客户自助用途；面向终端用户的展示与脱敏由 agent 侧负责。

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `POST /memories/query` | agent 主服务 | 语义+词法+精确过滤检索 | agent 服务面；purpose 服务端映射；`as_of` 仅 HISTORY_DEBUG/AUDIT |
| `POST /agent/memory-context` | agent 主服务 | Agent 注入式上下文 | agent 服务面；响应固定两段：`profile`（客户画像，**v1 = 确定性结构化摘要、v1.5 = LLM 画像**，调用方无需改代码）+ `memory_context` 明细；`instruction_boundary` + `trust=untrusted_data` 不变；原检索信封下移 `retrieval` |
| `GET /customers/{id}/memories` | 运营后台 | 运营查看客户记忆摘要 | 管理面；读前同事务审计；cursor 分页；脱敏摘要（160 字符） |

**注入预算与通道构成（`/agent/memory-context` 响应契约，需求 §3.2）**：总预算 ≤1200 token——画像 ≤400（**v1 = 结构化摘要**）/ 事实当前值 ≤400（**按 current 指针直取，不受查询相似度过滤；超预算按选择政策截取：高风险承诺保送 → 查询实体相关 → 最近变更，政策进 policy、受评测门禁**）/ 最强偏好 ≤200（按强度排序）/ 相关情景 ≤200（时间窗 + 相关性）；通道内按分数截断。明细融合：语义 + 词法 + 精确三路按排名倒数融合（RRF，无需调权），再按类型权重 × 新鲜度/强度加权；权重进 policy、受评测门禁（需求 §6.3）。

**v1 结构化摘要（需求 §3.2，R5）**：当前事实集（按上述选择政策）+ 最强偏好（现算有效强度 top-K）的确定性模板拼装，存 `customer_digest`（每客户一行、带来源指纹）；决策事务内增量维护（事实变更/偏好 top-K 变化/配额淘汰同事务刷新），无 LLM、无异步链路、无新鲜度缺口；BUSINESS 对账按来源指纹校验失配重算。忠实性天然成立（模板拼装不产生新论断）；摘要质量随采纳率基线评审，v1.5 LLM 画像替换内容时过忠实性门禁（Worker 第八章）。

### 7.2 写入域（同步决策 / 异步摄取）

**设计口径：两类摄取统一入队、Ingestion Worker 统一消费，两条路径仅差提取方**——路径 A 由本系统提取会话原文，路径 B 由投递方提前提取候选。受理只做"过闸 + 落 Job"，202 + job_id 即返回；提取与决策由 Worker 异步完成，与同步写端点共用同一写入主链。`candidates`/`feedback`/`review` 三个需要即时结果的入口保留同步决策：即时反馈、审核批准与业务写入同事务原子；**摄取队列支持纠正插队（CORRECTION 优先级），用户纠正排在批量摄取之前**（需求 §5.2：纠正 → 可被查 P95 ≤30s / P99 ≤120s）。详见[《Ingestion Job 设计》](../1-核心流程/1-Ingestion%20Job设计.md)第二十二节。

事件类型（event_type）枚举：

| event_type | 含义 | 提取方 |
|---|---|---|
| `memory.candidate.v1` | 候选事件（路径 B） | 投递方（上游已完成提取） |
| `memory.extraction.requested.v1` | 提取请求（路径 A） | 本系统（Worker 侧规则/LLM 提取器） |
| `memory.verification.requested.v1` | 回源核对请求（高风险事实） | 本系统（Worker 回源取权威值，跳过会话提取）；触发：**检索命中到期（主，7.1）+ Convergence #17 低频兜底扫描**，共用每日核对预算闸 |

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `POST /memories/candidates` | agent 主服务 | 单候选同步决策 | agent 服务面；与 Worker 共用同一写入主链；**200 返回决策结果（同步）**；**P95 ≤500ms 预算（问题转向量 150 + 查重查找 150 + 决策写库 100 + 合规与打包 100），不含大模型调用**；接受原文断言 + fact_key/slots + assertion_source：**结构化事实的规范句由 MMS 确定性模板内联生成（<1ms，版本=注册表 normalization_version）**，自由文本候选须自带规范句、没带转异步；未注册 fact_key 转异步（需求 §5.1） |
| `POST /extractions` | agent 主服务 | 会话提取受理（异步） | agent 服务面；受理落 Job，202 + job_id；先扣 EXTRACTION 配额；Worker 取数→提取→逐候选决策 |
| `POST /internal/ingestion/events` | 上游服务（业务系统/事件网关） | 候选事件异步摄取 | agent 服务面（摄取能力位密钥）；topic 白名单；Inbox+Job 双账本同事务；202 + job_id |
| `GET /internal/ingestion/jobs/{id}` | agent 主服务 / 运营后台 | 摄取任务状态查询 | agent 服务面或管理面；状态/attempts/last_error_code |

关键设计点：

- **会话级 Job**：路径 A 一行 Job 对应一次会话提取（一事件一 Job 的唯一键容纳不下 N 个候选）；候选级幂等由候选指纹 `(customer_id, source_system, source_event_id, candidate_fingerprint)` 承担，单候选失败不影响同批其他候选。路径 B 维持一事件一候选一 Job。
- **只传引用取数**：事件只携带会话/消息引用，Worker 以对会话存储的服务身份取数并校验归属；归属不符按安全事件处理，不提取。
- **失败分账**：提取失败属运行失败（可重试），候选拒绝属业务终态（不重试）。
- **Job 状态枚举**：`PENDING` / `RUNNING` / `READY` / `RETRY` / `DEAD_LETTER` / `CANCELLED`。
- **topic 白名单**：默认 `memory-candidates`（可配置）。
- **同步决策的向量降级**：候选规范句向量化超时/失败（同 150ms 预算）→ 退化为**精确指纹判重**（semantic_hash + fact_key 精确匹配）完成决策，响应标 `degraded=true`；语义查重缺口以 BACKFILL 优先级异步补检（发现重复/冲突再走决策链纠正），不阻塞 500ms 预算、不返回 503。
- **断言来源契约**：FACT 候选必带 `assertion_source`（AUTHORITATIVE_EVENT / CUSTOMER_EXPLICIT / INFERRED），CUSTOMER_EXPLICIT 必带 `assertion_quote`（脱敏引证 + 消息引用）；业务事件与 feedback 纠正的来源由服务端强制，不信任自报；FACT+INFERRED 不进事实槽（转情景或 IGNORE），事实 SUPERSEDE 仅接受权威事件/明确纠正（需求 §2.1①"系统不猜"的执行机制，见 Worker S4/S5）。

### 7.3 反馈与用量域（agent 服务面）

反馈类型（feedback_type）枚举：

| feedback_type | 含义 | 约束 |
|---|---|---|
| `INCORRECT` | 内容错误 | 必须携带 correction，纠正走决策引擎；**服务端强制 assertion_source=CUSTOMER_EXPLICIT，correction 文本即引证** |
| `OUTDATED` | 内容过时 | — |
| `IRRELEVANT` | 不相关 | — |
| `HELPFUL` | 有帮助 | 不自动提升可信度 |
| `PRIVACY` | 隐私问题 | 转隐私处理流程 |
| `OTHER` | 其他 | — |

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `POST /memories/{id}/feedback` | agent 主服务 | 反馈/纠正 | Idempotency-Key 必需；INCORRECT 必须带 correction；纠正走决策引擎 |
| `POST /memory-usage` | agent 主服务 | 回报实际采用的记忆 | request_id 关联检索 trace；answer_hash 校验 |

### 7.4 审核域（管理面）

审核决策（decision）枚举：`APPROVE`（批准，立即执行写入或删除）/ `REJECT`（驳回，终态）。

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `GET /review-queue` | 运营后台（审核台） | 待审列表 | 管理面鉴权 + 读前审计；**不套客户同意闸**（运营跨客户工作台，同意闸属于客户读写路径）；cursor 分页 |
| `POST /review-queue/{id}/decision` | 运营后台（审核人） | 审核决策 | `decision_version` 乐观锁；**单人复核**（提交方为系统/agent，审核人为运营账号）；4-eyes 仅适用于运营发起的删除、政策激活、批量覆盖（见契约 2.1）；批准即执行（与业务写入同事务） |

### 7.5 删除域（agent 服务面）

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `POST /deletion-requests` | agent 主服务（代表客户发起） | 请求全量删除 | Idempotency-Key 必需；scope 仅 `{all:true}`；建屏障；**客户自助删除自动批准，不等待人工**（需求 §5.2）；运营发起的删除才建审核任务 |
| `GET /deletion-requests/{id}` | agent 主服务 | 删除状态/证明 | `proof` 字段随状态返回 |

删除请求状态枚举（与 Convergence 文档第六章同一口径）：`RECEIVED`（受理，屏障 ACTIVE；运营发起待 4-eyes）→ `APPROVED`（自助/同意撤回自动批准；运营审核批准）→ `VERIFYING`（失效已提交，删除证明/墓碑 WORM 待落账）→ `COMPLETED`（墓碑已落账，屏障转 COOLING_DOWN，proof 可查）；分支终态 `REJECTED`（审核驳回）/ `CANCELLED`（重新授权撤销）。执行是单事务，不设可观察的中间态（无 IN_PROGRESS）。

删除屏障状态枚举：`ACTIVE`（删除进行中）/ `COOLING_DOWN`（冷却收尾）/ `COMPLETED`（完成）/ `CANCELLED`（重新授权撤销或审核驳回）。**撤销仅允许在失效执行前**（RECEIVED/APPROVED）；失效一旦提交，停读停写与投影销毁不可逆，屏障只能走 COOLING_DOWN→COMPLETED；内容物理擦除在冷却期满执行（擦除矩阵见下），误删恢复走备份恢复预案，不回滚屏障状态机。

**执行归属（一个执行实现，触发点枚举）**：失效在**删除执行事务内**完成——记忆置 DELETE_PENDING、活动 Job 置 CANCELLED、撤 current 指针，请求置 VERIFYING；随后**独立短事务**写 WORM 墓碑与 deletion_item(VERIFIED)，成功则请求 COMPLETED、屏障 COOLING_DOWN、pg_notify + 删除域 Outbox 事件。墓碑写失败不回落失效，请求留在 VERIFYING，由 Convergence 8.1 按 request+scope 幂等键限流重试（见 Convergence 文档 8.1）。触发点：**API**（自助受理、运营审核批准、同意撤回事件，见 7.6）与 **Convergence 8.2**（保留同意到期扫描），共用同一执行实现、同一事务形状；此外 Convergence 不做首次执行。

**批准口径（需求 §5.2）**：客户自助删除（agent 代表客户发起，密钥范围已校验）与同意撤回触发的删除**自动批准**；运营侧发起的删除由运营后台 4-eyes 审核（见 7.4）。任何删除**批准 → 执行完成、出证明 ≤7 天**，超时触发 `MMSDeletionDeadlineBreach`（page）。冷却收尾与墓碑清理归 Convergence（见第十一章）；撤回触发的删除同走此链路（见[《03-记忆授权与撤回》](../2-详细设计/03-记忆授权.md) 3.4）。

**擦除矩阵（R7）**：T0 失效事务停读停写 + 销毁投影 + 撤 current，决策/候选内容快照由 scrubber 独立短事务擦除（失败 Convergence 补偿，与墓碑同形状）；冷却期（30 天）memory/evidence 内容保留但屏障挡一切读写；冷却期满由 Convergence 8.4 以删除专用账号物理擦除内容与行；墓碑 400 天 > 备份 35 天 + 恢复窗口。删除证明 = 停读停写 + 投影销毁 + 墓碑落账（批准 → 出证明 ≤7 天）。

**按密钥删除熔断**：单密钥删除请求 > `deletion_rate_per_key_hour`（默认 20★）→ 新请求停 RECEIVED、暂停自动批准，`MMSDeletionRateAnomaly`（page）人工确认后放行；已执行不回滚。防密钥泄露批量删库；自助自动批准口径不变。

**误删恢复预案**：备份恢复 runbook（≤35 天窗口）——4-eyes + 同事务审计 + 恢复后 REBUILD 重建投影 + 墓碑账本追加恢复记录；不是屏障回滚，不开放"撤销已执行删除"接口。

### 7.6 同意与身份域（agent 服务面 · 上游服务密钥）

同意用途（purpose）枚举：

| purpose | 含义 |
|---|---|
| `MEMORY_WRITE` | 允许写入记忆 |
| `MEMORY_READ` | 允许读取/召回记忆 |
| `MEMORY_RETENTION` | 允许留存记忆 |
| `SENSITIVE_MEMORY` | 允许处理中/高敏感记忆 |
| `CROSS_BORDER_EMBEDDING` | 允许跨境向量化通道 |
| `CROSS_BORDER_TRANSLATION` | 允许跨境查询翻译通道（查询侧兜底，可选；不翻译、不存储记忆译文） |
| `CROSS_BORDER_RERANK` | 允许跨境重排通道 |

同意状态枚举：`GRANTED`（已授权）/ `DENIED`（拒绝）/ `WITHDRAWN`（撤回）。

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `GET/POST /internal/customers/{id}/memory-consent` | 上游服务（同意权威系统） | 同意快照读取/同步 | 版本单调；`MEMORY_RETENTION` 翻 DENIED/WITHDRAWN → **同事务**创建删除请求 + 屏障 ACTIVE + 自动批准 + 执行失效（事件驱动，不靠后台扫描，见 Convergence 8.2）；GRANTED 翻屏障 CANCELLED（仅失效执行前）；`expires_at` 到期由 Convergence 到期索引承接（见[《03-记忆授权与撤回》](../2-详细设计/03-记忆授权.md) 3.4） |
| `GET/POST /internal/customers/{id}/identity-ref` | 上游服务（身份服务/事件投递方） | `customer_ref` 读取/预置 | Kafka 假名（HMAC），幂等 |

### 7.7 管理域（管理面 + 同事务审计）

政策操作（action）枚举：`CREATE`（创建草稿）/ `ACTIVATE`（激活）/ `ROLLBACK`（回滚）。

死信队列类型（job_type）枚举：`INGESTION` / `OUTBOX` / `EMBEDDING`（v1.5 增 `PROFILE`）。

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `GET/POST /admin/policies` | 运营后台 | 政策列表/创建/激活/回滚 | 4-eyes（批准人≠创建人）；单 ACTIVE 约束 |
| `GET/POST /admin/fact-keys` | 运营后台 | 事实键注册表治理 | GET 只读；POST 提案/激活/弃用/别名（DRAFT→ACTIVE→DEPRECATED）；激活/弃用/别名需 ADMIN/POLICY + 4-eyes + 同事务审计；别名激活触发存量 RENORMALIZE（Convergence 职责五） |
| `POST /admin/embeddings/rebuild` | 运营后台 | 向量重建/回填 | 必须限定客户或记忆；**只登记 Job，Worker 执行** |
| `POST /admin/decisions/replay` | 运营后台 | 决策回放 | 钉住 policy/registry 版本重放规则矩阵 |
| `GET /admin/memories/{id}/lineage` | 运营后台 | 证据→决策→版本→向量→使用 | 读前审计 |
| `POST /admin/dlq/replay` | 运营后台 | 三类队列死信统一重放（v1.5 含 PROFILE） | 审计同事务，写失败重放不生效 |
| `GET/POST /admin/outbox-consumers` | 运营后台 | 外发消费者注册（endpoint + 凭证 + 订阅类型） | 管理面；至少一次投递、下游按 idempotency_key 去重（见 Worker 7.5） |
| `GET/POST /admin/eval-sets` | 运营后台 | 黄金评测集管理 | 真实问题脱敏采样 + 人工标注；季度轮换 20%★防"背题"；按审计口径管理，不进生产检索（需求 §6.3） |
| `POST /admin/eval-runs` | 运营后台 | 触发离线评测 | 对候选版本（抽取器/归一器/向量模型/决策政策）跑需求 §6.2 指标全集：抽取精确率/召回率（每周抽样 200 条★标注 + 100 段★会话回放）、决策准确率（决策回放 × 标注集）、换说法/跨语言命中率（前 10 条）；结果落评测运行记录 |

**版本启用门禁（需求 §6.3）**：政策激活、模型注册表影子转正式、抽取器/归一器版本切换，都必须引用**通过的评测运行记录**；任一指标破线阻断启用（`MMSEvalGateBreach`）。影子评测覆盖整条决策链，不只挑战向量模型。

**fact_key 治理（封闭注册表）**：单值事实判重、current 唯一槽、风险分级都挂在 fact_key 上，词表封闭——未注册键的候选转异步并生成键提案建议（DRAFT + 来源样本），运营审核激活；键冲突检测（规范句模板向量相似度聚类）只产出合并/别名建议，不自动合并。键的归一化模板与 normalization_version 是确定性渲染（修订 E）与 RENORMALIZE 的版本锚。

### 7.8 运维触发域（管理面）

对账类型（recon_type）枚举：

| recon_type | 含义 |
|---|---|
| `JOB` | 任务对账：超租约回收、死信回队 |
| `VECTOR` | 向量对账：缺向量回填、视图修复 |
| `BUSINESS` | 业务状态对账：状态与投影一致性 |
| `RECALL` | 自检探针：原文自召回采样（管道健康，不计质量 SLO，需求 §6.2） |

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `POST /internal/reconciliation/{type}` | 运营后台（运维工作台） | 手动触发四类对账 | 执行体与 Convergence 同一实现 |
| `GET /internal/customers/{id}/retrieval-capacity` | 运营后台（运维工作台） | 单客户检索容量报告 | 只读 |

### 7.9 健康与指标

| 端点 | 调用方 | 用途 | 关键口径 |
|---|---|---|---|
| `GET /health/live` | 基础设施（容器探针） | 进程存活 | 无依赖 |
| `GET /health/ready` | 基础设施（容器探针） | 硬依赖：主库可写 + pgvector + schema 初始化 | 任一缺失 503，Pod 摘流量 |
| `GET /health/deps` | 基础设施（运维诊断） | 软依赖：embedding provider、DB、副本 | 不参与 readiness，防 provider 故障摘全站 |
| `GET /metrics` | 基础设施（监控采集） | Prometheus 指标 | 与业务同端口 8000（Worker 用独立 metrics 端口） |

---

## 八、安全与合规闸（fail-closed 清单）

| 闸 | 判定位置 | 失败/拒绝行为 |
|---|---|---|
| 客户身份 | API 密钥 + `X-Customer-Id` 授权范围校验；越界即拒 | 401 `AUTH_FAILED` / 403 `CUSTOMER_MISMATCH` |
| 管理面 | 运营 JWT + 角色白名单 + reason + ticket | 401/403；缺理由 400；AUDIT 读前审计失败 503 |
| 屏障（读） | 主库快照 + 5s 缓存 + NOTIFY 失效 | `DELETE_IN_PROGRESS` 409 / `CUSTOMER_DELETED` 410 |
| 屏障（写） | advisory lock + 主库快照 + 代际/cutover 校验 | 拒绝或 `STALE_GENERATION`；校验异常回滚（`BARRIER_CHECK_FAILED` 503） |
| 同意（读） | `MEMORY_READ` 主库直查 | 空结果 + consent 标志，不泄露记忆是否存在 |
| 同意（写） | `MEMORY_WRITE`/`SENSITIVE_MEMORY` FOR UPDATE | **快照缺失/不可用：同步 403/503 可重试，异步 RETRY 有界退避（TTL 24h★，超期终态 REJECTED）；明确 DENIED/WITHDRAWN：终态 REJECTED 决策，不进 DLQ**（fail-closed 不变，见第四章注释） |
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
| `quota_embedding_per_day` / `quota_active_memories` | 20000 / 1000 | 向量预算 / 存量记忆上限（产品硬上限，需求 §4.1） |
| `query_cache_ttl_seconds` / `query_cache_max_entries` | 3600 / 4096 | 查询向量缓存（进程内） |
| `barrier_cache_ttl_seconds` / `barrier_cache_max_entries` | 5 / 4096 | 屏障缓存；TTL 超 5s 拒绝启动 |
| `read_replica_enabled` / `read_replica_write_hold_seconds` | false / 5 | 读副本开关与写后读主库保持窗 |
| `retrieval_parallel_lexical` | true | 语义/词法并行（副本上强制串行） |
| `injection_budget_tokens` | 1200 | memory-context 注入总预算（通道分配见 7.1） |
| `history_query_window_days` | 365★ | HISTORY_DEBUG/AUDIT 默认时间窗（可显式收窄，不可超过）；历史通道扫描集有界的保证 |
| `reranker_enabled` / `rerank_enabled_purposes` / `rerank_top_n` / `reranker_timeout_ms` | false / CURRENT_ANSWER,HISTORY_DEBUG / 20 / 80 | rerank 开关与预算 |
| `reranker_provider` | self-hosted | 默认自托管；切 external 需 `CROSS_BORDER_RERANK` 同意 |
| `lexical_min_query_chars` | 3 | 词法通道 = 模型 sparse 输出（BGE-M3，与 dense 同次外呼），**无需按语言配置**；最短查询长度低于此值跳过词法通道并计数 |
| `embedding_timeout_ms` | 150 | 查询向量 provider 超时；超时立即降级 sparse 词法单通道并计 `mms_retrieval_degraded_total`，不重试、不等待（需求 §5.1 硬要求；秒级超时与 300ms 总预算不相容，启动期校验拒绝 >150 的配置） |
| `admin_roles` / `policy_admin_roles` | ADMIN,POLICY,PRIVACY,OPERATOR,READONLY / ADMIN,POLICY | 管理面角色白名单（见 5.4） |
| `ingestion_topics` | memory-candidates | 摄取 topic 白名单 |
| `authority_sync_mode` | async | 同意权威同步模式：异步快照同步 |
| `consent_missing_retry_ttl_hours` | 24★ | 异步候选等待同意快照的最长退避时间；超期终态 REJECTED（CONSENT_MISSING_TIMEOUT） |
| `deletion_rate_per_key_hour` | 20★ | 单密钥每小时删除请求熔断阈值；超限暂停自动批准并 page |

---

## 十、可观测性总览

**黄金信号**（按路由模板统计而非具体 id，禁止客户标签）：

| 指标 | 口径 |
|---|---|
| `mms_api_requests_total{method,route,purpose,code}` | `purpose=query` 仅覆盖两个查询路由 |
| `mms_api_latency_seconds{route,path}` | `path=default/extended` 分档；admin 路由恒 extended |
| `mms_http_in_flight` | 在途请求 |

**检索质量**：`mms_retrieval_degraded_total{reason}`（**每请求一次**，SLI 分子是降级请求数不是原因数）、`mms_retrieval_zero_results_total`、`mms_retrieval_candidates`、`mms_retrieval_view_hits_total{view_kind,language}`、`mms_lexical_skipped_total{reason}`、`mms_rerank_total{result}`。词法通道为模型 sparse 输出；降级模式 = sparse 单通道；`mms_lexical_skipped_total{reason}` 记录跳过原因（查询过短/sparse 缺失）。

**安全与合规**：`mms_cross_customer_hits_total`（P0 page）、`mms_consent_check_total{result}`、`mms_consent_version_stale_total`、`mms_barrier_cache_age_seconds`、`mms_quota_rejected_total{bucket}`、`mms_admin_audit_write_failures_total`、`mms_fact_verify_registered_total{trigger=on_read|sweep}`、`mms_fact_verify_deferred_total`（预算顺延）、`mms_fact_verify_register_failed_total`（读路径登记失败）、`mms_consent_missing_retry_total{path=sync|async}`、`mms_consent_missing_timeout_total`。

**运行时**：`mms_event_loop_lag_seconds`、`mms_db_pool_connections{pool,state}`。

**记忆质量指标（需求 §6.1，持续采集、周度评审）**：

| 指标 | 口径（分子 / 分母） | 目标★ | 指标名 |
|---|---|---|---|
| 记忆采纳率 | usage 回报"实际用了"的记忆次数 / 有结果的检索次数 | ≥25%（首季建基线后修订） | `mms_memory_adoption_ratio` |
| 负反馈率 | (INCORRECT + OUTDATED) 反馈数 / 被采纳数 | ≤2%（7 天窗口） | `mms_negative_feedback_ratio` |
| 有害错误率 | 人工确认为高风险事实错误的被采纳记忆 / 被采纳数 | ≤0.5%，破线立即电话值班 | `mms_harmful_memory_ratio` |
| 零结果率 | 空手而归的检索 / 全部检索（剔除入库 <24h 新客户） | ≤10% | 由 `mms_retrieval_zero_results_total` 计算 |
| 陈旧残留率 | 抽样中"该被替换或过期、却还在用" / 抽样在用记忆 | ≤2% | Convergence BUSINESS 抽样 + 人工标注（见 Convergence 9.4） |
| 跨客户命中 | A 的检索出现 B 的记忆 | =0 | `mms_cross_customer_hits_total` |

> 抽取精确率/召回率、决策准确率、换说法/跨语言命中率属离线评测（需求 §6.2），由 7.7 评测端点承载，不在在线指标里。
> 采纳率同时报辅助口径"每次检索至少采纳一条"（per-search）：主口径（条数/次数）量纲混合、跨客户可比性弱，周度评审两口径并看（需求 §6.1 基线修订参考）。

**SLO 与告警**：

| SLI | 目标 | 告警 |
|---|---|---|
| 查询可用性 | 99.9%/30d | `MMSAvailabilitySLOBreach` |
| 默认档延迟 | P95 ≤300ms / P99 ≤800ms | `MMSDefaultLatencySLOBreach` |
| 扩展档延迟 | P95 ≤500ms / P99 ≤1500ms | `MMSExtendedLatencySLOBreach` |
| 同步决策延迟（candidates / feedback 纠正 / review 批准） | P95 ≤500ms / P99 ≤1500ms | `MMSDecisionLatencySLOBreach` |
| 异步受理延迟（extractions / ingestion events） | P95 ≤100ms / P99 ≤300ms | `MMSAcceptLatencySLOBreach` |
| 反馈（非纠正）/ 删除受理 / 状态查询延迟 | P95 ≤200ms / P99 ≤500ms | `MMSFeedbackLatencySLOBreach` |
| 管理面列表 / 溯源 / 审计读延迟 | P95 ≤1s / P99 ≤3s | `MMSAdminLatencySLOBreach` |
| 降级率 | <1%（告警 >2%） | `MMSDegradedRatioHigh` |
| 跨客户命中 | 0 | `MMSCrossCustomerHit`（page） |
| 配额 429 激增 | 5m ≤20 | `MMSQuotaRejections` |
| 同意快照缺失/版本落后 | 0 | `MMSConsentCheckUnavailable` / `MMSConsentVersionStale` |
| 审计写失败 | 0 | `MMSAuditGuardFailure`（page） |
| 同步写入 → 可被查 | P95 ≤10s / P99 ≤60s | `MMSWriteFreshnessSLOBreach`（ticket，需求 §7） |
| 用户纠正 → 可被查 | P95 ≤30s / P99 ≤120s | `MMSCorrectionFreshnessSLOBreach`（page，需求 §7） |
| 删除批准 → 执行完成 | ≤7 天 | `MMSDeletionDeadlineBreach`（page，需求 §7） |
| 审核队列积压 | ≤200 条 | `MMSReviewQueueOverflow`（ticket，需求 §7） |
| 有害错误率 | >0.5% | `MMSHarmfulMemoryRate`（page，需求 §7） |
| 负反馈率 | >2%（7 天窗口） | `MMSNegativeFeedbackHigh`（ticket，需求 §7） |
| 采纳率周环比 | 跌 >30% | `MMSAdoptionDrop`（ticket，需求 §7） |
| 换说法/跨语言评测破线 | 任一指标不达标 | `MMSEvalGateBreach`（阻断启用，非运行时告警，需求 §7） |
| 故障转移 | RTO ≤5min★ | `MMSFailoverOccurred`（page） |
| 审核流入 7 日均值 | ≤160 条/日★（预算 80%） | `MMSReviewInflowHigh`（ticket，先行指标） |
| 单密钥删除速率 | ≤20/h★ | `MMSDeletionRateAnomaly`（page） |

> 延迟四档与新鲜度目标均为需求 §5 的硬承诺。**新鲜度 SLI 测量**：决策事务写 `registered_at`（带路径标签 sync/correction/extraction），Embedding 终写同事务写 `ready_at`；Worker 导出 `ready_at - registered_at` 直方图（按路径分桶），即写入/纠正/提取三条新鲜度 SLI 的数据源；删除批准 → 完成由 deletion_request 时间戳计算（见 Convergence 8.1）。

**Trace/日志**：OTel Web 框架埋点 + Langfuse `retrieval_observation`（检索全链路）+ `X-Trace-Id` 回显；业务 trace_id 同时是 Langfuse/OTel trace id，供后续 feedback 精确打分；日志只落 `customer_hash` 不落正文。

---

## 十一、API 与 Worker/Convergence 的边界

Worker 与 Convergence 的完整设计见[《3-Worker组件设计》](3-Worker组件设计.md)与[《4-Convergence组件设计》](4-Convergence组件设计.md)。

| 维度 | API（本文） | Worker | Convergence |
|---|---|---|---|
| 实例 | 无状态多副本 | 多实例并行（`SKIP LOCKED`） | 单实例（advisory lock 主备） |
| 触发 | 请求驱动 | 任务驱动（轮询队列） | 定时轮询到期状态（到期索引 + 分片轮转） |
| DB 账号 | `mms_app`（向量表只读） | `mms_worker` | `mms_worker`（+deletion 提权） |
| 是否调外部 provider | 是：查询向量/rerank（受同意闸） | 是：提取/文档向量/事件外发/画像合成（v1.5） | 基本不调（自检探针例外） |
| 典型产物 | 在线响应、决策事务、Job/Outbox 登记 | READY 向量、已发布事件、客户画像（v1.5） | 状态推进、回队、删除补偿 |
| 关系 | **把请求变成事务与任务** | **把任务消费掉** | **把卡住的任务放回队列 / 作废任务** |

衔接点：

- **同一事务边界**：API 决策事务内登记 embedding Job（及 v1.5 profile Job）与 Outbox，Worker 随后消费——业务写成功 = 任务一定入队；
- **运维入口在 API，执行体在后台**：DLQ 重放、手动对账、向量重建都是 API 端点，但只回队/登记，实际执行归 Worker/Convergence；
- **屏障广播**：API 写路径与 Convergence 收尾都发 `pg_notify('mms_barrier_changed')`，每个 API 副本的 LISTEN 线程即时失效本地缓存（5s TTL 兜底）；
- **删除接力**：API 受理（屏障 ACTIVE）→ 批准（客户自助/同意撤回自动批准 / 运营发起 4-eyes）→ API 执行事务（同事务失效，请求 VERIFYING）→ 墓碑独立短事务（失败卡 VERIFYING，Convergence 8.1 幂等重试）→ Convergence 收尾（COOLING_DOWN→COMPLETED、DELETE_PENDING→DELETED、墓碑清理）；批准 → 完成 ≤7 天。

---