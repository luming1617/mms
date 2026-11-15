# Convergence（mms-convergence）设计

> 归纳自[《0-总体架构》](1-总体架构.md) 2.1.1（部署架构图与部署组件定义）、[《08-记忆的删除与失效》](../2-详细设计/09-记忆删除与失效.md)（过期清理 / 端到端删除）、[《11-记忆的生命周期与状态机》](../2-详细设计/12-状态机.md)、[《12-记忆检索》](../2-详细设计/11-记忆检索.md)（对账与一致性）。
> 姊妹篇见 [1-API组件设计.md](2-API组件设计.md) 与 [2-Worker组件设计.md](3-Worker组件设计.md)（同为部署单元层级总结：API 是"在线门面"，Worker 是"干活的手"，Convergence 是"兜底与收敛的大脑"）。本文按"由浅入深"：先讲定位与为什么，再讲部署形态与共性骨架，然后讲五个职责的顺序与协作，最后逐项展开职责与权衡。

---

## 一、一句话认识 Convergence

如果说 Worker 是"干活的手"，Convergence 就是**"兜底与收敛的大脑"**：它不处理任何单条业务事件，而是**定时按到期索引取出"到点该变的状态"**（绝不扫全库），把"该往前却没往前"的一切推进到终态——过期的让它过期，卡住的让它重来，不一致的让它对齐，该删的把它删干净。

总体架构 2.1.1 的官方定义：

> `mms-convergence`：生命周期扫描、Embedding 重试、对账、策略版本检查、模型升级。

部署架构图概括为：**状态扫描、重试、对账、模型与注册表版本检查**。

---

## 二、为什么需要它（它解决的本质问题）

DB Job 模型（异步 + 租约 + 重试）本身有几个"自己收不了尾"的缺口，必须有一个单实例的后台角色补上：

1. **状态不会自己过期**：候选 SLA、记忆 `valid_to/expires_at`、冲突最长存活期——这些是"到点必须变状态"的时间语义，没有请求触发时谁来改？→ 靠到期索引定时取行。
2. **Worker 会崩**：一个 Job 被认领成执行态后 Worker 进程挂了，任务会永远卡在执行态。→ 靠租约到期回收。
3. **异步链路会漂移**：向量删了视图没删、Job 标了 READY 但向量丢了、current 指针与记忆不一致……跨表一致性无法靠单事务长期保证。→ 靠对账自愈。
4. **删除必须"只有一条路"且可证明**：WORM 账本写失败会让删除卡在 `VERIFYING`；屏障冷却到期要收尾；墓碑要到期清除。→ 靠删除补偿。
5. **成本会失控**：视图/向量可能超预算堆积。→ 靠视图成本回收。

一句话：**凡是"没有天然触发时机"或"跨进程/跨表会失配"的收敛工作，都归 Convergence。**

---

## 三、部署形态：单实例 + advisory lock

Convergence 与 API/Worker 同镜像同库，用不同命令启动。它**必须单实例运行**（否则同一批状态会被重复推进、删除重试会打架）。实现方式是 PostgreSQL advisory lock：

```sql
-- 非阻塞抢占：抢到才跑，抢不到就空转轮询（主备语义）
SELECT pg_try_advisory_lock(hashtextextended('<锁名>', 0));
```

设计上有**两把独立的调度锁**，对应两类调度职责：

| 锁名 | 覆盖职责 | 节奏 |
|---|---|---|
| `mms:scheduler:lifecycle` | 职责一（生命周期扫描）+ 职责二（删除补偿）+ 职责三（定时对账） | 每 60s 一轮；其中对账每 300s 一次 |
| `mms:scheduler:view-cost` | 职责四（视图成本回收 + 容量观测） | 每 60s 一轮 |

> 设计意图：备实例不断轮询抢锁，主实例挂了锁释放，备实例顶上——**用一把 DB 锁实现主备切换，不引入额外选主组件**。

**部署入口**：`mms-convergence` 部署单元由两个独立单实例进程承载，各持一把调度锁：

| 命令 | 覆盖职责 | 说明 |
|---|---|---|
| `mms-lifecycle` | 职责一 + 二 + 三 | 生命周期扫描 + 删除补偿 + 定时对账；额外配 `MMS_DELETION_DATABASE_URL`（删除专用账号，供墓碑清理提权） |
| `mms-view-cost` | 职责四 | 视图成本回收 + 容量观测 |

> 注意区分：`mms-outbox` 是**独立的 Outbox 发布 Worker**（属 Worker 部署单元），不属于 Convergence；JOB 对账只是回收它的超租约行，并不代它发布事件。

---

## 四、共性执行骨架（五个职责的共性）

五个职责干的活不同，但干活的套路完全一样：**定时唤醒 → 抢单实例锁 → 扫描发现漂移 → 能修则幂等修复、修不了只上报 → 留痕打点 → 释放锁休眠**。本章集中讲共性；各职责的判据、数据来源与个性细节见第七~十一章。

### 4.1 共性骨架

```mermaid
graph TD
    A["定时唤醒（60s 轮询）"] --> B{"抢到本调度的<br/>单实例锁？"}
    B -->|否| Z["跳过（本机是备）→ 休眠"]
    B -->|是| C["按到期索引取到点的行（限量分批，绝不扫全库）<br/>（时间到期 / 租约超期 / 跨表失配 / 超预算）"]
    C --> D{"漂移能自己修吗？"}
    D -->|能| E["幂等修复：推进状态 / 回队 / 重建 / 淘汰<br/>修复动作可安全重复执行"]
    D -->|不能| F["只上报：计 mismatch / 打 gauge<br/>触发告警，绝不报绿"]
    E --> G["留痕：运行记录 + 逐通道指标"]
    F --> G
    G --> H["释放锁 → 休眠，进入下一轮"]
```

### 4.2 五个职责的共性清单

| 共性 | 说明 |
|---|---|
| 定时触发，不依赖流量 | 全部由调度循环周期性唤醒，没有新请求、没有新事件时照样收敛——这是它与 API/Worker 的根本区别 |
| 索引驱动、限量分批 | 各状态表按到期字段建部分索引，每轮只取到点的行、限量分批；对账与成本调度按客户分区分片轮转（见第九、十章）——1 亿记忆规模下全库扫描不可行（需求 §4.2） |
| 单实例主备 | 每类调度一把 advisory lock，抢到才干活；备实例轮询等位，不引入额外选主组件 |
| 工作对象是"状态漂移" | 管的都是"该往前却没往前"：时间到期未变状态、租约超期未回收、跨表投影失配、预算超额未回收、版本漂移未重建 |
| 幂等可重放 | 所有修复动作按当前状态判定、可安全重复执行；本轮修不完/修失败，下一轮接着修，不需要人工介入 |
| 只调度、不干活 | 只把任务**放回队列**（RETRY/PENDING）或**作废任务**（CANCELLED），从不消费任务、从不代 Worker 调 provider 算向量/合成画像（自检探针是唯一受控例外，且带同意闸，见 9.3） |
| 修复与上报分离 | 能自己修的计入 fixed；修不了的（死信存量、缺证据/缺决策等）只上报，导致对账 PARTIAL 并告警——**绝不把"修不了"报成"没问题"** |
| 异常隔离 | 单个通道/单类对账失败只记日志与运行记录，绝不杀死调度循环；一批收敛动作的原子性与各补偿子任务的独立性分开设计（见 4.3） |
| 合规与预算前置 | 涉及删除、外呼、成本的动作一律先过权限分离、同意闸与节奏控制（删除审核门控见 8.2，跨境同意闸见 9.2/9.3，重试限流见 8.1，视图预算见第十章） |
| 全程可观测 | 每个收敛通道打计数指标，每类对账落运行记录；Convergence 是系统最重要的"收敛是否在动"指标生产者（见第十三章） |

### 4.3 调度循环与事务边界

Convergence 有**两个互不隶属的调度进程**，各持一把锁、各按 60s 唤醒：`mms-lifecycle`（职责一/二/三）与 `mms-view-cost`（职责四）。事务边界遵循一条原则：**"分通道独立事务（通道内原子），补偿与外呼用独立事务/短连接"**——同一通道的状态收敛要么全生效要么全不生效；通道之间互不牵连，一个通道失败不拖垮其他；每个补偿子任务独立失败。

**生命周期调度：一轮的全貌（每 60s）**

```mermaid
flowchart TD
    W(["每 60s 唤醒"]) --> L{"抢到 lifecycle 锁？"}
    L -->|"否（本机是备）"| S(["休眠，等下一轮"])
    L -->|是| T1

    subgraph T1["职责一：生命周期扫描 ｜ 五通道，各独立事务"]
        A["按到期索引分通道取到点的行，限量分批：<br/>候选域 → 记忆/冲突域 → 屏障收尾<br/>→ 关闭与投影作废 → inbox 清理<br/>（通道内原子，一类失败不拖垮其他）"]
    end

    T1 -->|提交| T2

    subgraph T2["职责二：删除与保留补偿 ｜ 三个独立事务，顺序执行"]
        B1["8.1 VERIFYING 删除重试<br/>每条请求一个独立事务，单条失败不牵连"]
        B2["8.2 保留同意入队<br/>独立事务，入队前 FOR UPDATE 复核"]
        B3["8.3 墓碑到期清理<br/>独立事务 + 删除专用账号连接"]
        B1 --> B2 --> B3
    end

    T2 --> G{"距上次对账 ≥ 300s？"}
    G -->|否| R["释放锁"]
    G -->|是| T3

    subgraph T3["职责三：四类对账 ｜ 顺序执行，异常只记日志、绝不杀死循环"]
        C1["JOB：队列索引扫描"] --> C2["VECTOR：每分片单事务"]
        C2 --> C3["RECALL：短连接外呼<br/>不钉住对账事务"]
        C3 --> C4["BUSINESS：每分片单事务"]
    end

    T3 --> R
    R --> S
```

**视图成本调度：独立进程，与上图并行、互不等待**

```mermaid
flowchart LR
    W(["每 60s 唤醒"]) --> L{"抢到 view-cost 锁？"}
    L -->|"否（本机是备）"| S(["休眠，等下一轮"])
    L -->|是| C["职责四：超额视图淘汰<br/>+ 向量容量观测"]
    C --> R["释放锁"] --> S
```

**事务边界为什么这样划**

| 动作 | 事务边界 | 理由 |
|---|---|---|
| 职责一：14 项状态收敛 | **分通道独立事务**（五通道，通道内原子） | "记忆关闭 → 投影作废"的因果整体在**同一通道内按客户批原子**，不留"记忆已关闭但向量还活着"的中间态；通道间互不牵连，一类失败不拖垮其他；1 亿规模下跨全部通道的大单事务不可行（需求 §4.2） |
| 8.1 删除重试 | **逐条独立事务** | 每条删除幂等可重放；单条失败留在 VERIFYING 下轮再试，不牵连其他重试 |
| 8.2 保留同意入队 | 独立事务 | 复核与入队原子完成；失败只影响本子任务 |
| 8.3 墓碑清理 | 独立事务 + **删除专用账号** | 业务账号对墓碑无 DELETE 权限，必须换连接；失败独立暴露，不中断整批收敛 |
| JOB / VECTOR / BUSINESS 对账 | JOB 队列索引扫描单事务；VECTOR/BUSINESS 每分片一个单事务 | 一批修复动作原子生效；运行结果落 `recon_run` |
| RECALL canary | **短连接**，不开长事务 | provider 外呼耗时不可控，不能钉住对账事务与连接 |

**三个容易忽略的细节**
1. **锁只在干活期间持有**：从抢锁到释放就是一轮工作；备实例轮询等位，主实例崩溃即可接管。
2. **对账异常被捕获并记录**：provider/DB 抖动只让该类对账记 FAILED，绝不拖垮生命周期调度循环。
3. **pg_notify 广播**：屏障由 `COOLING_DOWN` 转 `COMPLETED` 时发 `mms_barrier_changed` 通知，让检索层缓存及时失效。

---

## 五、职责全景、先后顺序与协作关系

### 5.1 全景：一条主干 + 四种配套

五个职责不是并列的五件事，而是**"一条主干 + 四种配套"**：职责一负责状态随时间正确演进（日常新陈代谢），职责二保证删除链路一定走到终点（专项收尾），职责三保证走歪了能修回来（兜底体检），职责四保证修复别把预算修爆（成本制衡），职责五保证规则换了存量数据能跟上（版本演进）。

```mermaid
graph TD
    L1["职责一：生命周期扫描（主干）<br/>状态随时间正确演进"]
    L2["职责二：删除与保留补偿<br/>删除收尾"]
    L3["职责三：四类对账<br/>一致性兜底"]
    L4["职责四：视图成本与容量<br/>成本制衡"]
    L5["职责五：版本检查与重建<br/>版本演进"]
    L1 -->|"扫描事务提交后顺序执行"| L2
    L2 -->|"重试成功→屏障 COOLING_DOWN<br/>冷却期满由职责一 #8 收尾"| L1
    L1 -->|"每 300s 顺带触发"| L3
    L3 -->|"修漂移：Job 回队/视图重建/补指针<br/>DELETE_PENDING→DELETED 收尾"| L1
    L3 -.->|"9.2 回填缺向量做加法<br/>vs 淘汰超额视图做减法"| L4
    L5 -.->|"发起 REBUILD/RENORMALIZE 批任务<br/>收敛结果由职责三验证"| L3
```

> 实线 = 调用与接力关系；虚线 = 制衡与验证关系。职责一↔二、一↔三都是双向闭环：二把删除推到 COOLING_DOWN 交给一收尾；一把状态推进后遗留的投影交给三收尾。

**部署归属**：

| 职责 | 归属 | 节奏 | 单实例锁 |
|---|---|---|---|
| 一 + 二 | **同进程、同轮次**（上半场分通道独立事务扫描 + 下半场独立事务补偿） | `mms-lifecycle`，每 60s | `mms:scheduler:lifecycle` |
| 三 | **同进程顺带**（每 300s 跑一轮四类对账） | 同上，共用同一把锁 | 同上 |
| 四 | **独立进程**（另一把锁，与前三个互不隶属） | `mms-view-cost`，每 60s | `mms:scheduler:view-cost` |
| 五 | **低频版本检查**（设计口径：随生命周期调度顺带检查注册表/策略 ACTIVE 版本） | 批任务由 Worker 消费，结果由职责三验证 | 复用 lifecycle 锁 |

### 5.2 一轮调度内的先后顺序

生命周期调度每一轮（60s）内部是**严格顺序**的，顺序本身是设计的一部分：

| 步骤 | 执行内容 | 为什么排在这里 |
|---|---|---|
| ① | **职责一**：14 项状态收敛按五通道执行，各通道独立事务、到期索引取行、限量分批（候选域 → 记忆/冲突域 → 屏障收尾 → 关闭与投影作废 → inbox 清理） | 先把"到点必须变"的状态推进到位，给本轮后续动作一个**正确的状态基线**；先收敛记忆状态，再在**同一通道内**作废其投影与任务，避免为已关闭记忆白算向量 |
| ② | **职责二**：扫描事务提交后，顺序执行 8.1 删除重试 → 8.2 保留同意入队 → 8.3 墓碑清理（各自独立事务） | 删除补偿依赖①建立的状态基线（如屏障状态）；独立事务保证权限不足/单条失败不中断整批收敛；墓碑清理放最后，确保本轮新写墓碑不会被误清 |
| ③ | **职责三**：距上次对账 ≥300s 时，依次跑 JOB → VECTOR → RECALL → BUSINESS | 对账放在扫描与补偿**之后**，避免把正在推进的中间态误判成漂移；四类内部先修任务队列（JOB），再修数据投影（VECTOR），然后体检召回效果（RECALL），最后核对业务不变量（BUSINESS）——**先保证"任务在动"，再保证"数据对齐"，最后验证"效果正确"** |
| ④ | 释放锁、休眠 | 锁只在干活期间持有，备实例随时可接管 |

**职责四**在独立进程里按自己的 60s 节奏并行运行，与①②③无执行顺序依赖；它与职责三的"加法"（回填缺向量）通过**预算**间接制衡（见 5.3），不需要也不应该共用一把锁——成本回收不能被生命周期调度的故障拖住。

**职责五**是低频的版本演进编排：检查注册表/策略 ACTIVE 版本 → 发现漂移 → 发起 REBUILD/RENORMALIZE 批任务（BACKFILL 优先级，给在线任务让路）→ Worker 异步消费 → 职责三在后续轮次验证收敛结果。它不与①②③抢执行窗口，批任务天然削峰。

### 5.3 两两协作关系

| 协作对 | 关系 | 具体接力 |
|---|---|---|
| 一 ↔ 二 | **双向闭环**（时间语义 ↔ 删除收尾） | 二 8.1 重试成功 → 屏障转 `COOLING_DOWN` → 一 #8 在冷却期满后收尾 `COMPLETED`；一 #11–13 负责记忆关闭后的投影作废，二负责把删除请求本身推到终点 |
| 一 ↔ 三 | **互补**（时间推进 ↔ 漂移修复） | 一 #13 取消已关闭记忆的 Job，三 9.1 回收 Worker 崩溃留下的超租约 Job；一 #10 只**删**无效 current 指针，三 9.4 双向修（删无效 + **补**缺失）；一 #11/#12 删向量、视图置 DELETED，三 9.2 反过来**重建**缺失视图、回填缺向量 |
| 二 → 三 | **删除投影收尾** | 二把屏障推到 COMPLETED 后，遗留的 `DELETE_PENDING` 记忆由三 9.4 推进为 `DELETED`；无活跃屏障的 DELETE_PENDING 只上报 |
| 三 ↔ 四 | **制衡**（加法 ↔ 减法） | 三 9.2 回填缺向量是**做加法**保完整性（"该有的都有"），四淘汰超额视图是**做减法**保预算（"不该多的不多"）；四的级联淘汰手法（删向量 → 取消 Job → 删视图）与一 #11–13 完全一致，保证三处投影同生共死 |
| 五 → 三 | **发起 ↔ 验证** | 五发起 REBUILD/RENORMALIZE 批任务，Worker 消费后的收敛结果由三验证：VECTOR 对账删退役模型向量、回填 ACTIVE 模型缺向量（被动兜底），BUSINESS 对账以 `RENORM_LAG_RATIO` 观测规范化滞后是否归零 |
| 三/四 → Worker | **回队 ↔ 消费** | 三把卡住的任务打回 RETRY/PENDING、四把超额视图的 Job 置 CANCELLED，实际消费永远由 Worker 完成——Convergence 绝不越界干活 |

### 5.4 一次删除的完整接力（端到端示例）

1. **职责二 8.2**：保留同意失效（或用户发起删除）→ 创建删除请求、建屏障 `ACTIVE`（客户自助与同意撤回自动批准；运营发起受审核门控）；
2. **职责二 8.1**：批准后执行删除，WORM 账本写失败卡 `VERIFYING` → 限流重试直至成功 → 请求 `COMPLETED`、写墓碑、屏障转 `COOLING_DOWN`；批准 → 完成 ≤7 天，超时触发 `MMSDeletionDeadlineBreach`（需求 §5.2）；
3. **职责一 #8**：冷却期（30 天）满 → 屏障 `COMPLETED` + `pg_notify` 广播；
4. **职责三 9.4**：屏障 COMPLETED 后把遗留 `DELETE_PENDING` 记忆推进 `DELETED`；
5. **职责一 #11–13**：记忆关闭 → 物理删向量、视图置 DELETED、待办 Job 置 CANCELLED；
6. **职责二 8.3**：墓碑保留期（400 天）满 → 删除专用账号物理清除墓碑。

各职责的详细判据与数据来源见第七~十一章。

---

## 六、状态与枚举速查

本节内联定义本文涉及的枚举（只列本文用到的取值；完整状态机见[《11-记忆的生命周期与状态机》](../2-详细设计/12-状态机.md)与[《2-Worker组件设计》](3-Worker组件设计.md)各 Job 章节）。

**记忆生命周期（`customer_memory.lifecycle_status`）**：

| 状态 | 含义 |
|---|---|
| `ACTIVE` | 生效中，可被检索 |
| `CONFLICTED` | 存在未决冲突，走待核实通道 |
| `RESOLVED` | 时效到期自然解决（历史态，可被 HISTORY_DEBUG 召回） |
| `SUPERSEDED` | 被新记忆取代（历史态） |
| `EXPIRED` | 过期失效（历史态） |
| `DELETE_PENDING` | 删除进行中，等待屏障收尾 |
| `DELETED` | 已删除（终态，投影全部作废） |

**候选（`memory_candidate.candidate_status`）**：`RECEIVED`（已受理）/ `VALIDATED`（已校验）/ `PENDING_REVIEW`（待审核）/ `EXPIRED`（SLA 或 TTL 过期，内容字段清空）。

**冲突（`memory_conflict.status`）**：`OPEN`（未决）/ `IN_REVIEW`（审核中）/ `RESOLVED`（已解决）/ `DISMISSED`（已驳回）。解决原因：`RESOLVED_BY_EVENT`（目标记忆已解决联动）/ `CONFLICT_TIMEOUT_EXPIRED`（超时强制解决，禁止永久 CONFLICTED）。

**审核任务（`memory_review_task.status`）**：`PENDING`（待处理）/ `IN_REVIEW`（处理中）/ `EXPIRED`（关联对象过期/关闭而连带关闭）。

**删除请求（`memory_deletion_request.status`）**：`RECEIVED`（已受理，待审核门控）/ `VERIFYING`（执行中，WORM 账本待写成功）/ `COMPLETED`（删除完成，墓碑已落账）。

**删除屏障（`customer_deletion_barrier.status`）**：`ACTIVE`（删除进行中，挡新写入）/ `COOLING_DOWN`（删除完成，冷却期中）/ `COMPLETED`（冷却期满，终态）。

**Job 状态**（与 Worker 文档同口径）：

| 队列 | 状态枚举 |
|---|---|
| embedding / profile（v1.5） | `PENDING` / `RUNNING` / `READY` / `RETRY` / `DEAD_LETTER` / `CANCELLED` |
| ingestion | `RECEIVED` / `PROCESSING` / `COMPLETED` / `RETRY` / `DEAD_LETTER` / `CANCELLED` |
| outbox | `PENDING` / `RETRY` / `PUBLISHED` / `FAILED`（无 CANCELLED，被屏障挡住时延迟重试） |

**Job 取消原因码**：`LIFECYCLE_CLOSED`（记忆已关闭，职责一 #13）/ `VIEW_EVICTED`（视图超预算淘汰，职责四）。

**向量操作（`memory_embedding_job.operation`）**：`UPSERT`（生成/更新向量）/ `DELETE`（删除向量投影）/ `REBUILD`（模型/维度升级后重建，由职责五发起，BACKFILL 优先级）。另有投影重算批任务 `RENORMALIZE`（规范句/投影片段按新版本重算，职责五发起，见第十一章）。

**视图（`memory_search_document`）**：view_kind = `ORIGINAL`（证据原文语言）/ `CANONICAL`（规范句，und）；不建 TRANSLATED 译文视图（需求 §6.2 已决，跨语言召回由多语言向量模型承担）；status = `READY`（可检索）/ `DELETED`（已作废）。

**对账类型（recon_type）**：`JOB`（任务队列健康）/ `VECTOR`（视图/向量一致性）/ `RECALL`（自检探针：原文自召回，管道健康，不计质量 SLO）/ `BUSINESS`（业务不变量）。

**对账运行记录（`recon_run`）**：status = `RUNNING` / `COMPLETED`（mismatch 全部修复）/ `PARTIAL`（存在只上报项，触发告警）/ `FAILED`（执行异常）；triggered_by = `scheduler`（定时）/ `manual`（内部接口或命令行手动）。

**同意（`customer_memory_consent`）**：status = `GRANTED`（已授权）/ `DENIED`（已拒绝）/ `WITHDRAWN`（已撤回）；本文涉及的 purpose = `MEMORY_RETENTION`（记忆保留）/ `CROSS_BORDER_EMBEDDING`（跨境向量化）。

**模型注册表（`embedding_model_registry.status`）**：`ACTIVE`（生产在用）/ `SHADOW`（影子评估）/ `RETIRED`（已退役，其向量应被清除）。

**其他**：时效记忆类型 = `SERVICE_HISTORY` / `ACTIVE_ISSUE` / `COMMITMENT`（到期自动 RESOLVED）；基数 cardinality = `SINGLE_VALUE`（单值槽，需 current 指针唯一）；记忆种类 memory_kind = `FACT`（单值事实）/ `PREFERENCE`（多值偏好）/ `EPISODIC`（情景记录，到期归档，见第七章 #15）；客户画像为派生投影（v1.5，见[《3-Worker组件设计》](3-Worker组件设计.md)第九章）。

---

## 七、职责一：生命周期状态扫描与收敛

生命周期扫描按**到期索引**取到点的行，分五个通道执行，各通道独立事务（通道内原子、限量分批、本轮修不完下轮接着修），**每条状态都必须有出口、不依赖人工点击**（状态机设计见[《11-记忆的生命周期与状态机》](../2-详细设计/12-状态机.md)）。全表谓词扫描在 1 亿记忆规模下不可行（需求 §4.2）。逐项：

| # | 动作 | 触发判据 | 效果 |
|---|---|---|---|
| 1 | 候选 SLA 过期 | `RECEIVED/VALIDATED/PENDING_REVIEW` 超 `review_sla_days`（默认 30） | 置 `EXPIRED`、清 `content_redacted/semantic_text/source_refs`，计 `CANDIDATE_EXPIRED_BY_SLA` 并告警审核队列 owner |
| 2 | 候选 TTL 过期 | `expires_at<=now()` | 置 `EXPIRED` |
| 3 | 候选审核任务过期 | 关联候选已 EXPIRED | 审核任务 `EXPIRED` |
| 4 | 时效记忆到期 | `valid_to<=now()` 且类型∈SERVICE_HISTORY/ACTIVE_ISSUE/COMMITMENT | 记忆 `RESOLVED`、撤 current |
| 5 | 冲突事件解决 | 目标记忆已 RESOLVED | 冲突 `RESOLVED`（`RESOLVED_BY_EVENT`） |
| 6 | 冲突超时过期 | 超 `conflict_max_age_days`（默认 90） | `RESOLVED`（`CONFLICT_TIMEOUT_EXPIRED`），**禁止永久 CONFLICTED** |
| 7 | 冲突审核关闭 | 冲突已 RESOLVED/DISMISSED | 关联审核任务 `EXPIRED` |
| 8 | 屏障收尾 | `COOLING_DOWN` 超 `deletion_cooldown_days`(30) | 转 `COMPLETED` + `pg_notify` |
| 9 | 记忆过期 | `expires_at<=now()` 或 冲突超时 | 置 `EXPIRED`、撤 current |
| 10 | current 指针清理 | 非 current / 非活动态 | 删 `customer_memory_current` |
| 11 | 向量失效 | 记忆∈SUPERSEDED/EXPIRED/DELETE_PENDING/DELETED | **物理删除** `memory_embedding` |
| 12 | 视图失效 | 同上 | `memory_search_document` 置 `DELETED` |
| 13 | Job 取消 | 同上且活动态 | embedding/profile Job 置 `CANCELLED`（`LIFECYCLE_CLOSED`） |
| 14 | inbox 清理 | `COMPLETED` 且超 `inbox_retention_days`(7) | 删除幂等账本行 |
| 15 | 情景归档 | 情景记录超 `episode_archive_days`（策略，默认 365★） | 置 `EXPIRED`（归档语义），不再参与日常回答，历史查询仍可召回 |

**通道划分与到期索引**（每通道独立事务；所有取数必须走部分索引，禁止全表扫描）：

| 通道 | 覆盖项 | 到期索引（部分索引，只覆盖活动态） |
|---|---|---|
| 候选域 | #1–3 | `memory_candidate(created_at)` / `(expires_at)`，仅待处理状态 |
| 记忆/冲突域 | #4–7、#15 | `customer_memory(valid_to)`、`(expires_at)`，仅活动态；`memory_conflict(opened_at)`，仅未决 |
| 屏障收尾 | #8 | `customer_deletion_barrier(completed_at)`，仅 COOLING_DOWN |
| 关闭与投影作废 | #9–13 | `customer_memory(expires_at)` 活动态；投影作废由"本轮关闭的记忆"驱动，**按客户批同一事务**（删向量 → 视图作废 → 取消 Job），不留"记忆已关闭但向量还活着"的中间态 |
| inbox 清理 | #14 | `memory_inbox_event(updated_at)`，仅 COMPLETED |

每通道每轮 LIMIT `lifecycle_batch_size`（默认 500★）；通道失败只记日志与运行记录并告警，不影响其他通道。

每项计数都打点 `LIFECYCLE_TRANSITIONS{transition=...}`，便于观测各收敛通道是否在动。

**要点**：第 11–13 项正是"删除/过期只有一条路"在异步侧的落地——记忆一旦关闭，向量、视图、待办任务被同步作废，**绝不为已关闭的记忆继续算向量**。

**大致流程**（粗粒度，分通道独立事务、到期索引取行）：

```mermaid
graph TD
    A["按到期索引分通道取到点的行"] --> B["候选域收敛：SLA/TTL 过期<br/>连带关闭候选审核任务"]
    B --> C["记忆/冲突域收敛：时效到期 RESOLVED<br/>冲突解决/超时、记忆 EXPIRED、情景归档、关闭冲突审核"]
    C --> D["屏障收尾：COOLING_DOWN→COMPLETED<br/>逐客户 pg_notify"]
    D --> E["关闭与投影作废（按客户批同一事务）：<br/>撤 current 指针、物理删向量<br/>视图置 DELETED、两类 Job 置 CANCELLED"]
    E --> F["清理 inbox 幂等账本"]
    F --> G["各通道独立提交，逐通道打点 LIFECYCLE_TRANSITIONS"]
```

**数据来源与输出**（表级口径，编号对应上表；**下列过滤条件一律经到期部分索引实现，禁止全表扫描**；所有写回同时刷新 `updated_at`，各通道计数统一打点 `LIFECYCLE_TRANSITIONS{transition}`）：

| # | 数据来源（表 · 字段） | 过滤条件 | 输出（写回 / 指标） |
|---|---|---|---|
| 1 | `memory_candidate`（candidate_status, created_at, policy_version）+ `memory_policy`（按 name@version=policy_version 关联，取 config->>'review_sla_days'） | candidate_status∈{RECEIVED,VALIDATED,PENDING_REVIEW} 且 created_at 超 review_sla_days（缺省 30 天） | 置 EXPIRED，清 content_redacted/semantic_text/source_refs；`CANDIDATE_EXPIRED_BY_SLA` |
| 2 | `memory_candidate`（candidate_status, expires_at） | candidate_status∈{RECEIVED,VALIDATED} 且 expires_at<=now() | 同上：置 EXPIRED + 清三字段 |
| 3 | `memory_review_task` t + `memory_candidate` c（按 candidate_id 关联） | c.candidate_status='EXPIRED' 且 t.status∈{PENDING,IN_REVIEW} | t.status=EXPIRED、resolved_at=now() |
| 4 | `customer_memory`（lifecycle_status, valid_to, memory_type） | lifecycle_status∈{ACTIVE,CONFLICTED} 且 valid_to<=now() 且 memory_type∈{SERVICE_HISTORY,ACTIVE_ISSUE,COMMITMENT} | lifecycle_status=RESOLVED、current_flag=false |
| 5 | `memory_conflict` c + `customer_memory` m（按 m.id=c.current_memory_id 关联） | m.lifecycle_status='RESOLVED' 且 c.status∈{OPEN,IN_REVIEW} | c.status=RESOLVED（RESOLVED_BY_EVENT）、resolved_at=now() |
| 6 | `memory_conflict`（status, opened_at）+ `memory_policy`（全局 ACTIVE：status='ACTIVE' 且 memory_type IS NULL，按 effective_from 取最新，config->>'conflict_max_age_days'） | status∈{OPEN,IN_REVIEW} 且 opened_at 超 conflict_max_age_days（缺省 90 天） | status=RESOLVED（CONFLICT_TIMEOUT_EXPIRED）；`CONFLICT_TIMEOUT_EXPIRED` |
| 7 | `memory_review_task` t + `memory_conflict` c（按 c.id=t.conflict_id 关联） | c.status∈{RESOLVED,DISMISSED} 且 t.status∈{PENDING,IN_REVIEW} | t.status=EXPIRED、resolved_at=now() |
| 8 | `customer_deletion_barrier`（status, completed_at, updated_at, customer_id） | status='COOLING_DOWN' 且 COALESCE(completed_at,updated_at) 超 deletion_cooldown_days（30 天） | status=COMPLETED；逐客户 `pg_notify('mms_barrier_changed')` |
| 9 | `customer_memory`（expires_at, lifecycle_status）+ EXISTS `memory_conflict`（current_memory_id+customer_id 匹配、未决且 opened_at 超时） | expires_at<=now() 且 lifecycle_status∈{ACTIVE,CONFLICTED,RESOLVED,SUPERSEDED}；或 CONFLICTED 且冲突超 conflict_max_age_days | lifecycle_status=EXPIRED、current_flag=false |
| 10 | `customer_memory_current` c + `customer_memory` m（按 memory_id+customer_id 关联） | NOT m.current_flag 或 m.lifecycle_status∉{ACTIVE,CONFLICTED} | 物理删除指针行 |
| 11 | `memory_embedding` e + `customer_memory` m（按 memory_id+customer_id 关联） | m.lifecycle_status∈{SUPERSEDED,EXPIRED,DELETE_PENDING,DELETED} | 物理删除向量行 |
| 12 | `memory_search_document` d + `customer_memory` m（同上关联） | 同 #11 且 d.status<>'DELETED' | d.status=DELETED |
| 13 | `memory_embedding_job` / `memory_profile_job`（v1.5） j + `customer_memory` m（同上关联） | 记忆已关闭（同 #11）且 j.status∈{PENDING,RUNNING,RETRY} | j.status=CANCELLED（LIFECYCLE_CLOSED），清 locked_by/locked_at/lease_expires_at |
| 14 | `memory_inbox_event`（status, updated_at） | status='COMPLETED' 且 updated_at 超 inbox_retention_days（7 天） | 物理删除幂等账本行 |
| 15 | `customer_memory`（memory_kind, created_at, lifecycle_status） | memory_kind='EPISODIC' 且 lifecycle_status='ACTIVE' 且 created_at 超 episode_archive_days（缺省 365★） | lifecycle_status=EXPIRED（归档）；`EPISODE_ARCHIVED` |

---

## 八、职责二：删除与保留补偿

紧接扫描事务提交之后，在**独立事务**里顺序执行三个补偿子任务（事务边界见 4.3）：

### 8.1 VERIFYING 删除重试
WORM 墓碑账本写失败会让删除请求卡在 `VERIFYING`、屏障停在 `ACTIVE`，客户一直"删不掉"。Convergence 按 `deletion_retry_seconds`(300) 限流，对最老的至多 10 条重放删除执行（墓碑 key 按 request+scope 幂等，可安全重试）。同时导出 `DELETION_VERIFYING`（存量）与 `TOMBSTONE_LEDGER_LAG`（最老等待时长）供 `MMSTombstoneLedgerFailure` 告警。**删除时限观测（需求 §5.2/§7）**：请求批准 → 完成 >7 天 → `MMSDeletionDeadlineBreach`（page）。

### 8.2 保留同意驱动删除
`MEMORY_RETENTION` 判定点。明确 `DENIED`/`WITHDRAWN` 或已过期 `GRANTED` 的客户 → 失去持有合法基础，创建/复用删除请求推入删除队列（**同意撤回是客户行为，视同自助删除自动批准**，需求 §5.2；运营发起的删除才受审核门控）。
- **关键保守设计**：**仅"快照缺失"不批量建屏障，只告警**——否则一次同意服务同步中断会被放大成全量停摆。
- 入队前在事务内以 `FOR UPDATE` 复核，期间已重新 `GRANTED` 的跳过。

### 8.3 墓碑到期清理
`retain_until<=now()` 的 `deletion_tombstone` 物理清除。
- **权限使然必须独立于扫描事务**：扫描以业务账号跑，对 `deletion_tombstone` 无 DELETE 权限；用删除专用账号独立连接在扫描之后单独执行，既避免权限不足中断整批过期收敛，也让清理失败能独立暴露。

**大致流程**（粗粒度，三个子任务各自独立事务、顺序执行）：

```mermaid
graph TD
    A["扫描事务提交后"] --> B["8.1 VERIFYING 删除重试<br/>限流 300s，取最老 ≤10 条重放删除"]
    B --> C["8.2 保留同意驱动删除<br/>失去持有合法基础的客户入删除队列"]
    C --> D["8.3 墓碑到期清理<br/>删除专用账号物理删除到期墓碑"]
    B -.-> E["成功→COMPLETED+写墓碑<br/>失败→留 VERIFYING 下轮再试"]
    C -.-> F["入队前 FOR UPDATE 复核<br/>已重新 GRANTED 则跳过"]
```

**数据来源与输出**（表级口径）：

| 子任务 | 数据来源（表 · 字段） | 过滤条件 | 输出（写回 / 指标） |
|---|---|---|---|
| 8.1 VERIFYING 重试 | `memory_deletion_request` r（id, customer_id, approved_by, status, updated_at）+ `customer_deletion_barrier` b（按 customer_id 关联且 b.status='ACTIVE'）；另统计全量 VERIFYING 的 count 与 min(updated_at) | r.status='VERIFYING' 且 r.updated_at < now()-deletion_retry_seconds（300s），ORDER BY r.updated_at LIMIT 10 | 逐条独立事务重放删除执行：成功→请求 COMPLETED、屏障转 COOLING_DOWN（由职责一 #8 收尾）、写 `deletion_tombstone` 与 `memory_deletion_item`(VERIFIED)；失败→保持 VERIFYING；指标 `DELETION_VERIFYING`、`TOMBSTONE_LEDGER_LAG`、`DELETION_RETRY{result}` |
| 8.2 保留同意删除 | `customer_memory` m（customer_id, lifecycle_status）LEFT JOIN `customer_memory_consent` k（purpose='MEMORY_RETENTION'，取 status/version/expires_at） | m.lifecycle_status∈{ACTIVE,CONFLICTED} 且（k.status∈{DENIED,WITHDRAWN} 或 k.status='GRANTED' 且 k.expires_at<=now()）；快照缺失不入选只告警；入队前事务内 FOR UPDATE 复核 | 删除请求受理：`memory_deletion_request` 新增 RECEIVED（幂等键 retention-consent:{customer}:{version}，source=SYSTEM/CONSENT_WITHDRAWN）+ `customer_deletion_barrier` 置 ACTIVE + `memory_outbox_event` barrier.changed 事件 + 自动批准记录（同意撤回视同自助删除，无需人工审核）；指标 retention_consent_deletions |
| 8.3 墓碑清理 | `deletion_tombstone`（retain_until） | retain_until<=now() | 物理删除到期墓碑（删除专用账号独立连接，`trg_tombstone_purge_guard` 拒绝未到期删除）；指标 tombstones_purged |

---

## 九、职责三：对账自愈（每 300s，四类 recon）

**执行方式（需求 §4.2：禁止全库扫描）**：JOB 对账只查队列表的活动态部分索引（队列活动行天然少）；VECTOR/BUSINESS 对账**按客户分区分片轮转**——每轮只扫 1/N 个分片（`recon_shard_count`，默认 16★），N 轮覆盖全量，每分片一个单事务；RECALL 采样不变。下文各"数据来源与输出"表均为**单分片内**的逻辑口径。

每类对账每次运行先写一行 `recon_run(triggered_by='scheduler')`，结束按 `mismatch==fixed` 判定 `COMPLETED/PARTIAL/FAILED`。核心区分两类操作（共性口径见 4.2）：
- **修复（fix）**：MMS 能自己修的（计入 fixed）。
- **上报（observe）**：MMS 修不了、只能上报的（导致 PARTIAL 并触发对账告警，而非报绿）。

四类对账各管一片，通用执行骨架如下：

```mermaid
graph TD
    A["对账执行（按类型）"] --> B["写 recon_run：status=RUNNING<br/>triggered_by='scheduler'"]
    B --> C{"recon_type"}
    C -->|JOB / VECTOR / BUSINESS| D["每分片单事务执行<br/>修复(fix) / 只上报(observe)"]
    C -->|RECALL| E["短连接执行 canary<br/>不钉住对账事务"]
    D --> F{"mismatch == fixed？"}
    E --> F
    F -->|是| G["recon_run=COMPLETED"]
    F -->|否| H["recon_run=PARTIAL，触发对账告警"]
    D -.->|异常| I["recon_run=FAILED + error_code"]
    E -.->|异常| I
    G --> J["打点 RECON_MISMATCHES{recon_type}"]
    H --> J
```

### 9.1 JOB —— 任务队列健康
- **超租约回收**：embedding/profile（v1.5）的 `RUNNING`、ingestion 的 `PROCESSING` 且 `lease_expires_at<=now()` → 打回 `RETRY`、清锁，等待 Worker 重新认领（**这就是总体架构说的"Embedding 重试"——Convergence 不直接调 provider，只把卡住的任务放回队列**）。
- **Outbox 租约**：`PENDING/RETRY` 且锁超期 → 回 `RETRY`。
- **READY 缺向量**：Job 标 `READY` 但实际无 READY 向量、而记忆与视图仍有效 → 打回 `PENDING`、`attempts=0` 重算。
- **死信存量上报**：三表 `DEAD_LETTER` + Outbox `FAILED` 存量（运维队列，配告警）。

**大致流程**：

```mermaid
graph TD
    A["JOB 对账"] --> B["超租约回收：embedding/profile RUNNING、<br/>ingestion PROCESSING 且租约到期 → RETRY 清锁"]
    B --> C["Outbox 锁超期：PENDING/RETRY → RETRY"]
    C --> D["READY 缺向量：Job 标 READY 但无 READY 向量<br/>且记忆/视图仍有效 → PENDING 重算"]
    D --> E["死信存量观测：三表 DEAD_LETTER + Outbox FAILED<br/>只上报不修复"]
```

**数据来源与输出**：

| 检查项 | 数据来源（表 · 字段） | 过滤条件 | 输出（写回 / 指标） |
|---|---|---|---|
| embedding 租约 | `memory_embedding_job`（status, lease_expires_at） | status='RUNNING' 且 lease_expires_at<=now() | status=RETRY，清 locked_by/locked_at（fix） |
| ingestion 租约 | `memory_ingestion_job`（status, lease_expires_at） | status='PROCESSING' 且 lease_expires_at<=now() | 同上（fix） |
| profile 租约（v1.5） | `memory_profile_job`（status, lease_expires_at） | status='RUNNING' 且 lease_expires_at<=now() | 同上（fix） |
| outbox 租约 | `memory_outbox_event`（status, locked_at） | status∈{PENDING,RETRY} 且 locked_at < now()-outbox_lease_seconds（60s） | status=RETRY，清 locked_by/locked_at（fix） |
| READY 缺向量 | `memory_embedding_job` j + NOT EXISTS `memory_embedding` e（同 customer/memory/model/version/视图键，e.status='READY'）+ EXISTS `customer_memory` m（可索引态）+ EXISTS `memory_search_document` d（同视图键+view_text_hash，d.status='READY'） | j.status='READY' | j.status=PENDING、attempts=0、available_at=now()、清锁（fix） |
| 死信存量 | `memory_embedding_job` / `memory_ingestion_job` / `memory_profile_job`（v1.5）（status='DEAD_LETTER'）+ `memory_outbox_event`（status='FAILED'） | 各者计数求和 | observe dead_letter_stock：计 mismatch 不 fix，导致 PARTIAL |

### 9.2 VECTOR —— 视图/向量一致性

按客户分区分片轮转执行（见第九章开头）；下表为单分片口径。

- 观测各 `view_kind/language` 缺失的 READY 向量（`VIEW_MISSING`）；
- 重建缺失的 ORIGINAL/CANONICAL 文档，回填缺向量（补发 UPSERT Job，由 Worker 消费）；
- 删孤儿 ORIGINAL 视图（无对应证据语言）、hash/状态不符的旧向量、视图丢失/非 READY 的向量、不可索引文档、**退役模型的向量**。

**大致流程**：

```mermaid
graph TD
    A["VECTOR 对账"] --> B["观测缺失：按 view_kind/language 统计<br/>缺 READY 向量的视图（VIEW_MISSING）"]
    B --> C["重建缺失的<br/>CANONICAL / ORIGINAL 文档"]
    C --> D["清理不一致：孤儿 ORIGINAL 视图、hash/状态不符向量<br/>视图丢失向量、不可索引文档"]
    D --> E["退役模型向量删除<br/>（仅当存在 ACTIVE 注册表模型）"]
    E --> F["为缺向量的 READY 视图<br/>补发 UPSERT Job（回填）"]
```

**数据来源与输出**（缺失观测计 mismatch；重建/回填数量记 `recon_run.details`，不计 fixed）：

| 检查项 | 数据来源（表 · 字段） | 过滤条件 | 输出（写回 / 指标） |
|---|---|---|---|
| 缺失观测 | `customer_memory` m + `memory_search_document` d JOIN `memory_embedding` em（同视图键+view_text_hash，em.status='READY'）；ORIGINAL 按 `memory_evidence.content_language` 逐语言核对 | m.lifecycle_status∈{ACTIVE,RESOLVED,CONFLICTED} 且无对应 READY 视图+向量 | `VIEW_MISSING{view_kind,language}` gauge；计 mismatch |
| 重建视图 | `customer_memory`（semantic_text, content, content_hash, content_language）+ `memory_evidence`（content_language, evidence_excerpt_redacted, observed_at） | 可索引态且缺 CANONICAL/und 或 ORIGINAL/证据语言 的 READY 文档 | `memory_search_document` UPSERT 为 READY（CANONICAL 用 semantic_text；主 ORIGINAL 用 content；其他语言用证据脱敏摘录）；数量记 details.repaired_views |
| 孤儿 ORIGINAL 视图 | `memory_search_document` d + NOT EXISTS `memory_evidence`（同 memory_id+customer_id 且 content_language=d.view_language） | d.status='READY' 且 d.view_kind='ORIGINAL' | d.status=DELETED（fix） |
| hash/状态不符向量 | `memory_embedding` e + `customer_memory` m | m.content_hash<>e.content_hash 或 m.lifecycle_status∉{ACTIVE,RESOLVED,CONFLICTED} | 物理删除 e（fix） |
| 视图丢失/非 READY 向量 | `memory_embedding` e + NOT EXISTS `memory_search_document` d（同视图键+view_text_hash，d.status='READY'） | — | 物理删除 e（fix） |
| 不可索引文档 | `memory_search_document` d + `customer_memory` m | d.status='READY' 且（m.content_hash<>d.content_hash 或记忆不可索引） | d.status=DELETED（fix） |
| 退役模型向量 | `memory_embedding` e + `embedding_model_registry` r（按 model_id+version 关联） | 存在 ACTIVE 模型才执行；r.status='RETIRED' 或 r.status∉{ACTIVE,SHADOW} | 物理删除 e（fix） |
| 回填缺向量 | `memory_search_document` d + `customer_memory` m + LEFT JOIN `customer_deletion_barrier` b；模型取 `embedding_model_registry` 的 ACTIVE 行 | d.status='READY'、记忆可索引、无 ACTIVE/COOLING_DOWN/COMPLETED 屏障、ORIGINAL 需证据语言、（外部 provider 时）需 CROSS_BORDER_EMBEDDING 同意、无同模型 READY 向量、无活动 UPSERT Job | 新增 `memory_embedding_job`（UPSERT/PENDING，ACTIVE 模型）；数量记 details.enqueued_missing_vectors |

### 9.3 RECALL —— 自检探针（管道健康）

**定位（需求 §6.2）**：用记忆原文当问题去查、应命中自身——**只证明管道没坏，不证明语义质量，不计入质量 SLO**；语义质量（换说法/跨语言命中率）由离线评测门禁承担（API 管理域）。目标命中率 ≥99%，miss 率 >1% 触发管道告警。

- 按**每种有 READY 向量的语言**采样（`max(1, recon_batch_size//10)` 条/语言，按 `updated_at DESC` 取最近更新的），针对 **ACTIVE 注册表模型**（而非进程设置，避免换模型期误报）；
- **跨境同意闸在采样连接上先判**（stage='canary'）：未授权客户的样本直接跳过，且跳过数 `skipped_cross_border_customers` 可见（防止"同意通道关闭"被误读成"该语言无样本"）；
- 对每条样本做查询向量化后走检索（`HISTORY_DEBUG`、top-10、含敏感），若自身 `memory_id` 未被召回则计一次 miss；
- 用**短连接**执行 provider 调用，不钉住对账事务；结果落 `recon_run.details.semantic_canary`（model/version/per_language/scanned_total/skipped_cross_border_customers）。

**大致流程**：

```mermaid
graph TD
    A["RECALL 对账（短连接）"] --> B["取 ACTIVE 注册表模型<br/>无 ACTIVE → 跳过并记 details"]
    B --> C["按语言采样：每种有 READY 向量的语言<br/>取最近更新 max(1, recon_batch_size//10) 条"]
    C --> D["跨境同意闸（stage='canary'）<br/>未授权客户跳过且计数可见"]
    D --> E["逐样本：查询向量化 → 检索<br/>HISTORY_DEBUG / top-10 / 含敏感"]
    E --> F["自身 memory_id 未召回计 miss<br/>结果落 recon_run.details.semantic_canary"]
```

**数据来源与输出**（只读业务表 + 受控外呼，不写任何业务表；唯一写回是 `recon_run`）：

| 检查项 | 数据来源（表 · 字段） | 过滤条件 | 输出（写回 / 指标） |
|---|---|---|---|
| 模型锚定 | `embedding_model_registry`（model_id, version, status, activated_at） | status='ACTIVE'，ORDER BY activated_at DESC LIMIT 1 | 无 ACTIVE → details.semantic_canary.skipped='no_active_model'，本轮结束 |
| 样本采样 | `memory_search_document` d + `customer_memory` m + `memory_embedding` e（同视图键+view_text_hash+content_hash，e.status='READY'） | d.status='READY'、d.view_kind∈{ORIGINAL,CANONICAL}、m.lifecycle_status∈{ACTIVE,RESOLVED}；row_number() 按 view_language 分区、updated_at DESC，取 rn<=max(1, recon_batch_size//10) | 每语言样本集（customer_id, memory_id, content, view_kind, view_language） |
| 跨境同意闸 | `embedding_model_registry.provider` + `customer_memory_consent`（purpose='CROSS_BORDER_EMBEDDING'，status, expires_at） | provider≠'self-hosted' 时要求 GRANTED 且未过期 | 被阻客户跳过；`EMBEDDING_CROSS_BORDER_BLOCKED{stage='canary'}`；skipped_cross_border_customers 计数 |
| 召回判定 | 查询向量化（ACTIVE 模型）→ 检索（读 `memory_embedding`/`memory_search_document`/`customer_memory`） | HISTORY_DEBUG、top-10、include_sensitive=True | 自身 memory_id 未召回计 miss；details.semantic_canary{model, version, per_language{scanned,misses}, scanned_total, skipped_cross_border_customers}；scanned/mismatch 汇总入 recon_run |

### 9.4 BUSINESS —— 业务不变量

按客户分区分片轮转执行（见第九章开头）；下表为单分片口径。

- **决策安全观测**（gauge，不计入 mismatch）：重复 current 槽 `CURRENT_DUPLICATE`、未决冲突 `CURRENT_CONFLICTS`、规范化滞后 `RENORM_LAG_RATIO`；
- **current 指针修复**（fix）：删无效指针 `invalid_current_pointer`、补缺失指针 `missing_current_pointer`（`customer_memory_current`）；
- **完整性观测**（observe）：ACTIVE/CONFLICTED/RESOLVED 记忆缺证据 `memory_without_evidence`、缺决策 `memory_without_decision`；
- **删除投影收尾**：屏障 `COMPLETED` 后把遗留 `DELETE_PENDING` 推进为 `DELETED`（fix `delete_pending_after_completed_barrier`）；无活跃屏障的 `DELETE_PENDING` 上报（observe `delete_pending_without_barrier`）；
- **画像滞后观测与回填（v1.5）**：观测画像缺失或落后（来源记忆 `updated_at` 新于画像）的客户，补登记 profile Job（BACKFILL 优先级，由 Worker 消费，见[《3-Worker组件设计》](3-Worker组件设计.md)第九章）；gauge `PROFILE_LAG`；
- **陈旧残留抽样（需求 §6.1）**：抽样 ACTIVE 记忆核对"该被替换或过期、却还在用"（对照权威事件与更新证据），计 `STALE_RESIDUE_RATIO`（目标 ≤2%）；机器判不了的样本转人工标注校准——只上报，不自动改状态。

**大致流程**：

```mermaid
graph TD
    A["BUSINESS 对账"] --> B["决策安全观测（gauge，不计 mismatch）<br/>重复 current / 未决冲突 / 规范化滞后"]
    B --> C["current 指针修复（fix）<br/>删无效指针、补缺失指针"]
    C --> D["完整性观测（observe）<br/>缺证据 / 缺决策"]
    D --> E["删除投影收尾：屏障 COMPLETED 后<br/>DELETE_PENDING→DELETED（fix）；无屏障的只上报"]
```

**数据来源与输出**：

| 检查项 | 数据来源（表 · 字段） | 过滤条件 | 输出（写回 / 指标） |
|---|---|---|---|
| 重复 current 槽 | `customer_memory`（customer_id, fact_key, entity_type, entity_ref, current_flag, cardinality, lifecycle_status） | current_flag 且 cardinality='SINGLE_VALUE' 且∈{ACTIVE,CONFLICTED}，GROUP BY 四元组 HAVING count(*)>1 | gauge `CURRENT_DUPLICATE`（不计 mismatch） |
| 未决冲突 | `memory_conflict`（status） | status∈{OPEN,IN_REVIEW} | gauge `CURRENT_CONFLICTS` |
| 规范化滞后 | `customer_memory` m + LATERAL `fact_key_registry` r（同 fact_key、status='ACTIVE'、最高 version） | m.lifecycle_status∈{ACTIVE,CONFLICTED} | gauge `RENORM_LAG_RATIO` = normalization_version 不一致行数/总数 |
| 无效指针 | `customer_memory_current` c + NOT EXISTS `customer_memory` m（同 id/customer/fact_key/entity_type/entity_ref，SINGLE_VALUE+current_flag+∈{ACTIVE,CONFLICTED}） | — | 物理删除指针（fix invalid_current_pointer） |
| 缺失指针 | `customer_memory` m + NOT EXISTS `customer_memory_current`（同四元组） | m.cardinality='SINGLE_VALUE' 且 current_flag 且 lifecycle_status∈{ACTIVE,CONFLICTED} | INSERT 指针（ON CONFLICT 更新 memory_id/generation）（fix missing_current_pointer） |
| 缺证据 | `customer_memory` m + NOT EXISTS `memory_evidence` e（memory_id+customer_id） | m.lifecycle_status∈{ACTIVE,CONFLICTED,RESOLVED} | observe memory_without_evidence |
| 缺决策 | `customer_memory` m + NOT EXISTS `memory_decision` d（customer_id+new_memory_id=m.id） | 同上 | observe memory_without_decision |
| 删除投影收尾 | `customer_memory` m + EXISTS `customer_deletion_barrier` b（同 customer_id，b.status='COMPLETED'） | m.lifecycle_status='DELETE_PENDING' | m.lifecycle_status=DELETED（fix） |
| 无屏障 DELETE_PENDING | `customer_memory` m + NOT EXISTS `customer_deletion_barrier`（status∈{ACTIVE,COOLING_DOWN}） | m.lifecycle_status='DELETE_PENDING' | observe delete_pending_without_barrier |

> 对账除被生命周期调度每 300s 触发外，还可经 `POST /internal/reconciliation/{recon_type}` 手动触发（内部面鉴权见 API 组件设计第五章），或经命令行入口一键跑全部四类。

---

## 十、职责四：视图成本与容量

独立的 view-cost 调度（单实例锁 `mms:scheduler:view-cost`，每 60s），落实成本控制与容量评审；**同样按客户分区分片轮转**（每轮 1/N 分片，禁止全库 GROUP BY）：
- **超额视图回收**：对 READY 视图数超预算（`min(max_views_per_memory, 策略上限)`）的记忆，按成本键 `(命中数, 最近使用时间)` **升序排序**，从队首开始淘汰最少使用的视图直至回到预算内（CANONICAL 与主 ORIGINAL 为基线视图不淘汰；不建译文视图，超预算只可能来自多语言证据的 ORIGINAL）；淘汰时**级联删向量、取消 embedding/profile Job（`VIEW_EVICTED`）、删视图**，保持三处一致（与职责一 #11–13 同一套级联手法）。
- **向量容量观测**：导出每客户 READY 向量分布（`VECTORS_PER_CUSTOMER` 直方图 p50/p95/p99、`VECTORS_PER_CUSTOMER_MAX`、超软限客户数 `CUSTOMERS_OVER_EXACT_SOFT_LIMIT`），喂给容量评审。

**大致流程**：

```mermaid
graph TD
    A["view-cost 调度（60s，单实例锁，分片轮转）"] --> B["找出 READY 视图数超预算的记忆<br/>budget=min(策略上限, max_views_per_memory)"]
    B --> C["取该记忆可淘汰的 READY 视图<br/>按 (命中数, 最近使用时间) 升序排序"]
    C --> D["从队首淘汰直至回到预算内<br/>级联：删向量→取消 Job→删视图"]
    D --> E["容量观测：每客户 READY 向量分布<br/>p50/p95/p99、max、超软限客户数"]
```

**数据来源与输出**：

| 子任务 | 数据来源（表 · 字段） | 过滤条件 | 输出（写回 / 指标） |
|---|---|---|---|
| 超预算记忆 | `customer_memory` m + `memory_policy` p（按 name@version=policy_version 关联，config->>'max_views_per_memory'）+ `memory_search_document` d | d.status='READY'，GROUP BY m.id HAVING count(d.*) > LEAST(策略上限(缺省 6), max_views_per_memory(6)) | 超预算记忆集（budget 取两者较小值） |
| 成本键排序 | `memory_search_document`（view_kind, view_language, status）+ `memory_usage_event`（matched_views, created_at） | 视图：memory_id 匹配、status='READY'、非基线视图（CANONICAL 与主 ORIGINAL 除外）；用量：matched_views 对应视图键 | 排序键 (hits=count(*), last_used=max(created_at)) 升序，队首先淘汰 |
| 级联淘汰 | 同上可淘汰视图行 | remaining > budget，从队首逐条淘汰 | DELETE `memory_embedding`；`memory_embedding_job`/`memory_profile_job`（v1.5）置 CANCELLED（VIEW_EVICTED，仅 PENDING/RUNNING/RETRY）；DELETE `memory_search_document`；指标 `VIEW_EVICTED{language}`、`VIEW_COUNT` |
| 容量观测 | `memory_embedding`（customer_id, status） | status='READY'，GROUP BY customer_id | `VECTORS_PER_CUSTOMER` 直方图（p50/p95/p99）、`VECTORS_PER_CUSTOMER_MAX`、`CUSTOMERS_OVER_EXACT_SOFT_LIMIT`（>exact_scan_soft_limit=2000，即产品上限 1000 条 × 2 视图）；返回 evicted/customers/vectors_max |

---

## 十一、职责五：策略/模型版本检查与重建

总体架构把"策略版本检查、模型升级"列为 Convergence 职责。设计口径：**规则换了，存量数据必须能跟上**——版本演进由 Convergence 主动编排，而不是等检索报错才发现。**版本切换本身受评测门禁约束**（需求 §6.3，门禁执行见 API 文档 7.7）：Convergence 只为已正式启用的版本编排回填，不自行启用任何新版。

- **REBUILD 由 Convergence 发起**：模型/维度/视图升级时的向量重建（`memory_embedding_job.operation='REBUILD'`，枚举见第六章）不由普通决策触发；批任务以 BACKFILL 优先级入队，给在线任务让路。
- **RENORMALIZE 由版本对照驱动**：对照 `fact_key_registry` / `memory_policy` 的 ACTIVE 版本，发现规范化版本/策略漂移时发起 `RENORMALIZE` 批任务，重算规范句与投影；源文本变化同时触发画像重算（v1.5，profile Job，BACKFILL 优先级）。
- **canary 锚定 ACTIVE 模型**（见 9.3）：模型切换期以注册表为准，而非进程设置，避免换模型期误报。
- **被动收敛兜底**：VECTOR 对账删除退役模型向量、回填 ACTIVE 模型缺向量（9.2），BUSINESS 对账以 `RENORM_LAG_RATIO` 观测规范化滞后（9.4）——主动编排未覆盖或批任务失败的漂移，最终仍会被对账收敛。

**大致流程**：

```mermaid
graph TD
    A["版本检查（低频，随生命周期调度）"] --> B["对照注册表 ACTIVE 版本<br/>embedding_model_registry / fact_key_registry / memory_policy"]
    B --> C["发现模型/维度/规范版本漂移"]
    C --> D["发起 REBUILD / RENORMALIZE 批任务<br/>（BACKFILL 优先级，削峰让路）"]
    D --> E["Worker 消费批任务，重建向量/投影"]
    E --> F["职责三验证收敛：VECTOR 回填/清退役<br/>BUSINESS 观测 RENORM_LAG_RATIO 归零"]
```

**数据来源与输出**：

| 检查项 | 数据来源（表 · 字段 · 过滤） | 输出 |
|---|---|---|
| 模型/维度升级 | `embedding_model_registry`（status='ACTIVE' 的 model_id/version/dimension），与存量向量的 model/version/dimension 对照 | `memory_embedding_job.operation='REBUILD'` 批任务（BACKFILL 优先级）；收敛结果由 VECTOR 对账验证（9.2） |
| 规范/策略版本 | `fact_key_registry`（同 fact_key 的 ACTIVE 最高 version、normalization_version）、`memory_policy`（ACTIVE config），与存量记忆的 normalization_version/policy_version 对照 | `RENORMALIZE` 批任务重算规范句/投影；滞后程度由 `RENORM_LAG_RATIO` 观测（9.4） |

---

## 十二、关键配置参数（默认值）

配置统一经环境变量注入（前缀 `MMS_`）；策略类参数来自 `memory_policy.config`，按 name@version 关联：

| 参数 | 值 | 用途 |
|---|---|---|
| `lifecycle_poll_seconds` | 60 | 两个调度循环的轮询间隔 |
| 对账触发间隔 | 300 | 生命周期调度顺带跑对账的频率 |
| `recon_batch_size` | 100 | 对账批量基准；canary 每语言采样 `//10` |
| `recon_shard_count` | 16 | VECTOR/BUSINESS/视图成本按客户分区分片轮转的分片数（N 轮覆盖全量） |
| `lifecycle_batch_size` | 500 | 生命周期扫描每通道每轮取行上限 |
| `review_sla_days`（策略，缺省 30） | — | 候选 SLA 过期 |
| `conflict_max_age_days`（策略，缺省 90） | — | 冲突超时过期 |
| `episode_archive_days`（策略，缺省 365★） | — | 情景记录归档期 |
| `deletion_cooldown_days` | 30 | 屏障 COOLING_DOWN→COMPLETED |
| `deletion_retry_seconds` | 300 | VERIFYING 删除重试限流 |
| `inbox_retention_days` | 7 | 幂等账本清理 |
| `tombstone_retention_days` | 400 | 墓碑保留期（须 > 备份保留+恢复窗口） |
| `backup_retention_days` | 35 | 备份保留期 |
| `max_views_per_memory` | 6 | 每记忆视图预算（与策略上限取较小值） |
| `outbox_lease_seconds` | 60 | Outbox 锁超期判定 |
| `exact_scan_soft_limit` | 2000 | 每客户 READY 向量软限（产品上限 1000 条 × 2 视图，需求 §4.1） |

---

## 十三、可观测性与告警（Convergence 是重要指标生产者）

- **生命周期**：`LIFECYCLE_TRANSITIONS{transition}`（每通道推进量）、`CANDIDATE_EXPIRED_BY_SLA`、`CONFLICT_TIMEOUT_EXPIRED`、`EPISODE_ARCHIVED`。
- **删除补偿**：`DELETION_VERIFYING`、`TOMBSTONE_LEDGER_LAG`、`DELETION_RETRY{result}`、`MMSDeletionDeadlineBreach`（批准 → 完成 >7 天，page）。
- **审核预算（需求 §3.1/§7）**：审核队列积压 >200 条 → `MMSReviewQueueOverflow`（ticket）；每日新增人工审核预算 ≤200 条（高风险事实 + 抽检），超限动作是收紧"高风险"定义或接入更多权威数据源，不是加人。
- **对账**：`RECON_MISMATCHES{recon_type}`、`VIEW_MISSING{view_kind,language}`、`CURRENT_DUPLICATE`、`CURRENT_CONFLICTS`、`RENORM_LAG_RATIO`、`PROFILE_LAG`（v1.5）、`STALE_RESIDUE_RATIO`（陈旧残留抽样，目标 ≤2%，需求 §6.1）。
- **成本/容量**：`VIEW_COUNT`、`VIEW_EVICTED{language}`、`VECTORS_PER_CUSTOMER(_MAX)`、`CUSTOMERS_OVER_EXACT_SOFT_LIMIT`。
- **运行记录**：每次对账落 `recon_run`（type/status/scanned/mismatch/fixed/error/details），PARTIAL/FAILED 即对账告警来源。

---

## 十四、Convergence 与 Worker 的边界

| 维度 | Convergence | Worker |
|---|---|---|
| 实例 | 单实例（advisory lock 主备） | 多实例并行（`SKIP LOCKED`） |
| 触发 | 定时轮询到期状态（到期索引 + 分片轮转，绝不扫全库） | 事件/任务驱动 |
| 是否调外部 provider | 基本不调（自检探针是唯一受控例外，且带同意闸） | 是：提取/向量/画像合成（v1.5）的实际外呼 |
| 典型产物 | 状态推进、RETRY/PENDING 回队、删除补偿、视图淘汰 | READY 向量、业务记忆、客户画像（v1.5） |
| 关系 | **把任务放回队列 / 作废任务** | **把任务消费掉** |

一句话：**Convergence 负责"让系统在没有新流量时也能自己收敛到正确状态"**，可靠性、一致性、合规删除的最后一公里几乎都压在这里。