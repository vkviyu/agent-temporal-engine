# Agent 原生时间执行引擎设计

> 完整 V1 目标保持不变。当前格式：Control 6、Invocation 2、Continuation opcode 8、Work 1、Emission 3、Packet 5、Summary 2。严格验收仍开放；历史日期的已完成结论不能覆盖后续发现的反例。当前实现、验证和真实剩余项以 [2026-11-06 交叉审计](validation/V1_CROSS_AUDIT_2026-11-06.md) 为准。
>
> 2026-11-01 的逐项实现证据与剩余差距见 [文档—代码审计](validation/V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-01.md)。本文件的 V1 目标保持不变，不因当前实现缺失而缩减范围。

> 名称：Agent Temporal Engine，简称 ATE；独立 module 为 `github.com/vkviyu/agent-temporal-engine`。本文不据 module path 推断 Git 远端已发布。  
> 版本：0.4.0，联合审计基线；统一 Core、官方 AgentProfile、历史锚点、事件身份与多 Agent 恢复。  
> 日期：2026-10-30。  
> 状态：设计提案；本文件不表示已经实现、验证性能或获得协议冻结批准。  
> 交付范围：独立引擎的目标架构、数据模型、算法、持久化协议、执行语义、上下文与记忆协议、验证计划。  
> 实现约束：独立仓库与 Go module；自行实现数据库、KV、图存储、事务、索引、历史版本及回收；不依赖 GSEA、Eino、SQLite、bbolt、Badger 或现成图数据库。  
> 允许依赖：操作系统文件接口、Go 标准库、标准密码算法及经过审计的基础库。自研存储不意味着自创密码算法或替代操作系统文件系统。  
> 规范用语：“必须/禁止”为本提案的正确性约束；“应当”为默认实现选择；“候选/待验证”为尚需实验的部分。

本文件拥有 Core/Profile 语义、不变量与对象模型；[实施架构](ATE_V1_IMPLEMENTATION_ARCHITECTURE.md)拥有代码依赖、同步和落地门槛；[API 协议设计](ATE_API_PROTOCOL_DESIGN.md)拥有统一 API、Agent-facing 投影和 IPC；[执行场景](ATE_EXECUTION_PROFILE_WALKTHROUGH.md)提供与三者一致的对象流水和验收反例。三稿为同一联合审计基线，场景不再保留互相冲突的“另一套候选模型”。字节格式、平台支持和实现正确性仍须各阶段验证，本文不表示已经实现。

---

## 阅读索引

1. [目标与承诺](#1-目标与承诺)
2. [系统边界和威胁模型](#2-系统边界和威胁模型)
3. [架构与模块](#3-架构与模块)
4. [核心原语和身份](#4-核心原语和身份)
5. [图、因果与知识前沿](#5-图因果与知识前沿)
6. [版本化状态树](#6-版本化状态树)
7. [事务与并发协议](#7-事务与并发协议)
8. [磁盘格式和持久化协议](#8-磁盘格式和持久化协议)
9. [启动恢复与故障自愈](#9-启动恢复与故障自愈)
10. [历史、分叉与恢复执行](#10-历史分叉与恢复执行)
11. [多 Agent 执行与 Activity](#11-多-agent-执行与-activity)
12. [分支汇聚和冲突](#12-分支汇聚和冲突)
13. [查询与索引](#13-查询与索引)
14. [上下文管理](#14-上下文管理)
15. [长期记忆](#15-长期记忆)
16. [修剪、归档与垃圾回收](#16-修剪归档与垃圾回收)
17. [安全、隔离和隐私](#17-安全隔离和隐私)
18. [公开接口](#18-公开接口)
19. [版本与兼容](#19-版本与兼容)
20. [正确性不变量](#20-正确性不变量)
21. [验证和基准](#21-验证和基准)
22. [实现路线与外部宿主适配](#22-实现路线与外部宿主适配)
23. [决策记录、风险与审计问题](#23-决策记录风险与审计问题)
24. [理论与工程参考](#24-理论与工程参考)

## 0. 设计摘要

ATE 将“发生过什么”“某一版本是什么状态”“某个 Agent 当时能知道什么”“现在允许执行什么”分开表达。

核心组成：

- 不可变事件和内容对象；
- 有明确原子提交边界的分段日志；
- 结构共享的持久化状态树；
- 因果 DAG、版本 DAG、语义关系图；
- 可变且带 CAS 的分支引用；
- 明确知识截止和权限的 View；
- 不随历史倒带的运行控制面；
- 带回执和未知结果状态的外部 Activity；
- 可重建、受 View 约束的图查询索引；
- 有来源的 ContextPacket、摘要和 Memory；
- 与传输无关的命令、查询接口，以及后续可靠订阅的兼容基础；
- 随 ATE 交付的官方 agent/v1 Profile：统一 Query、Task、Execution、Step、Invocation 与恢复记录。

第一版为单存储所有者、单提交协调器、多并行 Agent、多快照读者。Agent 计算、模型请求、隔离工具可以并行；持久化状态发布经过协调器。跨机器共识、任意多写进程直接操作同一目录不属于第一版。

ATE 的确定性是“对已记录事实和已提交决议的确定性”，不是对大模型或外部世界的确定性。它不保证重新请求模型得到相同回答，也不承诺任意外部副作用 exactly-once。

默认审计历史追加保留。逻辑修剪和物理删除严格分离：若保留全部历史，就必须接受对应存储增长；如果选择遗忘原始数据，就必须显式降低这些历史点的可恢复性。

---

## 1. 目标与承诺

### 1.1 目标场景

一个长程任务可持续数小时或更久，包含：

- Root 与多层子 Agent；
- 动态创建、细化、取消和重新指派的任务；
- 多个并行探索方向；
- 多次模型请求、工具调用、上下文摘要；
- 工作区快照和分支合并；
- 用户中途变更、审批、预算暂停、网络故障；
- 从失败探索中提炼经验；
- 返回历史节点复盘或开启新的探索。

引擎应当允许宿主准确回答：

1. 某个结果依赖哪些观察、决策和工具回执？
2. 某个 Agent 在特定节点实际收到的上下文是什么？
3. 当时哪些信息已经对它可见，哪些只是其他 Agent 私下产生的结果？
4. 某个分支为何建立、为何放弃、哪些成果被采用？
5. 某个历史状态能否读取、回放或重新实例化执行？
6. 当前有哪些在途操作和结果未知的副作用？
7. 某条长期记忆来自何处、何时失效、适用于哪里？

### 1.2 独立产品定位

ATE 是可嵌入、可独立部署的时间执行内核，正式支持两种交付形态：Go 库和独立二进制服务。两者复用同一套存储、事务、图与恢复实现。GSEA 只是外部适配和验收对象，不是 ATE 的依赖、公共类型来源或核心仓库组成部分。

公开 API 不得出现 GSEA 的内部类型、Eino checkpoint 类型或特定模型供应商响应类型。统一为三层：

1. **Core**：通用身份、对象、事件、事务、状态树、图、版本、View 和持久发布；不懂 GSEA 业务。
2. **官方 agent/v1 Profile**：随库与二进制静态交付，可显式启用；对面向 Agent 的第一版验收必须启用。它拥有 Query/Task/Execution/LoopStep/Invocation 的 schema、受保护命令、恢复与查询投影，不留给宿主临时发明。
3. **宿主适配**：Agent Loop、模型/工具/工作区 Executor、业务验收策略和 UI；只能通过权限与版本匹配的命令接入。

Profile 使用编译时已知、版本化的纯转换，不在提交锁中运行任意插件/模型回调。Core-only 使用方式仍可存在，但不能冒充完整 Agent 第一版。每个 store 声明已启用及必需的 Profile/schema/transition 版本；库和服务同样检查能力与版本。agent/v1 是契约的显示名称，稳定 ProfileID 为 agent，版本单独记录，不因升级把 Task/Step 等 EntityKey 换成另一身份。

“图数据库”“执行日志”“上下文管理”“经验记忆”是同一事实体系上的不同能力。不可因为展示为图，就把所有语义降为任意节点和边。

### 1.3 可以严格验收的承诺

在已声明的文件系统与故障模型下：

- 已返回 DurableAck 的事务不会恢复成部分提交；
- 相同命令重试不会重复推进权威状态；
- 固定编码、状态操作版本和历史时，回放产生相同状态根；
- 保留且未损坏的历史 Revision 可精确读取；
- 分叉共享历史对象，不复制全部历史；
- 因果来源和合并决议可追溯；
- 历史输入管线不读取 View 截止之后才可见的数据；
- 旧执行实例不能以旧令牌推进新实例状态；
- 重放已确认结果不重新请求模型或调用工具；
- 不确定外部效果不会被虚构成“确定未发生”。

### 1.4 有条件或无法普遍保证的目标

| 目标 | 实际边界 |
| --- | --- |
| 恢复模型“思路” | 只记录公开输出、明确提交的计划/理由和可观察调用；无法恢复未暴露内部思维 |
| 重新执行得到同样答案 | 不能保证；新模型调用是新的非确定性 Activity |
| 回滚外部效果 | 依赖隔离、幂等、可靠查询或补偿；不能回滚现实世界本身 |
| 自动故障自愈 | 只能对有确定证据和已定义修复规则的故障收敛 |
| 摘要绝不遗漏重要信息 | 不能保证；可保存来源、召回与重新生成 |
| 经验跨项目一定有收益 | 属于需要评测的学习问题 |
| 删除所有错误同时保留完整审计 | 两者存在冲突，必须选择保留策略 |
| 从任意旧备份安全接管仍存活的 worker | 需要新部署身份、资源端 fence 或确认隔离旧 worker |
| 哈希证明历史真实 | 哈希只能证明相对于可信锚点的完整性，不能证明观察或模型声明真实 |

### 1.5 第一版不做的能力

第一版优先交付自研存储、事务与恢复、版本/因果图、分叉汇聚、节点/上下文查询和多 Agent 的 durable 执行边界，同时支持库模式与最小本地服务模式。

**第一版不实现广播、通用订阅、引擎主动通知、流式推送或被动反馈调度。** 第 3.2–3.6 节及第 18.3 节仅为后续方向和候选协议，不是第一版接口冻结或退出条件。第一版通信采用客户端主动请求/响应；这包括提交结果、节点查询和宿主主动读取当前状态。

本项延期不删除核心 Activity/outbox/receipt、持久消息、取消意图或执行恢复协议；这些保障执行正确性，第一版仍由宿主通过普通命令/查询协调，不依赖通用订阅运行。

也不实现通用 SQL/Cypher、分布式共识、通用向量数据库、任意进程内存快照、任意机器指令重放。全文与向量候选检索可在后续自研，不能成为早期正确性前提。

### 1.6 目标到验收的对应

| 最终目标 | 第一版必须具备的基础/证据 |
| --- | --- |
| 独立基础设施 | 独立 module、自研存储；库与服务共用 Core/Profile，无 GSEA 类型反向依赖 |
| 多 Agent 长程任务 | 动态执行父子关系、标准工作对象、当前控制与未知效果、可恢复动作，不预设计完整 DAG |
| 时间旅行与分叉汇聚 | 明确 Anchor、版本/知识视角、精确回放/新探索分型、显式冲突与来源 |
| 确定性审计与故障自愈 | 已提交事实重放、receipt/动作收敛、故障矩阵；不虚构外部 exactly-once |
| 上下文与真正可追溯记忆 | 原始证据、工作知识、实际输入、摘要、Memory/ExperiencePath 分层；支持召回而不只是日志浏览 |
| 经验筛选与复用 | 保留来源/反证、逻辑修剪、晋升/撤回/跨域导入；效果经评测，不以“保存了”代替有效性 |

广播/订阅是这些事实之上的后续交付层，不改变上述第一版核心。自研图/日志和数据结构是为这些能力服务，不把“造一个通用数据库”当成目标本身。

---

## 2. 系统边界和威胁模型

### 2.1 角色

- **宿主**：拥有引擎，提供已认证 Principal、策略和外部执行适配器。
- **Agent**：提出计划、查询、分叉、汇聚或记忆候选；不直接写权威磁盘。
- **Commit Coordinator**：校验事务并发布唯一合法提交。
- **Executor**：根据持久 Activity 意图调用模型、工具或沙箱。
- **Reader**：持有固定 View 和存储代际 pin 的查询者。
- **Operator**：执行导出、备份、修复和受控遗忘的管理员。

### 2.2 故障模型

区分以下情况，不混用“崩溃”：

| 类别 | 必须处理的情况 |
| --- | --- |
| 进程故障 | 任意指令处退出、SIGKILL、请求取消、线程停顿 |
| I/O 故障 | 短写、磁盘满、权限错误、读写/同步失败、文件缺失 |
| 掉电故障 | 未持久化数据丢失或撕裂、目录项尚未持久化 |
| 数据损坏 | 已确认数据的位翻转、长度改变、校验不符 |
| 回执故障 | ACK 丢失、重复/乱序/晚到结果 |
| 软件故障 | reducer 版本不匹配、索引缺陷、序列化不规范 |
| 非法输入 | 伪造身份、循环引用、越权读取、超大对象、路径逃逸 |

平台持久化保证必须以实际验证的 OS、文件系统、设备行为为前提。不得把 Go File.Sync 等同于所有平台的完整掉电语义。Linux 的文件和目录同步、macOS 的设备同步语义分别验证。[R4][R5]

对于任意恶意重写整个目录、同时替换所有本地哈希和锚点，本地 hash 链无法提供独立真实性保证；需要目录外可信摘要、签名或复制锚点。

### 2.3 明确的运行限制

第一版：

- 同一 store 只允许一个读写所有者进程；
- 所有者内可有多个 Agent/Executor 和并发读者；
- 库模式由嵌入宿主持有 store；服务模式由 ate serve 持有 store，其他进程经本地 IPC 提交命令/查询，或读取导出的只读快照；
- 同一 store 不能同时由库宿主和服务进程直接打开写入；库模式与服务模式切换须先关闭原 owner 或走既有接管协议；
- 不支持多个无协调的写进程 mmap/直接更新同一目录；
- 不把 network filesystem 默认列为严格耐久性支持目标；
- 不在数据库锁仍被旧所有者持有时接管写入。

---

## 3. 架构与模块

**阶段边界：第一版实现同一内核的库/二进制双形态，以及命令/查询请求响应。** 图中虚线为后续订阅扩展，不是核心提交链的依赖；第一版不创建订阅管理器、消费者 checkpoint 或通知路由器。

~~~mermaid
flowchart TD
    A[Agent / Workflow / External Host Adapter] --> H[Trusted Host API]
    H --> C[Commit Coordinator]
    C -. later .-> F[Published Change Feed]
    F -. later .-> R[Host Subscription Router]
    R -. later .-> A
    C --> G[Event / Revision / Branch / View Semantics]
    C --> S[Persistent State Tree]
    C --> L[Segmented Transaction Log]
    L --> M[Durability Manifest]
    G --> Q[Graph Query and Visibility]
    Q --> X[Context Renderer]
    X --> P[Immutable ContextPacket]
    G --> K[Scoped Memory]
    C --> O[Activity / Outbox / Receipt]
    O --> E[External Executor]
    E --> H
~~~

以下为概念职责分组；实际公共包与内部依赖以实施架构为准，不要求每个概念立即成为公开 Go 包：

~~~text
<independent-module>/
    engine/          公共事务、Revision、Branch、View、查询接口
    service/         服务模式的认证、请求分发、生命周期；调用同一内核
    client/          本地服务 SDK；领域请求/结果与库模式对齐
    transport/local/ 最小本地 IPC；不定义存储语义
    changefeed/      后续新增；第一版不建空壳、队列或 checkpoint 运行时
    activity/        外部操作和结果对账契约
    contextview/     来源选择、预算和 ContextPacket
    memory/          候选、验证、晋升、撤回
    codec/           版本化规范编码
    storage/         分段日志、manifest、对象定位、DurableIO
    graph/           因果、邻接、祖先、merge-base 算法
    state/           持久化 radix tree 与固定状态操作
    testing/         参考模型、确定性调度、故障注入
    cmd/ate/         serve、verify、inspect、export、doctor 入口
    examples/        可选示例；生产适配器位于外部宿主仓库，不进入 ATE Core
~~~

ATE 作为独立公开 Go module，不在公共 module 中使用 `internal` 目录；包的稳定性由文档和版本化 schema 分层管理。外部宿主适配器位于宿主或独立外部仓库，不进入 ATE 核心，也不从外部宿主反向导入。

### 3.1 当前通信与后续扩展方向

通信语义与传输分离。下表区分第一版范围和后续方向：

| 通道 | 方向 | 用途 | 阶段 |
| --- | --- | --- | --- |
| Command | Worker/宿主 → 引擎 | 提交事务、spawn、导入知识、更新引用 | 第一版 |
| Query | Worker/宿主 → 引擎 → 调用者 | 节点列表、详情、上下文、证据和历史查询 | 第一版 |
| Subscription | 引擎 → 宿主路由器 → Worker | 已提交变更、消息到达、结果就绪和依赖失效通知 | 后续版本 |

这里的 Worker 是执行 Agent Loop 的运行时实体，不是正在生成 token 的模型。引擎通知宿主运行时，宿主决定排队、唤醒、暂停、重新规划或在下一轮构造上下文；不要求向进行中的模型请求直接注入任意字节。

默认接入方式：

- 同进程嵌入：直接调用 Go 的命令/查询接口，不必经过网络和序列化；后续订阅可增加独立 Next(ctx) capability；
- 同机不同进程：由一个 store owner 暴露 IPC 适配器；macOS/Linux 候选为 Unix domain socket + 有界长度帧，Go 标准库支持该传输。[R10]
- 远程 SDK 或浏览器：可另加网络适配器，包括 WebSocket；不让这种适配成为嵌入式内核的前置条件。

Go channel 可以用于内部唤醒和有界队列，但不是可靠日志。广播需要每个订阅独立的游标/队列；多个 Worker 从同一个 channel 读取属于竞争消费，不能当作向每人广播。[R11] 第一版不引入外部消息中间件，也不宣称未经测量的 IPC 性能优势。

### 3.2 后续方向：已发布变更流

第 3.2–3.6 节保留候选设计便于审计兼容性，具体方法、字段、消费协议和调优参数将在订阅阶段重新审计。当前只承诺不作阻碍这些能力的核心设计，不要求第一版实现其运行时。

变更流只从 P1 已发布事务前缀产生，不从未提交 staging 发事件。通知的顺序是：

~~~text
Worker 1: Commit(RequestID)
    → 校验、持久化、发布
    → 唤醒变更流读取器
    → Worker 2 / Worker 3 各自接收匹配通知
~~~

其他订阅者可能先于 Worker 1 收到网络 ACK 得知提交，这仍是同一个已发布事实。CommitUnknown 后是否有变更记录，以恢复验证出的发布前缀为准。

FeedRecord 包含稳定记录身份、源事务/事件引用、类别、经授权的路由元数据与后续详情读取引用。默认不携带完整模型输出、工具产物或全部上下文。类别由版本化确定性投影或原事务中的显式通知声明产生；投影不能调用模型，不能把尚未执行的提案发布成事实。

后续订阅的候选类别包括 node.published、branch.advanced、message.available、activity.result_recorded、execution.cancel_requested、memory.retracted。领域类别和可路由字段注册版本与权限要求；Branch 过滤必须明确是 RefUpdate、某 Cursor 的可达历史还是显式 Task 归属，不能只看 OriginBranch 猜成员关系。

提交协调器只更新高水位并发出轻量唤醒，不执行订阅者回调，不等待订阅者和模型。读取器以高水位/条件变量的正确同步协议读取日志，避免“检查为空后、开始等待前”的丢失唤醒。进程崩溃丢失内存唤醒不影响日志补读。

### 3.3 后续方向：订阅游标、广播与断点补读

每个订阅拥有独立 FeedCursor。内部位置为发布事务的 (CommitLSN, ordinal)，并绑定：

- Store/Space、提交摘要与历史谱系锚点；
- feed/projection 版本、filter digest 和订阅身份；
- 当前访问主体及权限校验规则；
- durable consumer 的 claim/owner generation。

公开 cursor 使用有界且完整性受保护的不透明 token；采用服务端随机句柄或标准认证加密方案，不仅将明文 LSN 做 base64。不可因 token 暴露其他 Space 的全局事件数量，不能把它当作读取授权。物理 compaction 不改变逻辑位置；旧备份缺少的未来锚点、已删除范围或不兼容版本必须显式拒绝续读。

交付语义为 **at-least-once**：断线可能重复，通过稳定 FeedRecordID/EventID 及命令 RequestID 处理。每个订阅内部按日志顺序交付；并行消费的业务完成顺序需要宿主另行仲裁，不宣称所有 Worker 同时收到。

支持两种订阅：

- Ephemeral：UI/临时观察者自己保留 resume token，掉线后从 token 重连；
- Durable：消费者身份、已确认位置与 owner generation 保存在 LiveControlRoot，恢复后继续；旧 worker 不得替新 owner 确认位置。

广播是多个独立订阅读取同一已提交事实，每个订阅经过自己的过滤与授权。后续订阅的初始实现不把多个 Agent 默认放进竞争消费组；若将来引入 consumer group，必须另行定义分配、租约和交接。

### 3.4 后续方向：快照到订阅之间没有空隙

“先列节点，再开始监听”不能依赖瞬时内存广播，否则两步之间可能漏更新。

OpenSnapshotAndWatch 在同一捕获协议中取得 pinned ReadView/快照及发布 fence H，订阅从 EndOfCommit(H) 之后开始。快照分页固定在 H，后续事务通过流交付；快照读取与流缓存可重叠，调用者在完成基线后按顺序应用变更。

操作同时建立有配额和期限的日志保留 lease；快照关闭到订阅接管之间不能释放所需范围。续读范围失效返回 CursorExpired/ResyncRequired，要求重新捕获快照，禁止静默跳到最新。只有 ACK/处理进度推进后，才能按策略释放旧范围。

固定历史 View 永远不随订阅移动。把最新节点加入某个 Actor 的上下文，需要显式创建新 View/Revision 和知识导入；不能在旧 View 上悄悄追加未来节点。

### 3.5 后续方向：慢消费者与消费确认

每个订阅有 maxBufferedBytes、maxBatchBytes、maxLag 和 deadline。慢消费者不能阻塞提交。缓冲满后停止预取并从日志补读；超过允许的保留期限/空间则返回 SubscriptionLagged/CursorExpired，解除 pin 并要求重同步，不伪造“全部已交付”。AuditFull 保留日志也不代表允许订阅无限占用内存。

区分：

1. 已从日志读出；
2. 已交给宿主运行时；
3. 已被 Worker 持久处理；
4. 已实际呈现给模型。

这些不是同一个 ACK。durable ACK 只推进明确的消费者 checkpoint，不证明模型已经阅读。Agent 的可靠处理命令必须将消费记录、需要的 Observation/KnowledgeImport、下一动作意图与消费 offset 放入同一事务；不能先 ACK 再另写动作。重复批次通过 RequestID 和源记录身份幂等处理。纯 UI 观察者可只确认展示进度。

ACK 必须绑定 subscription/owner generation，只接受该订阅实际交付且连续完成的高水位；乱序处理不能跳过尚未完成的较早记录。过滤掉的记录由引擎提供扫描水位证明推进，消费者不能任意提交全局 LSN 跳过消息。

### 3.6 后续方向：权限、知识与被动反馈

订阅是当前授权下的宿主 live feed，不是任意历史 Actor View 的扩大权限。节点标题、通知类别、边、分支名和计数同样受保护；每次输出按第 17.6 节检查出站 grant，撤权可阻止后续尚未授权的交付。

收到“结果可用”的通知不自动让 Root 获得子 Agent 私有 payload。宿主先按允许的路由投递，再显式消费获准字段并更新知识根；之后构造的新 ContextPacket 才可使用这些观察。实时查询结果交给模型也遵循同样规则。

过滤器、范围或权限发生变化时，必须明确新订阅/重同步规则；新授权不会让旧 ACK 之前的历史自动重发，撤权后的补读也不能重放旧权限。严格历史回放只复用当时记录，不开启 live feed 混入未来信息。

通知可以触发宿主策略，例如依赖产物更新后重验、工具完成后继续、撤回来源后暂停相关工作。是否调用模型、取消任务或创建新的子 Agent 由宿主控制，动作必须回到 Command 通道并记录因果来源；不能在提交锁中递归执行。宿主需限速、合并唤醒与限制重入深度，避免通知反馈循环造成无限模型请求；需要可靠送达的领域消息不可被普通唤醒合并所丢弃。内部消费 checkpoint 更新默认不向自身领域订阅生成新的业务通知，避免 ACK → 通知 → ACK 自激循环；管理审计仍保留这些事务。

### 3.7 同一内核、库与独立二进制

二进制适合独立生命周期、跨语言客户端和多个进程共享访问；库适合嵌入 Go Agent、测试与免服务接入。两者是正式交付形态，不要求每个 Agent 都先启动守护进程。

~~~text
嵌入宿主 ──公共 Go API──────────────┐
                                   ├─ 同一 Engine / 单一 store owner
客户端 ─ SDK ─ 本地 IPC ─ ate serve ┘
~~~

图表示两种互斥的 store 所有权选择，不表示两个 owner 同时写同一目录。服务端导入同一核心库，不复制事务、校验、索引或恢复逻辑；client SDK 不直接打开服务器的数据文件。命令/查询返回的事实、幂等结果、错误分类与权限语义必须对齐，延迟和进程内句柄形式可以不同。

第一版 ate serve 只需本地 endpoint、受控目录/凭据映射、请求分发、优雅关闭、资源限制与命令/查询 API，不建设远程多租户平台或 pub/sub 服务。运行进程仍需从可信 OS/连接身份建立 Principal，不能信任客户端自报的 Actor/Space。

最小 IPC envelope 具备 protocolVersion、method、request correlation ID、领域 RequestID、类型化 body、稳定错误码和 capability 列表。传输 correlation ID 与持久幂等 RequestID 不能混同。wire 表示不暴露 Go 指针、channel、闭包或内存地址；后续可增加可选 stream 方法，不要求第一版实现占位 Watch，也不预留永久无语义的空 opcode。

连接断开/超时不代表事务取消。服务端已提交但响应丢失时，SDK 用同一 RequestID 对账；两种模式共享 CommitUnknown 与恢复契约。进程内 ReadView 在 IPC 下映射为有 owner/授权/期限的 opaque handle，明确 Close/连接回收/过期规则，避免遗留 reader pin。

verify/repair 等离线工具必须获得 store 独占权，或通过在线服务暴露的受控管理操作执行；不绕过 owner 打开正在运行的 store 修改文件。SDK/daemon 升级使用 capability/版本检查，不把未知方法当作空成功。

### 3.8 第一版必须保留的兼容基础

以下均直接服务第一版正确性，不为尚未实现的订阅引入后台系统：

- 稳定 EventID/TxID、CommitLSN、事务内顺序与提交摘要；
- 只发布完整事务、可重读的已发布事实和明确的历史缺口/保留策略；
- 固定快照与可获取的 published fence；
- 原子更新多根、幂等命令、版本化 schema/StateOps 和受保护控制命名空间；
- Actor 知识、当前权限、现实控制面与领域历史分离；
- transport 与核心无反向依赖，不让 Commit 等待未来通知消费者；
- 后续通知可从已发布日志推导，不依赖第一版原先丢弃的关键因果、消息或回执字段。

第一版不创建 durable subscription、consumer offsets、消费租约、通知缓冲和广播调度。后续增加这些能力可以使用显式格式/状态迁移；预留兼容方向不承诺旧二进制能无升级理解新 opcode。



---

## 4. 核心原语和身份

### 4.1 标识分类

- StoreID：一个逻辑存储的稳定随机身份。
- DeploymentIncarnation：当前部署身份；从旧备份接管时必须新建。
- SpaceID：安全和内容寻址域，通常对应租户或项目边界。
- TxID：提交前生成的随机事务身份。
- RequestID：宿主命令幂等身份；同一逻辑命令重试保持不变。
- EventID：不可变事件的内容身份，包含发生次数所需身份字段。
- RevisionID：不可变状态版本身份。
- ObjectID：类型、Space、版本和规范字节内容的摘要。
- BranchID：可变分支引用身份。
- EntityID：领域实体的稳定逻辑身份，实体属性由各版本状态解释；不同于内容对象 ID。
- ExecutionID / Incarnation：执行逻辑身份与当前执行代际。
- ActivityID / AttemptID：逻辑外部操作与每次派发尝试身份。

随机 ID 使用标准密码随机源。内容身份使用标准 SHA-256：Hash(type-domain || SpaceID || encodingVersion || canonical-body)。所有字段以规范编码分隔，不使用有歧义的字符串拼接。不同 Space 的相同内容默认得到不同 ID；不进行跨 Space 隐式去重。未来更换算法必须显式版本化。

### 4.2 内容依赖拓扑与无自引用

先预分配随机 TxID、RequestID/逻辑身份，再按类型化内容依赖 DAG 编码对象/Event/Revision，而不是把所有对象机械分为“先全部 Event、再全部 Revision”。常见前置链为 payload→Event→状态根/Revision；后置控制 Object/Event、Continuation 可以引用已经构造出的 Revision，但不能反向加入该 Revision.Events。

同一事务/批次内的强内容依赖必须拓扑在前；一般 body 允许类型混排。逻辑实体/源发布引用使用预分配逻辑身份，不当成包含自身摘要的内容边。schema 显式区分这些关系。

禁止：receipt 嵌最终 CommitDigest；Revision.StateRoot 含本 RevisionID；record 含引用它的当前 EventID；Branch head map 参与 RevisionID 同时包含它；CP.sourceAnchor 指包含自身的 Fence。反向 createdBy/createdIn 索引从权威日志派生。

命令 receipt 可含 TxID、LSN、确定结果，最终事务摘要由 envelope 查询。构造新知识后需要固定未来读取源时使用 §5.7 的 SourcePublicationRef，不嵌未算出的 CommitDigest。

### 4.3 Event、stream 与事实来源

统一概念结构；字段编号在格式工件冻结：

~~~go
type Event struct {
    Space              SpaceID
    Transaction        TxID
    Stream             EventStreamKey
    StreamSequence     uint64
    RecordedBy         PrincipalID
    Subject            OptionalNodeRef
    InitiatingActor    OptionalActorID
    Kind               EventKind
    SchemaVersion      uint32
    CausalParents      []EventID
    Payload            ObjectID
    StrongRefs         []TypedContentRef
    ObservedTime       RecordedTime
}

type EventStreamKey struct {
    Owner              ScopeRef
    ExecutionIncarnation OptionalUint64
}
~~~

ScopeRef 表示系统范围或已注册逻辑实体范围。agent/v1 的 Session、Query、Task 和 Execution 都可作为明确的事件归属；归属 scope 与安全 ACL scope 不是同一概念。

- Execution stream 绑定 E 及 incarnation，Actor 从不可变 ExecutionSpec 解析；非 Execution stream 不伪造 Execution/incarnation。
- QueryAccepted 可以在 Session/Query scope 发生，此时还没有 Root 执行。路由、系统事件不必造一个假 Agent。
- RecordedBy 由认证入口/协调器写入，表示谁提交这条记录；InitiatingActor 是可选触发归因，不是授权身份。
- Subject 是事件主要涉及的实体。工具回执可属于 E1 stream、Subject=G17，RecordedBy=Executor、InitiatingActor=A1；它不是模型自行声明的事实。
- StreamSequence 由协调器在 (Space, StreamKey) 内单调分配；同 stream 事件保留顺序，但不把不同 stream 的 LSN 大小当作知识传递。

EventID 由 Space、类型域、编码版本和上述规范内容计算。TxID/发生身份按规范先分配；StrongRefs 由对应 schema 提取并验证，调用者不能删减声明以逃避保留闭包。相同 payload 可共享 ObjectID，不同发生不去重。

事件没有唯一所属 Branch。OriginBranch 仅为来源元数据；可见性由 Anchor、Revision/知识选择和当前授权共同决定。

### 4.4 Revision 与 Commit

**Revision** 是逻辑状态版本：

~~~go
type Revision struct {
    Space              SpaceID
    StateParent        RevisionID
    JoinParents        []RevisionID
    StateRoot          ObjectID
    CausalFrontier     ObjectID
    ExecutionKnowledgeRoots ObjectID
    Events             []EventID
    Mutations          ObjectID
    MergeResolution    ObjectID
    SemanticsVersion   uint32
}
~~~

- StateParent 定义状态操作的基线；
- JoinParents 记录完整汇聚的版本来源；
- Mutations 是相对 StateParent 已解决冲突后的确定性变化；
- StateRoot 是应用这些变化后的领域/全局状态结果；
- CausalFrontier 是因果 cut 的压缩入口；
- ExecutionKnowledgeRoots 按 KnowledgeKey=(ActorID, ExecutionID) 指向观察集和知识根，不等于全局 StateRoot；
- 多个 parent 不表示自动把所有祖先 mutation 再执行一遍。

**Commit** 是一次物理原子事务，可以同时发布多个 Revision、更新多个 Branch、写控制记录、入队 outbox 并提交命令 receipt。

RevisionID 是内容身份；CommitLSN 是本 store 的持久总序号，不作为跨 store 全局时间。每个已发布事务记录可验证的 PostRootSet，含 BranchRefRoot、LiveControlRoot 与 CommandReceiptRoot；恢复 replay 必须核对它。某个 Revision 后可以有多个控制-only Commit，所以 Cursor 不能替代全 store 历史锚点。

### 4.5 Branch 与 Cursor

~~~go
type BranchRef struct {
    ID       BranchID
    Head     RevisionID
    Version  uint64
    Status   BranchStatus
}

type Cursor struct {
    Store     StoreID
    Space     SpaceID
    Revision  RevisionID
}
~~~

Cursor 指向不可变 Revision；BranchRef 是可移动引用，修改必须 CAS。

Fork 只新增 BranchRef 和审计事件，历史对象共享。旧对象不因新分支改写。Branch、Actor、Task 和 Execution 不要求一一对应：一个 Actor 可参与多个分支，一个分支可有多个 Actor；绑定关系由显式事件表达，不能用 BranchID 代替运行身份。

### 4.6 SnapshotAnchor、知识选择与 View

~~~go
type SnapshotAnchor struct {
    Fence       CommitFence      // Store、LSN、CommitDigest
    Space       SpaceID
    Cursor      OptionalCursor
    Knowledge   OptionalKnowledgeKey
}

type KnowledgeKey struct {
    Actor       ActorID
    Execution   ExecutionID
}
~~~

Fence 固定当时的完整 PostRootSet；Cursor 可选择其中已存在的历史领域/知识版本。校验 Store/Space、发布摘要、保留状态和 Cursor 不晚于该 Fence 已发布（≤，可以由同一事务产生），拒绝把未来 Cursor 配到旧 Fence。KnowledgeKey 必须与 ExecutionSpec 的 Actor 及选定 Revision 匹配。已有 Anchor 时不得另行覆盖其 Cursor/Knowledge；API 附带选择字段必须相同，否则 AnchorMismatch。latest 模式可用选择字段构造唯一返回 Anchor。

agent/v1 的执行知识按 KnowledgeKey 隔离，同一 Actor 的不同 Execution 不自动共享工作记忆；incarnation 恢复不清空同 E 的知识。Query 路由等尚无 Execution 的活动可以只有 Fence/Scope/来源集合，不要求知识 Cursor。

View 在 Anchor 上加时间模式、可见性模式（ExecutionKnowledge/AuditSnapshot/Discovery）、当前 Principal/ScopeGrant、算法版本、当前输出授权和物理代际 pin。ViewRef 是临时能力，不能当持久恢复锚点；语义 ViewDigest 不含临时 handle、pin 地址或租约时间。

历史实体使用 Anchor 的控制根解释，领域/知识使用所选 Cursor；不能拿当时 Cursor 加今天 TaskControl 冒充同一历史。未给 Anchor 而请求 latest 时，由协调器原子捕获一次并返回明确 Anchor，不逐字段追随最新状态。

StrictCutReconstruction 的输入来源只能来自选定 cut；Discovery 是显式发现视角，不可组合成偷偷读取未来资料的“严格过去”。Query 路由无 KnowledgeKey 时只使用 Anchor 中已获准的 Query/Session 来源。

Cursor/Anchor 都不授予权限。历史权限仅供审计，输出仍受当前 ACL/UseBan/隐私约束。公开 token 对未授权主体不得暴露跨 Space 的全局计数；wire 可用 CommitDigest/不透明引用经认证索引解析内部 LSN，不能把原始全局位置直接渲染进模型输入。

### 4.7 规范编码 ATE-CANON-1

内容身份和 RequestDigest 必须使用同一明确的规范编码，而不是依赖 Go map 迭代顺序、默认 JSON 或内存布局。

候选规则：

- 固定 schema 的字段顺序、类型编号和域分隔；必需字段缺失即拒绝；
- 固定整数宽度的磁盘 header 使用 little-endian；语义对象的变长整数使用唯一最短表示，拒绝溢出和非最短编码；
- bytes/string 带长度；string 明确 UTF-8 校验规则，不隐式做 Unicode normalize；
- map 按编码后的 key 字节排序，禁止重复 key；有序列表保留顺序，集合按规范元素字节排序并去重；
- absent、null、空 bytes、空列表和数值 0 必须有不同表示；
- v1 权威状态不接受未定义浮点值，成本/时间使用明确单位的整数或定点数；
- 未知 mandatory 字段/枚举拒绝，不忽略后继续写；扩展容器使用独立版本；
- 原始 provider JSON、模型文本和工具字节作为 opaque bytes 保存；其原始字节摘要和解析后语义摘要分开，不能重新序列化后声称保存了原始请求。

规范还必须发布空对象、边界整数、重复集合、非规范输入和跨语言测试向量。此节是候选约束，不代表二进制格式已经冻结。

### 4.8 概念之间的关系

~~~text
Event：发生了一次观察或决策
Revision：某次状态变化后的版本
Commit：上述对象被原子、持久地接受
Branch：当前选择从哪个 Revision 继续
View：谁以哪种知识和时间规则读取它
~~~

以下对象分工对应长程任务主路径。历史事实、执行输入、经验沉淀和状态发布各有独立的版本与验证边界。

| 对象 | 记录什么 | 与其他对象的关系 |
| --- | --- | --- |
| Session、Query、QueryAnswer | Session 是宿主会话标识。Query 是一次请求，QueryAnswer 是发布的答复。 | Query 由 Space、主体和 Query ID 定位，记录所属 Session。最终答复绑定已接受的模型结果、所采用的 TaskOutcome 和最终模型输入证据。 |
| Task、Execution、Step | Task 定义目标与验收边界。Execution 是推进 Task 的一次执行。Step 是该 Execution 中有序的工作单元。 | Query 启动根 Task 和 Execution。子 Execution 记录父 Execution，可执行自己的 Task。Step 记录输入位置，后续 Step 依赖前一步完成。TaskOutcome 记录接受的任务产物与验证证据。 |
| Invocation、Activity、Attempt、ActivityResult | Invocation 定位工具调用的逻辑槽。Activity 定义一次模型或工具工作及输入。Attempt 记录其派发尝试，ActivityResult 保存捕获结果。 | Activity 可绑定 Query 或 Execution 下的 Step，也可绑定 Invocation。接受结果和完成 Step 是显式转移。多次尝试仍归属于同一 Activity。 |
| Work、Continuation、PendingAction | Work 把需要一起可见的控制转移提交成一笔事务。Continuation 记录恢复位置。PendingAction 固定下一次提交的请求身份与命令。 | 恢复以已发布回执、Continuation 和可执行授权为准。对于已跨出引擎边界的工具副作用，宿主先对账，再由引擎记录结果。 |
| Event、SnapshotAnchor、NodeRef | Event 记录已提交的事实。Anchor 固定发布位置及可选选择器。NodeRef 定位事实、实体或内容。 | Event 因果边只来自已记录的依赖。EventID 与 Anchor 组成可重读的引用，读取仍应用当前权限。 |
| TaskTrace、Investigation | TaskTrace 按 Query 或 Task 投影关键事件。Investigation 保存对历史引用的调查进展。 | TaskTrace 可附宿主选择的节点、解释关系与证据。解释、反证和支持关系保留来源，记录中的因果 DAG 由提交事实产生。可通过引用展开事件前后及原始内容。 |
| Observation、Knowledge Revision | Observation 记录某个 Execution 明确采用的一项来源。Knowledge Revision 固定已采用观察的前沿。 | 工作知识按 Actor 和 Execution 隔离。跨执行通过 Import 采用，保留原始来源。 |
| Summary、ContextPacket | Summary 是可追溯的压缩内容。ContextPacket 固定某次模型输入的选择、来源位置和实际请求字节。 | Summary 可作为 Observation 的来源。Activity 绑定 Packet。Packet 固定本次请求，写入 Packet 不推进 Knowledge Revision。重新派发时复核来源可用性。 |
| Memory、ExperiencePath | Memory 是经验证、可推广的一条复用主张。ExperiencePath 是由这些主张、原节点及验证证据串成的经验路径。 | 两者均可回到带 Anchor 的源记录。ExperiencePath 显式关联步骤证据和排除项。新的 Execution 可通过 Observation 采用某个可复用版本，受当前适用性、替代版本和撤回规则约束。 |
| Branch、Revision | Branch 指向状态探索的可移动头部，Revision 固定一份不可变状态。 | 局部分支记录分叉基线和可选执行来源，独立写入，三方合并后按差异发布。旧修订在历史位置仍可审计。 |

执行汇聚由 `JoinExecutions` 记录接受了哪些子执行结果。工作知识由 `Import` 或显式 Observation 采用。状态分支由 `JoinLocal` 与 `PublishBranch` 更新。宿主可分别提交这些动作，使结果选择、模型见闻和状态变更各自可重放。

会话间可按 Space、主体和 Query ID 读取已有轨迹。模型若要使用先前任务的资料，宿主选取引用并将其显式纳入新执行的上下文。跨 Space 的内容由 Transfer 及其授权规则处理。

### 4.9 实体版本、NodeRef 与引用提取

稳定逻辑实体与不可变版本对象分开。EntityKey=(ProfileID, EntityKind, CanonicalLocalID)，agent/v1 的 Task/Execution/Invocation 等使用命名逻辑 ID，Step 使用 (ExecutionID, StepSequence) 的规范复合 key。它们统一映射到 NodeRef.Entity，不把 TaskID 当 ObjectID。

GetNode(EntityRef, Anchor) 根据 schema 指定的根解析当时记录；可包含 immutable specRef 与 stateRef。GetNode(ObjectRef) 读取固定字节。定义、状态版本与发生事件均可追溯，但不是每个内部树节点都成为默认业务列表节点。

每个 Core/Profile schema 必须登记：规范编码、强内容引用提取、强历史发布依赖、逻辑实体关系、事件来源/causal 引用和索引投影版本。结构归属用字段，独立可撤回关系用 Assertion/Event，反向边由索引重建。

稳定实体 ID 本身不自动 pin 某个“最新版本”或全部历史；历史证据必须引用确切 RecordID/EventID，或受保留保护的 Anchor。强引用闭包不能靠字段名或任意 JSON 猜测，Event.StrongRefs 也不能代替对象自身的 schema 引用提取。Record 不反向包含引用它的当前 EventID、RevisionID 或 CommitDigest。

---

## 5. 图、因果与知识前沿

### 5.1 三类图

| 图 | 边的含义 | 是否允许环 |
| --- | --- | --- |
| 因果图 | 事件发生依赖已存在的事件 | 否 |
| Revision 图 | 状态前驱、完整 join 来源 | 否 |
| 语义图 | 支持、反驳、关联、替代、实体关系 | 可以；特定关系可另设约束 |

“引用了一个对象”不必然表示“知道它的全部历史”；关系类型必须明确。

语义关系采用不可变 RelationAssertion，而不是原地修改某个邻接链表：

~~~go
type RelationAssertion struct {
    Subject       NodeRef
    Predicate     RelationKind
    Object        NodeRef
    Qualifiers    ObjectID
    Scope         ScopeID
}
~~~

NodeRef 为明确类型的 EntityID、EventID、RevisionID 或 ObjectID，不接受模糊字符串 ID。一个 assertion 作为事件 payload 的条目保存，其发生身份为 (asserting EventID, ordinal)；payload 不反向包含自己的 EventID，避免 hash 自引用。

- 实体属性存在版本状态树中，事件和证据对象不可原地覆盖；
- 支持/反驳等边的存在与可见性由 assertion event 和 View 决定；
- 撤销关系通过新的 Retraction 事件引用 assertion 身份，历史关系不被删除；
- 关系两端可指向已有节点，后来追加相反方向的 assertion，因此语义图可成环；
- 内容对象的结构强引用仍不得产生 hash 自引用环。需要互相引用的实体使用稳定 EntityID，通过独立 assertion 连接，不把可变语义边塞进彼此内容 hash；
- predicate schema 指定允许的端点类型、是否传递/对称、作用域与可选无环约束；查询不能把普通“相关”边当作因果依赖。

v1 不使用隐式推理补齐事实。由模型或规则推导的新关系必须有独立事件、来源和策略版本。

### 5.2 因果 DAG 构造

新事件只允许：

- 引用已持久提交的 parent；
- 或引用同事务中拓扑顺序在前的事件；
- 或引用同一个原子发布批次中、更早且已完整验证的 staging 事务事件。若前驱被拒绝，所有依赖它的 staging 事务必须重新验证或一起拒绝，不能只发布后继。

禁止悬空 parent 和同批前向循环。跨 store 导入必须先验证并拓扑导入依赖闭包，或使用明确的外部证据锚点；锚点不冒充本地可回放历史。

Lamport logical time 可用作排序提示，不能用数值大小推出完整因果关系。[R1]

### 5.3 CausalCut 与执行知识分离

CausalFrontier 是一组事件 tip，闭包形成因果一致 cut：事件在 cut 内，其 causal parents 也必须存在于 cut。正常沿同一历史前进，cut 不倒退；完整 join 的因果 cut 包含所有 join parent 的 cut。

**因果依赖不等于 payload 已向某个 Agent 披露。** 子 Agent 的消息 m 可以因果依赖私有草稿 s；Root 消费 m 后，其因果闭包包含 s，但不能因此获得 s 的内容。

为此分别保存：

1. CausalCut：解释“这个状态依赖哪些发生过的事件”；
2. ObservationSet：指定 KnowledgeKey 已被宿主接受/显式导入的观察、可披露字段与来源；
3. KnowledgeStateRoot：按 KnowledgeKey 仅由上述观察与导入形成的版本化工作知识；
4. 当前 ReadGrant：现在是否还允许读取这些内容。

因果祖先默认仅可作为不透明证据锚点。要读取其 payload，仍需该 View 的观察范围与当前授权；全局审计者可在额外权限下查询。完整 join 合并因果来源，不自动把所有子 Agent 私有 payload 变成 Root 已知。

第一版先使用显式依赖、规范集合/稀疏前沿和精确遍历；前沿优化必须保持这些集合的语义，不给每个事件附带无限增长的完整向量。

### 5.4 ExecutionKnowledgeState 的版本化

例子：Root 已观察 answer=old。Agent B 将全局领域状态更新为 answer=new，但 Root 尚未消费 B 的消息。

此时 Root 的历史 View 必须读到 old；直接读全局 StateRoot 会泄漏 new，而简单过滤 new 后返回 absent 也不正确。

第一版按 KnowledgeKey=(ActorID, ExecutionID) 使用独立 KnowledgeStateRoot，以持久化状态树共享结构。只有 Observation、MessageConsumed、KnowledgeImport 等显式操作可更新它，更新与 ObservationSet 原子提交。跨 Actor 或同 Actor 的另一 Execution 的状态变化，不隐式推进本 KnowledgeKey。Observation 必须 CAS 当前 E 的知识位置，避免并行消费相互覆盖；父子身份本身不导入 payload。

知识状态保存“该 Actor 已观察到的声明及证据”，不宣称声明为真。相互冲突的声明都保留；哪些声明作为当前工作假设，由版本化宿主规则或显式决议决定，不能用全局 last-write-wins 擦掉反证。

ReadAt 的领域全局状态与 OpenView 的 KnowledgeKey 执行知识读取是不同 API 权能。构造 Agent 上下文默认读取知识状态；宿主全局审计使用额外授权的领域读取。返回 NotObserved 和业务不存在是不同结果。

消息发送、持久入队、消费和知识导入分别记录。未来全局反向索引既不能直接补入内容，也不能通过边的存在暴露尚未观察的推断。

### 5.5 全局一致快照

单 store 由协调器选定 durable commit fence，并捕获：

- 各 Branch head；
- Actor/Execution 前沿；
- 消息/outbox 状态；
- Activity/receipt/unknown 状态；
- timer；
- 状态根；
- 控制面高水位的审计引用。

它是一个一致 cut，不能通过分别读取多个变化中的 Agent 状态拼接。跨节点版本需要另外设计一致快照或共识协议，不把单节点 fence 直接推广为分布式保证。[R2]

### 5.6 可见性与实际输入分开保存

“当前有权读”“宿主为执行接受 Observation”“本次 CP 选择了材料”“请求可能已发送”“已取得对应回执”分别记录。Observation 不证明远端模型已阅读或理解；CP/RequestEnvelope 保存确切输入，派发/回执提供实际通信证据。RecordedReplay 不用今天重新渲染的输入替换历史 bytes。

V1 的 ContextPacket.sourceAnchor 只引用 CP 所在批次开始之前已经发布的 SnapshotAnchor，不能指向本 Commit 或同 batch 未发布 staging 的 Fence/Revision。Observation/知识 Revision 先发布，再由持久 pending-render 动作建 CP；中间退出恢复渲染即可。一般对象仍可引用同事务拓扑较早对象；后置 Continuation 可引用新 RevisionID，但不能引用包含自身的 CommitFence。这是避免摘要自引用的明确阶段规则。需要同事务留下后续渲染动作时，使用 §5.7 的源发布绑定，不在恢复时选择 latest。

### 5.7 SourcePublicationRef：固定尚在构造的源提交

~~~go
type SourcePublicationRef struct {
    Store        StoreID
    Space        SpaceID
    ProducerTx   TxID
    Cursor       OptionalCursor
    Knowledge    OptionalKnowledgeKey
}
~~~

PendingAction 的 InputSource 是已发布 SnapshotAnchor 或具体 SourcePublicationRef。新知识/后续动作同事务构造时，Profile 可在编译输入声明 this_publication；kernel 预分配 TxID 后、计算内容摘要前将它绑定为具体 ProducerTx。持久对象不保留“恢复时取当前”的浮动占位符。

发布后，只能从已验证事务链按 ProducerTx 解析**那笔事务自己的 LSN、CommitDigest、PostRootSet**，加所选 Cursor/Knowledge 得到唯一 SnapshotAnchor。不能用 latest、批次结束 fence 或另一个同内容 Revision 的创建位置代替。ProducerTx 尚未发布/缺失/不可用则不执行渲染，明确 blocked/unavailable。

精确 RenderContext 命令仍以该稳定 source ref 为输入；解析是确定性求值，不修改原 RequestID 的 body。CP 只在源事务已随前一批次发布后构造，其实际 sourceAnchor 保存解析结果。外层创建命令的 RequestDigest 不含后来生成的 TxID，已提交重试取原 receipt；内部输出对象则包含已绑定 ProducerTx。

注册表将 SourcePublicationRef/Anchor 声明为强历史保留依赖，与强内容边、逻辑实体边区别提取。ProducerTx 是预分配身份而非事务摘要，因此自身关联不会制造 hash 自引用；保留遍历用 visited 集合处理这种发布关系。PostRootSet/Txn locator 可重建，但不能在未认证日志上猜来源。

---

## 6. 版本化状态树

### 6.1 选择

第一版自研规范化的、路径压缩的持久化 radix tree，按原始字节 key 路由。

理由：

- 前缀范围和有序扫描自然；
- 不需要把 key 先 hash 成随机顺序；
- 便于定义稳定编码与状态摘要；
- 写时复制共享未变化子树；
- 比第一版同时实现 B+Tree 分裂合并与 MVCC 页回收更易缩小证明面。

此处“持久化数据结构”指所有保留版本仍可访问，不仅指写磁盘。[R3]

### 6.2 节点规范

一个节点包含：

- 公共路径前缀；
- 可选 ValueObjectID；
- 按 byte 升序排列、首 byte 唯一的 child 引用；
- childCount；
- 格式版本。

规范化规则：

1. 删除无值、无 child 的空节点；
2. 无值且只有一个 child 时合并路径；
3. child 顺序固定；
4. key 为原始 bytes，不隐式 Unicode normalize；
5. 一个逻辑 map 必须得到同一种规范树形；
6. 节点对象 ID 不含磁盘 offset；
7. 空 map 使用规定的 EmptyRoot 对象；空 key 的值由根的可选 value 表达，不能用空路径与不存在混淆。

由此可比较不同构建路径得到的 StateRoot。必须用属性测试验证 canonicalization，不能只靠“用了 Merkle hash”假设同逻辑状态同 root。

### 6.3 操作

- Get(key)
- Put(key, valueID)
- Delete(key)
- ScanPrefix(prefix, cursor, limit)
- Diff(rootA, rootB)
- Apply(mutationBatch)

Put/Delete 仅复制经过的节点；旧 root 不改。

Diff 必须按**相同绝对 key 范围**比较子树，只有位置对齐且 ObjectID 相同时才可跳过。路径压缩后的局部子树对象可能相同，但挂在不同前缀下；例如 {ax, ay} 与 {bx, by}，不能因为后缀对象相同而漏报删除与新增。

算法使用 (absolutePrefix, node, compressedPathOffset) 游标，先按最长公共前缀对齐；某侧没有对应范围时枚举该侧为新增/删除。测试覆盖路径拆分、压缩合并、空 key 与同后缀不同前缀。

### 6.4 状态操作语言

第一版使用固定、版本化的 StateOpsV1：

- AssertHead(branch, expectedRevision, expectedVersion)
- AssertValue(key, expectedValueOrAbsent)
- Put(key, valueID)
- Delete(key)
- AppendEvent(eventID)
- AdvanceCausalFrontier(...)
- RecordObservation(...) / UpdateExecutionKnowledge(...)
- EnqueueActivity(...)
- AcceptActivityReceipt(...)
- UpdateBranchRef(...)
- WriteCommandReceipt(...)

高层 Task/Agent 语义由宿主验证后编译为这些操作。每个 opcode 固定其写入目标：领域 Put/Delete 作用于指定 Revision root；观察操作更新知识根；Branch refs、控制账本和 receipts 更新第 7.7 节各自的根。禁止通用 Put 越过命名空间覆盖受保护控制字段。

WriteCommandReceipt、分配序号、fence 和出站 grant 等受保护操作由协调器生成/校验，不把调用者提供的“已审批/已结算”字段直接写成权威事实。内核执行固定操作而不是在提交锁内运行任意 Go 插件或调用模型。

后续自定义纯 reducer 必须具有明确的输入、版本、燃料/时间限制和确定性测试；第一版无需引入这种可执行扩展。

### 6.5 复杂度

令 L 为 key 字节长度，d 为某路径节点 fanout（d≤256），R 为变化节点数：

| 操作 | 第一版算法成本 |
| --- | --- |
| 单 key 查找 | O(L + 路径上的 child 查找成本)，有序 child 查找为 log d |
| Put/Delete | 同路径遍历 + 新节点编码/哈希；不复制整张状态 |
| Fork | O(1) 条引用元数据，不含持久提交 I/O |
| PrefixScan | 定位前缀 + 输出访问成本 |
| Diff | O(访问的不同子树节点 + 输出变化)，最坏仍可达全树 |
| 枚举整个历史状态 | 与输出大小成正比，不能称 O(1) |

路径树比 B+Tree/HAMT 更适合与否仍须基准验证；不能写成已经验证的最优解。

### 6.6 Logical Snapshot 与 Physical Checkpoint

- Revision.StateRoot 是逻辑版本入口；
- Physical Checkpoint 是启动加速材料，如状态/索引基线；
- snapshot 不包含任意 goroutine、栈或模型隐藏状态；
- 引用同一状态根不等于已经安全恢复外部资源；
- Physical Checkpoint 必须记录 source commit fence、格式和语义版本，并能对日志校验。

---

## 7. 事务与并发协议

### 7.1 并发模型

第一版：多并行生产者、单提交协调器、多不可变快照读者。

Agent 可以同时计算；只有状态验证和提交发布串行化。协调器不得持锁等待模型、shell、网络或用户审批。

Group commit 将多个合法事务按确定序组成一个持久发布批次。后继事务对批内 staging read root 校验 CAS，不能让两个事务都针对同一个旧 head 成功；批外仅能看到已发布 root。

### 7.2 命令幂等

每条可重试命令包含：

- Principal/Space；
- RequestID；
- canonical RequestDigest；
- 期望的 Branch head/version；
- 需要验证的跨分支读取版本；
- 当前执行代际令牌；
- 预期结果形态。

命令 receipt 与原命令效果同事务提交。持久幂等针对已提交 receipt；校验拒绝不默认产生 durable receipt，因此不保证跨重启保留同一拒绝理由。修正请求仍必须使用新 RequestID。

幂等键明确绑定 Space、稳定 Principal 和 RequestID；同键同摘要在进程内可共享一次在途执行，但 inflight 缓存不替代持久账本。

对已有持久 receipt 或在途身份，相同 RequestID + 相同摘要返回原结果或等待同一次执行；同 RequestID + 不同摘要返回 IdempotencyConflict。不得自动用最新版本替换原 expected head 后仍称相同命令重试。

### 7.3 事务校验

按以下顺序：

1. 校验调用身份和当前 receipt 读取权限；
2. 查询 RequestID：摘要相同即返回原 receipt，摘要不同即冲突；该路径不再次消耗配额或要求原 Branch head 仍然当前；
3. 仅对新命令校验当前执行权限、配额、writer/execution fence；
4. 验证所有显式输入 Cursor 和依赖对象；
5. 校验读版本与写 Branch CAS；
6. 执行固定 StateOps；
7. 验证因果闭包、对象强引用和 Revision root；
8. 构造事务 frames；
9. 执行持久化协议；
10. 发布 immutable read root；
11. 返回 DurableAck。

命令校验与历史投影重放分开：历史重放按原版本执行已提交 StateOps，不能拿今天的 ACL、余额或模型规则重新决定过去是否提交。保存原校验策略版本和证据用于审计；今天的权限只决定能否读取或继续执行。

单 Branch 写以整个 Branch head CAS 为默认冲突粒度。跨分支“读取当前状态后作决策”必须在事务中校验全部依赖 head；历史快照导入则作为明确的不可变输入，不伪称读取了当前版本。

### 7.4 读者语义

- 一次 ReadView 固定逻辑 Revision/cut，不追随 Branch head；
- 列表分页 token 绑定 ViewDigest、索引版本和排序规则；
- reader pin 同时保护逻辑根与物理 catalog generation；
- 捕获 published root 与增加 pin 必须在同一短临界区/正确的 epoch 协议完成；不能先读取旧指针、等 GC 删除后再 pin；
- GC retire 与 capture+pin 通过同一发布/回收协议互斥，reader Close 后才可释放其代际；
- 当前 ACL 撤销通过第 17.6 节的出站授权线性化规则限制后续读取和派发；
- 快照历史状态不可用于恢复已撤销权限。

### 7.5 线性化、耐久和 ACK

协调器确定事务顺序和 CAS 结果；持久化批次发布是对外读可见的线性化边界。

新读者在发布前看不到该批次，发布后看到全部合法事务；已有 View 继续看到旧状态。

存在正常窗口：

~~~text
事务已持久发布
    ↓
客户端连接断开，未收到 ACK
~~~

客户端必须通过相同 RequestID 对账。超时不等于回滚。

### 7.6 结果类型

- DurableAck：事务确定已提交；
- Rejected：该次校验执行拒绝，确定此次未提交；
- NotSubmitted：本次调用未被接受，不能据此判断更早同 RequestID 的尝试；
- WaitInterrupted：入队后调用者停止等待，结果未知，store 不因此自动 fence；
- CommitUnknown：同步/发布阶段失败，可能已持久，必须查询原 RequestID；
- StoreFenced：禁止继续写入、新 View 及新的对外读取/派发 grant，需要重开验证；
- CorruptionDetected：不得自动回退到旧历史；
- Conflict：CAS/读版本冲突；
- ResourceLimit：明确资源限制。

返回值必须区分这些情况，不能全部包成 retryable error。成功入队之后，调用者 context 取消或断线只停止等待，不撤销该命令。持久化失败导致的 CommitUnknown 与单纯等待中断是不同原因。

Fenced 时 CURRENT 可能已持久新权限而内存尚未发布，因此不能继续用旧 ACL 对外输出；旧 pins 只供内部收尾。Draining 则可让已准入操作与必要对账在当前授权下完成结果返回，同时拒绝新业务/派发。

RequestID 查询的 NotFound 必须绑定已验证的 published fence。CommitUnknown 后，在 writer 尚未重开完成前不能仅凭旧内存 receipt 表回答“确定未提交”。恢复确认原 RequestID 不在发布前缀后，才能用原请求重试。

AuditFull 下命令 receipt 默认长期保留。未来若要有界幂等窗口，需要客户端单调序号/明确关闭窗口协议；已关闭窗口的旧请求必须返回 Expired 而非当成新命令执行，不能任意 GC receipt 后允许重复效果。

### 7.7 历史状态与当前控制面的根

协调器一次发布的 immutable PublishedView 至少包含 BranchRefRoot、LiveControlRoot、CommandReceiptRoot、对象 catalog generation、published LSN 与摘要。这些根作为同一批次捕获和 pin，不拼接不同提交时刻的值。

| 根/状态 | 可按历史读取 | Restore/Fork 能否覆盖当前值 |
| --- | --- | --- |
| Revision.StateRoot、ExecutionKnowledgeState | 能 | 只能建立新的历史状态，受第 10 节知识规则约束 |
| BranchRefRoot | 能审计 | 更新必须针对当前 head/version CAS |
| LiveControlRoot：执行 epoch、当前授权、现实预算、效果账本 | 能审计旧值 | 禁止倒带，执行使用当前值 |
| CommandReceiptRoot：命令幂等结果 | 能审计 | 禁止倒带或复活已消费 RequestID |

历史读取旧控制事实不授予执行权。不能仅将所有对象存进一棵可回滚的 StateRoot，再依靠调用者“记得别覆盖”。

---

## 8. 磁盘格式和持久化协议

### 8.1 文件布局

~~~text
store/
    IDENTITY
    LOCK
    CURRENT
    manifests/MANIFEST.<generation>
    segments/SEG.<id>
    indexes/...
    checkpoints/...
    quarantine/...
~~~

事实来源是提交日志中的事件、状态操作和决议。Manifest 仅证明持久提交下界和物理布局；不得用它伪造日志中不存在的逻辑状态。

### 8.2 Segment

Segment 追加写，封存后不可原地修改。跨 segment 的 predecessor 指向前一封存段的物理摘要；轮转时先完成其封存材料，不能引用尚在变化中的“最终摘要”。Header 包含：

- magic；
- storageFormatVersion；
- StoreID；
- SegmentID；
- 创建代际；
- predecessor 物理摘要；
- header checksum。

第一版一个持久批次可以触及多个 segment。批次外读者和事务不得引用尚未发布对象；同一原子批次中，后继 staging 事务可引用较早、已完整验证的 staging 对象，整个依赖批次一起持久发布。

### 8.3 Frame v1 候选格式

候选固定 Header 为 48 bytes：

| 字段 | bytes |
| --- | ---: |
| magic | 8 |
| formatVersion | 2 |
| kind | 2 |
| flags | 4 |
| headerLength | 4 |
| payloadLength | 4 |
| TxID | 16 |
| ordinal | 4 |
| headerCRC32C | 4 |

Header 后为 payload、零填充至 8-byte 对齐及 8-byte trailer（recordCRC32C + end magic）。

CRC 用于检测物理写损；对象摘要用于内容身份。两者不能互相替代。

整数使用固定 endian 和明确溢出检查。所有 length 在分配内存前校验；拒绝未知 mandatory flag、非零 padding 和重复 ordinal。

### 8.4 事务 frame

~~~text
TX_BEGIN
BODY_RECORD*    # OBJECT / EVENT / STATE_OPS / REVISION 按内容依赖拓扑混排
REF_UPDATES
CONTROL_UPDATES
COMMAND_RECEIPT
TX_COMMIT
~~~

BODY_RECORD 不按类型整体分组；允许 Revision 后出现引用它的后置控制对象/事件。REF_UPDATES/CONTROL_UPDATES 为已构造 body 内容的逻辑更新描述。COMMAND_RECEIPT 确认/引用 body 中已编码的 canonical receipt record；receipt 树引用该 record，不引用尾部 frame 或最终 CommitDigest。所有 content refs 在被引用的 body 记录之前已存在，逻辑 TxID 发布引用按 §5.7 校验。

TX_COMMIT 绑定：

- TxID；
- CommitLSN；
- 前一事务摘要；
- frame 数量与顺序摘要；
- Revision/RefUpdate/ControlUpdate 摘要与 PostRootSet（含各控制/引用/receipt 根）；
- RequestID/RequestDigest；
- receipt 引用。

没有完整 TX_COMMIT 的批次不是事务。完整 TX_COMMIT 但尚未获得持久发布证明，也不能向客户端报告 DurableAck。

### 8.5 大对象

大模型响应、文件快照和上下文不能直接占满事件元数据。

- Event 保存类型化 blob refs；
- blob 按候选 1 MiB chunk 分块；
- chunk manifest 记录顺序、总长度、摘要；
- 新对象 frame 必须在同事务内较早出现，或在此前已发布；同一原子批次的较早 staging 事务对象也可引用，受批次依赖验证约束；
- 第一版不使用“先异步上传 blob、先提交 ref”的弱一致路径；
- 引用未持久化对象必须拒绝。

空间优化不能使已发布 Revision 指向半个 blob。

### 8.6 严格持久化协议 P1

P1 为优先可审计、允许 group commit 的初始协议；性能须测量。

CURRENT 是发布证明的一部分，不是可丢弃的缓存。所有 manifest/CURRENT 更新——包括提交、checkpoint、compaction——必须经唯一 publication owner 仲裁。generation 使用 CAS，且新 durableFloor 不得小于当前 floor；后台维护不能拿旧 manifest 覆盖新提交。

首次创建先在空目录建立 IDENTITY、初始 segment 和 MANIFEST.0/CURRENT，以同等同步规则发布 genesis。非空但缺少完整 genesis 的目录作为未完成创建/损坏处理，禁止自动当作新库覆盖。

1. 串行验证批次内所有事务，生成 staging read root。
2. 追加所有 frame。
3. 对所有触及的 segment 执行平台 DurableSync。
4. 若创建新 segment，持久化其目录项。
5. 写不可变 MANIFEST.g.tmp，包含：
   - generation 与前一 manifest 摘要；
   - durableFloorLSN / CommitDigest；
   - 可用 segment 清单、active/sealed 状态、publishedEndOffset 与认证前缀摘要；
   - checkpoint/index 的版本和 coverage hints；
   - 所需对象/格式版本。
6. DurableSync manifest 临时文件。
7. rename 为 MANIFEST.g，并同步 manifests 目录。
8. 写 CURRENT.tmp，包含 generation、manifest 摘要和 checksum。
9. DurableSync CURRENT.tmp，原子替换 CURRENT，同步 store 目录。
10. 原子发布内存 immutable root。
11. 返回该批次所有 DurableAck。

active segment 的已认证前缀与实际文件尾部必须分开；追加未发布 tail 不能使旧 manifest 因“全文件摘要变化”失效。

Manifest.g 文件存在不代表该代际已发布；以经过验证的 CURRENT 指向为准。恢复不能扫描最大的 manifest 文件名就自行提升提交前缀。

P1 明确承担额外同步成本。后续可用双 superblock/control journal 优化，但必须重新证明同等正确性，不以性能为由省略目录持久化。

### 8.7 持久提交下界与尾部损坏

必须区分：

- 已证明 durable floor 及其之前的数据；
- floor 之后、从未获得 ACK 的 staging tail；
- CURRENT/manifest 自身缺失或损坏导致的未知边界。

任何 ≤floor 的缺失、CRC/摘要不符必须报损坏，禁止静默截断。

> 单凭最后一个 frame 的 CRC 错误，不能区分未确认写入的撕裂与已确认历史后来损坏。

只有已验证的持久前缀及明确的故障模型证明损坏位于未发布尾部时，才允许隔离/截断尾部。CURRENT 本身含糊时 fail-stop；禁止猜测旧 manifest 并继续写入，伪装成成功恢复。

### 8.8 同步失败

在写入或同步阶段发生错误：

- 不返回 DurableAck；
- 标记 writer fenced；
- 返回 CommitUnknown，或在确定未开始持久写时返回 Rejected；
- 后续查询必须使用 RequestID；
- 重新打开时验证 CURRENT、manifest 和日志；
- 不继续向可能不一致的旧内存状态追加新事务。

### 8.9 索引不是额外权威事实

新索引可滞后、损坏并重建，但：

- 不得使缺少索引的事实被误判为不存在；
- query 必须知道 coverage fence；
- coverage 不足时扫描权威记录、等待重建或返回明确 IndexUnavailable；
- 不允许用 Bloom filter 的否定结果证明越权或关键状态不存在。

---

## 9. 启动恢复与故障自愈

### 9.1 Open 流程

1. 验证目录权限和 store identity，获得独占写锁；
2. 验证 CURRENT 的校验、generation 和 manifest 摘要；
3. 验证 manifest 引用的 segment/checkpoint；
4. 扫描所有已发布事务，校验 frame、事务链和内容对象；
5. 从兼容 checkpoint + 后续 StateOps 恢复根；
6. 校验恢复得到的状态根与 Revision 声明一致；
7. 恢复 Branch refs、RequestID receipts、控制高水位；
8. 检查索引 coverage，必要时重建；
9. 隔离被证明未发布的尾部；
10. 输出 RecoveryReport，再开放读写。

第一版可接受完整扫描作为正确性基线；启动加速不可依赖未校验的缓存。

### 9.2 故障矩阵

| 中断点 | 重开行为 |
| --- | --- |
| TX_BEGIN 之前 | 无该事务 |
| frames 写一半，CURRENT 未变 | 忽略/隔离未发布 tail |
| TX_COMMIT 完整，segment 未同步 | 无 ACK；按发布证明恢复，不能假定完整字节即持久 |
| segment 已同步，manifest 未发布 | 数据可能存在，但不自动提升为已发布事务 |
| manifest 完整，CURRENT 仍旧 | 使用旧 CURRENT，保留新文件供诊断 |
| CURRENT 替换/目录同步结果未知 | 无 ACK；重开校验，必要时 CommitUnknown/fail-stop |
| CURRENT 已持久、内存未发布 | 重开恢复该批次；客户端重试查 receipt |
| ACK 已发、客户端未收到 | 相同 RequestID 返回原结果 |
| 已发布 segment 位翻转 | CorruptionDetected；不回退到更早成功历史 |
| 派生索引丢失 | 重建，不重新执行模型或工具 |

### 9.3 自愈级别

- **L0 检测**：识别损坏、失配、未知外部结果；
- **L1 重建**：重建派生索引、缓存、检查点；
- **L2 确定性补齐**：补发 durable outbox、补结算已接受 receipt；
- **L3 受控修复提案**：模型提出修复，宿主验证并在新分支执行；
- **L4 不可证明修复**：需要 operator、外部对账或恢复副本。

禁止模型把 CorruptionDetected 改成“忽略该错误继续”。

### 9.4 备份与旧备份接管

备份必须固定 published generation、pin 文件集合并输出 manifest 摘要。

从旧备份恢复必须创建新的 DeploymentIncarnation。历史 epoch、预算和外部操作表不能被当成现实最新状态。

若旧部署仍可能运行，必须：

- 隔离其资源；
- 或由资源端拒绝旧 incarnation；
- 或确认旧进程/远程 worker 已停止。

单纯给恢复后的本地计数器加一，不能证明它大于仍存活旧部署的 epoch。

---

## 10. 历史、分叉与恢复执行

### 10.1 四种读取/执行模式

| 模式 | 含义 | 是否允许新外部请求 |
| --- | --- | --- |
| RecordedReplay | 复用当时实际 ContextPacket、请求、响应和回执 | 否 |
| StrictCutReconstruction | 从当时可见来源按指定规则重建视图 | 默认否；新摘要需独立 Activity 并标注 |
| CounterfactualContinuation | 从历史状态用新模型/策略或导入后来经验探索 | 可以，agent/v1 建新 Task/Execution 与当前授权，不复活旧控制状态 |
| RetrospectiveAnalysis | 允许查看后续结果解释过去 | 分析本身可只读；新模型调用仍记录 |

后三者不能冒充历史原样回放。

引擎能保证管线来源符合 cut，不能证明新模型参数没有未来世界知识。

### 10.2 ReadAt

ReadAt/OpenView 使用 SnapshotAnchor。Cursor 选择领域/知识 Revision，Fence 选择全局控制状态；若仅提供 Cursor，必须同时明确捕获的 Fence，不能推断唯一控制历史。执行知识读取要求 KnowledgeKey，宿主全局审计是额外权能。

对已经受控遗忘、外置归档未取回或损坏的对象，返回 HistoryUnavailable/ContentUnavailable，不能用当前状态填补。

### 10.3 Fork

Fork 输入：

- source SnapshotAnchor（其中 Cursor 指定分叉 Revision）；
- expected source identity；
- branch purpose；
- initial View mode；
- Principal；
- 保留/预算策略；
- fork reason。

输出新 BranchRef，指向源 Revision。状态对象共享。

创建执行分叉时，agent/v1 以 SourceAnchor 建新 Task/Execution（引用 forkedFrom），不得回滚原 TaskControl。Core 的只读 Checkout/Fork 不自动新建 Agent。执行分叉还必须产生 ResumePlan：

- 新 Execution/Incarnation；
- 新 Task/Execution 范围及待绑定外部资源；
- 作为来源证据继承的已确认结果；跨执行显式 import/reuse，不把旧操作直接标成新 Activity 的 accepted result；
- 未完成 Activity 的处理；
- 当前审批/权限检查；
- 预算账户映射；
- checkpoint/模型/工具兼容性结果。

没有通过 ResumePlan 的分支保持只读或 blocked，禁止直接运行历史 outbox。

### 10.4 Restore 与回滚

公开默认操作：

- Checkout：读取过去；
- Fork：从过去分出新路径；
- Restore：在当前历史追加“采用历史状态”的新 Revision；
- Compensate：记录新的外部补偿操作。

不默认提供抹除旧提交的 destructive reset。

同一 Branch 的 Restore 以当前 head 为 StateParent，追加确定性 mutation 采用选定历史领域值；当前 causal cut 和已发生的知识观察必须保留，再记录 Restore 事件。它表示“知道发生过什么之后，决定采用旧值”，不能声称忘记未来。

若要从旧知识视角探索，必须新建 Fork/Execution View，并明确使用 StrictCutReconstruction 或 CounterfactualContinuation；不能在原 Branch 倒退知识。

Restore 不能恢复旧执行令牌、撤销现行权限、复制旧预算余额或把已执行外部操作变成未执行。逻辑状态与活控制面必须分离。

### 10.5 到任意“节点”的准确含义

- 任意 Event 可追踪其来源和后果；
- 只有一致 Revision/commit boundary 可作为完整状态恢复点；
- 同一事务内部的中间 Event 不保证对应独立可执行状态；
- 用户选择该 Event 时，引擎应返回所属事务边界和可用 Cursor；
- 任意机器指令、网络半包或模型生成中途不能自动成为安全重启点。

### 10.6 查看未来并重新探索

在 RetrospectiveAnalysis 可查看后代，但来源必须标明为“后续事实”。

把后来经验带回旧点继续时，创建 Counterfactual branch，并记录 KnowledgeImport。不能从原始节点偷偷注入未来信息，再称“当时本来就会这么做”。

---

## 11. 多 Agent 执行与 Activity

### 11.1 执行边界与工具分类

ATE 不替代宿主 Agent Loop。Core 提供通用事务/恢复，官方 agent/v1 提供标准执行记录和纯状态机，宿主执行模型、工具、工作区与业务验收。

每个模型要求的工具调用先有稳定 ToolInvocation，固定类型化 DecisionRef、accepted model result、providerCallKey、工具定义和参数。DecisionRef 为 Execution 的 LoopStep 或 Query 的 (QueryID,DecisionSequence)，不为路由/短交互伪造 Task/Execution。providerCallKey 是适配器按冻结规则规范化的响应内调用键（保留提供 ID/ordinal，缺失或重复按规则处理），不把远端调用 ID 当作全局幂等身份。按工具契约分为：

| 类别 | 路径 | 恢复依据 |
| --- | --- | --- |
| EngineCommand | Spawn、消费、受保护状态更新 | 原子事务、receipt、逻辑动作唯一绑定 |
| EngineQuery | 固定 Anchor 的节点/证据读取 | 确切查询参数、检索版本、结果对象与 Observation |
| ExternalActivity | 模型、shell、网络、live 文件读取、资源操作 | Activity/Attempt、派发证据、raw result、acceptance、unknown |

名称“读工具”不等于 EngineQuery；grep 读取外部变化中的目录仍是 ExternalActivity。内部命令不用伪装成外部 uncertain effect；只读查询本身不写领域状态，但把结果用于模型前必须持久保存披露和来源。

### 11.2 Activity 的正交状态

不能用一条 result_recorded→settled 线代表所有阶段：

| 维度 | 含义 |
| --- | --- |
| Attempt 状态 | created/dispatch_recorded/result_recorded/effect_unknown 等，每次尝试独立 |
| Activity acceptance | 逻辑操作至多一个合法 acceptedResult，CAS 选定并保留证据 |
| Settlement | 费用、资源等后续账务 pending/partial/settled/unknown，可先于或晚于消费 |
| Consumption | 每个接收 scope/观察批次的披露记录；Execution 接收必须带 KnowledgeKey 并原子推进知识，不是 Activity 单个全局布尔值 |
| Cancellation/effect | 取消或运行终止不证明外部未执行，unknown 需独立对账 |

raw result_recorded 仅证明回执已捕获。接受失败/无匹配输出也可能是合法 acceptance，但不证明内容正确或目标成功；不合法或晚到回执仍可审计，不自动进入工作知识。

Observation 默认引用该逻辑操作的合法 acceptance；未被接受的 raw/旧 epoch 结果只能经明确 EvidenceImport/Reconcile 审批作为证据，不能悄悄满足新执行或新 Activity。已选定的接受事实不原地替换；需要重新执行使用明确的新操作/来源关系。

reconciled 结论区分 Occurred、DefinitivelyNoEffect、Compensated、Unresolved，并覆盖全部相关 attempts 与接收方契约。即使强一致查询暂时返回 not-found，旧请求仍可能稍后到达；无终结禁止未来效果、资源端 fence 或覆盖全部 attempts 的有效幂等契约，不得宣称非幂等重试安全。补偿不等于从未发生。

### 11.3 Activity 身份、归属与来源

ActivitySpec 为不可变逻辑操作描述，至少含：

- ActivityID、OwnerScope（Query/Execution/系统或其他已注册实体范围）；
- role（routing、primary_decision、aux_summary、external_tool、resource_prepare、validation 等）；
- 类型化 Origin：可含 QueryRef、StepRef、InvocationRef、SourceAnchor、输入对象和 GoalRef；按 role 校验必需项；
- 确切请求/ContextPacket（模型活动必需）、请求摘要、接收方/effect 契约及版本；
- 幂等作用域、期限、资源基线与允许派发的 authority 范围。

Attempt 另有 AttemptID、当前 claim/authority、dispatch evidence、raw result refs。ActivityControl 另有 acceptance 与 settlement，不将尝试和逻辑操作混为一个对象。

Query 路由/直接聊天可无 Task/Execution/Step，以 Query scope、DecisionSequence 和 SourceAnchor 固定输入；有工具的短交互仍可登记 Invocation/结果/披露，下一 Query Packet 固定这些来源，不强制进入目标任务；工作区准备可在首个 LoopStep 前发生；辅助摘要不假装成 primary 模型重试。无 Execution 时使用经授权的 store/Query controller，不伪造 Execution epoch。

### 11.4 四个不同承诺

| 承诺 | 内核能否独立保证 |
| --- | --- |
| 请求只登记一次 | 能，RequestID + 原子 receipt |
| 同一逻辑操作的合法结果至多接受一次 | 能，CAS acceptance；不等于输出内容为真 |
| 外部工具实际只运行一次 | 通常不能 |
| 外部业务效果只发生一次 | 取决于接收方幂等/事务/查询契约 |

Outbox 解决“状态提交与派发意图一起持久化”，不消除“外部完成、本地没记回执”的窗口。[R7]

### 11.5 Effect 能力

不用单个 risk 字符串代替真实重试能力。采用正交元数据：

- 是否允许外部变化；
- 是否为已验证纯计算；
- 是否能读取历史快照；
- 幂等键作用域、保留期和同键异参行为；
- 是否可可靠查询结果；
- 是否支持资源端 fence；
- 是否有补偿操作及其限制。

当前 Control 5 的 ActivitySpec 持久化不可变 `EffectContract`，并固定接收方、精确请求和操作 key。
`IdempotencyGuarantee` 是独立的有限接收方证明，覆盖全部既有 Attempt 和下一 sequence，
包含 receiver epoch、保留窗口以及晚到请求的 deadline 或 epoch fence 证据。最新版本可撤销保证。
宿主为当前 authority 和下一 ActivityControl 版本签发 `RetryAuthorization`，在 Retry、Claim 和
Dispatch 分别提交窗口内的 `RetryUse`，检查时刻按该顺序单调不减。当前版本、Attempt 集合、时序或
接收方约束不匹配时保持 blocked。
无保证的重试限于 `pure` 或所有可能已派发的 Attempt 均有终结 `no_effect` 且 `FutureEffectFenced`。
宿主提供可审计的显式纳秒时序与终结证明，并在发送时执行接收方 deadline 或 epoch fence。重放使用已记时序。

成功纯读也可能观察到与过去不同的外部状态；重新执行不能当成事实回放。

### 11.6 执行隔离和旧 worker

Execution authority 包含 DeploymentIncarnation、owner epoch、ExecutionIncarnation 和 lease 状态。

这些是当前控制面，不随 Branch 恢复到过去。

验证位置至少包括：

- 命令接受；
- Activity claim；
- 派发；
- 结果接受；
- Workspace 发布。

仅在调用前检查 epoch 仍有 TOCTOU。文件系统/shell 需要隔离 view，或确认旧进程树退出后再移交；可支持 fence 的远端资源必须在资源端校验。

旧 epoch 的晚到回执：

- 不推进新运行状态；
- 不满足新分支 Activity；
- 进入隔离审计/费用/外部效果对账通道；
- 不直接丢弃，避免掩盖已经发生的扣费或发信。[R8]

### 11.7 Spawn、消息、取消与重派

Spawn 以 (OwnerScope, Invocation) 的唯一逻辑动作槽为守卫，不能只依赖调用者总会复用 RequestID。Query scope 若请求工作派生，必须先通过 StartWork 建立明确的父/Root 执行绑定；普通 Execution Spawn 再校验父约束。一次事务 CAS InvocationControl pending→fulfilled，绑定唯一 SpawnResult、child Execution/admission、父子关系、初始 Continuation 和必要根更新；换新 RequestID 也不能生成第二个子执行。外部 Invocation 同样唯一绑定逻辑 Activity，网络重试只增加 Attempt。

创建时同时验证当前 Task/Execution/祖先取消屏障、目标版本与资源约束。无隐式 detached worker；一个父 Step 收到 SpawnResult 可完成，但父 Execution 终止须对未结束子树完成 join 或显式取消/责任交接，不随 Step 关闭抛弃子执行。

消息发布、投递、消费分开。消费以 (MessageID, recipientExecution) 的唯一绑定/CAS 与 Observation、知识位置和下一动作原子更新；重读可新增显式证据访问，不能把换 RequestID 解释为第二次首次消费。父子关系不自动导入私有 payload。

取消先原子写祖先取消屏障，后续 Spawn/Claim/派发据当前祖先链拒绝；再用 durable pending actions 传播取消、回收/隔离资源。传播中断可恢复，不宣称远端立即停止。Task 取消覆盖其所有承担执行及规定后代；只取消单 E 则针对该执行子树。

重派默认取消旧 E 子树，保留旧父子关系、取消责任与未知效果账本，不把活孙 Agent 偷改挂到新 E。新 E 只有在资源 fence/停止证据或隔离条件成立后才能使用重叠资源；否则 blocked。取消未收敛由当前 Supervisor/控制账本持续负责，不能失去归属。

### 11.8 Join

支持 all、any、quorum，规范如下：

- 成员集冻结，或通过显式版本化命令变更；
- any 的赢家按第一个被宿主持久接受的合法结果确定；
- 不能以未定义的“物理最快”作为可回放规则；
- timeout 与完成以提交序决定唯一结果；
- 取消输家不抹去其外部效果；
- Task join 与 Branch state merge 是不同操作，不混同。

### 11.9 时间、随机、预算

- 需要影响决策的时间和随机值作为输入记录；
- timer deadline 的墙钟/逻辑时钟定义显式版本化；
- 墙钟回拨和重启后逾期 timer 按固定规则处理；
- 恢复历史不返还现实已消费预算；
- fork 不复制可花费余额；
- provider 未返回精确 usage 时标 estimated/unknown；
- 模型请求未知时可能已收费，不能承诺恢复零重复费用。


### 11.10 Continuation、PendingAction 与恢复

官方 agent/v1 定义标准记录；宿主只提供版本化 adapter checkpoint，不能靠聊天记录重新推断程序位置。Continuation 绑定 owner scope、阶段、当前 Step/知识 Cursor、adapter 版本及下一批 PendingAction refs。

每个 PendingAction 记录：

- 稳定 ActionKey（owner+动作槽），generation、当前控制版本；
- 完整原 RequestKey（Space、稳定幂等主体、RequestID）；
- exactCommandRef/digest、预期 head/控制版本、不可变输入 refs 与 InputSource（Anchor 或 SourcePublicationRef）；
- authority binding、作用域与资源前提；预期当前控制版本使用数值版本/不含自身的前驱引用，不能把包含自身的记录 hash 嵌回 exactCommand；
- 原已知结果、状态与 supersedes 关系。

ViewRef、当前内存指针和仅一个 NextAction 枚举都不足以恢复。QueryControl 的 pending-intake/StartIntent 也遵循同一协议；接收 Query 去重不自动覆盖后续 Root 初始化。

恢复按每个动作执行：

1. 验证发布前缀并恢复全局 RootSet，取得当前合法 owner/资源隔离条件。
2. 查询完整原 RequestKey；已提交则复用原结果，只补尚未完成的处理。
3. 结果未知先恢复/对账，不生成新 ID 规避；外部 Attempt unknown 仍受 effect 契约约束。
4. 只有证明原动作未提交、旧 authority 已 fence，才用 RebindPendingAction CAS 原动作版本，生成新 generation、新 RequestID 和明确更新的授权/前置条件；原 body 不改写。
5. 同时校验当前目标/取消/资源限制。纯 Query 使用持久 Anchor、确切筛选/排序/版本补读，无法重建则 blocked，不能换 latest 冒充旧结果。
6. 先补结算合法结果，再推进必要 Observation/下一动作；不重生成已接受的计划、Spawn 或合并决议。

接管者必须获准查询原幂等命名空间；认证身份变化不能被当作“同一命令的新 body”。Rebind 只解决引擎命令准入，不授权重跑未知外部效果。

PartialOutput 保存已持久 chunk 与完整性，不能将不同模型生成片段拼成同一响应。Fenced 时不能接收新 RecordResult；外部已收到但未持久回执不承诺无损，恢复保持 unknown 并按接收方契约对账。

### 11.11 Workspace 与外部资源恢复

文件系统状态不是数据库状态树的自动延伸。外部 Workspace 适配器应保存 WorkspaceSnapshot/Artifact，包括内容对象、相对路径、文件类型/模式、基线摘要和排除规则；不能把任意绝对路径当作可安全恢复的引用。

从历史点继续，先在隔离工作区物化可验证快照，校验 manifest/root，再赋予新执行写权限。旧 shell 进程仍可能写入的目录不能复用为新执行的安全基线。发布文件成果使用基线 CAS/冲突检查，并把实际产物、检查命令与结果绑定到该产物版本；“在别的版本上测试通过”不能作为当前结果的验证。

无法快照的远端服务、未跟踪文件、凭据、环境和外部设备作为 ResumePlan 的显式缺口。引擎保证这些缺口不会被隐藏，不承诺替代资源适配器的恢复能力。

### 11.12 官方 agent/v1 Profile 的最小契约

该 Profile 是第一版随 ATE 提供的标准能力，不使用 GSEA 类型。关闭 Profile 的 Core-only 模式不能宣称通过 Agent 第一版验收。

| 逻辑实体/记录 | 统一规则 |
| --- | --- |
| Session / QueryRequest / Answer | Query 不包住全部执行生命周期；Answer.respondsTo 指向 Query，进度/澄清/最终答复分型；普通聊天不强制创建 Task |
| TaskDefinition / TaskControl | 同 TaskID；定义不可变，当前目标、取消、结果接受在受保护 Profile 控制范围；parent/origin 固定，不随换目标重写 |
| Actor / Execution | E 固定一个 Task、Actor、至多一个父 E；Actor/Task 可有多个 E；恢复保留 E 增 incarnation，重派新 E；暂不引入 AssignmentID/模糊 RunID |
| ExecutionSpec / Control | 固定初始来源/Seed 与当前 acceptedGoal、InheritedGoalBindings、authority、Continuation 分离；ActorSpec 不赋予执行权限 |
| LoopStep | (ExecutionID, StepSequence)，序号跨恢复延续；每 E 主步骤串行，固定实际 GoalRef 与输入 Anchor；最多一个 primary 模型操作；改变主决策 prompt/参数必须新 Step，辅助活动不能替换该 primary acceptance |
| ToolInvocation | 来源唯一键=(DecisionRef,acceptedResultRef,providerCallKey)，绑定工具版本/参数/dispatchClass；执行使用 Step，Query 短交互使用 (Query,DecisionSequence)，不重复登记 |
| Activity / Attempt | Invocation 为 external 时绑定唯一逻辑 Activity；模型/路由活动可无 Invocation；Attempt 不覆盖旧发生 |
| Message / Observation | 消息主体固定，收件 scope 分别消费；Observation 是事件+payload，无额外同义实体 ID；Execution 接收带 KnowledgeKey，Query 可只记输入选择/披露，不伪造知识 Revision |
| Result / Artifact / Validation / Summary | 不可变 ObjectID；仅有长生命周期逻辑版本需求时才额外建实体 ID；验证绑定确切 Goal/产物/命令/环境与授权接受规则 |

QueryControl 在接收时原子保存 pending-intake 和下一动作；StartIntent 固定路由结果、输入、配置、资源基线和 RequestKey。StartWork 同事务 CAS 启动槽并保存 Query→Task/Root 绑定，换命令键也不能重复同一槽。一个 Query 可有多个明确启动槽。最终 Answer 也绑定 Query 的明确响应槽（进度可有独立序号）；恢复重发已有正文不重新执行任务或创建第二个首次完成结果。

GoalAdopted 仅更新 TaskControl/ExecutionControl 的目标绑定。下一 Step 必须先正式 Observation 新目标，并让 CP 绑定该 GoalRef；派发检查 Task 当前目标、E acceptedGoal、Step/CP 目标关系。旧结果保留，但不能在目标已变后自动执行旧计划。

子任务还必须保存实际继承的强制约束：InheritedGoalBinding{sourceTask, goalRef, goalEpoch, inheritedConstraints}。它只列显式继承/持续依赖的父约束，不默认复制全部祖先目标。父 TaskControl 的 goalEpoch 单调递增；后代在新 Step、Claim、外部 grant 和完成门禁重新核对绑定。版本不兼容或新约束尚未观察时进入 Blocked。GoalRebase 产生新的不可变子目标/依赖版本并 CAS 更新绑定；兼容性必须有受信策略或用户确认的具体证据，不能由模型自称兼容。旧 Step/CP 不改写，已签 grant 依其线性化顺序收敛。

Task 完成依据当前目标、继承约束和正式验收证据，不依据模型一句“完成”。

### 11.13 记录粒度与完成条件

| 动作 | 必须保存/原子更新 | 版本边界 |
| --- | --- | --- |
| Query 接收/路由 | 输入、来源、pending action、唯一启动槽 | 可仅控制 Commit，无需假 Agent Revision |
| 主模型响应接受 | raw/acceptance、唯一 Invocation 集合、Step 与下一动作 | 不重新请求模型恢复已接受决议 |
| Spawn/内部命令 | Invocation 唯一结果绑定、child/状态/receipt | 同事务，成功后才启动 Worker |
| 外部派发 | Attempt、当前授权/预约、dispatch intent | 持久后执行；结果回执另记 |
| 结果接受/结算 | acceptance 与各自幂等账本 | 可合法同事务，不与消费混同 |
| 观察/目标导入 | 确切披露/来源/顺序、KnowledgeKey CAS、知识根 | 生成一致知识 Revision；下一 CP 只用已发布 Anchor |
| Query 工具结果 | Anchor/参数/版本、确切 ResultSnapshot、Observation | 读操作本身不写；披露前保存，恢复不改 latest |
| 子消息消费 | inbox 唯一消费、Observation、知识根/Continuation | 同事务；消息发布不等于父已知 |
| 任务完成/答复 | Outcome+Validation、当前 Goal CAS、独立 Answer/投递状态 | 目标已完成但答复失败，只补答复 |

Step completed 要求 primary 结果已处理，所有必需 Invocation 已有合法结果/明确处理的拒绝或失败，必需观察与下一 Continuation 已持久；settled 不能替代这些条件。Spawn 的合法结果可以只是 child 身份，子树责任仍归父 Execution。未知必需效果使步骤/任务 blocked 或明确终止，不能默认为成功；明确取消/aborted 与成功完成分型。

注册表为每种命令定义 schema、前置版本、命名空间权限、纯转换、引用/索引提取和拒绝规则；Profile controller 不能普通 Put 系统 authority/预算字段。业务验收由宿主策略提供证据，Profile 校验版本、来源、产物绑定和允许的验收规则，不宣称能独立证明现实观察为真。

---

## 12. 分支汇聚和冲突

### 12.1 合并不是重新播放所有祖先

对 DAG 取一次确定性拓扑排序，只能确定顺序，不能证明非交换状态操作具有正确合并语义。

Revision 恢复必须使用：

~~~text
StateParent.state
    + 已记录的 resolved Mutations
    = Revision.StateRoot
~~~

不得对多个 parent 的全部历史做并集再重复 apply。

### 12.2 MergePlan

MergePlan 包含：

- 输入 Cursor 和 expected target head；
- 完整 join 或 selective import；
- 共同基点集合；
- 各来源状态差异；
- 所有冲突；
- 采用/拒绝的来源；
- 自动规则及版本；
- 人或模型提出的决议；
- 宿主验证证据；
- 预期结果 StateRoot。

模型可提出解决方案，但不能直接覆盖 head。

### 12.3 多共同基点

DAG 不保证唯一 LCA/merge base。[R6]

第一版使用精确算法，边从 Revision 指向 parents，包含 StateParent 与完整 JoinParents：

~~~text
AL = AncestorsInclusive(L)
AR = AncestorsInclusive(R)
C  = AL intersect AR
nonBest = union(directParents(c) for c in C)
bases = C - nonBest
~~~

共同祖先集合 C 对 parent 闭合，因此任何不是最佳基点的共同祖先，都会是某个共同节点的直接 parent；这一步不需要逐对做 reachability。成本为实际访问子图的 O(Vq + Eq) 与集合开销。

- bases 为空：没有共同历史，要求显式无基线导入；
- 只有一个：可做三路比较；
- 多个：默认 AmbiguousMergeBase，要求明确基线和逐冲突决议，不任意选一个；
- 后续再单独验证 virtual-base/confluent merge。

缓存 generation=1+max(parent generation) 只能用于负向剪枝：较小 generation 不可能是较大 generation 的后代；数值相等或大小本身不能证明祖先关系。算法输出必须与无优化参考模型一致。

### 12.4 默认状态冲突规则

单一明确 base 时，对每个变化 key：

| A 相对 base | B 相对 base | 默认结果 |
| --- | --- | --- |
| 不变 | 修改 | 采用 B |
| 修改 | 不变 | 采用 A |
| 改为相同值 | 改为相同值 | 采用该值 |
| 修改为不同值 | 修改为不同值 | 冲突 |
| 删除 | 修改 | 冲突 |
| 已确认外部效果 | 逻辑恢复为未执行 | 禁止自动归并 |

禁止对 Task completed/failed、权限、预算和事实真假使用通用 last-write-wins。

absent 与 null/空 bytes 是不同值，删除也属于明确 mutation。字段级无冲突不代表跨字段不变量成立：例如约束 x+y≥1，两个来源分别从 (1,1) 变成 (0,1) 与 (1,0)，逐字段自动合并得到 (0,0)，仍必须拒绝。MergePlan 需要宿主领域不变量验证。

有交换性证明的集合操作可注册自动规则；规则版本和操作原始证据必须保存。两个来源各发生一次 +1，不能因为 payload 相同就当作同一次发生；是否累加必须由明确规则决定。

### 12.5 Selective import

选择性导入某个结果或经验时：

- 引用选中 Event/Memory 的来源和因果闭包；
- 不把整个 source head 谎称为完整 join parent；
- 仍记录导入行为；
- 适用范围和当前权限独立校验。

### 12.6 提交

MergePlan 验证后，同事务写入：

- MergeResolution；
- 结果 StateRoot；
- 新 Revision；
- 合并事件；
- Branch CAS；
- 相关 Task/Activity 状态；
- 命令 receipt。

MergeResolution 分别保存 sourceDelta、知识导入/拒绝决议、applicationPlan，以及最终 deltaFromStateParent。它们不能只用一个“diff”字段混同。已观察来源但决定保留 target 值时，最终状态 delta 可以为空，但仍须提交新的合并/知识事件。

Workspace 文件 patch 的验证与发布由适配器负责。引擎内状态 merge 不等于 OS 文件 merge。

---

## 13. 查询与索引

### 13.1 必需查询

- Node/Event ID 点查；
- 某 Cursor 的状态点查和前缀扫描；
- 祖先、后代、因果路径；
- Actor/Execution/Activity 时间线；
- 两 Revision 的状态差异；
- Branch 历史和共同基点；
- 某结果的证据闭包；
- 某 Memory 的派生/撤回影响范围；
- 某 View 下的上下文候选；
- 当前未结算或结果未知的 Activity。

### 13.2 自研物理索引

第一版候选：

- active segment：内存 hash map，按提交发布；
- sealed segment：按 ObjectID 排序的定位表 + 稀疏块目录；
- CommitLSN → segment/offset；
- EventStreamKey/StreamSequence → EventID；
- EventID → causal parents；
- parent EventID → children 的反向邻接；
- entity/type/Scope → 有序 posting；
- RevisionID → generation、parent refs；
- SourceID → derived Memory/Summary/ContextPacket。

sealed 索引带 coverage fence 和 checksum，可重建。压缩不得改变逻辑 ObjectID。

### 13.3 算法成本

令 Vq/Eq 为实际访问的子图节点/边：

- 因果遍历：O(Vq + Eq)，加对象定位 I/O；
- 祖先判定：基线 DFS/BFS，最坏 O(Vq + Eq)；
- merge-base：第 12.3 节的祖先集合与直接 parent 标记，O(Vq + Eq) 加集合/定位开销；
- 反向边查询：定位 posting + 输出成本；
- Context 候选：受限窗口与图扩展，不能默认遍历整张图。

禁止默认给每个节点保存全部祖先，避免 O(V²) 空间。可研究 landmark bitmaps、代际、跳跃索引，但它们必须保持查询精确性或显式标 approximate。

### 13.4 View 对索引的约束

索引只是候选来源。每个输出必须再经过：

- Space/ACL；
- 逻辑存在性；
- Branch/知识 cut；
- 有效/撤回状态；
- 数据保留状态；

校验。

语义边自身也是在某次事件中产生的声明，必须校验 assertion event 对 Actor 的可见性，不能只看两个端点已存在。例如 A、B 都在旧 View 中，后来 C 才声明“A 反驳 B”；不能在旧 View 显示这条边。因果路径中的私有祖先仅提供不透明锚点，不能借遍历授权其 payload。

严格过去不仅过滤结果 ID，还约束排序统计、缓存和检索词典。后来文档改变 IDF、向量索引或候选裁剪顺序，同样可能污染历史重建。

RecordedReplay 直接复用历史检索结果与 ContextPacket。StrictCutReconstruction 只能使用相应 cut 的索引快照，或在可见语料上重新确定性计算；否则返回不支持，不偷用当前全局排名。

### 13.5 资源限制

所有图查询需要 maxNodes、maxEdges、maxBytes、deadline 与稳定分页。达到上限返回 truncated + continuation，不能把截断结果称为完整证明。

### 13.6 节点列表、详情和检索

节点浏览采用三级按需加载，不把整张图或某个节点全部历史一次塞进模型输入：

| API | 输入关键项 | 输出 |
| --- | --- | --- |
| ListNodes | 固定 ReadView、类型/Actor/Execution/领域过滤、排序、limit、pageToken | 轻量 NodeSummary 列表、nextToken、snapshot fence、coverage |
| GetNode | ReadView、类型化 NodeRef、projection、maxBytes | 节点本体元数据、获准 payload/引用、来源、anchors、保留状态 |
| SearchNodes | ReadView、结构条件/明确文本条件、检索版本、limit | 节点简介、匹配理由、稳定排序、continuation 和覆盖状态 |
| GetNodeContext | ReadView、NodeRef、选定 anchor、模式、分区及范围上限 | 分区化上下文、证据 refs、缺口与继续展开的位置 |
| Trace | ReadView、起点、关系类型、方向、深度和访问上限 | 可见路径/邻域、范围和截断信息 |

NodeSummary 候选字段为 NodeRef、kind、displayLabel、简述、Actor/Execution、提交排序键、获准时间与状态、payload 大小、详情引用。模型生成的标题和简介是带来源的 Annotation，不能假装是原事件中已存在的事实。列表默认只返回元数据和短摘录；大对象使用获准的内容读取接口按范围加载，校验总长度与摘要。

v1 SearchNodes 支持结构索引与明确的文本匹配条件；无相关索引时允许有界精确扫描并报告截断，或返回 IndexUnavailable。不把未实现的语义检索或自动总结藏在 SearchNodes 内，不宣称关键词匹配等价于理解。后续语义检索仍受同一个 View 和来源规则约束。

分页 token 固定 ViewDigest、filter/projection/sort、检索版本和 continuation，使用稳定 NodeRef/提交序作 tie-break。不得从第一页到第二页追随最新 head，也不能把已失效 token 自动解释为新快照。权限撤回后，后续页面、详情和内容读取重新校验；标题、计数和边的存在性不得成为越权侧信道。

### 13.7 “节点详细上下文”的四种语义

“完整”必须相对于明确的 Actor、时点、关系范围和保留情况定义；一个 NodeID 不能代表无限闭包。

| contextMode | 返回什么 | 边界 |
| --- | --- | --- |
| RecordedInput | 与该节点明确关联的原 ContextPacket/RequestEnvelope 引用及获准原始 bytes | 许多工具、控制或实体节点没有模型请求，返回 NoRecordedInput；禁止临时生成一个当作原输入 |
| AsKnownAtAnchor | 指定 KnowledgeKey=(Actor,Execution) 在选定 Anchor 当时的知识、目标与可见来源 | 使用 KnowledgeStateRoot，不能偷读全局未来状态 |
| EvidenceNeighborhood | 节点来源、前驱、相关状态差异、冲突、回执和产物的有界邻域 | 按 View 授权，只展开明确关系，不把所有祖先 payload 都披露 |
| Retrospective | 可包含后续结果、失败原因与后来更正的复盘邻域 | 必须启用允许未来的 View 模式，标出后续事实，不能冒充当时已知 |

GetNode 返回有关系类型的 anchors，而非擅自选唯一恢复点。Event 可能参与多个 Revision/分支，同一事务也可能发布多个 Revision；恢复/知识上下文请求必须明确选定 SnapshotAnchor 和 KnowledgeKey。仅有 commit anchor 的独立控制事件，不伪造完整可执行 Revision。anchors 列表自身也受权限、固定快照和分页限制。

GetNodeContext 可分别展开 node、inputs、upstream、downstream、stateDiff、messages、activityReceipts、artifacts 等分区。请求必须带 maxNodes/maxEdges/maxBytes/depth，渲染给模型时可另带 tokenLimit；返回 visited scope、truncated、continuation、可披露的 unavailable/omitted reasons。

对于 RecordedInput，容量不足时返回原对象引用或分块读取方式；部分片段标明 partial，不声称是完整原始输入。对于权限不足的内容，Agent 侧返回不泄漏对象存在性的结果；具备额外审计权的宿主才可得到细化原因。

### 13.8 查询与 Agent 新观察

查询保持只读，不因 GetNodeContext 就调用模型生成摘要，也不因当前检索而修改冻结历史 View。需要生成新摘要时，通过独立 Activity 和 Context 协议显式执行。

宿主查询可发现当前允许读取但某个 Actor 尚未观察的事实；若要交给模型，需先提交 Observation/KnowledgeImport，保存确切查询结果、来源、排序/分页范围和将呈现的 payload 摘要，形成新知识根，再构造新 ContextPacket。重放使用记录下来的检索结果，不能把今天重查的不同结果当作过去同一输入。

如果查询本身已限定为该 Actor 当时的知识 View，就只能返回其中可见内容，不由“检索”自动扩大观察范围。引擎提供发现能力，宿主负责使发现、实际披露和下一动作形成可恢复边界。


### 13.9 官方导航与聚合查询

ListNodes 明确 layer=entities/events/content；SDK 默认业务导航为 entities，不偷偷把每个 blob、树节点或事务 frame 混入默认列表。原始 Event 审计入口始终可用。agent/v1 提供 Query→Task→Execution→Step→Invocation→Activity/Attempt/Result 的标准关系投影，关系类型/来源和分页由注册表定义。

实体在 AuditSnapshot/Discovery 视图按 Anchor 的根解析 specRef/stateRef；ExecutionKnowledge 视图只返回对应 KnowledgeKey/Revision 中已采用的版本，不回退到全局“最新”。Host discovery 查询得到的新材料，仍需 Observation 才用于后续模型输入。

默认按 Execution/Step 折叠大量 grep，不删除原始发生。任意“检索阶段/重要节点”分组必须绑定明确成员或确定性规则版本、SourceAnchor、覆盖范围；模型简介是有来源的 Annotation。未持久化的聚合行只是投影，不伪造可执行 NodeID/Revision。

必须可展开每个调用的参数、工具定义版本、派发尝试、raw/accepted 结果、披露片段、后续使用与产物。空结果/失败/重复调用不因不重要而免于登记；逻辑折叠不被宣称为减少原始保留成本。
---

## 14. 上下文管理

### 14.1 原始历史不是 prompt

引擎保存完整可保留事实；模型只接收某个任务与 View 的派生 ContextPacket。

ContextPacket 至少绑定：

- source SnapshotAnchor/ViewDigest/frontier；执行输入另有 KnowledgeKey 与实际 GoalRef；
- 主体和 Scope；
- mandatory anchors；
- 选中节点及呈现顺序；
- 使用的摘要与 Memory；
- 省略对象、原因和召回引用；
- renderer/tokenizer/model framing 版本；
- 实际 bytes/ObjectID 和 token 计数质量；
- 生成活动/命令；
- 权限与撤回状态版本。

真实 provider 请求如有额外 framing，由适配器保存 RequestEnvelope。API key 等凭据不得进入可检索 packet。模型 Packet 的 role/OwnerScope 决定输入选择：Query 路由可只有 Fence 和 Query 来源，无需伪造 Task/Execution/Cursor。Execution Packet 必须以已发布知识 Cursor 和 KnowledgeKey 固定实际输入，目标变更须先 Observation。

当前授权、现实预算、取消屏障是执行 gate，不是可以偷偷注入过去 prompt 的未来业务事实。

### 14.2 选择算法 v1

1. 固定 SnapshotAnchor、KnowledgeKey（若为执行输入）、知识根和权限；
2. 从该 View 读取当时已知目标、验收条件、未完成义务和已知 unknown effects；禁止读取当前全局目标填入历史 prompt；
3. 将必要约束列为 mandatory；
4. 根据任务实体、因果距离、明确决策和证据引用扩展有限可见候选；
5. 将候选组织为“结论 + 支撑证据 + 冲突/失败经验”的 bundle；
6. 按模型输入上限减去输出预留和 framing 预留，计算准确或明确标记的估计 token 成本；
7. 按固定策略做边际收益/成本的贪心选择，使用稳定 ID tie-break；
8. 在上下文容量内渲染，记录所有选择和省略；
9. mandatory 本身超容量则返回 ContextBudgetUnsatisfied，不静默削弱验收条件；
10. 新请求派发前，独立检查当前授权、UseBan、执行 fence、现实账户余额与出站 grant；它们决定能否执行，不暗中改变历史事实。

这里的 token 预算是模型上下文容量，**不是团队费用预算**。不得因为现实余额降低，就选择性删除验收条件或使用劣质摘要继续；资源不足应暂停或明确返回限制。ReadOnly 历史重建不把当前现实余额当作过去知识的一部分。

v1 选择算法是可解释启发式，不声明最优。语义相关性可由额外模型 Activity 提案，结果需记录并受同一 View 约束。

执行级 `primary_decision` 的 ContextPacket 使用当前 Packet schema 5，必须包含 `agent_mandatory_context/v1` Requirements。引擎从
Packet Source 的历史 LiveControl 推导当前任务与祖先目标、同目标 epoch 的最新验收要求、待处理或未结算调用和行动、
未消费消息、待 Join 以及已派发或 unknown Attempt，并核对该 KnowledgeKey 下已采用的精确原始内容。
这些事实必须进入 Packet Items。若记录了 Selection，也必须列入其 Mandatory。`HostPolicyVersion`
标识宿主渲染、语义候选和计数策略。RecordedInput 持久化宿主实际请求的完整 bytes，宿主对如何渲染事实负责。
当前复用和派发权限仍独立检查。 [Packet 格式](formats/domain-core-experimental-1.md)

### 14.3 摘要是派生对象

摘要记录：

- SourceSetDigest 和原始证据 refs；
- 来源范围和知识 cut；
- 实际生成时间、可见时间；
- 模型、prompt、策略版本；
- 被覆盖的主题、显式省略项；
- 适用范围、置信状态；
- 原始输入与输出；
- 验证/复核结果。

后来生成的摘要即使引用全是旧节点，也不能声称完全没有未来语义。RecordedReplay 必须复用当时实际摘要；新摘要属于重新构造或反事实。

### 14.4 不保证零遗忘

保持证据可召回与保证摘要语义完整是不同问题。ATE 保证追溯和重建能力，不保证模型一定主动召回每个关键节点。

上下文效果必须通过长程任务评测验证，包括：

- 很早出现、很晚才相关的约束；
- 失败分支中有价值的反例；
- 相互矛盾的结论；
- 压缩多次后的验收条件保真；
- 多 Agent 私有结论不提前泄露；
- 强制召回后的任务恢复率。

---

## 15. 长期记忆

### 15.1 Memory 不是原始 Event 的另一个名字

Memory 是经提炼、验证、限定范围的声明：

- Claim；
- 类型：事实、决策、程序、失败模式、偏好等；
- 来源 Event/Activity/Run；
- 证据与反证；
- 适用条件、项目/版本范围；
- confidence/verification 状态；
- 生成/验证策略版本；
- recordedAt、visibleAt、asserted validity interval；
- Scope/隐私分类；
- supersedes/retracts/derivedFrom；
- 过期、复核和撤回原因。

记录时间与声明的生效时间必须分开。事后补录的旧事实不能倒灌进当时的严格 View。

### 15.2 生命周期

~~~text
candidate -> validated -> promoted -> superseded / retracted
                 |
                 +-> rejected
~~~

晋升是宿主接受的事务，不由模型一句“记住了”完成。

失败分支的经验默认留在该范围，不能自动进入全局。跨任务、跨项目晋升需额外规则和脱敏校验。

### 15.3 撤回传播

Source → Derived 的反向关系用于找出受影响的：

- 摘要；
- Memory；
- 检索 posting；
- ContextPacket 的后续复用资格；
- 向量/全文索引；
- 派生经验。

撤回有不同语义，必须分开记录：

- FactualCorrection：后来更正事实；严格历史按当时 cut 读取，事后分析可附加后来更正，不能改写历史实际 bytes；
- UseBan：禁止把某来源用于新的检索/模型派发；新请求按当前控制面检查；
- PrivacyErasure/ACLRevocation：当前隐私和访问权约束，可明确拒绝历史读取，返回 HistoryAccessDenied/ContentUnavailable，而不是悄悄删改内容后仍声称精确回放。

异步索引和缓存清理期间，新访问/出站必须查权威状态。历史已发送 ContextPacket 不被改写；“来源后来被撤回”作为外部注释，不注入其原始输入。已发给外部服务的字节无法由本地撤回保证收回。

可参考 PROV 对生成、派生、失效的区分；ATE 不依赖 RDF 或其实现。[R9]

### 15.4 记忆效果是研究问题

“完整日志”不会自然变成正确经验。必须评测经验的适用性、过时、污染、自我强化和跨项目迁移收益。引擎提供可追溯实验条件，不先验承诺解决这些学习问题。

### 15.5 ExperiencePath 与跨任务复用

ExperiencePath 是独立的派生经验记录。它绑定 SourceAnchor，按顺序列出锚定节点及对应的 Memory，并保存验证、适用范围与必要反例。省略项保留可回查引用。复用时检查路径及支撑它的 Memory、来源和当前适用性。原始因果图仍可通过 TaskTrace 查阅。

默认逻辑隐藏噪声，不物理删审计事实。只有经验证/晋升与作用域审查的经验才可跨任务复用；跨 Space 按导出/导入重算 ID、脱敏并保留来源映射。Source 撤回同时影响摘要、经验路径和后续复用资格。

第一版验收分别测量：长程约束保真、早期证据召回、失败分支反例利用、分支筛选可解释性、跨项目经验适用性。存储正确不等于记忆有效，后者必须有实际对照结果。
---

## 16. 修剪、归档与垃圾回收

### 16.1 三种不同操作

- **Hide/Annotate**：查询默认忽略噪声，但保留事实；
- **PruneBranch**：删除/封存可继续执行的分支引用；
- **Erase/GC**：释放物理对象或受控遗忘，可能降低历史可恢复性。

不得把三者都命名为 delete。

### 16.2 标注无意义错误

模型可提出 duplicate、superseded、irrelevant、retracted 标签，宿主接受后写新 Annotation。

标签可撤回。原始工具失败不是因标签改变就从审计中消失。某次“无意义失败”的判断本身也要有来源和理由。

### 16.3 保留根

默认 AuditFull 的首要根是 **AuditLedgerRoot**：覆盖全部已发布事务 envelope、事件、控制更新、命令 receipt 和必要对象。没有 Revision 的独立消息、授权、对账和取消事件也必须保留；只沿 Branch heads 回溯会漏掉它们。该根由 manifest/fence 锚定的发布事务链表达，不在当前事务 payload 内再嵌入包含自身的最终事务摘要。

在此基础上，GC 至少从以下根遍历：

- active/archived Branch；
- pinned Cursor/tag；
- 所有要求保留的历史 Revision；
- 活跃 Reader generation；
- Execution/Activity/unknown-effect 对账引用；
- outbox、accepted receipt 和幂等账本；
- snapshot/checkpoint；
- promoted Memory 及其必要证据；
- 备份/导出任务；
- legal/audit retention anchors。

删除一个分支不能删除其他分支共享祖先。

### 16.4 必须承认的保留矛盾

如果保留 head，并将所有 parent/causal 引用设为强引用，则所有祖先都可达。普通 mark-and-sweep 无法“自然回收旧历史”。

Snapshot 只降低读取/回放成本，不自动解除历史证据引用。

因此定义两种保留策略：

1. **AuditFull（默认）**：保留全部已提交事件、必要对象和历史版本；修剪只减少默认检索与可运行分支，不承诺显著释放历史空间。
2. **BoundedRetention**：允许归档或驱逐指定 payload；保留摘要、边界和删除证明，但对应历史点明确返回 HistoryUnavailable/NeedsArchive。

不存在同时保证无限历史、精确全部回放、有界磁盘的默认配置。

### 16.5 第一版物理 GC 范围

优先回收：

- 从未提交的 staging 对象；
- 重建后被替换的索引文件；
- 已验证完整复制的新 segment 布局所替代的旧物理副本；
- 确认无根引用的对象；
- 显式执行保留政策允许遗忘的内容。

不在第一版自动删除权威日志中的失败事实。

### 16.6 Compaction 协议

第一版选择可验证的 maintenance 模式：**停止新逻辑写入完成一次布局重写**，已有固定快照读者仍可读取。暂停成本必须在基准和 UI 中如实呈现，不能先承诺无停顿在线回收。

1. 经协调器获取 maintenance gate，排空既有提交，固定 published floor、generation、所有保留根；
2. 在 capture+pin 协议下 pin 源代际，禁止新写入改变可达闭包；
3. 标记根及完整强引用闭包，明确 AuditFull/当前保留策略；
4. 将权威事务迁入新物理 segment：v1 必须保留完整 canonical TX frames、CommitLSN、事务顺序与摘要链，包含 StateOps、控制更新和 receipts；不能仅复制当前可达状态对象就删除原日志；
5. 新物理 segment 使用新的 SegmentID、物理 predecessor 链和来源 manifest/floor 锚点；逻辑事务身份/摘要不依赖物理 offset，故可保持不变；
6. 生成新定位表、索引和 checkpoint，并独立验证对象闭包、事务链、状态根和控制根；
7. DurableSync 新文件和目录；
8. 通过唯一 publication owner 按 P1 发布新 manifest/CURRENT，CAS 原 generation，durableFloor 不倒退；
9. 原子发布新内存 catalog，释放写屏障；新 Reader 使用新代际；
10. 通过同一 capture+pin/retire 协议封闭旧代际，等待所有旧 Reader、备份和导出 pins 释放；
11. 删除旧文件并同步目录。

中断后要么旧布局完整可用，要么新 CURRENT 指向完整新布局。写入新文件后、CURRENT 发布前退出留下的副本属于可清理 staging；禁止先删除旧文件。

v1 的 compaction 首先回收重复物理副本、废弃索引和 staging，不承诺删减 AuditFull 的权威历史。跳过权威事务 payload 的紧凑日志/retention checkpoint 是独立后续协议，必须明确新的验证锚点与不可回放缺口。

后续在线回收不能仅“追赶新 tail”：新事务可能引用旧布局中此前未标记的对象。必须有引用写屏障、重新捕获全部根并求闭包，或等价的严格协议；发布仍由同一 owner 仲裁。

### 16.7 隐私遗忘

选择物理删除后，完整审计和精确回放承诺必须相应降低。保留 ErasureRecord 指明范围、原因、授权和不能恢复的对象。

加密/密钥销毁可以作为后续能力，但必须使用标准密码实现。不能以哈希仍存在为由声称 PII 已匿名；也不能保证撤回已经发给外部服务或被复制的内容。

---

## 17. 安全、隔离和隐私

### 17.1 模型权限边界

模型只能提出：

- 计划/分叉/汇聚方案；
- 记忆候选；
- 标注与修剪建议；
- 修复建议。

模型不能直接：

- 修改权威 Actor/Execution epoch；
- 扩大 Scope；
- 返还真实已消费预算；
- 宣称外部未知操作未执行；
- 自行把“测试通过”升级为验证证据；
- 物理删除审计对象；
- 修改 CURRENT/manifest。

### 17.2 当前授权优先

历史授权用于展示和审计。历史分支重新产生外部 effects 必须通过当前策略；恢复不能让过期审批或已撤销权限复活。

### 17.3 Space 与内容寻址

ObjectID、EventID、RevisionID 的身份域必须包含 SpaceID。相同 plaintext 在不同 Space 中得到不同 ID，不进行隐式跨 Space 去重；物理索引键与缓存键也必须包含安全域。

这只减少跨域相等性泄漏，不提供加密，也不防同一 Space 的已知 plaintext 猜测。若威胁模型要求抵御离线猜测，需要标准 keyed hash/加密及独立密钥管理；须在格式冻结前单独完成安全审计。

跨 Space 共享通过显式导出/导入，重新计算目标域 ID，保存 source→destination 映射和来源证据。公开对象池须独立授权，不能用“公共”标签绕过原 Scope。

### 17.4 不可信内容

工具输出、网页、子 Agent 消息、导入图和经验都是数据，不因被持久化就成为宿主指令。

View 渲染需要区分系统约束、用户授权、模型声明、工具观察、推断与验证证据。

### 17.5 导出和导入

导出需包含：

- 对象闭包或显式缺口；
- 格式/语义版本；
- 来源 Store/Space；
- 权限与脱敏策略；
- 摘要清单；
- 外部效果只读证据。

导入不授予来源系统权限，不自动派发其 outbox，不继承旧预算或执行令牌。

### 17.6 出站授权和撤回线性化

固定快照不等于永久发送授权。任何对外读取输出或模型/工具派发，必须取得绑定内容与目的地的 ReadEmissionGrant/DispatchGrant，至少包括：

- Principal、Space、目的地、内容/ContextPacket 摘要；
- 依赖来源集合或其可验证摘要；
- 当前 ACL、UseBan、隐私状态版本；
- 当前执行/部署 fence；
- 如适用，预算 reservation 和 grant 失效条件。

grant 签发与撤回在同一控制面串行仲裁：

- 撤回先提交，则后续 grant 拒绝；
- grant 先签发，则该次派发已获授权，撤回不能保证收回已经交给执行器/网络的字节；
- 执行器必须校验 grant 与请求完全绑定，不可把旧 grant 用于新目的地或新内容；
- 流式输出分 chunk 授权并约束 grant 大小/有效期，使撤回可阻止后续未授权 chunk；
- 若需要更强的“撤回后绝不出站”，必须将资源端发送与撤回纳入同一可证明屏障，不能靠发送前的一次本地 ACL 检查宣称实现。

ContextPacket 保存实际历史 bytes；当前禁止发送时返回明确拒绝，不能把删改后的 packet 冒充原样回放。grant 本身不解密、不提高 Scope，也不能复活已撤销的执行令牌。

内嵌库和 Unix 服务的有界节点查询、状态读取、Knowledge/Memory/Agent/Query/Summary/Emission/Annotation、图查询和 Transfer 读取均在响应释放前形成一次性的读取许可。许可绑定认证目的地、请求摘要、固定 View 或历史 Anchor、当前策略切面和已解析的来源闭包；服务端消费许可后才写出 JSON 响应。固定 View 的后续页和 ReadContent 分块重新签发许可。OpenView 只登记服务端连接拥有的 View 句柄，服务端要求后端具备读取许可能力，随后内容查询仍须经过许可。

---

## 18. 公开接口

### 18.1 第一版能力接口

语义 API 草案；字段编号/完整签名在格式工件冻结。统一使用 ViewDescriptor/ViewRef，不再同时维护另一份返回裸 ReadView 的公开契约：

~~~go
type Commands interface {
    Commit(context.Context, CommitRequest) (CommitResult, error)
    LookupRequest(context.Context, LookupRequestRequest) (LookupRequestResult, error)
    Fork(context.Context, ForkRequest) (BranchRef, error)
    Diff(context.Context, DiffRequest) (DiffIterator, error)
    PlanJoin(context.Context, JoinRequest) (MergePlan, error)
    CommitJoin(context.Context, CommitJoinRequest) (CommitResult, error)
    Prune(context.Context, PruneRequest) (CommitResult, error)
}

type NodeReader interface {
    OpenView(context.Context, OpenViewRequest) (ViewDescriptor, error)
    CloseView(context.Context, CloseViewRequest) error
    ListNodes(context.Context, ListNodesRequest) (NodePage, error)
    GetNode(context.Context, GetNodeRequest) (NodeDetail, error)
    SearchNodes(context.Context, SearchNodesRequest) (NodePage, error)
    GetNodeContext(context.Context, NodeContextRequest) (NodeContext, error)
    Trace(context.Context, TraceRequest) (TraceIterator, error)
}

type AgentCommands interface {
    ApplyAgent(context.Context, AgentCommandRequest) (CommitResult, error)
}
~~~

OpenViewRequest 明确 SnapshotAnchor 或 latest 原子捕获、时间模式、可见性模式与 KnowledgeKey；ViewDescriptor 返回 Ref、Anchor、ViewDigest、ExpiresAt。Query DTO 携带 Ref。只读客户端可只取得 NodeReader；AgentCommands 使用 agent/v1 的封闭版本化命令类型并复用同一 receipt/事务路径。

ExecutionKnowledge 只读已采用知识；AuditSnapshot/Discovery 需要额外授权并明确是历史全局或发现视角。ReadAt 作为便捷方法也必须归一到 Anchor/OpenView，不保留只给 Cursor 就暗取当前控制根的行为。

RootSet/格式与 Profile 版本作为 capabilities 宣告，未启用/不兼容 Profile 明确拒绝写或提供有限 inspect 模式，不能无语义返回成功。Core-only 与完整 Agent 第一版验收分开。

Verify/inspect 等受权限控制的只读管理能力与写能力分开提供；不绕过在线 owner。kernel registry 唯一拥有 View/pin，库和服务不重复固定根；CloseView 幂等释放，ViewRef 不是持久锚点。Query 不调用模型或生成新业务状态；输入披露由显式命令接受。

不返回可修改的 mmap slice；iterator/stream 有 Close、资源上限与稳定错误类型；不可信模型不能直接提交任意 StateOps/控制键。

### 18.2 Agent-facing API 的层级与统一入口

对外不按每一种存储对象暴露一组互不相同的模型工具，也不把底层 `Commit`、`StateOps`、frame、manifest 或任意 ObjectID 直接交给模型。统一 API 的共同输入是：

~~~text
ViewRef / SnapshotAnchor
  + 可选 KnowledgeKey
  + subject NodeRef 或结构过滤器
  + layer / projection / contextMode
  + limit / depth / bytes / token / continuation
~~~

统一 API 的共同输出是：类型化摘要、稳定 NodeRef、关系类型、来源 Anchor、可展开引用、截断/不可用状态和 continuation。内部 Go/IPC 可以保留更细的能力接口；Agent-facing 层只暴露经过 Profile 和权限裁剪的只读能力。

推荐的 Agent 浏览能力只有少数几类：

| Agent 能力 | 默认层级 | 作用 |
| --- | --- | --- |
| `list` | Entity / Execution / Step / Activity 摘要 | 先知道有哪些对象、当前状态和下一步可展开项 |
| `inspect` | 一个 NodeRef 的 typed detail | 查看一个任务、执行、Step、Activity、Result 或 Artifact 的本体 |
| `expand` | Context 分区 | 按需展开 inputs、upstream、downstream、stateDiff、messages、receipts、artifacts |
| `trace` | 因果/来源/后果路径 | 查询有限深度的关系，不把全图塞入上下文 |
| `read_content` | Object/Blob 的获准范围 | 读取原始输出、ContextPacket、patch 或文件内容的分块/投影 |
| `search` | 固定 View 下的结构/明确文本条件 | 发现候选，不直接改变知识，也不调用模型生成摘要 |

这些能力可以在 wire 上由一个统一 `ExploreRequest` 的 typed operation 表达，也可以由 Go SDK 拆成 `ListNodes/GetNode/GetNodeContext/Trace/SearchNodes`；两者的语义必须完全相同。建议 SDK 保留拆分后的稳定方法，Agent tool layer 提供少量 progressive tools，避免一个拥有任意字段和任意深度的万能工具绕过资源/权限限制。

渐进式查看的默认路径是：

~~~text
Task/Execution 摘要
 → Step 摘要
 → ToolInvocation/Activity 摘要
 → Attempt/Result/Artifact 详情
 → 原始 Blob 或 ContextPacket 分块
 → Event/Commit/Frame 审计细节（明确进入审计模式）
~~~

模型可以选择往下展开，但每次展开仍绑定同一 View/Anchor、KnowledgeKey、权限、预算和 continuation。它不能通过换 `NodeRef` 的 kind、直接给 ObjectID 或提高 depth 读取未授权内容。Agent 看到的节点摘要/标题可以是来源明确的派生 Annotation，不成为新的事实。

写入 API 与读取 API 分开：模型提出 GoalChange、Spawn、ToolInvocation、AcceptResult、MessageConsume、MergeProposal、GoalRebase 等 agent/v1 命令；宿主/Profile controller 做权限、版本、目标、取消屏障和唯一动作槽校验，再提交事务。模型没有通用 `Commit`、原始 `StateOps`、`UpdateTaskControl` 或 `WriteObject` 工具。

时间旅行也使用相同读取入口：先选择 `SnapshotAnchor` 和模式，再以 NodeRef/Context 请求逐层读取；只有 `Fork/Restore/CounterfactualContinuation` 等受保护命令能创建新历史。`RecordedReplay` 直接读取实际 ContextPacket/Result，不让模型通过普通 search 重新拼一个“看起来相同”的输入。

### 18.3 第一版双形态接入

库模式通过 Open/Close 管理内核和 store 生命周期；服务模式由 ate serve 拥有同一个 Engine，并将授权后的领域请求交给它。两种模式支持相同核心命令/查询能力，不要求以同一 Go interface 强行表达 IPC 连接与进程内所有权。

SDK 使用类型化领域请求/结果和明确的 ViewHandle、分页 token；wire schema 独立版本化。服务端只跟踪 session→ViewRef 归属，核心 registry 唯一拥有 pin，支持显式释放和连接/租约失效回收。客户端不能拿到服务端文件 offset、内存对象或直接恢复 owner 的权限。

提交 receipt、Cursor、published fence 与稳定错误类型可跨接入方式理解。第一版对同一操作序列验证库调用和 SDK→服务调用得到相同领域结果；传输失败仍保留 RequestID 对账，不因改用二进制削弱幂等性和持久保证。

### 18.4 后续可选能力：订阅接口候选

以下不是第一版 Engine 的一部分，不要求第一版建占位实现、状态表或接受此类 StateOps；仅用于检查模块边界可以扩展：

~~~go
type ChangeReader interface {
    Watch(ctx context.Context, req WatchRequest) (ChangeStream, error)
    OpenSnapshotAndWatch(ctx context.Context, req SnapshotWatchRequest) (SnapshotWatch, error)
    AckChanges(ctx context.Context, req AckChangesRequest) (CommitResult, error)
}

type ChangeStream interface {
    Next(ctx context.Context) (ChangeBatch, error)
    Close() error
}

type SnapshotWatch struct {
    Snapshot ReadView
    Changes  ChangeStream
    Fence    PublishedFence
}
~~~

后续冻结时须明确 WatchRequest 的过滤/授权、consumer owner、resume cursor、buffer/lag 限制；SnapshotWatch 固定的逻辑 View、fence 与保留 lease；ChangeBatch 的稳定身份和扫描水位。不能只接入内存 channel 就称为可靠订阅。

后续 ACK/consumer checkpoint 是新版本化命令。Agent 的消费观察、业务动作意图和 checkpoint 需原子提交；Close stream 不默认删除 durable consumer。新增接口按 capability 暴露，旧客户端继续使用命令/查询，旧实现对未支持方法明确拒绝。

---

## 19. 版本与兼容

### 19.1 分别版本化

- storage frame；
- canonical encoding；
- Event schema；
- StateOps 语义；
- Activity effect contract；
- Context renderer/tokenizer；
- Memory promotion policy；
- 索引格式；
- adapter checkpoint。

一个总版本号不能代替这些边界。

### 19.2 升级规则

- 未知必需格式拒绝打开写入；
- 不修改已发布对象字节以“升级原事件”；
- 新状态语义通过新基线/新 Revision 表示；
- 历史回放使用原 StateOps 版本或经验证的显式转换；
- migration 是可中断、可对账的管理操作；
- 旧模型/tool/checkpoint 不兼容时只提供历史读取或反事实 fork，不能伪称精确续执行。

### 19.3 格式冻结

冻结前必须有：

- 规范编码测试向量；
- 跨平台 golden corpus；
- 截断/损坏 frame fuzz；
- 幂等和 CommitUnknown 契约；
- manifest/CURRENT 恢复反例；
- 哈希无自引用证明；
- 备份/GC 兼容测试。

---

## 20. 正确性不变量

以下按阶段验收。I-01–I-28、I-32–I-42 随第一版对应里程碑落实；I-29–I-31 属后续订阅，不阻塞第一版。文档一致性不代替实现验证。

| ID | 不变量 |
| --- | --- |
| I-01 | DurableAck 的事务可从已发布日志完整重建 |
| I-02 | 对读者发布的 Branch 不引用未发布对象 |
| I-03 | 同一幂等键的已提交 receipt 只对应一个不可变请求摘要和逻辑结果 |
| I-04 | Event 因果 parent 必须存在且无环 |
| I-05 | Event 发生身份与 payload 去重身份分离 |
| I-06 | 同一 Branch head/version 的 CAS 最多一个成功推进 |
| I-07 | 固定 StateOps 历史重放得到同一 StateRoot |
| I-08 | Join 的结果由已记录决议决定，不能恢复时重新交给模型 |
| I-09 | StateParent、JoinParents 与 knowledge cut 的语义不混同 |
| I-10 | 历史 View 不通过反向索引、排序或缓存获取截止后的管线来源 |
| I-11 | RecordedReplay 不新调用模型/工具 |
| I-12 | 旧 incarnation 不推进新执行，晚到证据仍可审计 |
| I-13 | 外部结果未知不会被记成确定无效果 |
| I-14 | fork/restore 不复制现实预算、恢复旧权限或令牌 |
| I-15 | 活跃 reader 所需物理 generation 不被回收 |
| I-16 | 删除分支不会删除其他根共享对象 |
| I-17 | 索引缺失/滞后不造成关键事实假阴性 |
| I-18 | ≤durable floor 的损坏不能静默回退 |
| I-19 | 同步失败不返回成功 ACK，后续 writer 被 fence |
| I-20 | 标注/修剪不修改原始事件 |
| I-21 | 受控遗忘后必须显式降低对应历史恢复能力 |
| I-22 | Activity accepted receipt 与后续结算分开，补结算不重跑操作 |
| I-23 | 当前控制面高水位不随历史状态倒带 |
| I-24 | 模型声明不得自行成为授权、验证成功或 Memory 晋升事实 |
| I-25 | 因果祖先不自动成为 KnowledgeKey 已观察 payload，知识读取使用相应知识根 |
| I-26 | maintenance/提交共用 publication owner，物理重排不丢权威事务或倒退 floor |
| I-27 | 出站 grant 与撤回有唯一顺序，grant 绑定内容、目的地和执行身份 |
| I-28 | Continuation、step 结果及下一动作意图原子提交，partial 不冒充最终结果 |
| I-29 | 【后续订阅】仅交付已发布事实，内存唤醒丢失后可按保留范围补读 |
| I-30 | 【后续订阅】广播各有游标，业务消费/观察/动作意图与 checkpoint 原子提交 |
| I-31 | 【后续订阅】Snapshot→Watch 以同一 fence 接续，范围过期明确要求重同步 |
| I-32 | 节点上下文声明模式与 anchor，部分邻域不得冒充完整历史输入 |
| I-33 | Core/Profile/宿主边界、版本和受保护命名空间在库与服务一致 |
| I-34 | Event stream/主体/记录者/触发归因分离，非执行事件不伪造 Agent |
| I-35 | SnapshotAnchor 的 Fence 固定控制根，Cursor 不被用来猜控制历史 |
| I-36 | KnowledgeKey 隔离同 Actor 的不同 Execution，恢复 incarnation 不清空知识 |
| I-37 | PendingAction 固定命令与原 RequestKey，rebind 先收敛旧结果并 fence 旧 authority |
| I-38 | Query 启动/Invocation 结果存在逻辑唯一槽，换 RequestID 不重复生子执行/Activity |
| I-39 | 取消祖先屏障与重派保留子树责任，无隐式 detached/活体改挂 |
| I-40 | CP.sourceAnchor 仅指此前已发布快照，不含自身 CommitDigest/后置 Revision |
| I-41 | schema 区分强内容引用、逻辑实体关系和索引投影，历史证据不靠 latest 解释 |
| I-42 | 父目标更新单调传播 goalEpoch；子执行继承约束失配先 Blocked，GoalRebase 有受信证据 |

---

## 21. 验证和基准

### 21.1 参考模型

先实现小型、容易验证的内存参考模型：

- 不追求性能；
- 保留全历史；
- 显式集合/图；
- 精确、无缓存查询；
- 规范状态操作。

磁盘引擎、索引优化、checkpoint、GC 和渲染都与参考模型做差分比较。参考模型不是生产存储依赖。

### 21.2 属性与变形测试

至少覆盖：

- Put/Delete 的不同合法顺序产生同逻辑 map 时 root 一致；
- 全量 replay = checkpoint 后 replay = 当前状态根；
- fork 后修改不改变源；
- shared ancestor 不因分支修剪消失；
- 单基点三路合并的对称规则；
- 多共同基点明确报歧义；
- 重复请求、回执不重复推进；
- 所有严格 View 输出均满足 causal/ACL 谓词；
- renderer 相同版本、相同输入得到相同 bytes；
- GC 前后保留 Cursor 的读取与 Trace 一致；
- 物理布局变化不改变对象身份；
- 同后缀不同绝对前缀与路径拆分/合并的 Diff 不漏报；
- 私有祖先不因消费摘要消息而暴露，未消费新值时仍读到旧知识值；
- 只有端点旧、关系声明新的边不能进入旧 View；
- 字段无冲突但违反跨字段约束的合并被拒绝；
- Restore 保留已观察知识，新 Fork 与 RetrospectiveAnalysis 明确区分；
- 独立控制事件和没有 Revision 的 receipts 在 AuditFull compaction 后仍可审计；
- 同 Actor 的两个 Execution 观察隔离，Anchor 固定控制-only 历史，逻辑实体在正确根解析；
- Event 的 stream/recordedBy/subject 不把用户/工具来源混成模型声明；
- CP 不引用本 Commit/未发布 staging Anchor，强引用/逻辑关系按注册表提取；
- 官方 Profile 的唯一启动/Invocation/消费槽拒绝换 RequestID 重复工作；
- NodePage 固定 View、过滤器和排序，后续新节点不污染旧分页；
- 节点多 anchor 不任意选唯一恢复点，RecordedInput 缺失不临时生成；
- 只读 GetNodeContext 不改变知识根，新披露通过 Observation 明确提交；
- 库模式与二进制服务模式对相同命令序列得到相同 receipt/状态根/查询结果；
- 同一 store 已有 owner 时，另一形态不能绕过锁直接打开写入。

后续订阅阶段另行加入广播独立消费、游标补读与 snapshot→watch 属性；第一版只审计其兼容基础，不运行不存在功能的占位测试。

### 21.3 故障注入矩阵

每个写、同步、rename、目录同步和发布边界都可以被确定性中断：

| 场景 | 必须观察到 |
| --- | --- |
| 任意 frame 短写/截断 | 不发布部分事务 |
| TX_COMMIT 前后退出 | 与 manifest 证明一致 |
| segment sync 失败 | 无 DurableAck，writer fenced |
| manifest/CURRENT 中断 | 不猜测未来 manifest、不静默回退已确认历史 |
| ACK 丢失 | 相同 RequestID 得到原结果 |
| 已提交数据损坏 | 明确 CorruptionDetected |
| reader 与 compaction 并发 | 无悬空引用、旧文件不提前删除 |
| 外部完成后 receipt 前退出 | effect_unknown，受契约约束对账 |
| 旧 worker 暂停后恢复 | 不推进新实例；证据可入隔离通道 |
| 父取消与 spawn 竞态 | 唯一合法归属，无未归属 worker |
| join 完成与超时竞态 | 一次合法决议 |
| 未来摘要/Memory/子结果进入旧 View | 管线拒绝并报告来源 |
| ACL 撤回但索引未更新 | 新读取/派发不越权 |
| Memory source 撤回 | 依赖读取失效，不通过缓存复活 |
| 磁盘满、断电、wall clock 回拨 | 不虚构成功或重复兑付 |
| 从旧备份接管 | 新部署身份，不恢复旧授权 |
| compaction 安装与新提交竞态 | 唯一 owner、generation CAS；floor 不回退 |
| 读取旧 view 后延迟 pin | capture+pin 原子，旧代际不提前 retire |
| 原命令耗尽最后配额后重试 | 有权限查询时返回原 receipt，不再次扣费 |
| not-found 后旧请求才抵达接收方 | 不误判安全重试非幂等效果 |
| grant/revoke 交错、流式中断 | 按已定义顺序授权，无旧 grant 换内容复用 |
| Continuation 提交与 worker 启动中断 | 不漏归属、不重复 spawn/已确认动作 |
| provider 流式输出中途断开 | partial 独立保留，不拼成虚假最终结果 |
| 节点翻页期间撤销权限 | 后续输出按当前 grant 校验，元数据不越权 |
| 服务提交后 IPC 响应丢失 | 原 RequestID 对账，与库模式的未知提交契约一致 |
| SDK/服务版本不兼容、未知方法 | 明确版本/能力错误，不执行错误命令 |
| 客户端断开或遗留 ViewHandle | 按已定义 Close/租约规则回收 reader pin |
| C01 后/C02 前退出、C02 ACK 丢失 | 固定 intake action/RequestKey 与启动槽，只生成原 Root |
| 换 incarnation/authority 恢复旧动作 | 先查原结果；unknown 不 rebind；明确未提交且 fence 后才新代际 |
| GoalAdopted 后未 Observation 即继续 | 新 Step/派发 blocked，不改写旧 CP/Step |
| 旧父重派与孙执行 Spawn 交错 | 祖先屏障拒绝新工作，旧子树/unknown 有归属 |
| raw/settled 已记但未合法 accepted/consumed | 不推进新知识或父 join |
| 引擎 Query 后、Observation 前退出 | 固定 Anchor 重建或明确 blocked，不偷换 latest |
| ExperiencePath 隐藏错误/来源撤回 | 原失败/缺口可回查，派生复用资格同步受限 |

后续订阅启用前追加以下矩阵，不作为第一版实现要求：

| 后续场景 | 必须观察到 |
| --- | --- |
| 提交后、内存唤醒前退出 | 从已发布日志补读通知 |
| 交付后、消费 checkpoint 前退出 | 可重复交付，业务命令幂等处理 |
| 消费动作与 ACK 之间故障 | 两者同事务，全有或全无 |
| 快照分页期间新事务持续提交 | 基线固定 H，Watch 补齐 H 后的合法变更 |
| 慢消费者、保留租约到期 | 不阻塞提交，明确 lag/过期，不静默跳过 |
| 新 owner 接管后旧订阅者 ACK | 旧 owner generation 被拒绝 |
| 订阅期间撤销权限 | 后续输出按当前 grant 校验，元数据不越权 |

故障注入须区分进程退出、掉电模型和事后介质损坏。单纯 kill 进程测试不能替代全部掉电测试。

### 21.4 模型检查

对简化状态机枚举：

- Commit/ACK/重试；
- Fork/Prune/Reader pin；
- Activity dispatch/result/unknown；
- spawn/cancel；
- join/timeout；
- GC/manifest publish。

Snapshot/Watch/retention lease，以及 Delivery/消费提交/ACK/owner 接管的模型检查属于后续订阅阶段。

可以使用模型检查工具辅助验证，但不引入其运行时作为生产存储依赖。形式模型、实现和故障模型的对应关系必须写清。

### 21.5 初始测试规模矩阵

以下是实验输入，不是已达到的性能：

| 维度 | 候选规模 |
| --- | --- |
| Event 数 | 10⁴、10⁵、10⁶、10⁷ |
| 并行 Actor | 1、8、64、256 |
| Branch 数 | 10、10³、10⁵ |
| 因果形状 | 长链、宽扇出、反复 join、稀疏跨流依赖 |
| Object 大小 | 小元数据、1 KiB、64 KiB、1 MiB chunk |
| 历史模式 | 重复压缩、深分支、失败探索、撤回链 |
| 存储压力 | 热缓存、冷启动、近满磁盘、reader 长时间 pin |

### 21.6 性能指标

- 持久提交 p50/p95/p99、fsync 次数和 group 大小；
- 写吞吐、空间/写放大；
- ReadAt 点查、PrefixScan、Trace 和 Diff；
- Fork 元数据和总提交延迟；
- merge-base 与冲突计划时间；
- 启动扫描和 checkpoint 恢复时间；
- Index rebuild / compaction 暂停；
- reader pin 带来的回收延迟；
- ContextPacket token 成本和渲染延迟；
- 长程任务成功率、关键约束召回、经验迁移收益。

不得先声明数量级优势，再选择有利数据验证。数据结构选择应由相同语义、相同耐久性、相同硬件的比较支撑。

### 21.7 第一条端到端验收

1. 两个 Agent 从同一节点 fork；
2. 各自产生模型、工具、消息和状态；
3. 一个分支失败但留有有价值反例；
4. 在提交/回执/合并边界逐点中断；
5. 重开后仅恢复已提交事实；
6. 用原 ContextPacket 重放；
7. 开严格 View 和未来可见复盘 View；
8. 显式 merge 被采用的结果；
9. 晋升有来源的 Memory；
10. 逻辑 prune 失败分支，验证共享祖先仍可读；
11. 在 reader 活跃时 compaction，结果保持一致。

---

## 22. 实现路线与外部宿主适配

### 22.1 里程碑

| 阶段 | 交付 | 退出条件 |
| --- | --- | --- |
| M0 协议审计 | 三稿联合基线、Core/Profile 接口、引用/Anchor/错误语义与首批工件 | 首条纵切面 P0 关闭，其余有对应阶段门槛 |
| M1 存储纵切面 | 原生 frames/事务/manifest/对象/CAS/readers | ACK、未知提交、损坏与尾部测试通过 |
| M2 版本图 | State tree、Revision、Fork、Diff、Trace、JoinPlan | 与参考模型一致，复杂合并有明确定义 |
| M3 多 Agent | 官方 agent/v1、Query/Task/Execution/Step、唯一动作槽、Activity/消息/取消/恢复 | 场景及 rebind/重派/unknown 故障矩阵通过；无需订阅 |
| M4 上下文 | Profile 导航、Anchor/KnowledgeKey、CP、摘要来源与召回 | 同 Actor 多 E 隔离、严格过去、无 hash 环、实际输入可重放 |
| M5 记忆和修剪 | Memory/ExperiencePath、撤回、保留、维护回收/归档 | 原证据可追溯、无权限复活/误删，经验效果单独评估 |
| M6 双形态与外部宿主验收 | 库、最小 ate serve/SDK、真实多 Agent 任务与并行对照 | 两种接入语义与故障行为对齐，正确性、开销和效果有实测报告 |
| M7 后续订阅 | ChangeReader、广播、游标/ACK、主动通知与被动反馈接入 | 单独冻结协议，通过 I-29–I-31 与订阅故障矩阵 |
| M8 扩展研究 | 自研检索、压缩优化、跨节点、语言绑定 | 独立设计与验证，不倒灌未验证语义 |

M0–M6 构成第一版范围；M7 起为后续版本。库与最小服务/SDK 应在 M1–M4 随已有命令/查询逐步实现，M6 验收，不等核心完成后再另写服务端语义。第一版以日志、恢复、图与查询的正确性为主，不因未实现订阅拖延关键能力。

M1–M3 应尽早形成完整纵切面，而不是多年只做底层、没有真实 Agent 反馈。

### 22.2 外部宿主与验收适配

外部宿主（GSEA 是一个验收对象）提供：

- Loop/模型/工具适配；
- Task/Agent 领域校验；
- Workspace view 和 effect 执行；
- 用户审批与 UI；
- 场景、故障样本和评测。

ATE 提供：

- 官方 agent/v1 的标准记录、纯状态转换、恢复与导航查询；
- 原子事实提交；
- 版本、因果、分支与 View；
- Activity/receipt/执行 fence 协议；
- Context/Memory 来源和保留机制。

Eino 等执行器继续可用；ATE 不依赖其内部 checkpoint 格式。

### 22.3 避免两个真相

禁止通过无事务的双写，让任何外部宿主数据库和 ATE 同时成为同一状态的权威来源。

实验期可采用：

- 外部宿主数据库仍为权威，ATE 是明确标注 coverage 的影子投影；不据此承诺 ATE 原生恢复；
- 或在独立测试流程中以 ATE 为唯一权威；
- 正式切换时逐领域定义 authority owner、cutover fence 和 rebuild 规则。

不能先双写，再假设“两个库一般不会不一致”。

### 22.4 不受现有代码限制

新内核不复用任何外部宿主的 schema migration 号、不继承其内部包边界、不为了兼容旧字段牺牲语义。

旧历史可通过导入器转换为带缺口标签的证据。不能伪造旧日志没有记录的输入、知识前沿或回执。

---

## 23. 决策记录、风险与审计问题

### 23.1 当前提案决策

| ID | 决策 | 理由 |
| --- | --- | --- |
| D-01 | 独立 Go module、自研全部存储/索引/事务 | 目标是可独立使用的原生基础设施 |
| D-02 | 单写所有者、多 Agent 并行计算 | 缩小一致性证明面，后续再研究复制 |
| D-03 | Event/Revision/Commit/Branch/View 分离 | 避免事实归属、状态和知识混淆 |
| D-04 | append log + manifest publication | 保留原子历史与持久下界 |
| D-05 | persistent radix tree | 原生历史、前缀和结构共享 |
| D-06 | 固定 StateOpsV1 | 不在核心事务中执行不受控代码 |
| D-07 | 显式 resolved merge | 非交换操作不能靠拓扑排序证明正确 |
| D-08 | 历史视角四分法 | 防止未来知识与原样回放混淆 |
| D-09 | 控制面不倒带 | 防止旧权限、旧 epoch、预算复活 |
| D-10 | unknown effects 一等状态 | 承认外部调用的不确定窗口 |
| D-11 | 默认 AuditFull | 先保护证据，物理遗忘须显式降级 |
| D-12 | 查询索引可重建 | 索引优化不能改变权威事实 |
| D-13 | 第一版命令/查询，可靠订阅后续增加 | 保留日志和接口兼容基础，先完成核心，不强制 ChangeReader |
| D-14 | 同一内核的 Go 库与独立二进制 | 双形态复用全部事务/恢复语义，同一 store 单 owner |

### 23.2 明确拒绝的简化

- “每个事件只属于一个 Branch”；
- “所有关系都是 DAG”；
- “一个 logical timestamp 就等于因果和知识时间”；
- “给 DAG 固定排序就完成了状态合并”；
- “任意两个 head 都有唯一 LCA”；
- “存在 CRC 错误就截断最后一段日志”；
- “落盘一个 snapshot 就可以删除全部历史”；
- “lease 到期证明旧 worker 已死”；
- “outbox 自动保证外部 exactly-once”；
- “hash 相等证明模型结论真实”；
- “旧来源的新摘要必然没有未来知识”；
- “无限日志、完整审计和固定磁盘可以同时无条件满足”。

### 23.3 阶段冻结与待验证事项

以下是同一设计的实现/格式验证门槛，不是仍然并存的候选语义；先关首条纵切面问题，其余在对应阶段关：

| 门槛 | 已选设计 | 尚须证据 |
| --- | --- | --- |
| M0/M1 | Core/Profile/模型身份、Event stream、Anchor、唯一动作槽 | schema/错误/引用与首批 golden 规范 |
| M1 | P1、CURRENT/floor/PostRootSet、active prefix | 逐 I/O/rename/目录 sync 中断、损坏与未知提交测试 |
| M1/M6 | 平台 DurableIO 支持矩阵 | 分别记录模拟/kill/真实 OS/文件系统/设备证据，不以编译通过认证 |
| M1/M2 | radix canonicalization、无 hash 环、显式 merge | 独立参考模型、空 key/路径压缩/多基点/跨字段不变量 |
| M3 | PendingAction/authority rebind、取消树与 unknown | 入口/Spawn/换键/旧 worker/孙执行竞态及对账测试 |
| M4 | KnowledgeKey、SnapshotAnchor、查询/CP、当前权限 | 同 Actor 多 E、控制历史、来源无未来泄漏与撤权场景 |
| M5 | AuditFull、引用闭包、维护回收、Memory/ExperiencePath | 不误删/不复活权限、撤回传播、经验适用与任务效果评测 |
| M6 | 库/服务同实现、外部宿主唯一 authority | IPC 身份/句柄/未知提交、切换 fence、真实任务对照报告 |
| 后续订阅 | 稳定日志投影与独立游标方向 | 另行冻结接口/保留 lease/交付语义，非第一版前置 |

格式编号、具体平台/资源默认值尚未冻结；语义承诺和故障处理不能因此随实现各自解释。文档审查不是实现正确性证明。

### 23.4 开放研究问题

- 对 Agent 工作负载，radix tree、块式邻接和日志布局的最优组合；
- 大量短命 Agent 的前沿表示和 reachability 加速；
- 多基点语义合并与可验证的冲突解决；
- 上下文选择的因果完整性、成本和相关性平衡；
- 摘要/经验的语义完整性与有效性评估；
- 有限保留下的审计证明、归档和可恢复性；
- 非幂等外部系统的自动对账上限；
- 真正跨节点时的排序、复制、快照与分区行为。

### 23.5 设计文档的完成标准

文档进入下一版前：

- 每条严格保证都有对应不变量和测试；
- 每个优化都写明保真条件与回退；
- 每个“自动”都有宿主校验边界；
- 每种不可恢复情况都有明确输出；
- 每个性能宣称都标为实测或待测；
- 所有开放问题保留在风险清单，不藏在实现细节中。

### 23.6 本轮联合审计的统一决策

| 编号 | 已消除的冲突/缺口 | 统一落点 |
| --- | --- | --- |
| U-01 | 宿主任意 Task schema 与官方受保护 Profile 冲突 | §1.2、§11.12；实施模块/版本，场景对象 |
| U-02 | Event 必填 Execution 无法表示 Query/系统 | §4.3；Stream/recordedBy/subject 分离 |
| U-03 | 按 Actor 的知识与按 E 隔离冲突 | §4.4–4.6、§5；KnowledgeKey 与查询/CAS |
| U-04 | Cursor 不能固定控制-only 历史 | §4.6；SnapshotAnchor、PostRootSet |
| U-05 | Profile ID/复合 Step 无通用 NodeRef 映射 | §4.9、§13.9；注册表与实体版本解析 |
| U-06 | 路由/资源准备不一定有 Step/Execution | §11.1–3；OwnerScope/role/Origin |
| U-07 | Continuation 枚举不足、换 epoch 改旧 body | §11.10；PendingAction 与受控 rebind |
| U-08 | 只靠 RequestID 不能防换键重复启动/Spawn | §11.7、§11.12；逻辑唯一槽与 CAS |
| U-09 | 线性 Activity 状态混同接受/结算/消费 | §11.2；正交记录与 acceptance 证据 |
| U-10 | 重派后旧子树责任不明 | §11.7；祖先屏障、取消、隔离与未知效果 |
| U-11 | GoalAdopted 被误当作模型已知 | §11.12；Observation、Step/CP 目标版本 gate |
| U-12 | 新 Anchor 可能制造 CP 当前提交自引用 | §5.6；先发布知识，再建 CP |
| U-13 | 引用/图索引/聚合可能各成真相 | §4.9、§13.9；schema 提取、明确 layer/来源 |
| U-14 | 保存日志被等同于经验/真正记忆 | §1.6、§15.5；工作知识/实际输入/派生经验分层 |
| U-15 | 后续 pending-render 无法在本事务固定未来 Fence | §5.7；ProducerTx→该事务已验证 Anchor，不取 batch-end/latest |
| U-16 | 固定类型 frame 顺序与后置控制对象冲突 | §4.2、§8.4；内容 DAG 与 typed body 拓扑混排 |
| U-17 | 子目标未随父目标约束更新 | §11.12、§11.13；InheritedGoalBindings/goalEpoch/Blocked/GoalRebase |

仍未冻结的字段编号、平台 durable profile、性能参数与效果结论在对应阶段验证；这些不是继续保留相互冲突模型的理由。

---

## 24. 理论与工程参考

这些资料用于理论和协议审查，不表示采用其数据库、产品实现或源代码。本文的数据结构组合、API 和存储协议是待验证的独立提案。

- **R1** Leslie Lamport, *Time, Clocks, and the Ordering of Events in a Distributed System*。支持因果部分序与逻辑排序的区分。  
  [原文](https://www.microsoft.com/en-us/research/publication/time-clocks-ordering-events-distributed-system/)
- **R2** K. Mani Chandy, Leslie Lamport, *Distributed Snapshots: Determining Global States of a Distributed System*。支持一致全局 cut 的设计依据，不意味着本提案已实现分布式算法。  
  [原文](https://www.microsoft.com/en-us/research/publication/distributed-snapshots-determining-global-states-distributed-system/)
- **R3** Driscoll, Sarnak, Sleator, Tarjan, *Making Data Structures Persistent*。支持历史版本与结构共享概念；本文 radix tree 的具体成本仍须单独验证。  
  [原文](https://www.cs.cmu.edu/~sleator/papers/making-data-structures-persistent.pdf)
- **R4** Linux man-pages, *fsync(2)*。文件与目录持久化不是同一保证。  
  [原文](https://man7.org/linux/man-pages/man2/fsync.2.html)
- **R5** Apple archived system manual, *fsync(2)*。平台设备缓存与 F_FULLFSYNC 需要独立考虑；实际 APFS/设备保证仍需测试。  
  [原文](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html)
- **R6** Git documentation, *git-merge-base*。说明 DAG 可能存在多个最佳共同祖先；本文不依赖 Git 实现。  
  [原文](https://git-scm.com/docs/git-merge-base)
- **R7** AWS Builders' Library, *Making retries safe with idempotent APIs*。幂等键、晚到请求和同键异意图的外部契约。  
  [原文](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)
- **R8** Martin Kleppmann, *How to do distributed locking*。资源端 fencing 与租约过期的边界。  
  [原文](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
- **R9** W3C, *PROV-O*。生成、派生、失效等来源关系的概念参考；ATE 不依赖 RDF/OWL。  
  [原文](https://www.w3.org/TR/prov-o/)
- **R10** Go standard library, *net.Listen / UnixConn*。支持 Unix domain socket 等传输；不证明本方案的性能或协议正确性。  
  [文档](https://pkg.go.dev/net#Listen)
- **R11** Go language specification, *Channel types / Receive operator*。channel 的收发与队列语义参考；可靠广播、持久游标由本设计另行实现。  
  [规范](https://go.dev/ref/spec#Channel_types)

## V1 closure decisions (2026-11-02)

Query intake, cross-root Agent work, typed Summary provenance, typed Validation,
NodeContext projections and durable Emission issue/claim/revoke are now concrete V1
transitions. They preserve the design's receipt-first, source-before-consumer and
single-publisher invariants. A revoked emission stops future claims and does not pretend
to recall bytes already sent by an external adapter. See the implementation addendum for
replay and evidence boundaries.

### 2026-11-02 交叉审计后的契约补强（历史）

Execution-scope Invocation schema 2 固定 accepted primary ActivityResult ID、工具定义
ObjectID、工具版本和 dispatchClass，并沿 Activity 的 Claim/Dispatch 校验同一调用和 effect。
`model_decision` Work 必须提交与 DecisionBinding 一致的完整调用清单；带 pending 工具的决议先保存
acceptance、调用和 Continuation，工具结果完成前不能宣称 Step completed。旧 record/control 格式保留
原始 replay 字节。Query-scope DecisionRef 和所有 legacy 入口的新写入升级尚未完成，见审计 A1/A3。

Control schema 4 的 Continuation.Position 固定 Space/Principal、Step、KnowledgeKey、已发布的
Knowledge Fence+Cursor、AdapterVersion 和下一批 PendingAction 的确切 generation。新元数据不允许
被后续保存降回空值；旧日志不凭空补出这些字段。恢复迁移与初始尚未发布的 seed 边界以 A2 的
实施记录为准。

Emission schema 2 的 Issue 保存显式 provenance closure/digest 和签发时 policy Fence/root；Claim
仍受当前策略约束。该证据只覆盖引擎掌握的明确依赖，不能识别任意复制字节，也没有代替外部
执行器的 send/reconcile 协议。完整四文档一致性仍以交叉审计报告逐项结论为准。

schema 2 的 range 固定 `payload` 或 `value_bytes` 投影，并记录全量/范围 SHA-256 和总长度；
超出实际字节的 offset/length 必须拒绝。当前只支持 Fence-only 源，Cursor/Knowledge selector
会被拒绝，避免表面接受选择条件却按另一切面授权。

### 2026-11-03 主路径收口补充

短 Query 使用独立的 `(QueryID, DecisionSequence)` 身份：`QueryProgress` 持久化 adapter、source
cut、完整 Invocations 和 exact next request；`query_open/decision/tool/answer` 通过 Work 原子
推进，不创建 Task、Execution 或 Step。Query 的工具 Activity/Attempt 仍使用既有 Activity 状态机，
但 owner 是 Query，不能伪造 Execution authority。历史 QueryProgress 的输入、Packet、Summary
和工具定义依赖受当前 ACL/UseBan/隐私策略重新检查。

这条规则区分两种完成语义。若 Query 没有提交工作启动动作，它就是 Query-only：最终
`QueryAnswer` 只需绑定 Query-scoped 的已接受结果，不要求伪造 `TaskOutcome`。若 Query
显式启动了 Task，则它转为 Task-backed Query；最终答复必须逐一绑定所有已启动 Task 的
Execution 和已记录 outcome，不能通过省略 outcome 退回 Query-only，也不能添加未启动的
Task。是否创建 Task 由语义上的工作启动决定，不由持久化需求决定。

跨 Space Transfer 只导出经过当前策略验证的 Memory/ExperiencePath/注册 provenance closure；
显式 whole-record omission 记录 Gap，导入后保存带 source Anchor 和 source→target 映射的只读
foreign evidence。它不能安装目标域 Task/Execution/Authority/Continuation，也不能自动 promote；
目标域必须通过新的本地 Observation/Memory candidate 显式采用证据。

Emission schema 3 可将 grant 绑定到 Query 或 Execution 的 Activity/Attempt、ExactRequestDigest、
内容投影和确定性 `NotAfterLSN`。绑定 grant 只能由同一 Work 的 exact dispatch 边界消费；本地提交
先于网络发送，网络中断保持 unknown 并由 Attempt reconciliation 处理，不能宣称外部 exactly-once。

## 26. Strict V1 交叉审计状态（2026-11-04）

当前协议使用 Control 5、Invocation 2、Continuation opcode 8、Work 1、typed completion validation、
Emission 3 和 Packet 4。被替换的预发布 Profile 和 Packet 字节在公开入口与重放中拒绝。当前受支持格式的
已发布历史、receipt、Snapshot、View、Cursor 和 Branch 继续可重放。 [格式清单](formats/ate-v1-manifest-2026-11-02.md)

受保护的 Root 和 child 初始知识、authority 和 Continuation 在同一 Work 发布。消息消费绑定接收方
Observation 与指向本次生产事务及最终 Knowledge Revision 的 Continuation。主模型接受（含已对账的旧
incarnation 结果）绑定完整 Invocation 集合和恢复位置。`child_spawn` 使用已登记的 Invocation。
所有模型和工具派发以同一 Work 消费精确 Emission grant 并记录 Attempt dispatch。Activity 已指定
ContextPacket 时，许可须绑定该 Packet 身份及完整 RecordedInput。正式实体事件、独立参考差分、实测效果和开销仍须按四文档
条款和最终源树逐项验收。全零知识位置只描述确实尚无知识的状态。

订阅/广播原本属于后续；本地 receipt 不证明远程 exactly-once；Transfer 可以有显式缺口，且不导入
authority。以上是 11-04 历史审计快照，当前实现收口见本文末的 11-05 段落和 [当前交叉验收续记](validation/V1_CURRENT_CROSS_CHECK_2026-11-05.md)。

## 2026-11-05 当前实现收口

当前 clean-cut V1 的公开写入与重放只接受 Control 5、Invocation 2、Continuation opcode 8、Emission 3、Packet 4 以及 registry 中列出的当前格式。历史 View、Fence、Cursor、receipt、Event 和 Branch 继续由当前 V1 语义读取。预发布 Agent/Profile admission 路径不作为新写入或特殊 replay 分支。

所有模型和工具派发都在 `emission_dispatch` Work 中消费绑定 Activity/Attempt grant。Activity 绑定 ContextPacket 时，许可选择同一 Packet 和完整 RecordedInput。工具调用还绑定已发布 ToolDefinition 的名称、版本、schema、dispatch mode/class 和 EffectContract。受保护的 Root/child/message/model decision/FinishStep 使用对应 Work 原子边界。

读取响应使用目的地绑定、一次性、短期的 ReadDeliveryGrant。来源关系沿 Packet、Observation 和 Summary 参与当前撤回判断，固定历史内容保持原始 bytes。Unix 服务缺少读取许可能力的后端时 fail closed。具体逐条证据和外部限制见 [当前交叉验收续记](validation/V1_CURRENT_CROSS_CHECK_2026-11-05.md)。

## 2026-11-06 语义核对

每次响应的持久分块是 Attempt 下的 OutputChunk 记录，拥有独立内容身份及日志发生身份。Capture 的 full/truncated/partial/unknown 与 Outcome 分开；partial/unknown 不能接受，模型结果只接受 full。分块已经确认的前缀在终结结果出现之前就可恢复；终结 Result 必须精确绑定全部已记录分块。

最终 Answer 使用一个已接受的完整成功模型结果，正文不得由宿主另行拼接。任务的成果与验收先完成，再由 Query-scoped 最终模型通过 FinalAnswerInput 绑定确切 TaskOutcome。模型答复失败只留下待恢复答复，不撤销已完成成果。见证的渲染语义由命名宿主策略承担，引擎认证身份、来源、精确字节与当前策略。

Packet 5 / Summary 2 的逻辑记录和原始请求/输出的来源邻接同事务发布。最终 Activity 的原始请求和 witness 来源邻接与控制状态同事务发布。重复内容的明确来源单调合并，读取和复用检查跨两个根的来源闭包。§17.6 的普通嵌入式读取已接入统一的一次性出站许可。 [实现与验收 L-02](validation/V1_CROSS_AUDIT_2026-11-06.md)
