# ATE 第一版实施架构

> 本文保留完整 V1 目标。当前候选格式为 Control 5、Invocation 2、Continuation opcode 8、Work 1、Emission 3 和 Packet 4。2026-11-03 独立审计保留当时的反例和证据边界。本文末尾记录当前实现与待验收项，严格 V1 验收仍开放。 [独立审计](validation/V1_INDEPENDENT_CURRENT_AUDIT_2026-11-03.md) · [格式清单](formats/ate-v1-manifest-2026-11-02.md)
>
> 2026-11-01 的领域命令、Key/Cursor 与 Node 边界核对见 [文档—代码审计](validation/V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-01.md)。以下保留完整实施目标；未收口项不能通过改写文档被称为已完成。

> 版本：0.2.0，联合审计实施基线。日期：2026-10-30。  
> 语义规范：[Agent 原生时间执行引擎设计](AGENT_TEMPORAL_ENGINE_DESIGN.md) v0.4.0；API 协议：[ATE_API_PROTOCOL_DESIGN.md](ATE_API_PROTOCOL_DESIGN.md) v0.1.0；验收流水：[执行 Profile 场景](ATE_EXECUTION_PROFILE_WALKTHROUGH.md) v0.2.0。  
> 状态：第一版目标架构已完成主要纵切面；磁盘格式仍为实验格式，未取得设备/电源故障耐久性认证。  
> 初稿范围：文档；后续已在独立 Go module 实施，不自动切换任何外部宿主的数据权威。  
> 目标交付：独立 Go module 中的库、同内核的本地二进制服务及 SDK；当前 module 为 `github.com/vkviyu/agent-temporal-engine`，本文不虚构 Git 远端。  
> 开发工具链：初始沿用用户指定的 Go 1.27.0；构建版本与平台耐久性认证是两个独立事项。

## 阅读索引

1. [文档职责与范围](#1-文档职责与范围)
2. [代码结构与依赖](#2-代码结构与依赖)
3. [公共契约与类型](#3-公共契约与类型)
4. [不可变对象与编码](#4-不可变对象与编码)
5. [状态树与图的实现边界](#5-状态树与图的实现边界)
6. [运行时所有权与同步](#6-运行时所有权与同步)
7. [提交管线和请求幂等](#7-提交管线和请求幂等)
8. [存储与 DurableIO](#8-存储与-durableio)
9. [持久发布与异常处理](#9-持久发布与异常处理)
10. [创建打开恢复和关闭](#10-创建打开恢复和关闭)
11. [视图查询与权限](#11-视图查询与权限)
12. [多 Agent 与外部执行](#12-多-agent-与外部执行)
13. [库服务与本地协议](#13-库服务与本地协议)
14. [索引检查点与回收](#14-索引检查点与回收)
15. [资源配置与诊断](#15-资源配置与诊断)
16. [测试结构与验收](#16-测试结构与验收)
17. [实施顺序与阶段门槛](#17-实施顺序与阶段门槛)
18. [决策冻结与变更](#18-决策冻结与变更)

## 1. 文档职责与范围

### 1.1 与语义规范的关系

上位规范拥有 Core 与官方 agent/v1 的语义、不变量和领域边界；本文拥有依赖、接口实现、并发/生命周期与门槛；场景稿拥有与两者一致的对象流水/反例。三稿组成同一设计基线，场景不再是尚待合入的另一套对象模型。

不重复全部领域规则。本文中的工程选择若改变上位语义，必须先记录冲突并同步修订三份文档及相关工件，不能由实现偷偷重新解释。上位文档中的目录是概念分组；本文将其收敛为实际 Go 包边界。文件数、私有函数和局部结构可随实现调整，公共契约、依赖方向、原子性与恢复规则须经过变更审查。

### 1.2 第一版与首个纵切面不同

| 范围 | 内容 |
| --- | --- |
| 第一条存储纵切面 | 规范编码、最小状态操作、单事务提交、P1 发布、重开恢复、RequestID 对账 |
| 第一版完整范围 | 在上述基础上完成官方 agent/v1、版本/因果图、分叉汇聚、执行知识视图、标准导航/上下文、多 Agent 恢复、Memory/ExperiencePath、受控回收及库/服务双形态 |
| 后续范围 | 广播、通用订阅、主动推送、被动反馈调度、远程服务平台、跨节点共识及高级检索 |

首个纵切面允许 batch size=1、单段、无持久索引、全量恢复扫描。它必须显式报告限制；不能把阶段原型称为完整第一版。

第一版仍实现 Activity/outbox/receipt、持久消息、取消意图与 Continuation。宿主通过普通命令和查询协调它们，正确性不依赖订阅。

### 1.3 实现原则

- Core＋官方静态 agent/v1＋宿主适配三层；一个内核，同一份提交/Profile 转换和恢复代码，库与服务复用。
- 一个 store 只有一个读写 owner；并行发生在 Agent 计算、外部执行和固定快照读取。
- 领域状态、Actor 知识、当前控制面、幂等账本独立建根，同一事务原子发布。
- 算法计算产生不可变候选，只有协调器能持久发布。
- 先用简单同步和参考模型证明行为，再基于测量优化。
- 第三方数据库、KV、图引擎不作为内核依赖；标准密码与平台基础接口可以使用。

## 2. 代码结构与依赖

### 2.1 建议工程布局

~~~text
<independent-module>/
    ate.go                       package ate：Open/Create/Close、组装入口
    options.go                   配置与平台支持声明
    model/                       公共 ID、Event、Revision、Cursor、NodeRef 等值类型
    profile/                     agent/v1 公共值类型、命令定义、实体 key 与版本；只依赖 model
    api/                         命令/查询 DTO、结果、错误码、能力接口（可引用 profile 值）
    client/                      本地服务 SDK
    cmd/ate/                     serve、inspect、verify、export、doctor
    engine/                     稳定门面、协调器组装与生命周期
    codec/                      公开但实验性的规范编码与 schema 工具
    storage/                    公开但实验性的日志、段、manifest、DurableIO
    state/                      公开但实验性的持久 radix tree 与状态差异
    graph/                      公开但实验性的祖先、merge-base 与关系算法
    agentprofile/               agent/v1 纯转换、引用/索引提取与受保护命令
    activity/                   通用 OwnerScope/Activity/Attempt/acceptance/settlement
    contextview/                上下文候选、渲染与来源校验
    memory/                     记忆声明、晋升与撤回的纯转换
    query/                      节点浏览、检索、证据邻域、分页
    service/                    身份映射、句柄、请求分发、服务生命周期
    protocol/                   wire envelope、DTO 映射与错误映射
    transport/local/            本地连接、有界请求/响应帧
    reference/                  公开的内存语义参考模型（实验性）
    testkit/                    故障 I/O、调度器、场景工件
    tests/
        reference/               独立内存语义模型
        conformance/             两种接入方式共用的契约场景
        recovery/                崩溃与损坏场景
        testdata/                golden、invalid corpus、恢复镜像
    docs/
        formats/                 按阶段冻结的编码与磁盘规格
        decisions/               影响语义/格式/依赖的决策记录
~~~

ATE 公开 module 不使用 `internal` 目录。所有包都可以被外部项目导入，但稳定性分层明确：`model/api/engine/profile/protocol/client` 是优先稳定的公共契约；`codec/storage/state/graph/agentprofile/activity/contextview/memory/query/reference` 在各自文档标为实验性或实现级 API，变更需遵守版本化和迁移规则。公开不等于承诺每个实现细节永远兼容。

ATE 不把任何外部宿主（包括 GSEA）的适配器放入本仓库核心包。外部适配器位于宿主或独立仓库，通过公共 API 接入。

第一版不建立 changefeed 空包、订阅 goroutine、consumer 表或占位 Watch。也不为 header、CRC、LSN 等单个结构分别拆包。

### 2.2 公开包的稳定性分层

去掉 `internal` 后，外部可见性和稳定性不能混为一件事。下表描述达到稳定发布后的目标；当前所有已实现的包都处于 v0 experimental，尚未宣布冻结。包文档必须写明：

| 稳定层 | 包 | 承诺 |
| --- | --- | --- |
| Target stable contract | `model`、`api`、`engine`、`profile`、`protocol`、`client` | 只做兼容扩展；破坏性变化进入新主版本 |
| Versioned experimental | `codec`、`storage`、`state`、`graph`、`agentprofile`、`activity`、`contextview`、`memory`、`query`、`reference` | 可被外部实验导入，但 schema/语义版本和迁移规则必须显式 |
| Operational public | `service`、`transport/local`、`cmd/ate` | 公开可运行；跨平台和 wire 能力按支持矩阵声明 |

任何公开包都不得要求调用者访问未文档化的结构字段来完成正确使用。实现级包可以先以 `v0`/experimental 标记发布；不能靠 `internal` 隐藏不成熟设计，也不能把实验 API 伪装成 stable。

### 2.3 依赖 DAG

~~~mermaid
flowchart TD
    ENTRY[ate / cmd composition] --> K[engine]
    ENTRY --> SVC[service]
    SVC --> API[api]
    SVC --> P[protocol]
    SDK[client] --> API
    SDK --> P
    SDK --> T[transport/local]
    SVC --> T
    K --> API
    K --> ST[storage]
    K --> ALG[state / graph / query / activity / contextview / memory]
    K --> C[codec]
    ALG --> C
    ST --> C
    K --> AP[agentprofile]
    AP --> PV[profile values]
    API --> PV
    PV --> M[model]
    API --> M
    C --> M
    ALG --> M
    ST --> M
~~~

表中的依赖指允许的直接导入上界，不要求每个包都导入全部候选。

| 包 | 可以依赖 | 禁止反向依赖 |
| --- | --- | --- |
| model | 标准库 | api、门面、engine、storage、传输 |
| profile | model、标准库 | api、engine、存储/网络实现 |
| api | model、profile 值、标准库 | 内核、存储、服务实现 |
| codec | model、标准库 | engine、state、graph、服务 |
| storage | model、codec、平台 I/O | api、engine、领域模块、服务 |
| reference | model、api、codec、engine 的规范命令编码、标准库；不复用生产状态 apply/存储算法 | storage、service、传输 |
| state、graph | model、codec、消费方定义的读取接口 | storage 具体实现、engine |
| activity、memory、contextview | model、必要纯算法与消费接口 | engine、服务、数据库所有权 |
| agentprofile | model、profile、codec、必要纯算法/读取接口 | engine、api、storage/service 实现 |
| query | model、profile、api 查询值、state/graph、agentprofile 的纯投影、消费接口 | engine、服务、磁盘私有句柄 |
| engine | api/model、上述实现能力 | service、client、cmd |
| protocol | api/model、标准库 | engine、storage |
| service | api、protocol、transport、标准库 | storage、engine 的私有实现 |
| client | api、protocol、transport、标准库 | store 目录、engine、storage |
| 门面与 cmd | 所需组装模块 | 不被下层反向导入 |

同目录静态依赖检查在 CI 中验证。Go 编译能发现循环导入，但不能发现“服务偷偷直接写日志”这样的分层违规，因此还需允许列表检查。

### 2.4 接口归消费方，数据格式归拥有者

state 定义它需要的 ObjectReader；storage 的固定快照读取器自然满足接口，不必导入 state。engine 定义它需要的 journal 能力，其参数使用 storage 拥有的格式类型；storage 不导入 engine。

接口只在替身、跨层或实际多实现边界使用。不为每个私有结构创建接口，也不建立万能 Repository、插件容器或任意 reducer 框架。

agentprofile 不 import engine：engine 将已认证命令与固定读取视图交给纯转换，得到前置条件、候选记录/事件/StateOps，再统一发布。codec 只拥有 Core 编码原语/对象 envelope；Profile 自己的 schema 编解码和引用提取归 agentprofile，不让 storage 经 codec 反向耦合 Task。启用版本编译进库与 ate serve，在 store capabilities/required versions 中登记；不运行任意动态 reducer。

~~~go
// state，reader 已绑定固定 catalog generation。
type ObjectReader interface {
    ReadObject(context.Context, model.ObjectID) (model.Object, error)
}

// Apply 不执行 fsync，不更新 BranchRef，不发布 root。
func Apply(
    context.Context, ObjectReader, model.ObjectID, []Mutation,
) (TreeDelta, error)

// engine：按存储拥有的类型依赖 journal。
type CommitJournal interface {
    PublishBatch(storage.PreparedBatch) (storage.Publication, error)
}
~~~

TreeDelta 返回新根、待存不可变对象、必要读取依赖与资源统计。协调器将其与事件、控制更新、receipt 一起提交。PublishBatch 不接收调用者请求的取消 context；生命周期停止也不得在 P1 中间伪装事务撤销。

## 3. 公共契约与类型

### 3.1 门面、能力与所有权分开

ate.Create/OpenExisting 返回本地 Engine，client.Client 只拥有连接，不拥有 store。完整 V1 能力名称/职责以语义稿 §18.1 为准；下列是 M1 的能力子集，不再用同一个 Commands/NodeReader 名字定义不同方法集合：

~~~go
type Committer interface {
    Commit(context.Context, CommitRequest) (CommitResult, error)
    LookupRequest(context.Context, LookupRequestRequest) (LookupRequestResult, error)
}

type ViewOpener interface {
    OpenView(context.Context, OpenViewRequest) (ViewDescriptor, error)
    CloseView(context.Context, CloseViewRequest) error
}

type BasicNodeReader interface {
    ListNodes(context.Context, ListNodesRequest) (NodePage, error)
    GetNode(context.Context, GetNodeRequest) (NodeDetail, error)
    GetNodeContext(context.Context, NodeContextRequest) (NodeContext, error)
}
~~~

完整 NodeReader 增加 Search/Trace，完整 Commands 增加 Fork/Diff/Join 等；启用 agent/v1 时提供同一 AgentCommands.ApplyAgent 封闭命令能力。Core-only 明确不等于 Agent 第一版。

OpenViewRequest 选择 SnapshotAnchor（或 latest 原子捕获）、时间/可见性模式与 KnowledgeKey。ViewDescriptor={Ref,Anchor,ViewDigest,ExpiresAt}；不把独立 Cursor/Fence 字段维护成第二份真相。查询 DTO 都携带 Ref。

engine registry 唯一拥有 View 生命周期和一个 generation pin；service 只维护 session→ViewRef 归属，库便利 View.Close 和 SDK 调用同一个 CloseView。Ref 本身不跨 Principal/owner/session 授权。CloseView 幂等释放资源，不需要 durable RequestID，也不回滚事务。

所有内部 RootSet/Authority 读必须区分：Anchor 固定历史数据；当前 published bundle 决定现在能否输出/执行。不能为历史任务读取偷偷取最新 TaskControl。

### 3.1.1 Profile 的公共与存储映射

profile 值类型包含 Query/Task/Execution/Step/Invocation/Message 等闭合 schema。稳定 ProfileID=agent，agent/v1 的版本不进入实体身份 key；schema/transition/提取器版本独立登记。稳定 key 为 (ProfileID, EntityKind, LocalID)，Step.LocalID 为规范 (ExecutionID,StepSequence)；它们映射到 NodeRef.Entity。不可变记录仍用 ObjectID，二者不能互换。

每种实体注册 spec/state 的根归属和历史解析；每种对象注册 canonical 编码、强内容引用、逻辑关系与投影版本。agentprofile 输出纯转换，engine 校验版本/权限/所有前置 root 并发布。Profile 仅能使用授权的控制命名空间，不可普通 Put 系统 epoch/预算。

各 namespace 的源版本、权限、目标、先决条件和拒绝规则进入 contract corpus；未识别必需 Profile/transition 版本拒绝写，有限 inspect 可报告 opaque 内容而不猜语义。

### 3.1.2 Agent-facing 查询与命令投影

Core 内部 API 可以按 storage、graph、query、profile 拆分；对外使用统一请求 envelope 和固定能力集合：

- 查询请求固定 `ViewRef/SnapshotAnchor`、可选 `KnowledgeKey`、NodeRef/过滤器、layer、projection、contextMode 和资源上限；
- 返回 typed summary/detail、关系类型、来源 Anchor、continuation、truncated/unavailable；
- Agent-facing 工具只提供 list/inspect/expand/trace/read_content/search 这组渐进能力，SDK 方法可拆分但语义一致；
- Agent 不取得通用 Commit/StateOps/WriteObject，也不直接提交控制根。Spawn、GoalRebase、MessageConsume、AcceptResult 等经 agent/v1 `AgentCommands` 版本化校验；
- Agent 看到的导航分组/标题是投影或 Annotation，不能成为新的权威节点。

渐进路径固定为 Entity/Execution 摘要 → Step → Invocation/Activity → Attempt/Result/Artifact → Blob/ContextPacket → Event/Commit 审计。每次展开仍沿同一 View/Anchor/KnowledgeKey，不可通过更换 NodeRef kind、ObjectID 或深度绕过权限和预算。

wire 可以将这些 operation 编成统一 Explore union；Go SDK 继续提供稳定的 `ListNodes/GetNode/GetNodeContext/Trace/SearchNodes`。不实现一个拥有任意查询和写入字段的万能端点。

### 3.2 model 的值类型

- 随机逻辑 ID 与内容摘要使用不同的命名类型；不能用任意 string 混用。
- NodeRef 带 kind，区分 Entity、Event、Revision、Object；Entity 使用 Profile/Kind/规范 key，逻辑 ID 不冒充内容摘要。
- 字节内容、集合、map 与返回对象由引擎拥有不可变副本，不暴露可变内部 slice。
- 调用者不得在传参时并发修改输入。接收边界做长度检查与深拷贝后，不保留调用者可修改的 backing array。
- 当前权限、租约、进程内文件定位不进入历史领域对象的公共可变字段。
- 公共类型不得依赖 Eino、GSEA、任何外部宿主或 provider SDK。

### 3.3 命令 envelope

每个可重试命令至少包含 Space、稳定 Principal 身份、RequestID、command/schema 版本、类型化 body、expected heads/读版本和执行 authority。认证凭据由可信接入边界产生，不接受 body 自报权限。Event.RecordedBy 从认证主体产生，触发 Actor、Subject 和 StreamOwner 另有明确字段；Session/Query/System 事件可以没有 Execution。

RequestDigest 覆盖逻辑命令及前置条件；排除连接 ID、传输 correlation ID、等待 deadline、重试时刻及服务端后来分配的 TxID/LSN。修正请求 body 或 expected head 必须换 RequestID。恢复须先按原 RequestKey 对账；接管者经授权使用原幂等命名空间，不能因换认证连接/authority 就悄悄换原命令含义。

第一版一个领域命令在单 Space 内原子更新多个 Branch/根；跨 Space 操作走显式导出/导入。若要跨 Space 事务，另行扩展授权与原子性契约，不把它暗藏在通用 Put 中。store 级 owner/control 命令具有独立受保护的系统范围。

### 3.4 结果、错误与执行状态正交

至少分开表达：本次调用是否被接受、逻辑结果是否已知、故障原因，以及 store 是否仍可用。

| 情况 | 必须表达的含义 | store 状态 |
| --- | --- | --- |
| 本次入队前取消/队列满 | NotSubmitted，本次调用未被接受 | 不受影响 |
| 已入队，调用者停止等待 | WaitInterrupted，携原 RequestID，结果未知 | 通常仍 Ready |
| CAS/权限/参数校验拒绝 | Rejected/Conflict，本次执行确定未提交 | 通常仍 Ready |
| P1 写入/同步/发布不确定 | CommitUnknown，携批次诊断信息 | Fenced |
| 已持久发布 | DurableAck，包含 receipt 与 fence | Ready 或随后 Draining |
| 数据损坏 | CorruptionDetected，禁止猜测历史 | Fenced/打开失败 |

NotSubmitted 仅描述本次尝试，不能证明同一 RequestID 在更早尝试中从未提交。Go error 可以保留 context 取消原因，但恢复逻辑依据稳定结果码和 RequestID，不能只检查 context.DeadlineExceeded 或错误字符串。

LookupRequest 返回 Committed、InFlight、NotFoundAtFence 或 Indeterminate；负面结论必须绑定已验证的发布前缀。无权限时不借此泄漏 receipt 是否存在。

## 4. 不可变对象与编码

### 4.1 三种字节不可混淆

1. 原始模型/工具/provider 字节：原样保留，凭据单独处理。
2. 领域对象的规范编码：决定 ObjectID/EventID/RevisionID。
3. 物理 frame/wire 表示：包含长度、校验和协议字段。

IPC JSON 的字段顺序不能决定 RequestDigest；对象物理位置不能决定内容身份。格式解析器不依赖平台文件 API，便于独立 fuzz。Event 的 StreamOwner/StreamSequence/RecordedBy/Subject/Origin 各有 schema；序号由协调器分配，RecordedBy 由认证入口确定，不把触发 Actor 当记录者。

### 4.2 编码注册表

在 docs/formats 中维护 Core 与 agent/v1 注册表：类型域、编号、schema/transition/投影版本、字段顺序、整数单位、可选/集合规则、长度限制、未知 mandatory 拒绝规则，以及强内容引用/强历史发布依赖/逻辑关系/来源提取。逻辑 ID 不自动保留最新/全部版本；历史证据绑定 RecordID/EventID 或保留 Anchor。

Core codec 与 Profile codec 按所属 schema 显式编码；不使用反射遍历任意 Go struct 作为长期格式。若 registry 增加未知必需类型，旧 writer 拒绝写入，不静默忽略。

ATE-CANON-1 的具体编码和 golden corpus 在 M0/M1 冻结前完成。本文不把候选 48-byte frame header 或某个 magic 提前宣布为稳定 ABI。

### 4.3 对象构造顺序

~~~text
预分配 TxID/逻辑身份
 → 按内容依赖 DAG 生成 typed objects/events/roots/revisions
 → 允许 Revision 后生成引用它的控制对象/Continuation
 → 完成 RootSet/receipt record
 → body typed frames 拓扑混排、尾部更新/receipt 确认
 → TX_COMMIT、manifest/CURRENT
~~~

V1 CP 只使用其批次开始前已发布的 SnapshotAnchor；Observation/Revision 先发布，后续 pending-render 再建 CP。该动作固定 SourcePublicationRef(预分配 ProducerTxID,Cursor,KnowledgeKey)，不能预嵌当前摘要；发布后解析 producer 事务自己的 fence/根，不取 latest 或批次结束位置。当前事务的后置 Continuation 可引用较早生成的 RevisionID，但不能嵌当前 CommitFence。最终 TX 摘要不回写进自己的 payload 或 receipt root。receipt 保存 RequestID、TxID、逻辑结果与 LSN，由事务 envelope 提供最终提交证明。审计根由已发布事务链/fence 表达，不制造 hash 自引用。

相同 payload 可去重；发生身份由 EventStreamKey/StreamSequence 与事务身份区分，Execution stream 另绑定 incarnation。命令 digest、对象 digest、物理 CRC 使用不同域/用途。

### 4.4 大对象与事务限额

固定元数据和大 payload 分离。blob 按版本化 chunk manifest 组织；同事务内引用必须拓扑在前，批次外只能引用已发布对象。

单批超限时明确返回 ResourceLimit。可以先用若干对象发布命令持久化 chunk，再提交引用它们的 blob manifest 与领域状态；这些是可审计的已提交对象，不是假装已持久的临时上传。中断留下的已提交、未采用对象按保留策略处理。

chunk、frame、transaction、queue、query 的长度上限分别配置。初始数值在 M0 测试配置中登记，不把未测量常量当性能结论。

## 5. 状态树与图的实现边界

### 5.1 state 是纯结构变换

实现规范路径压缩 radix tree，节点按字节路径和有序 child 编码。EmptyRoot、空 key、absent/null/空值、单 child 路径压缩具有唯一表示。

Apply 读取固定根和对象视图，返回 TreeDelta。engine 决定是否采纳，storage 决定怎样持久化。state 不分配提交 LSN、不更新当前 head、不做审批、不访问模型。

Diff 游标包含绝对前缀与压缩路径 offset；仅在相同 key 范围下按相同 ObjectID 跳过子树。第一批反例必须包含同后缀不同前缀、路径拆分/压缩与空 key。

### 5.2 RootSet 与发布根

engine 使用类型化 RootSet，不让调用者自由拼接控制路径：

| 根 | 管理内容 | Restore 规则 |
| --- | --- | --- |
| Revision.StateRoot | 领域状态 | 可在当前历史追加采用旧领域值 |
| ExecutionKnowledgeRoots | KnowledgeKey=(Actor,E) 的观察集、工作知识与因果前沿 | 同 E 恢复不清空；同 Actor 其他 E 不自动继承 |
| BranchRefRoot | Branch head/version/status | 当前 CAS 更新 |
| LiveControlRoot | 系统 authority/预算/效果；受保护 Profile Query/Task/Execution/Step/Invocation/消息/PendingAction 状态 | 禁止随历史倒带 |
| CommandReceiptRoot | 已提交命令结果 | 禁止倒带或任意删除 |

每个事务持久 PostRootSet 并在恢复投影后核对。SnapshotAnchor.Fence 选定这些根；Cursor 只选领域/知识版本。已发布根可经保留日志/索引重建，cache 不构成额外权威。存储只接受内核确定字节，不从 key 名推断 Task 语义。

### 5.3 StateOps 分编译与应用

命令 handler 先验证领域条件，编译为版本化 StateOps，再按确定顺序应用。普通 Put/Delete 不能覆盖控制命名空间；序号、receipt、epoch 和派发授权由受保护 handler 生成。

状态投影重放只应用原版本已接受的操作，不用今天的余额或 ACL 重新裁定过去。校验策略版本和证据随事务记录，但历史投影不执行任意外部回调。

### 5.4 图与合并

graph 通过消费接口读取不可变 parent/关系；反向索引只是加速。因果 DAG、Revision DAG 与允许成环的语义图分别处理。

merge-base 先实现无优化的精确祖先集算法；多个最佳基点返回 AmbiguousMergeBase。MergePlan 保存 source delta、知识导入决议、最终 application plan 与相对 StateParent 的变化。

跨字段领域不变量由受信宿主提供可验证决议或固定校验规则；模型建议不是验证证明。第一版不提供事务锁内任意 Go validator 插件。官方 agent/v1 在纯状态机中固定通用 Task/Query/Step/取消/恢复约束；工作区 Executor 和业务验收策略留宿主，不能用模型陈述直接制造通过证据。

## 6. 运行时所有权与同步

### 6.1 可变状态的唯一拥有者

| 状态 | 所有者 |
| --- | --- |
| 写队列 admission、inflight | admissionMu 保护的短临界区 |
| staging roots、LSN、日志 writer、批次 | 一个提交协调 goroutine |
| 已发布 bundle、代际 pin、retire、读授权版本 | publishMu |
| ViewRef registry、在途操作计数、关闭/过期与单次 pin 释放 | publishMu |
| 服务端 opaque handles | service 自己的 handle-map mutex |
| 外部模型/工具进程 | 宿主 Executor，不归提交协调器执行 |

第一版不采用 lock-free publication、RCU 或复杂 epoch 回收。已有瓶颈证据和等价性测试后再替换 mutex。

### 6.2 PublishedBundle

~~~go
type PublishedBundle struct {
    Fence              model.PublishedFence
    BranchRefRoot      model.ObjectID
    LiveControlRoot    model.ObjectID
    CommandReceiptRoot model.ObjectID
    CatalogGeneration  uint64
    // 私有不可变 catalog 与已编译授权快照引用。
}
~~~

协调器在 P1 成功后一次交换 bundle。根、catalog 和权限快照不能各自独立更新。batch 内 staging 只给协调器看，公开查询永远不读取未发布根。

### 6.3 锁规则

- admissionMu 与 publishMu 不嵌套；锁内不阻塞入队、不做 I/O。
- OpenView 先在 publishMu 内捕获 bundle、增加 provisional generation pin；锁外校验/装载 Anchor 的历史 RootSet；再在锁内检查生命周期并将同一 pin 转交 registry。失败释放临时 pin，不持锁查磁盘。
- latest 的 Anchor 固定为首次捕获 fence；显式历史 Anchor 不能在注册时被换成更新版本。Close 必须等待 provisional pins 和在途操作，不能只数已注册 ViewRef。
- 每次查询在 publishMu 内检查 Ref/主体/session/过期与 closing，增加该 entry 的 activeOps 后才拿到只读能力；执行在锁外，完成时递减 activeOps。
- CloseView/过期在同一锁内将 entry 标为 closing，阻止新 acquire；activeOps 为零时只释放一次 pin、删除 registry entry 并唤醒关闭等待者。等待期间不持锁，也不提前删除尚有 activeOps 的 entry。
- 不再引入 per-view mutex；固定对象不可变，registry 的操作计数覆盖关闭竞态。
- 缓存锁和服务 handle-map 锁不跨调用核心 API 持有。
- 文件 I/O、hash 大对象、图遍历、网络和模型调用均在上述全局短锁之外。
- retire 判定在 publishMu 内，实际关闭/删除在锁外由维护所有者执行。

读者先拿指针、GC 后删除、读者再 pin 的写法禁止。一次性检查引用计数后立即删除也不足，必须关闭该代际的新 pin 入口并保证既有 pin 为零。

### 6.4 生命周期状态机

~~~text
Opening → Ready → Draining → Closed
             ├→ Maintenance → Ready
             └→ Fenced → Draining → Closed
Maintenance ──持久化/发布不确定──→ Fenced
~~~

Opening 不暴露可调用 Engine。Ready 接受命令与新 View。Draining 停止新领域命令、View 与 Activity 派发，排空已接受命令，允许已准入操作及必要对账的结果授权和资源释放；这些输出仍检查当前权限。

Close 请求是 sticky 状态：Maintenance 完成不能无条件回到 Ready；若已请求关闭则转入 Draining，发生持久化不确定则保持 Fenced。多个控制路径不得各自修改 lifecycle 布尔值而互相覆盖。

Fenced 不是“只禁写”：CURRENT 可能已持久新撤权，而内存仍是旧 ACL。禁止新提交、新 View 和新对外读取/派发 grant；旧 pin 只用于内部收尾。不能凭旧内存返回确定 NotFound。仅提供不泄露业务内容的故障报告，完成 Close 后重新 Open 验证；不在原对象上自动解 fence。

## 7. 提交管线和请求幂等

### 7.1 接收与 inflight

请求先完成类型/长度检查、认证、规范 digest 与必要深拷贝，再进入有界 FIFO。admission 与停止接收在 admissionMu 下原子决定；队列满明确返回限制，不在锁内等待空位。

幂等键为 (Space, stable Principal, RequestID)，带版本。相同键/摘要的在途请求共享一次执行；不同摘要冲突。等待者彼此独立，取消某一等待不取消其他等待或共享命令。inflight ID 总数与每个 ID 的等待者数分别受限，去重不能绕过资源上限。

inflight 只解决本进程并发，不替代 CommandReceiptRoot。每个等待者获取结果仍经过其当前读取授权，不因共享执行而共享权限。

### 7.2 协调器处理顺序

1. 查询已提交 receipt；有权读取且摘要相同，返回原结果，不再查旧 head/扣配额。
2. 对新命令校验当前权限、执行 fence、资源限额和跨 Branch 读版本。
3. 对 batch staging 根验证预期 head/CAS；handler 产生纯变更/事件草稿，协调器固定 TxID、逻辑身份和各 stream 序号，再按内容依赖 DAG 编码事件/StateOps、候选根与结果；不得先算 EventID 后改序号或 TxID。
4. 用 schema 提取验证强内容引用/逻辑关系、因果顺序、StreamSequence 和资源成本；Profile 同时验证动作槽、KnowledgeKey、目标/取消屏障。
5. 将完整候选纳入批次并最终分配 LSN，组装 receipt、相关控制/receipt 根和规范事务；在 EventID 算出后不得再替换 TxID。
6. 执行 P1，发布 bundle，完成对应 receipt 等待者。

时间、随机与服务端生成身份按规定作为显式输入记录；不能在恢复 replay 时重新采样。

### 7.3 已提交幂等与拒绝的边界

持久幂等承诺针对已提交 receipt。校验 Rejected 不创建 durable receipt，不保证跨重启保存相同拒绝理由；同一原始命令可在重开后再次校验，但若修正内容必须新 RequestID。

进程内终态缓存只作优化，不能将其当持久拒绝账本。未来若要求拒绝也永久稳定，须增加明确的 durable rejection 协议和存储成本。

receipt 长期保留是默认 AuditFull 的一部分。不能清掉 receipt 后把旧命令当作新命令执行。

### 7.4 取消边界

第一版选择简单规则：**成功入队之后，调用者取消只停止等待，不撤销命令。** 协调器不把 caller context 传入 P1 作为任意中断令牌。

入队前取消可以确认本次未接受；入队后返回 WaitInterrupted 并提示原 RequestID 对账。连接断开也遵守同一规则。确定未开始的新命令在 store 进入 Fenced 后可拒绝执行，但不能由此证明该 RequestID 在过去不存在。

纯查询可响应 context 取消；中止遍历不产生事务。显式 CancelExecution 是新的 durable 命令，与取消一次 Commit 等待不同。

### 7.5 Group commit

M1b 首先 batch size=1；单事务故障矩阵通过后，M1d 已启用有界 maxBatchCount/maxBatchBytes/maxBatchDelay，具体实现见 [M1d 状态](M1D_STATUS.md)。

协调器按 FIFO 对前一个 staging 结果验证后继。批内先前事务产生的对象可被后继引用；前驱校验拒绝时，相关依赖后继拒绝或重新验证，不能继续引用丢弃对象。所有被纳入同一发布批次的事务共同升入一次 CURRENT 认证前缀；每笔 Ack/receipt 保留自己的 Fence/RootSet，CURRENT 与全局 Snapshot 指向批末 tip。

P1 开始后发生错误，整个未确认批次进入结果未知处理，不挑选内存中“看起来成功”的子事务 ACK。恢复按 CURRENT 验证；不自动接受完整但尚未发布的 TX_COMMIT。

### 7.6 逻辑动作槽与 PendingAction

RequestKey 去重之外，官方 Profile 保存业务动作唯一槽：Query 的 start slot、(OwnerScope,Invocation) 的结果/Activity/child 绑定、(Message,recipientE) 的首次消费。新 RequestID 也不能绕过槽 CAS；合法重复返回已绑定结果或明确冲突。

Continuation 指向 PendingAction 版本记录。记录包含 ActionKey/generation、完整原 RequestKey、canonical domain commandRef/digest、输入 Anchor/对象 refs、预期版本、authority、已知结果与 supersedes。它不保存 wire JSON 或临时 ViewRef 作为唯一恢复依据。

精确命令的预期控制版本使用已定义的数值版本/不含自身的前驱引用，不能把“包含这个 PendingAction 的当前控制记录 hash”再放回命令制造循环。新知识所在事务内的 pending-render 使用 SourcePublicationRef，engine 在内容编码前绑定预分配 ProducerTxID；解析经已验证 TxID→LSN/digest/PostRootSet 索引完成。这个引用登记为强历史依赖，缺源不换 latest。当前新 Revision 可由后置动作引用，但不能嵌本 CommitDigest。

恢复流程：Lookup 原 RequestKey → 已提交只补处理 → 未知先收敛 → 确定未提交且旧 authority 已 fence，才由 RebindPendingAction CAS 产生新 generation/RequestID/明确输入与授权。原 body 不改写，换 owner 不意味着能丢掉原幂等命名空间。Rebind 不许可重跑 unknown 外部效果。

Query intake、模型决议解释、Spawn、结果接受/结算/观察均使用这个机制。纯 Query 动作固定持久 Anchor、筛选/顺序/版本并保存 ResultSnapshot；中断后重开相同视图，不能用 latest 拼历史输入。
## 8. 存储与 DurableIO

### 8.1 模块内分文件，不提前拆成大量包

storage 内可按 frame、segment、manifest、catalog、journal、recovery、durableio、platform、maintenance 拆文件。底层不导入 api 或 Agent 模块；它处理类型化记录与经过准备的事务。

catalog 记录逻辑对象到物理位置的定位与 coverage，随 PublishedBundle 固定代际。索引缺失回源扫描或明确失败，不能返回伪造的不存在。

### 8.2 DurableIO 消费契约

下面是目标接口示意。M1b 已落地的窄接口以 `storage/durableio.go` 为准：参数为受限相对路径，增加 Size/List/Mkdir，profile 由 IO 实例固定；InstallNew 表示同目录原子无覆盖安装。具体发布与恢复行为见 [实验 store registry](formats/store-experimental-1.md)。

~~~go
type DurableIO interface {
    CreateExclusive(path string) (DurableFile, error)
    OpenRead(path string) (ReadFile, error)
    OpenAppend(path string) (DurableFile, error)
    RenameNew(from, to string) error
    ReplaceAtomic(from, to string) error
    SyncDir(path string) error
    Remove(path string) error
    AcquireOwnerLock(path string) (OwnerLock, error)
}

type DurableFile interface {
    ReadAt(p []byte, off int64) (int, error)
    Write(p []byte) (int, error)
    Truncate(size int64) error
    Sync(profile DurabilityProfile) error
    Close() error
}
~~~

ReadFile/OwnerLock 的实际小接口在 M0 随使用点确定，包含只读定位/关闭和明确释放；不抽象整个 os 包。

RenameNew 不覆盖已有不可变文件；ReplaceAtomic 用于 CURRENT 等可替换入口。平台适配若无法满足语义，明确返回不支持，不能用不安全的“先删后 rename”模拟原子替换。

完整写循环处理短写、零进展和 n>0 同时有 error。一个写句柄只由协调器追加。短写后的修复不是重发整个业务命令，而是进入该次持久化结果的恢复流程。

DurabilityProfile 明确平台能力，不把所有 File.Sync 都解释成完全相同的掉电承诺。初始开发优先当前 macOS 环境，Linux 另建适配与验证；本提案尚未认证任何实际 OS/文件系统/设备组合。

### 8.3 Frame 与事务规格工件

上位文档的 48-byte header 是初始候选。冻结时 docs/formats/frame-v1 必须给出逐字节字段、magic、endian、kind/flags 编号、长度上限、CRC 覆盖范围、padding 与 ordinal 规则。

CRC32C 与 SHA-256 摘要分工不变。建议固定：header CRC 校验不包含自身；record CRC 校验 header/payload/padding 而不包含自身 trailer；具体字节向量核对后才冻结。

TX_COMMIT 不能把自身包含进自己的 frame 摘要。冻结规范须明确 body frame count、ordinal/body digest 的范围，以及 CommitDigest 如何绑定 TxID、LSN、前事务摘要与 PostRootSet。恢复必须核对投影的根，SnapshotAnchor 的历史控制读取据此解释。一个事务可跨段，但一个 frame 不跨段；跨段能力在轮转阶段通过后启用。body 允许 OBJECT/EVENT/STATE_OPS/REVISION 按内容 DAG 混排，支持后置控制对象；不是按类型四段分组。尾部 Ref/Control/Receipt 引用已构造内容，receipt 树不依赖尾部 frame/CommitDigest。

### 8.4 Manifest 的认证范围

每个 segment descriptor 至少包含 SegmentID、header digest、active/sealed、publishedEndOffset、认证前缀摘要和提交范围。

**active 段不能用不断变化的全文件最终摘要代表旧发布状态。** 新追加的未发布 tail 不得使旧 CURRENT 的 manifest 失效。恢复只认证 manifest 指定字节范围与事务 fence；publishedEndOffset 与实际文件长度分开。

封存时写入并同步固定 footer/摘要，再构造 successor 的 predecessor 摘要。若 seal/新段尚未发布，它们仍属于未确认布局；不能因为 footer 完整就提升事务。

M1e 已实现上述封存/轮转与跨段规则。物理段目录和逻辑日志地址分离：对象定位、Scanner 与 PublishedEnd 使用稳定逻辑偏移，后续 header/footer 不进入逻辑流；每段 DataEnd 才是物理前缀边界。具体格式和当前上限见 [M1e 状态](M1E_STATUS.md) 与 [分段格式](formats/segmented-store-experimental-2.md)。

### 8.5 格式状态

| 状态 | 可以做什么 | 禁止承诺 |
| --- | --- | --- |
| Experimental | 改 schema、重建实验库、显式迁移测试镜像 | 长期兼容、任意旧库可无损自动升级 |
| Candidate | 冻结候选字节与 corpus，开展恢复/平台测试 | 测试未完成前称稳定格式 |
| V1 frozen | 编号、向量、拒绝规则和迁移契约入库 | 原地重解释已发布对象 |

读取到不支持的必需格式必须拒绝写入。experimental 数据也要正确恢复，不能拿实验标签豁免 ACK 和损坏语义。

## 9. 持久发布与异常处理

### 9.1 唯一发布者

engine 决定事务顺序；storage journal 执行唯一物理发布路径。提交、checkpoint、compaction 安装都由同一协调器仲裁 generation，禁止后台维护直接替换 CURRENT。

维护安装不得降低 floor；读取定位变化也须与 roots/catalog 一起发布。

### 9.2 P1 的具体边界

~~~text
确定 staging 事务与根
 → 写全部 frames
 → sync 所有触及段 / 新段目录
 → 写并 sync 不可变 manifest 临时文件
 → 安装 manifest 并 sync manifests 目录
 → 写并 sync CURRENT 临时文件
 → 原子替换 CURRENT 并 sync store 目录
 → 在 publishMu 下交换 PublishedBundle
 → 对具备输出授权的等待者返回 DurableAck
~~~

CURRENT 是发布证明，不是可丢缓存。manifest 文件存在、TX_COMMIT 可解码、文件大小足够，都不能代替已认证 CURRENT。

持久发布成功但进程在内存交换前退出，重开必须恢复该批次。客户端没收到 ACK 时，仍通过原 RequestID 查到结果。

### 9.3 写失败与 Fenced

确定该命令未开始持久写时可返回此次未提交；若因读取已发布历史损坏失败，store 仍必须 Fenced，不能将其当作普通参数拒绝。开始写入后，任何无法证明未提交的 I/O/安装异常都标记 CommitUnknown，并阻止后续发布和新对外 grant。

保持原错误链、步骤标签、尝试 TxID/批次与可披露 fence，禁止带 API key 或完整 prompt。调用者不能通过自动换 RequestID 重试掩盖未知提交。

已验证 floor 以内损坏报 CorruptionDetected。floor 以后的字节只有在 CURRENT/manifest 认证成功、确认从未发布时才隔离或截断。默认先输出 RecoveryReport、保留诊断材料；不自动尝试旧 manifest 伪装恢复。

### 9.4 批次与崩溃可见性

新读者只看旧 bundle 或完整新 bundle。任何目录同步、manifest 替换和恢复尾部处置都可注入故障。

测试同时保存客户端看到的 ACK 集合、结果未知集合和恢复发布前缀。不能只验证“数据库还能打开”，也不能要求未知命令必定消失。

## 10. 创建打开恢复和关闭

### 10.1 Create 与 OpenExisting 分开

Create 仅接受空/明确允许初始化的目录，获得独占锁后建立 IDENTITY、genesis 段、MANIFEST.0 与 CURRENT，按 P1 同等级别同步。不把任意缺 CURRENT 的目录当空库。

OpenExisting 验证身份、锁、CURRENT、manifest、发布范围、事务链、对象摘要与版本支持，再重建根与 receipts。第一条纵切面完整扫描权威前缀，不依赖 checkpoint/index。

创建中断或边界未知时提供 InspectIncomplete/verify 报告。不得自动覆盖；受控重建或回收是显式管理操作。

### 10.2 恢复结果

RecoveryReport 至少包括已认证 fence、使用格式/语义版本、已验证范围、尾部处置、重建索引、不可用历史与阻塞执行项。报告不包含未授权内容。

恢复数据库状态不等于允许派发历史 outbox。Execution 必须获取当前 authority，经 ResumePlan 确认旧 worker、预算与资源隔离；旧备份接管采用新 DeploymentIncarnation。

### 10.3 Close/Drain

1. 原子停止新 admission 和新 View；
2. 完成或明确拒绝尚未开始的已接受命令，已开始 P1 的批次走到确定边界或 Fenced；
3. 停止新的外部 claim/grant，等待内核在途操作；
4. 等待既有 ReadView、查询、维护与文件 pins 释放；
5. 关闭文件，最后释放 owner lock，进入 Closed。

Close(ctx) 超时返回 CloseIncomplete，原对象保持 Draining/Fenced，不能提前释放锁。重复 Close 等待同一次关闭；不要求用户通过强制解锁完成关闭。

关闭 ViewRef 时保留 closing entry 直到 activeOps 清零；CloseView 等待超时只停止等待，不使 entry 重新可用。最终一次 EndRead 或后台关闭流程释放该 pin。未知/已完成 Ref 的幂等 Close 不披露其他主体的句柄存在性。

本地 View 的 Close 是调用者责任。服务 View 另有明确 lease 和连接回收规则。正在使用文件的查询未结束时，不能先关文件或仅将计数设零。

### 10.4 单 owner

库模式和服务模式是同一 store 的不同所有权选择，不可同时直接写目录。服务已经拥有 store 时，其他宿主通过 SDK 访问。

OS 锁不证明远程 Executor 已停止。接管外部工作仍需要资源端 fence、隔离或停止证据；数据库锁和执行 authority 不能互相替代。

## 11. 视图查询与权限

### 11.1 ReadView 捕获与历史实体解析

OpenView 按 §6.3 两阶段协议捕获 bundle/provisional pin、锁外解析历史 RootSet，再注册相同 pin。SnapshotAnchor.Fence 认证同一时刻 PostRootSet，Cursor 只选择其中已发布的领域/知识 Revision；拒绝未来 Cursor/错误 Store/Space。latest 请求一次捕获并返回 Anchor。

EntityKey→spec/state 解析由注册 schema 指定根。AuditSnapshot/Discovery 使用 Anchor 的控制根，ExecutionKnowledge 使用 KnowledgeKey=(Actor,E) 及 Cursor 的已采用记录，不偷偷回退到全局当前记录。同 Actor 其他 E 不自动共享；Observation CAS 对准具体 E/知识位置。

历史解析用当时根，当前 ACL/UseBan/取消/authority 用最新已发布控制门禁；两者不能混拼成一份过去 prompt。SourceAnchor/ViewDigest 不含进程 pin、租约或 handle 地址。CP 只引用已发布 Anchor，不允许同 batch staging 输入快照。已给定 Anchor 时，另传 Cursor/Knowledge 必须一致，否则 AnchorMismatch；latest 请求只捕获一次选择。

因果祖先不因遍历就获得 payload 授权。语义边 assertion event 也要可见，不能只校验端点。

### 11.2 节点读取分层

ListNodes layer 明确为 entities/events/content，SDK 默认 entities 的 Profile 导航。实体按 Anchor 解析记录；未持久化分组只是投影，不伪造可执行 NodeID。ListNodes 返回元数据/短简介；GetNode 返回类型化详情和内容引用；GetNodeContext 按范围展开；大内容按获准范围读取。SearchNodes v1 用结构条件/明确文本匹配或有界扫描，不隐式调用模型。

上下文分 RecordedInput、AsKnownAtAnchor、EvidenceNeighborhood、Retrospective。节点多个 anchor 要求调用者选择；纯控制事件没有 Revision 时不能伪造可执行 Cursor。partial、truncated、NoRecordedInput、HistoryUnavailable 明确区分。

### 11.3 读授权不变成每次写事务

只读查询的候选在锁外生成；对外输出前，以预编译的授权快照在 publishMu 下确认当前 ACL/UseBan/隐私版本并签发绑定内容、目的地和 View 的短期 ReadEmissionGrant。

锁内不得加载大对象或执行外部策略回调。权限版本变动则重新校验或返回 AuthorizationChanged。撤权发布与 grant 签发通过同一 publishMu 排序；grant 先于撤权时，不能承诺收回已经授权出站的字节。

读 grant 不推进领域状态，不要求每次查询落盘。内嵌调用在返回前立即消费，Unix 服务以连接 ID 和请求 ID 作为目的地的一部分，消费后才写出响应。读取 JSON 的序列化发生在锁外，签发前再次比较出版 fence；变更返回 AuthorizationChanged。将结果实际交给模型时，宿主通过 Observation/ContextPacket 持久记录确切输入。Activity 的授权、预算预约与派发意图仍是 durable 命令。显式 Packet 的 RecordedInput 与 Summary 来源会反向参与原始对象读取的当前撤回检查。

Fenced 禁止所有新的对外 grant；Draining 只允许已准入操作和必要对账完成所需的结果 grant，禁止新业务和 Activity 派发。两者都不能拿旧 View 的 ACL 代替当前授权。

### 11.4 确定性与缓存

缓存键至少含 Space、SnapshotAnchor、KnowledgeKey/知识 Cursor、视图模式、算法/索引版本；缓存命中仍重新检查当前输出权限。严格历史排名不使用未来语料统计。

索引 coverage 不足时回源、等待或返回明确错误，不用空结果掩盖。图查询预算限制访问节点/边/bytes/时间；截断不是完整证明。

## 12. 多 Agent 与外部执行

### 12.1 Core、官方 Profile 与宿主责任

agentprofile 静态实现 agent/v1 的 Query/Task/Execution/Step/Invocation/消息/Goal/取消/恢复纯转换，拥有对应 schema 编码、引用和导航提取。engine 经封闭分派器校验版本、权限/前置根并提交，storage 不认识 Task。

activity 实现通用 OwnerScope 的 Activity/Attempt/acceptance/settlement 转换。Query 路由/短交互可以没有 Task/Execution/Step，以 (QueryID,DecisionSequence) 作为 DecisionRef；工具 Invocation 来源 key 与 Execution Step 统一为类型化 DecisionRef，披露在 Query 控制范围记录，无需假知识 Revision；资源准备可在第一个主 Step 前；auxiliary 摘要明确角色。模型活动需 CP，外部工具可用确切请求/资源基线，无需假 CP。

宿主提供 Eino 等 Loop、业务验收策略和 Executor，不重新发明恢复 schema，也不能任意 Put 受保护状态。模型仅提出动作；Profile 记录合法接受，Executor 执行外部资源并回传证据。

### 12.2 第一版无需订阅的调度

~~~text
宿主 Query 待处理 outbox / Continuation
 → Claim 命令（CAS + 当前 owner/执行 authority）
 → 提交授权、预约与 dispatch intent
 → 宿主调用外部资源
 → RecordReceipt 命令
 → Settle 命令 / 恢复补结算
~~~

允许有界轮询或宿主本地直接继续；持久状态是依据，不依赖一次内存通知。重启后先扫描已接受结果/未结算项，再处理 unknown 和未派发工作。

### 12.3 正交结果、恢复与取消责任

Attempt.raw result、Activity 唯一 acceptance、settlement 与各 KnowledgeKey 的 Observation 分别更新。settled 不能当模型已知；默认 Observation 引用合法 acceptance，raw/旧 epoch 证据只可经明确 Reconcile/EvidenceImport，不能自动满足新 E/Activity。

Continuation/PendingAction 与状态变化/下一意图原子提交。模型结果已接受就解释原结果，消息未消费就消费原消息，纯 Query 以原 Anchor 重建，旧 authority 按 §7.6 先对账再显式 rebind。

取消先写祖先屏障，Spawn/Claim/派发按当前 Task/E/祖先链检查；再 durable 传播至子树并回收/隔离资源。重派默认取消旧 E 子树，不改挂孙执行；旧未知效果继续由 Supervisor/控制账本负责。新 E 使用重叠资源前必须 fence/隔离，否则 blocked。

GoalAdopted 仅改当前绑定。TaskControl 的 goalEpoch 单调提升；子 Execution 的 InheritedGoalBindings 只列实际继承的父强制约束（sourceTask、goalRef、goalEpoch、constraints），不默认继承全部祖先。下一 Step、Claim、外部 grant、任务完成前重新校验当前源 goalEpoch。失配或未观察新约束时 Blocked；GoalRebase 生成不可变新子目标/依赖版本并 CAS，兼容性需受信策略/用户证据。下一 Step 必须 Observation 新目标；Task.currentGoal、E.acceptedGoal、Step/CP.goal 与继承绑定相容，旧 Step/CP 不改写。

Fenced 不接收新 RecordResult，未持久的外部回执不能承诺不丢；Draining 只完成已准入动作和必要结果授权，不新建业务/派发。资源/网络未知按 effect 契约收敛，不因取消或重派消失。

### 12.4 可扩展边界

动态任务与临时子 Agent 不预先设计完整 DAG。官方 Profile 固定通用身份、关系与恢复规则；宿主可扩展业务字段/验收策略，但不替换受保护通用语义。外部宿主特有的 Task/Reviewer/UI 可映射到标准类型，不反向导入内核。

未来通知只是已发布事实的交付层。稳定 ID、LSN/ordinal、SnapshotAnchor、明确保留闭包、唯一动作槽与原子命令保留扩展空间；第一版不建消费者账本。

## 13. 库服务与本地协议

### 13.1 组装关系

~~~text
嵌入宿主 → ate.Create/OpenExisting → engine → 同一存储

客户端 → client SDK → local transport → service → api 能力
                                              ↑
cmd/ate serve → ate.Create/OpenExisting ────────┘
~~~

service 只接收能力接口，由 cmd 注入已经打开的同一个 Engine。service 不导入 storage，也不实现一套自己的 receipt、权限或恢复状态机。

### 13.2 第一版协议选择

本地端点优先 Unix domain socket；其他平台的端点适配单独声明支持。第一版采用有界长度帧 + 版本化 JSON envelope，**JSON 只用于 IPC，不用于存储内容身份**。

候选帧为固定宽度长度前缀和指定长度的 UTF-8 JSON；prefix endian、最大长度、未知字段/枚举规则在 protocol-v1 registry 冻结。单连接顺序请求/响应，不实现 server push、订阅或多路复用；并行客户端可使用有界连接池。

envelope 包括 protocolVersion、method、correlationID、领域 RequestID（写命令）、body、结果/稳定错误码。64-bit 整数以明确 wire 形式表示，避免跨语言精度歧义；字节与 ID 具有唯一传输格式，映射为 model 值后再计算领域 digest。

capabilities 同时公布 Core/storage/agent Profile 的版本；未启用或不兼容方法明确拒绝，库与服务使用同一静态实现。服务在分配内存前检查长度和连接限额。解析、认证、领域校验分别处理；未知 method/capability 明确返回不支持，不用成功空响应占位。

### 13.3 身份与目录权限

endpoint 放在受控目录，校验所有者和权限，拒绝不安全路径/符号链接替换。连接由 OS 身份验证或明确的本地凭据协议映射到 Principal；具体平台机制在服务阶段冻结并测试。

不得信任客户端自报 Actor、Space 或 admin 布尔值。协议 method 权限和核心领域授权都要检查；SDK 不能直接读取服务端数据文件。

### 13.4 断线与句柄

SDK 在可能已送达命令后断线，保守返回结果未知，通过原 RequestID 查询。不自动更换 ID，也不把连接 deadline 当成回滚。

wire ViewHandle 对应第 3.1 节的同一 ViewRef，绑定 owner generation、Principal、ViewDigest 与 lease。service 的 session 表只跟踪归属；取出 Ref 后释放 map 锁再调用核心，真正的 acquire/closing/pin 释放由 engine registry 负责。

第一版选择连接绑定：SDK 的 View 包装持有原连接的租用权，分页/详情/Close 保持连接亲和性，不转发到连接池的另一条连接。断开时 session 先标记关闭、使新调用失效，再经同一个 CloseView 回收 Ref；竞态中的在途操作按 activeOps 结束后释放 pin。服务连接绑定信息来自可信接入上下文，不接受 body 自报 session。

断线返回 ViewExpired，不能偷偷换连接续页。需要继续固定历史查询时可按不可变 Cursor 重开 View，不承诺旧分页 token 自动可用。SDK 的新业务请求可以使用池中的其他连接。

### 13.5 服务关闭和离线工具

先停止接新连接/请求，再停止 admission，排空已接受内核命令，关闭句柄与连接，最后 Close Engine。短时间关闭失败仍保留 store owner，不强制解锁。

verify/repair/export 要么使用在线受控能力，要么获得离线独占 owner。inspect 不因只读展示就绕过权限或认证范围。

## 14. 索引检查点与回收

### 14.1 正确性基线

第一条纵切面无持久索引与 checkpoint，全量扫描恢复。随后引入对象定位、Actor timeline、邻接 posting，均记录 coverage fence/版本，可丢弃重建。

checkpoint 保存 source fence、RootSet、格式与语义版本、完整性摘要。恢复验证其对应日志前缀；不可认证时回源或明确失败，不重请求模型。

### 14.2 Maintenance compaction

第一版由协调器进入 Maintenance，排空旧写并暂停新逻辑写，固定 roots/floor。已有 pinned readers 可读旧布局；新 View 可等待或明确 Busy，不无限排队。

重写保留完整 canonical TX frames、事务顺序/LSN/摘要、StateOps、控制和 receipts。新物理段拥有新 SegmentID、封存链和来源锚点；安装走同一 P1、generation CAS，floor 不倒退。

发布后关闭旧代际的新 pin 入口，等待已有 pins/备份/导出释放，再删除旧文件。Maintenance 阶段磁盘空间不足提前拒绝或失败进入已定义恢复路径，不能先删旧文件腾空间。

### 14.3 保留策略

AuditFull 保留整个发布事务链，包括不属于任何 Revision 的独立控制事件。闭包由每种 schema 的强内容引用提取；稳定 EntityID 关系与索引不代替强引用。Branch prune 只改变导航/可继续执行引用，不自动释放祖先。

Memory/ExperiencePath 保存精确 SourceAnchor、筛选成员、必要反例/省略缺口、验证与适用范围，逻辑修剪不抹原事实；跨任务/项目复用经过晋升/撤回/脱敏与目标适配评测。不能把缩短后的浏览路径称为原始确定执行。

有界遗忘、权威 payload 删除和日志稀疏化须独立协议；第一版不能用“对象已复制”当作删原事务的依据。Memory、unknown effects、幂等账本与活跃 readers 都是保留根。

## 15. 资源配置与诊断

### 15.1 配置分层

| 配置 | 归属 | 改变时的约束 |
| --- | --- | --- |
| 格式、schema、StateOps 版本 | store | 不能只改配置重解释旧字节 |
| batch/queue/frame/query 限额 | 运行时 | 校验上下界，不能取消已发布保证 |
| durability profile | 平台/store 打开策略 | 必须与声明支持矩阵匹配 |
| 权限、UseBan、epoch、现实预算 | durable 控制面 | 普通历史 Restore 不覆盖 |
| endpoint、连接数、handle lease | 服务 | 不改变领域 digest |
| model framing/输出预留/renderer | 上下文策略 | 记录版本与实际输入来源 |

费用预算只控制是否准入/继续执行，不削弱关键约束或偷偷使用低质量摘要。Context token 容量与现实预算分别命名、分别报错。

### 15.2 最小诊断

日志记录 RequestID、TxID、LSN/fence、步骤、耗时与稳定错误码；默认不记录 prompt、工具全文、认证令牌、密钥或用户文件内容。

指标覆盖队列等待、验证/编码、持久步骤延迟、batch 大小、fenced 原因、pin 数与年龄、扫描/索引 coverage、查询截断、未知 Activity。指标不能作为业务权威，debug 端点不自动获得管理员权限。

### 15.3 不做未经验证的默认承诺

所有资源默认值在配置 registry 与基准报告中登记。性能报告标明硬件、文件系统、durability profile、数据形状和缓存状态。模拟器、进程 kill、真实设备测试是不同证据，不合并成“完全耐久”。

## 16. 测试结构与验收

### 16.1 独立参考模型

tests/reference 用普通 map、显式集合/图和全量副本实现语义，不复用生产 radix tree、merge-base 优化或 StateOps apply 函数。可共享 model/api 值和冻结测试工件。

优先比较领域 map、可见事件、合并决议、控制状态、receipts 与效果结论。状态根再通过独立 golden 样本验证，避免两边共享同一个错误编码器而一致通过。

### 16.2 必需格式工件

- canonical registry：字段、域、整数/集合/缺失规则；
- frame registry：编号、flags、边界、CRC 与摘要覆盖范围；
- manifest schema：active/sealed、认证范围、floor、代际链；
- golden corpus：结构 → 规范 bytes → ID/digest，含人工推导或独立实现核对；
- invalid corpus：重复 key、非最短整数、溢出、未知必需字段、错 padding/ordinal、截断；
- recovery images：每个同步/安装边界前后的可恢复镜像及期望结果。

不由生产编码器单独生成全部预期 bytes 再用它自证。golden 改动必须说明是否改变格式及旧数据处理。

### 16.3 DurableIO 故障模型

测试 I/O 区分可见字节、持久字节、文件内容同步、目录项同步、rename/replace 和 owner lock。掉电模拟丢弃未持久状态；进程退出保留模型允许的文件系统状态；介质损坏另行翻转已确认字节。

确定性调度枚举短写、零写、部分成功带 error、磁盘满、段同步、manifest/CURRENT 安装、目录同步、内存发布、ACK 丢失、读者 pin/retire、Close 超时与旧 owner。

### 16.4 首条必须跑通的闭环

~~~text
Create
 → Commit(RequestID, expected head, mutation)
 → DurableAck(receipt, fence, state root)
 → Close 或注入崩溃
 → OpenExisting
 → LookupRequest(同 RequestID)
 → 相同逻辑结果、领域状态与 StateRoot
~~~

同时覆盖同 ID 同参重试、同 ID 异参、CAS 冲突、最后一份配额已用尽后的 receipt 查询、≤floor 损坏和 >floor 撕裂。

### 16.5 两种交付形态共用契约

conformance 将相同场景分别送入嵌入库和 SDK→本地服务，比较领域结果，不要求两次独立创建的随机 StoreID/TxID 字节天然相同；需要精确对比时注入记录的时间/身份输入。

服务专测：响应丢失、未知版本、身份伪造、超大帧、断开后的 pin 回收、连接池并行、服务与库争用同 store。拒绝第二 owner，不以重试打开绕过锁。

### 16.5.1 联合场景的必测反例

- C01 已接收、C02 前退出：从原 intake action 恢复，不换最新路由/配置。
- Start/Spawn 已提交而 ACK 丢失；故意换 RequestID：Query/Invocation 唯一槽仍只绑定一套任务/子执行/逻辑 Activity。
- 同 Actor 两 E 的私有观察互不泄漏；只给旧 Cursor 不能读错控制-only Commit 后的 Task 状态。
- Observation 发布后、CP 前退出：pending-render 重建原已发布 Anchor，无当前 CommitDigest 自引用。
- raw result 已记录但无 acceptance/消费：不满足后续 Step/父 join；settled 也不意味着已知。
- GoalAdopted 后、Observation 前退出；旧模型结果晚到：不能派发未观察/不相容的新目标动作。
- 父 Task goalEpoch 更新而子约束未 rebase：Claim/grant/完成门禁返回 Blocked，GoalRebase 需具体兼容证据。
- 换 incarnation 恢复 PendingAction：原 receipt 优先；unknown 不 rebind，确定未提交且旧授权 fence 后才生新代际。
- 重派旧父与孙执行 Spawn 竞态：祖先屏障拒绝新工作，旧子树/unknown 仍有责任人，新 E 资源不重叠写。
- Query 工具读完、保存/披露前退出：原 Anchor/结果可重现或明确 blocked，不换 latest。
- 相同 EntityRef 在不同 Anchor 解析到正确记录；强引用/逻辑边注册表拒绝缺失/伪造引用。
- ExperiencePath 筛掉失败节点后仍能回查原失败/来源，撤回 Source 使后续复用失效。

### 16.6 不变量映射

| 实施工件 | 上位不变量/关键要求 |
| --- | --- |
| 编码/实体/引用注册表 corpus | I-05、I-07、I-34、I-41；无 hash 自引用 |
| P1 与恢复故障矩阵 | I-01、I-02、I-18、I-19 |
| receipt/inflight/PendingAction/唯一槽 | I-03、I-06、I-37、I-38；未知不伪装拒绝 |
| 图、Fork、MergePlan | I-04、I-08、I-09、I-16、I-20 |
| Anchor/KnowledgeKey、查询与输入 | I-10、I-11、I-24、I-25、I-32、I-35、I-36、I-40 |
| Activity/Continuation/取消接管 | I-12、I-13、I-14、I-22、I-23、I-28、I-39 |
| pins、索引、维护与保留 | I-15、I-17、I-21、I-26 |
| 当前授权、出站 grant、GoalEpoch 与 Core/Profile 边界 | I-27、I-33、I-42 |

I-29–I-31 留到后续订阅阶段，不生成空壳测试或拖延第一版核心。

## 17. 实施顺序与阶段门槛

### 17.1 M0：工程契约与参考模型

产出 model/profile/api 最小类型、包依赖、Event/Anchor/实体/强引用 registry、首批 golden、参考模型、DurableIO/故障原型和实验格式标识。先将场景中的 Query→Start 唯一槽、PendingAction、结果/知识分层变成内存契约，不要求最早存储提交已实现完整 Profile。

先冻结首条纵切面涉及的身份、幂等、根、错误与 P1 边界。Actor 合并、复杂记忆评测等问题在对应阶段关闭，不把所有未来研究变成开工前置条件。

退出：能对相同小型操作序列得到明确语义结果；存储帧的字节与拒绝规则可独立验证；没有未分配责任的核心接口。

### 17.2 M1：存储纵切面分小步

| 步骤 | 新增内容 | 退出条件 |
| --- | --- | --- |
| M1a | 事务/frame 编解码、扫描 | 正/反 corpus 与 parser fuzz 通过 |
| M1b | Create、单段、batch=1、P1 | ACK/未知提交/恢复逐点中断通过 |
| M1c | receipts、CAS、RootSet、Reopen（core mutation子集已实现） | 第 16.4 节 core 闭环及幂等反例通过；完整profile另见当前状态 |
| M1d | inflight、取消、group commit | 无双提交、无旧 head 双成功、批依赖正确 |
| M1e | 封存、轮转、跨段事务 | 认证范围与每个跨段中断点正确 |
| M1f | reader pins、最小服务/SDK | 单 owner、快照/关闭、双形态基本契约通过（首条 state View 与 local protocol 已实现） |

M1a/M1b 可先使用 map 参考投影验证 journal，但不能把它作为正式持久状态实现交付。最小 radix 与生产 RootSet 在 M1c 接上；完整 Diff/Fork/Join 在 M2。如此早期就能验证真实状态根，又不会先实现全部图系统。

### 17.3 M2–M6：扩大能力

| 阶段 | 主交付 | 不可跳过的门槛 |
| --- | --- | --- |
| M2 | 持久树、Revision/因果图、Fork/Diff/Join | 纯算法、graphstore replay、core BranchRefs typed command、领域/知识 Cursor、核心联合 Fork/Join/Prune、正式 Event 和 bounded Trace 已有对应实现与回归 |
| M3 | 官方 agent/v1、Query/Task/Step/Invocation、PendingAction/消息/取消/GoalEpoch 恢复 | 场景、唯一槽、rebind、旧 worker/孙执行与父目标变更竞态正确 |
| M4 | KnowledgeKey/Anchor、Profile 导航、CP/实际输入 | 同 Actor 多 E 隔离、控制历史正确、CP 不引用未发布源 |
| M5 | Memory/ExperiencePath、来源/晋升/撤回、索引/维护回收 | 无来源丢失/权限复活/误删，记忆效果独立评测 |
| M6 | 完整库/本地服务/SDK 与外部宿主 adapter | 同语义验收、真实任务报告、格式与支持矩阵冻结 |

M1–M4 随核心能力逐步扩充服务 method；不等最后再写一套服务逻辑。外部宿主先用独立试验或有 coverage 的影子模式，正式切换另定唯一 authority 与 cutover fence。

### 17.4 并行开发边界

先统一 model/profile/api、Event/Anchor/KnowledgeKey、PendingAction/唯一槽、对象读取/引用提取、TreeDelta、PreparedBatch/PostRootSet 与错误语义，再并行工作。

- 编码/格式与故障工件可以并行，但共享同一 registry；
- state/graph 与 journal 可并行，通过固定接口和参考场景汇合；
- service/client 在首条核心闭环稳定后接入，不阻塞最早存储验证；
- P1、恢复、manifest、compaction 安装由同一发布协议责任线维护，不拆出互相独立的发布者；
- 不按“每个概念一个 Agent/包”机械划分任务。

## 18. 决策冻结与变更

### 18.1 本实施稿提出的决策

| 编号 | 决策 | 冻结前证据 |
| --- | --- | --- |
| IA-01 | 顶层 ate 门面 + model/api + 内部分层 | 无循环依赖，服务不能越层写存储 |
| IA-02 | 单协调 goroutine + mutex publication/ViewRef registry | 并发、取消、Close、retire 交错测试 |
| IA-03 | 入队后取消只停止等待 | 等待者独立、请求对账与故障测试 |
| IA-04 | 持久幂等只覆盖已提交 receipt | 拒绝/重试/重开与文档语义一致 |
| IA-05 | active segment 按认证前缀而非全文件校验 | tail/轮转/CURRENT 恢复镜像 |
| IA-06 | Fenced 禁止新的对外读取/派发 grant | 撤权持久但内存未发布的故障反例 |
| IA-07 | 本地 IPC 使用有界帧+版本化 JSON | 跨方式 DTO、整数/bytes、错误与认证测试 |
| IA-08 | Query 的短期读 grant 与 durable Activity 授权分开 | Query 不写领域状态，撤权仍有唯一顺序 |
| IA-09 | 核心独占 View/pin，服务连接绑定，SDK 保持连接亲和性 | 断线/过期/provisional pin/查询完成竞态 |
| IA-10 | 静态 agent/v1＋Core＋宿主三层 | 版本/命名空间/纯转换边界在库/服务一致 |
| IA-11 | SnapshotAnchor 与 KnowledgeKey | 控制-only 历史、同 Actor 多 E、CP 发布顺序反例 |
| IA-12 | 完整 PendingAction＋业务唯一槽 | 同键/换键/换授权/重派竞态无重复工作 |
| IA-13 | schema 拥有强引用/实体关系/投影 | 导入/GC/历史实体解析不猜 JSON 字段 |

### 18.2 必须在对应阶段确定的事项

| 门槛 | 决策/工件 |
| --- | --- |
| 建仓前 | 独立仓库位置、module path、许可证与文档原件位置 |
| M0/M1a | model/profile/api、Event/Anchor、PendingAction/动作槽/引用契约与首批 golden/限额 |
| M1b | genesis、manifest/CURRENT 逐字节规格、DurableIO 平台能力和失败分类 |
| M1d/M1f | batch/取消/关闭状态机、锁顺序、句柄生命周期、IPC 版本与身份机制 |
| M2–M4 | MergePlan/ExecutionKnowledge/查询模式/出站证据的可执行反例 |
| M5 | checkpoint/保留/维护回收与 privacy 缺口协议 |
| M6 发布前 | V1 frozen 格式、迁移规则、支持矩阵、故障与真实任务报告 |

没有一项可用“与其他 Agent 类似”代替证据，也不能因文档审查未发现矛盾就宣称实现通过。

### 18.3 文档维护

语义稿保留 Core/Profile 规范，本文保留代码组织与阶段门槛，场景稿提供同一协议的对象流水与故障反例；formats 工件单独拥有字节规格。变更需同步引用，不能继续保留互不兼容的候选对象模型。

开发中影响持久字节、原子性、权限、外部重试或恢复语义的变更，先登记决策和反例，再更新规范与实现。纯私有函数重命名无需升级全套文档。

这份稿件的下一步验收是架构审计与 M0 工件，不是直接铺开全部第一版代码。广播、订阅和主动反馈仍明确属于后续阶段。

## 2026-11-02 V1 implementation closure

The original architecture's single-publisher rule now also covers `agent.work.apply/v1`,
`knowledge.summary.apply/v1` and `agent.emission.apply/v1`. Work reducers run against a
private candidate and publish one receipt; Summary and Emission use their own typed
canonical records while remaining under the Knowledge/LiveControl root ownership rules.
The executable closure and external evidence limits are listed in
`validation/ATE_V1_IMPLEMENTATION_ADDENDUM_2026-11-02.md`.

### 交叉审计后的版本和提交边界（2026-11-02，历史）

反射 Control codec 的字段顺序是持久格式。Invocation 新字段和 Continuation.Position 只由 schema 4
编码；schema 1–3 继续按原字段数解析，禁止携带新元数据后选择旧版本而静默丢字段。
Invocation 独立 record 使用 schema 2，Continuation 生命周期用新增 opcode 8；旧版本保留原字节。

模型决议与工具执行分两次合法推进：第一次 Work 原子保存 accepted primary result、完整 Invocation
contract 和 Continuation，pending 工具不会强制完成 Step；后续结果已接受、必需条件满足后才 FinishStep。
工具定义属于强内容引用，并在新定义绑定及 managed Claim/Dispatch 时接受来源和当前复用策略检查。

新 Work/Summary/Emission 的 embedded 与 Unix SDK conformance 使用真实 Store、真实 Unix socket，
包含历史读取、错误分类、幂等、隔离和重开后重试；客户端还拒绝与请求历史 Anchor 不一致的
Emission 响应。证据为 `service/work_summary_emission_test.go` 和 `client/emission_test.go`。
这些补强不等同于 Profile Event registry、完整 reference model 或 frozen 格式门槛已经完成。

Emission 的 range 投影复用 `ReadContent` 的 `objectContentProjection`，不单独实现另一套字节
解释。schema 2 Proof 保存总长度、全量及范围摘要，Issue 做不截断的边界校验；schema 1 replay
不补生成这些字段。对当前不支持的 Cursor/Knowledge 源选择条件采用明确拒绝。

### 2026-11-03 主路径实现记录（历史）

Query-only 不扩展反射 Control union，而是作为 Work schema 1 的闭合 `QueryTurn` 结构持久化；
它的 `QueryProgress` 记录与 ordinary Agent snapshot 分区相同，进入 LiveControl/ExecutionKnowledge
根的 P1 事务。原请求 reservation、QueryProgress version 和 adapter version 一起形成恢复门禁。

Profile Event registry version 1 通过 core event schema 2 的有序清单选择回放规则；旧 core event
schema 1 不补生成新 Profile Event。A5 当前已覆盖 Agent/Work/Query 多 stream，非 Agent domain
method 仍保持 core-only，避免将局部事实归因冒充全局事件完成。

Transfer、formats registry 和 tests/reference 都是公开包或独立测试边界。Transfer 的 foreign
archive 不写 LiveControl、DomainStates 或本地 authority；formats registry 已冻结为 v1.0.0，
迁移 planner 对跨版本请求 fail-closed，必须使用新导出/导入或新版本。

## 20. Strict V1 实施审计状态（2026-11-04）

当前候选格式为 Control 5、Invocation 2、Continuation opcode 8、Work 1、Emission 3 和 Packet 4。
同一 reducer 校验已发布的当前 V1 命令、StateOps、receipt、event 和 snapshot。被替换的预发布
Control、Invocation、Continuation、Emission、Packet 字节在对应解码边界拒绝。

`engine` 的共享 planner 在公开 Agent 提交处拒绝受保护的 Work 子转换。
`prepareWork` 先验证整个组，再在私有候选运行子 reducer。Root/child 的 seed、authority 和 producer-bound
Continuation 一起发布，`child_spawn` 使用已登记 Invocation。`message_consume` 同时发布接收方
Observation 和绑定同一生产事务、确切最终 Knowledge Revision 的 Continuation。`model_decision` 是执行级
主模型的接受边界，也支持对账过的旧 incarnation 结果导入。所有模型和工具的派发经 `emission_dispatch`，
对同一 Attempt 的受绑定 Claim 和 Dispatch 在同一 receipt 下发布，并重新核验 policy、authority、LSN 和
精确字节。Activity 已绑定 ContextPacket 时，grant 同样须绑定该 Packet 的身份。

`agentprofile` 的 `safeRetry` 由 Retry、Claim 和 Dispatch 共用。Recovery 保存 immutable contract、
版本化 finite guarantee、单一 sequence 的 RetryAuthorization、ActivityControl.RetryObservedAt 和 Attempt
的 ClaimUse/DispatchUse。Retry、Claim、Dispatch 的已记检查时刻必须按顺序单调不减，replay 使用这些事实。
协调器的当前内容复用检查和 Emission 校验继续独立运行。网络发送与接收方终结证据由受信适配器负责。

`agentprofile.ContextRequirements` 从 Packet Source 的历史控制快照派生结构化要求。`engine` 校验执行级
`primary_decision` 的 Packet 4，逐项核对该 KnowledgeKey 历史选定前缀中的已采用 raw ValueBytes。纯 selector
验证宿主给定的候选和成本，Packet 同时含 Selection 时结构化要求必须在 Mandatory。HostPolicyVersion
记录宿主 renderer 和候选策略，RecordedInput 记录完整实际请求。 [Agent 格式](formats/agent-core-experimental-1.md) · [Domain 格式](formats/domain-core-experimental-1.md)

§16.1 的独立 oracle、实体事件归因、C02/C05 初始 seed 以及 M6 真实任务、效果和开销继续按原阶段
门槛验收。2026-11-03 独立审计保留其当时结论，当前实现须用最终源树证据核验。

当前严格 V1 及文档交叉验收未全部完成；最终工程重构仍在其后。

### V1 sourced Annotation / Hide（2026-11-03）

公开 `annotation` 包的接受/撤回复用 sole coordinator 与 typed replay，独立 owner partition
写入 ExecutionKnowledge。判断不写 LiveControl 或 BranchRefs，不重写原工具失败、事件或上下文。
普通固定 View 的 List/Search 默认隐藏 active Hide，IncludeHidden 可审计完整结果；knowledge
cut 只按显式 adoption 决定成员。当前披露门还校验历史注解 provenance，避免来源离开 live key
后通过注解或其事件文本绕过撤权。格式及实际边界见 `formats/annotation-v1.md`。

## 2026-11-05 当前实现收口

11-02 和 11-03 段落中的“历史”状态只描述当时审计快照。当前源树已将 Work 原子门禁、有限重试、ToolDefinition、Emission dispatch、ReadDeliveryGrant 和 Packet 派生来源检查接入共享协调器。格式清单已加入 ToolDefinition schema 1，所有模型与工具派发经过 `emission_dispatch`。

当前逐项交叉证据、普通/race/vet/build/Linux 交叉编译命令、真实模型工件和仍然需要外部平台证明的边界记录在 [V1 当前交叉验收续记](validation/V1_CURRENT_CROSS_CHECK_2026-11-05.md)。
