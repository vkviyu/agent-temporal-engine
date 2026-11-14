# ATE API 与协议设计

> 完整 V1 目标保持不变。当前格式：Control 6、Invocation 2、Continuation opcode 8、Work 1、Emission 3、Packet 5、Summary 2。严格验收仍开放；历史日期的已完成结论不能覆盖后续发现的反例。当前实现、验证和真实剩余项以 [2026-11-06 交叉审计](validation/V1_CROSS_AUDIT_2026-11-06.md) 为准。
>
> 2026-11-01 实现核对与未收口项见 [文档—代码审计](validation/V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-01.md)。下文的完整目标保留；有界查询、领域 Cursor 和 Key 筛选的实现不等于 Event、执行知识 Revision 或回收目标已经完成。

> 版本：0.1.0，API 协议审计草案。日期：2026-10-30。  
> 关联：[语义设计](AGENT_TEMPORAL_ENGINE_DESIGN.md)、[实施架构](ATE_V1_IMPLEMENTATION_ARCHITECTURE.md)、[执行 Profile 场景](ATE_EXECUTION_PROFILE_WALKTHROUGH.md)。  
> 本文是 API 语义、Agent-facing 投影和本地 IPC 的集中规范；字段编号、canonical bytes、frame magic 和平台支持仍由 `docs/formats` 与实施阶段冻结。  
> ATE 是独立基础设施。GSEA、Eino、模型供应商和具体 UI 都是外部适配对象，不是本协议的依赖或公共类型来源。

## 1. 协议目标和边界

ATE 同时提供：

1. **嵌入式 Go API**：Go Agent 宿主直接调用同一内核；
2. **独立服务 API**：`ate serve` 拥有 store，客户端通过本地 IPC 调用；
3. **Agent-facing 能力**：模型或 Agent Loop 只看到受保护的渐进查询和版本化 Profile 命令。

三种接入共享相同的对象身份、SnapshotAnchor、权限、错误、幂等和恢复语义。传输、序列化和句柄形态可以不同。

第一版包括命令、查询、上下文展开和本地请求/响应 IPC。广播、订阅、server push、多路复用和远程服务平台属于后续版本，不进入本协议的第一版实现条件。

### 1.1 三层责任

~~~text
Core API
  通用事务、View、Anchor、NodeRef、持久发布和恢复

agent/v1 Profile API
  Query、Task、Execution、LoopStep、Invocation、消息、目标、取消、恢复

Host Adapter
  Agent Loop、模型、工具、Workspace、业务验收、用户界面
~~~

ATE 不提供 GSEA 专用 API。GSEA 可以实现一个外部 Host Adapter，并用本协议做集成验收；ATE 仓库和 module 不导入 GSEA。

### 1.2 绝不直接暴露的能力

Agent-facing 客户端禁止直接调用：

- 任意 `StateOps`；
- `WriteObject`、任意 RootSet 修改；
- `CURRENT`、manifest、segment 或物理 offset；
- 任意 CommitDigest 拼接；
- 预算、epoch、authority、ACL 的普通字段写入；
- 未经 Profile 校验的 Task/Execution 状态更新。

模型提出意图，Host/Profile Controller 负责校验和提交。审计管理员可以有单独的 Verify/Inspect 能力，但它不等于 Agent 写权限。

## 2. 统一身份和引用

### 2.1 逻辑身份

| 类型 | 作用 |
| --- | --- |
| `EntityRef` | `ProfileID + EntityKind + CanonicalLocalID`，例如 agent/Task/T0 |
| `NodeRef` | 带 kind 的 Entity/Event/Revision/Object 引用 |
| `ObjectRef` | 不可变内容对象，直接按 ObjectID 读取 |
| `EventRef` | 不可变发生事实，按 EventID 读取 |
| `SnapshotAnchor` | 持久历史读取锚点：Fence + 可选 Cursor/KnowledgeKey |
| `ViewRef` | 临时查询能力和 generation pin，不是历史锚点 |
| `RequestKey` | Space + 稳定 Principal + RequestID，命令幂等身份 |
| `CorrelationID` | 传输请求关联号，不参与业务幂等或内容摘要 |
| `ContinuationToken` | 固定 View/过滤/排序/投影后的分页或展开位置 |

已有 `SnapshotAnchor` 时，API 另带的 Cursor/KnowledgeKey 必须与 Anchor 一致，否则返回 `AnchorMismatch`。`latest` 只能由服务端一次捕获并返回完整 Anchor，不允许调用者逐字段拼接当前状态。

### 2.2 统一请求上下文

所有读取都拥有同一组语义字段：

~~~go
type ReadContext struct {
    ViewRef      ViewRef
    Anchor       SnapshotAnchor
    KnowledgeKey *KnowledgeKey
    Mode         ViewMode
    Principal    PrincipalRef // 由可信边界绑定；wire body 不自报权限
}
~~~

服务端可以只在 wire 中传 `ViewRef`；核心在 registry 中解析它对应的 Anchor、主体和 pin。客户端不能自行构造一个具有权限的 ViewRef。

## 3. 第一版请求/响应模型

### 3.1 命令 envelope

~~~go
type CommandEnvelope struct {
    ProtocolVersion uint32
    Method          MethodName
    CorrelationID   CorrelationID
    RequestKey      *RequestKey // 有业务效果/幂等要求的命令必填
    Body            TypedBody
}
~~~

`RequestKey` 的摘要覆盖逻辑 Body、Space、稳定 Principal 和前置版本；不覆盖连接、CorrelationID、等待 deadline、重试时间、服务端 TxID/LSN。命令响应丢失时，客户端用同一 RequestKey 对账。

命令结果至少区分：`DurableAck`、`Rejected`、`Conflict`、`WaitInterrupted`、`CommitUnknown`、`StoreFenced`、`CorruptionDetected`、`ResourceLimit`。错误字符串不是协议分支条件。

### 3.2 查询 envelope

~~~go
type ExploreRequest struct {
    Read         ReadContext
    Operation    ExploreOperation // list/inspect/expand/trace/search/read_content
    Layer        ExploreLayer     // entities/events/content
    Subject      *NodeRef
    Filter       *TypedFilter
    Projection   ProjectionID
    ContextMode  ContextMode
    Limits       QueryLimits
    Continuation *ContinuationToken
}
~~~

查询响应统一包含：

~~~go
type ExploreResponse struct {
    Anchor       SnapshotAnchor
    Items        []TypedSummaryOrDetail
    Relations    []TypedRelation
    Continuation *ContinuationToken
    Truncated    bool
    Unavailable  []UnavailableRange
    Coverage     CoverageFence
}
~~~

查询是只读的。查询发现的结果要交给模型，必须经过显式 Observation/KnowledgeImport；查询本身不扩展 KnowledgeKey，也不修改冻结历史 View。

### 3.3 Profile 命令

`agent/v1` 通过封闭的 typed command union 暴露：

- `StartWork` / `GoalAdopted` / `GoalRebase`；
- `Spawn` / `Claim` / `Cancel`；
- `AcceptResult` / `Reconcile` / `Settle`；
- `MessageConsume` / `Observation` / `KnowledgeImport`；
- `MergeProposal` / `CommitMerge` / `PublishArtifact`；
- `Resume` / `RebindPendingAction`。

每个命令由 Profile 注册表声明：schema/transition 版本、命名空间、前置 Root/版本、强内容引用、逻辑关系、唯一动作槽和错误条件。没有 `agent/v1` capability 的 Core-only store 不接受这些命令。

Profile 命令不能携带“模型已验证”“权限已扩大”“外部效果一定未发生”等自声明字段来绕过 Host 校验。

## 4. Agent-facing 渐进查询

### 4.1 能力集合

模型默认只看到少量、职责清晰的工具：

| 工具 | 默认层级 | 作用 |
| --- | --- | --- |
| `list` | Entity/Execution/Step/Activity 摘要 | 发现对象和可展开引用 |
| `inspect` | 一个 NodeRef | 查看类型化详情和来源 |
| `expand` | Context 分区 | 展开 inputs、upstream、downstream、stateDiff、messages、receipts、artifacts |
| `trace` | 有界关系路径 | 查看因果、来源或后果 |
| `read_content` | Object/Blob 范围 | 分块读取输出、patch、ContextPacket 或文件内容 |
| `search` | 固定 View | 结构过滤和明确文本条件，返回候选 |

Agent 不默认从 Event、Commit、Blob 或 Frame 开始。审计模式可以展开这些层，但仍受 Anchor、权限、limits 和当前 grant 约束。

### 4.2 渐进路径

~~~text
Task/Execution 摘要
  → LoopStep 摘要
  → ToolInvocation/Activity 摘要
  → Attempt/Result/Artifact 详情
  → 原始 Blob/ContextPacket 分块
  → Event/Commit 审计细节
~~~

每次操作都返回稳定 NodeRef、关系类型、来源 Anchor、继续位置和截断状态。模型可以选择继续展开，但不能把任意 ObjectID、深度或 projection 当成授权提升。

### 4.3 统一入口和 SDK 方法

wire 协议可以把上面的操作编码为一个 `ExploreRequest` typed union。Go SDK 同时提供语义更清晰的：

~~~go
ListNodes
GetNode
GetNodeContext
Trace
SearchNodes
ReadContent
~~~

这些方法必须等价地执行相同的 View/Anchor/权限/limit 规则。服务端响应还必须消费绑定 method、请求摘要、认证 session、View/Anchor 和来源闭包的一次性 ReadDeliveryGrant。内嵌库会在返回前立即消费同一许可。禁止实现一个同时拥有任意查询和任意写入字段的万能端点。

### 4.4 任务轨迹

`agent/task-trace-v1` 提供 `get_task_trace`、`get_task_trace_evidence` 和 `apply_task_trace`。轨迹按 Space、认证主体和 Query 或 Task 定位。Query 请求可附 Session 核对归属。首次查询返回 Anchor，续页固定相同 Anchor 和筛选条件。范围外的直接因果前驱可通过标记为 `Boundary` 的边展开。服务以连接认证主体为准，并复用普通读取的出站授权。写入以 RequestKey 幂等提交，以 `ExpectedVersion` 检查现有轨迹说明版本。

字段和调用方式见 [API Reference](ATE_API_REFERENCE.md#任务关键轨迹)，本地协议请求定义见 [task_trace.go](../protocol/task_trace.go)。

## 5. 本地 IPC 协议

### 5.1 传输

第一版本地服务使用 Unix domain socket（其他平台单独声明支持）和有界长度帧。帧内是版本化 UTF-8 JSON envelope；JSON 只用于传输，不参与 ObjectID、EventID 或 RequestDigest。

单连接顺序请求/响应；不实现 server push、订阅或多路复用。并行客户端可以使用有界连接池。

长度前缀、最大帧、未知字段/枚举、整数、bytes、ID 和错误码进入 `protocol-v1` registry。服务端在分配内存前检查长度。

### 5.2 身份、权限和句柄

- Unix socket 的 OS 身份或本地凭据映射到 Principal；客户端不能在 Body 中自报 admin/Actor/Space；
- 服务端只保存 `session → ViewRef` 归属，核心 registry 独占 generation pin；
- ViewRef 与连接绑定，断线后不能偷偷用另一连接续页；可按 Cursor/Anchor 重新打开 View；
- service 不导入 storage、不实现第二套 receipt/恢复/权限状态机；
- 同一 store 只能有一个 owner，库模式和 server 模式不能同时直接打开目录。

### 5.3 断线、ACK 和未知结果

连接断开不撤销已接收命令。响应丢失时返回未知并用原 RequestKey 对账。`CorrelationID` 可以变化，`RequestKey` 不能为同一逻辑重试变化。

ReadView/Explore 的响应只证明服务端返回了数据；不证明模型已阅读。Agent 处理结果、Observation、下一动作和必要的命令 receipt 按普通 Commit 规则记录。

## 6. 版本、能力和错误兼容

协议版本、Core schema、agent Profile、projection、query index 和 renderer/tokenizer 分别版本化。未知 mandatory method/transition/字段明确拒绝，不当作空成功。

服务启动时声明 capabilities，例如：

~~~text
core/storage-v1
agent/v1
protocol/local-v1
query/explore-v1
~~~

客户端不得根据字符串猜能力。Profile 不兼容只能 inspect/只读或明确失败，不能让新客户端把旧状态当成同一语义。

## 7. 与 GSEA 的关系

GSEA 可以实现：

- Agent Loop Adapter；
- 模型与工具 Executor；
- Workspace/Artifact Adapter；
- UI 和业务验收策略；
- 真实长程任务的外部验收。

ATE 不导入 GSEA 包、不读取 GSEA 内部 schema、不依赖 GSEA 的 SQLite 或 checkpoint。ATE 仓库中不得把 GSEA adapter 作为核心生产包；适配器应在 GSEA 仓库或独立外部仓库，通过本协议接入。

## 8. 冻结和验收

协议进入 `protocol-v1` 前必须有：

- envelope/错误/能力 golden corpus；
- Go 库调用与 SDK/IPC 调用的 conformance 场景；
- Anchor/KnowledgeKey/分页/权限撤回测试；
- Query/StartWork/Spawn/AcceptResult/MessageConsume 的幂等与 ACK 丢失测试；
- 超大帧、未知字段、未知 capability、断线和 ViewRef 回收测试；
- 不同 Profile/renderer 版本的拒绝和只读降级测试。

本文是协议集中草案，不表示 `protocol-v1` 已冻结。


## Core state 实现映射（始于 M1c）

Agent-facing Node/View/Context 的第一版有界查询已加入 `api/contracts.go`、`engine/node_queries.go` 和根门面；完整远程 Explore union 仍按查询能力分层，不提供任意万能 endpoint。可信宿主 core state 类型仍在 `api/state.go`、`api/contracts.go`，根门面在 `ate.go`：

| 接口 | 当前颗粒度与保证 |
| --- | --- |
| Create / OpenExisting | existing private directory，单 owner；Open 成功代表物理与已注册 core 语义均校验 |
| Mutate / Commit | Mutate 为一个 Space 的原子 binary-key batch；Commit 为已注册 method 的 canonical envelope；受保护 domain method 不接受原始 StateBatch |
| LookupRequest | 完整 RequestKey；原 Ack 或绑定已验证全局 Fence 的 absence，未知/失败不返回假不存在 |
| GetState | 当前一个 binary key；明确 Found/null/bytes，返回该 space StateRoot、全局 RootSet 与 Fence |
| Snapshot | 当前五根与 Fence；不代表历史 ReadView 或权限过滤后的节点导航 |
| Close | 资源退休可重试；cleanup 不能丢 owner，不能因 Close 自动提升未确认事务 |

根包是嵌入库，`init-state/recover-state` 是同一语义引擎的本地二进制入口；Unix local-v1 的 service/client 已实现。Principal 在 core 嵌入模式由可信宿主提供，local service 通过受控 token 映射到 Principal；不能把通用宿主 mutation 能力直接发给 Agent。历史 View 提供有界 StateEntries，以及下述已实现的 Node/Profile 查询子集。

Core payload 的确切字节见 [core-state registry](formats/core-state-experimental-1.md)，原 M1c 的阶段证据见 [M1c 状态](M1C_STATUS.md)，当前跨阶段已实现/未实现边界见 [文档—代码审计](validation/V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-01.md)。


## M1d 的等待、在途与关闭补充

Commit 调用在入队前取消/容量拒绝为本次 CodeNotSubmitted；入队后取消为 CodeWaitInterrupted，保留ErrWaitInterrupted和原ctx原因，但命令继续。它们都不能替代原 RequestKey 的 Lookup，也不能据此生成新请求重复副作用。已接受 key 的查询可返回 CodeInFlight；该状态只在本进程有效，不能作为 durable receipt。

原 durable receipt 在 admission/写入配额之前校验并返回；同 key 不同 digest 不共享。每个 batch 一次 CURRENT 发布，但每笔 Ack/Fence/RootSet 保持自己的原始 proof；Lookup.ObservedFence 和 Snapshot 是批末完整 tip。

CloseContext 先停止新 admission/读取，排空已接受工作；ctx只停止该次等待，不提前释放owner。Root门面支持AdmissionOptions/BatchOptions，纯Get/Lookup/Snapshot等待core锁也响应ctx。完整边界见 [M1d 状态](M1D_STATUS.md)。


## M1e 的地址与恢复诊断补充

物理分段不改变 CommitFence、RequestKey、receipt 或业务 API。库中的 PublishedEnd/RecordLocation 使用稳定逻辑日志地址，不能直接用于某个段文件的物理 IO。公开底层 RecoveryReport 增加 OrphanSegments/QuarantineFiles；TailBytes 汇总 active tail 与孤立段，零字节孤立段仍需显式恢复。默认不修改未发布布局；完整物理与语义认证完成后才允许显式隔离。边界及实验格式见 [M1e 状态](M1E_STATUS.md)。


## M1f 实现映射

View/pin、Unix local-v1、protocol/client 的当前保证与限制见 [M1f 状态](M1F_STATUS.md) 和 [local protocol format](formats/local-protocol-experimental-1.md)。当前已实现 GetViewState、GetViewEntries、ListNodes/GetNode/GetNodeContext、Graph/Memory/Knowledge 专用 local wire facade 和 agent/v1 核心查询；传输仍是有界请求/响应，不引入订阅后台。

## M6 Agent core 实现映射

`ApplyAgent` 的静态封闭命令和 `GetAgentState` 的分区/历史查询已接入同一个 core publisher。库以 `Options.AgentProfile=true` 启用，local service 以连接身份覆盖 Principal，启用时才宣告 `agent/v1`；原 RequestKey 通过统一 receipt 对账。CLI `serve/recover-state` 使用 `--agent-profile`。当前命令记录覆盖 Task/Execution/Step/Invocation、settlement、Goal/取消、PendingAction，以及 Query/Activity/Attempt/Result/Message/Consumption/Continuation；完整 parent/child 外部副作用恢复已在嵌入库和 Unix socket SDK 两条路径完成联合验收。

Graph、Memory、Knowledge facade 只冻结原始 canonical 领域命令并送入 core；协调器先查 receipt，再在私有候选中执行 reducer。Graph 更新 BranchRefs，Memory/Knowledge 更新 ExecutionKnowledge，Agent 更新 LiveControl，普通宿主 mutation 更新 DomainStates；共同更新 CommandReceipts，并由同一个 P1 原子发布五根。重放调用相同转换并比较 StateOps 和 PostRootSet，不能用客户端替换 snapshot 绕过领域校验。

| 当前查询 | 已实现的选择与边界 |
| --- | --- |
| GetKnowledgeState | 必须绑定 Principal，按 Space/Principal 分区；可按 Anchor.Fence 和 KnowledgeKey 读取，认证该 Actor/Execution 的历史与当前所属，返回所选 Anchor/Frontier |
| OpenView execution_knowledge | Key 必须明确；查询只展开已采用内容，不能降级到全部领域状态或整个主体的 Agent ledger |
| OpenView + 领域 Cursor | 从指定 Fence 的历史 BranchRefs 中认证 Revision，读取 Revision.StateRoot；控制面仍由 Fence 选择，拒绝未来/缺失 Revision |
| ListNodes / GetNode | entities 为 Agent 及生命周期实体；content 为认证领域对象、Revision，或 Knowledge View 的采用内容/Observation/Packet；typed memory/experience_path 以逻辑 Entity JSON 投影返回；先查 View 成员关系，后读 bytes |
| GetNodeContext | 有界父子及来源展开，沿同一 View；普通 Cursor 仅展开所选 Revision 的祖先，audit_snapshot 可显式浏览同 Fence 其他历史分支 |
| Node 分页 | 绑定 View/filter/sort，session 拥有 ViewRef；资源上限不是授权提升，非法/未来引用不因不存在而误 fencing |

KnowledgeKey+Cursor 的执行知识 Revision 绑定、正式 Event occurrence 和 Event 导航、bounded Trace/Search/Content、核心联合 Fork/Join/Prune、SourcePublicationRef、RecordedInput 及 AuditFull compaction 已按本轮代码接入。AuditFull 保留完整权威日志，选择性历史 GC、广播订阅、容量优化和设备断电认证仍是明确边界。确切字节与容量边界见 [core registry](formats/agent-core-experimental-1.md)、[local protocol](formats/local-protocol-experimental-1.md) 和 [审计清单](validation/V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-01.md)。

## V1 复合工作、摘要和出站许可 API

`ApplyWork` 接收一个有界 `WorkUnit`，用一个 RequestKey 和一个 receipt 将已注册的 Agent、Knowledge 或 Emission reducer 作为一个私有候选提交。`query_intake` 在 Task 尚不存在时保存 Query、路由、配置、确切的下一请求和请求保留。`query_start`、`model_decision`、`message_consume`、`child_spawn` 和 `checkpoint` 各自校验允许的对象关系。`emission_dispatch` 只接受两步，依次为 Activity-bound Emission Claim 和同一 Attempt 的 `DispatchAttempt`。`GetQueryIntake` 可在最新或指定已发布 Fence 读取，恢复以该记录保存的确切请求为准。

`ApplySummary` 保存带 Source Key+Cursor+Frontier、source-set digest、topics/omissions、生成 Activity/Attempt/Result、精确输入/输出和 Validation 的不可变 Summary。`GetSummary` 按 Space/Principal/ID 和可选 Fence 读取；Summary 的逻辑 ID 与其底层 ValueBytes 地址分开。`PayloadKind=summary` 的 Observation 只引用已发布且未拒绝的 Summary，NodeContext 只展开该 Summary 明确声明的来源。

`ApplyEmission` 的 Issue/Claim/Revoke 由同一 coordinator 串行化。Issue 固定内容对象、历史 Anchor、字节 offset/length、目的地、Task/Execution/GoalEpoch 和 AuthorityVersion；Claim 重新校验当前执行权威、目标、来源撤回策略和相同范围；Revoke 增加版本并阻止后续 Claim。`GetEmission` 返回历史许可记录。该协议不声称能追回受信外部适配器已经发送的字节；每一个后续 chunk 必须另发一个 Issue。

### 交叉审计后的 typed contract 和 SDK 验证（2026-11-02，历史）

`Invocation.AcceptedResult` 是 ActivityResult 的稳定字符串 ID，不能当作原始 ObjectID 读取。
`ToolDefinition` 是历史输入 cut 已发布的工具定义 ObjectID；`ToolVersion` 和 `DispatchClass`
与 Work.Decision 中的对应绑定必须相同。含新字段的 RecordInvocation 自动选择 Control schema 4；
独立 Invocation record 使用 schema 2。旧日志保持原始 schema 和编码。

`Continuation.Position` 包含 owner、当前 Step、确切知识 Fence/Key/Cursor、adapter 版本和有序
PendingAction generation 集合，使用 Control schema 4。宿主按这些持久数据恢复，不以 latest
或模型重新推测替代记录的位置。

新 Emission schema 2 的 `EmissionRecord.Proof` 返回 Space/Principal、PolicyFence、PolicyRoot、
排序去重的 Dependencies 和 DependencyDigest；旧 schema 1 记录可没有 Proof。
SDK 对返回值的 Anchor/Fence/RootSet、记录身份及 Proof 的主体/来源/摘要进行一致性验证；
成功帧违反协议会关闭连接。真实 Unix socket 与嵌入库对照测试覆盖三组新增 API。
Proof 是历史签发证据，不能作为绕过当前撤权的授权凭证。

Emission schema 2 的 `Spec.Projection` 与 `ReadContent` 共用投影：空值表示 `payload`
（对象的 canonical Payload），`value_bytes` 表示 core ValueBytes.Data。Issue 严格验证
Offset/Length 完整落在所选投影内，不允许越界后截断；Proof 同时保存解析后的 Projection、
TotalBytes、整个投影的 ContentDigest 和确切范围的 RangeDigest（SHA-256）。执行器必须发送
该范围的该投影字节。当前 schema 2 源 Anchor 只接受 Fence-only，带 Cursor/Knowledge
selector 的源明确拒绝，不能静默忽略选择条件。schema 1 的历史字节和 replay 保持原语义。

### Query-only 与 Transfer API（2026-11-03）

`WorkUnit.Kind` 的 `query_open`、`query_decision`、`query_tool` 和 `query_answer` 共用一个
`QueryTurn`，其 `QueryProgress` 可由 `GetQueryProgress` 按最新或历史 Anchor 查询。受管理 Query 在单独创建 Activity 时必须使用进度记录保留的原 RequestKey、请求体和 fence。SDK 必须验证
Query/DecisionSequence 连续性、Activity/Result/Invocation 唯一性、exact Next 方法和主体，并在
非法成功响应时关闭连接。Query turn 不返回 Task/Execution 身份。

`ExportTransfer` 返回带 source publication、policy cut、closure entries、dependencies 和 gaps
的 canonical Manifest；`ImportTransfer` 使用 caller-approved Manifest digest，在目标 Space 写入
只读 ArchiveEntry 和 mapping。`GetTransfer` 返回历史 ImportRecord；service/client 对 Space、
Principal、Anchor、Digest、Mapping 和 source/target 不重合进行一致性校验。任何本地 promoted
Memory 或 Observation 采用都必须另发目标域命令。

`EmissionSpec` schema 3 增加 Query/Activity/Attempt/RequestDigest、ContentKind 和 NotAfterLSN。
受绑定 grant 只能通过 `emission_dispatch` Work 进行 exact claim/dispatch。Schema 3 也允许签发
不绑定 Activity 的范围许可并直接 Claim。外部网络送达使用独立的宿主证据。

读取许可的目的地由 `Principal + session + correlation/request` 组成。节点、图、状态、知识、记忆、Agent、Query、Summary、Emission、Annotation 和 Transfer 的 Unix 响应都经过同一个 service capture/consume 边界；capture 期间只传递引擎拥有的 DTO，响应体在锁外序列化并按出版 fence 再确认。重复消费、换请求、换连接、修改后的 View 或策略切面都会拒绝。

## 25. Strict V1 协议审计状态（2026-11-04）

库与 Unix service/SDK 共用当前 V1 候选协议，Control 5、Invocation 2、Continuation opcode 8、
Work 1、typed validation、Emission 3 和 Packet 4。被替换的预发布格式在准入和重放均拒绝。
历史 API 仍选择当前 V1 的已发布 Anchor/Root，重开和压缩不得改变其事实。

受保护的 `StartWork`、`SpawnChild`、`ConsumeMessage`、Control `RecordInvocation` 与执行级主模型
结果接受必须通过相应 Work 提交。先前 incarnation 的主模型结果经 `occurred` 对账后，也在
`model_decision` 内通过 `ImportActivityResult` 接受。`child_spawn` 消费已登记的 Invocation，
完整主模型调用集合由 `model_decision` 声明。约束位于协调器，typed `ApplyAgent`、通用 `Commit` 和 Unix SDK
共用这一入口。Root/child 初始 Continuation 固定生产事务与 Knowledge Cursor。消息消费产生的
Observation 与接收方 Continuation 同组发布，Continuation 固定该事务与确切的最终 Knowledge Revision。
全零知识位置仅供尚无知识的执行。所有模型和工具的 Attempt 派发都经 `emission_dispatch`，包括纯模型调用。
Activity 已绑定 ContextPacket 时，grant 必须选择同一 Packet 及其完整 RecordedInput，哪怕另一个
Packet 或原始对象的 bytes 相同也不能替代。

`ReadContent` 以 View、投影、字节范围、内容摘要和当前权限切面签发进程内单次许可。Unix 的目的地
由认证 Principal、服务端连接 ID 与请求 ID 组成；只允许将许可中的固定内容送入该请求响应。撤回在
签发之前时拒绝，签发之后可释放已授权分块，后续分块重新核验。内嵌调用在返回前立即消费许可，
原有 SDK 请求及响应字段保持相同。许可不写 journal，也不对外提供可转交的令牌。

Control 5 的 `PutIdempotencyGuarantee` 和 `AuthorizeRetry` 记录有限接收方证明和单次 sequence 授权。
`RetryActivity`、`ClaimAttempt` 和 `DispatchAttempt` 接收 `RetryUse`。客户端按原 RequestKey 对账
已发布请求。新调用必须满足当前 authority、control version、exact request/key、Attempt 集合和有效窗口。
受信宿主负责测量时序并执行接收方的 deadline 或 epoch fence。 [Control 5 字节规格](formats/agent-core-experimental-1.md)

创建执行级 `primary_decision` 时必须提供已发布的 Packet 4。`RecordedInput` 固定完整实际请求，
`Requirements` 固定该 Packet 历史来源切面的结构化事实与已采用 Observation。引擎重新推导
要求集合并认证内容。若 Packet 含 Selection，要求项也必须属于其 Mandatory。宿主以
`HostPolicyVersion` 声明渲染和候选策略。 [Packet 4 字节规格](formats/domain-core-experimental-1.md)

远程 send/reconcile 由宿主完成，receipt 不是送达证明；订阅不在 V1，Transfer 需目标域显式采用。
这些边界不缩减实体事件 API、恢复、独立验证和 M6 的原要求。该段属于 11-04 历史状态，当前协议收口见本文末的 11-05 段落。

### V1 Annotation / Hide（2026-11-03）

`ApplyAnnotation` / `GetAnnotations` 在 embedded facade 与 Unix SDK/service 共用
`annotation/v1`。Record 必须有原因、确切已发布 Source Anchor 和真实可见 Target/Evidence；
标签撤回追加新记录。owner 分区中的判断只影响同一固定普通 View 的 ListNodes/SearchNodes，
`IncludeHidden` 是显式审计模式并绑定分页；GetNode/Trace/context 保留事实和必需约束。
Execution Knowledge 不隐式采用导航判断，旧 Knowledge Cursor 不受全局较新标签改变。
当前 ACL/Privacy 仍约束历史注解及其事件文本。完整格式见 `formats/annotation-v1.md`。

### 调查检查点 `investigation/v1`

`UpdateInvestigation` 与 `GetInvestigation` 在 Go 库和本地服务共用契约。Unix 方法名为
`update_investigation` 和 `get_investigation`，所有者取自已认证的连接。调查 ID 在 Space 和
Principal 内唯一。`ExpectedVersion=0` 与 `Subject` 创建版本 1，后续按同一调查的版本写入，
每次成功递增 1。版本不符返回 `precondition_failed`，同一 `RequestKey` 和相同命令重试返回原回执。
其他记录的提交不会推进调查版本。最新检查点保存在 `ExecutionKnowledge` 的独立所有者分区，
旧版本通过原发布锚点读取。更新经同一个协调器发布和重放。

`Findings` 按 ID 新增或替换，`RemoveFindings` 移除指定判断。`OpenQuestions`、`NextFocus` 和
`Position` 只在字段出现时更新。判断由调用方提交。ATE 校验各引用的历史成员关系和当前披露权限，
调用方通过 `Sources` 申报进展文本依赖的其他来源。该字段累积到后续版本。原命令和检查点值的
来源关系随更新原子发布，撤权覆盖历史检查点和审计事件引用的原命令。每次命令最多 128 KiB，
检查点最多 256 KiB 和 32 个不同的锚定引用。当前版本的证据引用限定在同一 Store 和 Space 内。

`GetInvestigation.Anchor` 选择检查点的发布切面。恢复浏览时用 `Checkpoint.Position.Focus.Anchor`
和 `Position.ViewMode()` 打开新的 View，以 `Focus.Ref`、`Mode`、`Sections` 调用 `GetNodeContext`，
并为每次读取指定 `QueryLimits`。尚无 `Position` 时可从 `Subject` 开始。分页令牌随 View 生命周期结束，
恢复从保存的节点开始。宿主按模型窗口选择和渲染结果，实际历史模型请求以 `RecordedInput` 为据。

## 2026-11-05 当前协议收口

当前 wire 和库门面共用 V1 registry。读取方法通过 `ReadDeliveryGrant` capture/consume 绑定认证 session、correlation/request、method、View/Anchor 和来源闭包；没有 grant-capable backend 的本地服务拒绝读取路由。模型与工具派发通过 `emission_dispatch` 原子消费 grant，纯模型调用也受同一边界约束。当前行为与四文档的交叉证据见 [V1 当前交叉验收续记](validation/V1_CURRENT_CROSS_CHECK_2026-11-05.md)。

## 2026-11-06 输出与最终答复契约

Control 6 的 `CaptureOutputChunk`（opcode 34）提交一个 OutputChunk：ID、Activity、Attempt、RequestDigest、Incarnation、Response、Sequence、Payload。序号从 1 连续递增，同一 Attempt 只允许同一个 Response；分块累计字节不超过 1 MiB。ID 是嵌入记录的逻辑内容身份，不是可以任意按 raw core ObjectID 读取的独立对象。统一节点 API 使用 agent/OutputChunk 实体投影进行渐进查看。

`ActivityResult` 新增必填 Capture（full/truncated/partial/unknown）、可选 Response 与按序 OutputChunks。终结清单必须逐项对应所有分块，Payload 必须等于连接后的确切字节。非流式可省略 Response/OutputChunks。partial/unknown 仅作审计事实，不能变成接受结果；模型必须 full。Outcome 保留独立成功/失败含义。

`ActivitySpec.FinalAnswerInput` 是可选的不可变 ValueBytes 见证引用；已启动任务的最终答复必须提供。见证 schema 1 绑定 Query、严格按 Task 排序的完整 Outcomes、ContextPacket、Request、RequestDigest、HostPolicyVersion，必须在 Activity.InputFence 已发布，renderer version 与 Packet.Renderer 相同。原始 provider 请求保持逐字节原样。

`AnswerQuery` 与持久 QueryAnswer 绑定 GeneratedFrom、Outcomes、InputEvidence。GeneratedFrom 必须是本 Query 最新 accepted、full、success 的 query_decision 结果，决策无工具调用；Body 等于其确切 Payload。已启动任务的全部完成成果和验收证据必须与 witness 一致。引擎校验链接与发布切面，宿主 renderer 对请求中的证据呈现语义负责。

当前格式为 Control 6、Packet 5、Summary 2。其它版本的支持范围见格式注册表。普通嵌入式调用和 Unix 服务共用一次性读取交付，任务轨迹同样遵守固定切面与当前披露规则。 [实现与验收 L-01、L-02](validation/V1_CROSS_AUDIT_2026-11-06.md)
