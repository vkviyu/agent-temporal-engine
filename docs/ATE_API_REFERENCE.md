# ATE API 参考

面向把 ATE 接入 Agent、工具或本地应用的开发者。本文列出当前 Go SDK 和本地 Unix Socket 接口。方法表中的参数与返回值使用 Go 字段名。直接实现 Unix 客户端时，按链接中的 `protocol/` DTO 和 JSON 标签编码。

- [快速开始](#快速开始)
- [身份、锚点和调用结果](#身份锚点和调用结果)
- [接口索引](#接口索引)
- [保存并恢复一次证据调查](#保存并恢复一次证据调查)
- [Go 维护接口](#go-维护接口)

## 快速开始

### 在 Go 进程内使用

从 `github.com/vkviyu/agent-temporal-engine` 导入根包 `ate`，从同一模块的 `api` 和 `model` 包导入 DTO 和标识符。`ate.Create(dir, options)` 初始化一个已存在的私有空目录。`ate.OpenExisting(dir, options)` 校验并重放已有存储，返回 `*ate.Store`、`storage.RecoveryReport` 和 `error`。收到成功的 Store 后调用 `Close()` 或 `CloseContext(ctx)` 释放所有权。 [Go 门面](../ate.go)

`ate.Options{AgentProfile: true}` 启用 Agent 执行领域，`EventProfile: true` 启用原生事件记录和浏览。打开已有日志时，应启用该日志依赖的 Profile。 [存储选项](../engine/engine.go)

### 启动本地服务

在仓库根目录执行以下命令。`ate serve` 在前台运行，调用端从启动输出取得 Socket 路径后连接。 [启动参数](../cmd/ate/state.go) · [服务参数](../cmd/ate/serve.go)

```sh
ate_demo_dir=$(mktemp -d /tmp/ate-api.XXXXXX)
mkdir "$ate_demo_dir/store"
go build -o "$ate_demo_dir/ate" ./cmd/ate
export ATE_AUTH_TOKEN='local-api-example-token'
"$ate_demo_dir/ate" init-state --agent-profile --event-profile "$ate_demo_dir/store"
"$ate_demo_dir/ate" serve --agent-profile --event-profile \
  --space demo --socket "$ate_demo_dir/ate.sock" "$ate_demo_dir/store"
```

Go 调用端用 `client.Dial(ctx, socketPath, client.Options{Token: token})` 建立连接并完成认证。`client.New(ctx, conn, options)` 可接管已有的 `net.Conn`。`Identity()` 返回握手获得的 `Principal`、允许的 `Spaces` 和 `Capabilities`。客户端用完后调用 `Close()`。 [Go 客户端](../client/client.go)

### 直接调用 Unix 协议

每帧以 4 字节大端无符号整数表示 JSON 字节数，随后发送 UTF-8 JSON，帧载荷最大 16 MiB。请求包含 `version`、`correlation_id`、`method` 和 `body`。当前 `version` 为 `1`。每条连接一次处理一个请求，响应回显 `correlation_id`。服务校验方法的 DTO，带 `[]byte` 类型的字段使用 JSON Base64 字符串。 [帧与信封](formats/local-protocol-experimental-1.md) · [Go wire DTO](../protocol/local.go)

第一帧调用 `hello`，`body` 只包含 `token`。后续请求的主体身份取自该连接。先查握手返回的 `capabilities`，再使用对应的方法。已注册的 capability 与方法映射可从注册表读取。 [认证与能力协商](../service/session.go) · [注册表](../formats/registry.json)

```json
{"version":1,"correlation_id":"1","method":"hello","body":{"token":"local-api-example-token"}}
```

接着读取 `demo` 空间中的 `output` 键。`b3V0cHV0` 是该键的 Base64 编码。

```json
{"version":1,"correlation_id":"2","method":"get_state","body":{"space":"demo","key":"b3V0cHV0"}}
```

## 身份、锚点和调用结果

| 类型 | 调用时如何使用 |
| --- | --- |
| `SpaceID`、`PrincipalID` | `SpaceID` 选择数据空间。嵌入式 Go 由受信宿主传入主体，Unix 服务从凭据绑定主体和允许的空间。 |
| `RequestKey` | 由 `Space`、`Principal` 和 `RequestID` 组成。为每个逻辑写操作分配一个稳定键。相同逻辑操作重试时复用该键和原始命令。 |
| `CommitFence`、`SnapshotAnchor` | Fence 包含 `Store`、`LSN` 和 `CommitDigest`。Anchor 加入 `Space`，可带 `Cursor`、`Knowledge` 选择历史修订和执行知识。把要长期引用的 Anchor 保存下来。 |
| `ViewRef` | `OpenView` 签发的临时视图句柄。Unix 下绑定当前连接。断线或过期后用原 Anchor 重开视图，再开始分页。 |
| `NodeRef` | 用 `Kind` 选择 `Entity`、`Event`、`Revision` 或 `Object` 中恰好一个字段。读取时用已认证视图验证该引用的成员关系与当前权限。 |

类型定义见 [标识符和节点](../model/ids.go)、[锚点](../model/anchor.go) 和 [请求键](../model/request.go)。

### 写入和重试

写接口通常返回 `CommitResult` 与 `error`。即使 `error == nil`，也要检查 `CommitResult.Code`。`durable_ack` 表示写入已持久发布，包含 `TxID`、`Fence`、请求与结果摘要等。`conflict` 的 `Conflict` 字段区分 `idempotency_conflict`（同键不同请求）和 `precondition_failed`（版本或条件已变化）。`rejected`、`resource_limit`、`not_submitted` 表明该次调用未得到持久回执。 [结果 DTO](../api/contracts.go)

遇到 `wait_interrupted`、`commit_unknown` 或客户端的 `*client.OutcomeUnknownError`，保留原请求键和命令。使用同一主体重新连接并调用 `LookupRequest`。`durable_ack` 可取回原回执，`in_flight` 表示仍在处理，`not_found_at_fence` 表示截至 `ObservedFence` 无持久回执。可重试时提交完全相同的请求。收到 `precondition_failed` 后先重读状态，再把修改后的请求作为新逻辑操作提交。 [对账接口](../api/contracts.go) · [断线处理](../client/client.go)

### 读取错误

嵌入式 Go 使用 `ate.ClassifyError(err)` 取得公共错误分类。Unix 返回 `error.code` 和 `error.message`，Go IPC 客户端可用 `errors.As` 取出 `*protocol.Error`。常用分类如下。 [错误枚举](../api/errors.go) · [错误映射](../engine/errors.go)

| 错误分类 | 含义 |
| --- | --- |
| `invalid_request`、`unsupported` | 参数或命令格式无效，或后端未启用所需能力。 |
| `unauthorized`、`not_found` | 请求缺少授权，或引用在所选视图中不可见。读取有 `Found` 字段的结果时也可能以 `Found=false` 表示缺失。 |
| `anchor_mismatch`、`invalid_view`、`view_expired` | 历史选择不匹配，或临时视图失效。 |
| `resource_limit` | 请求或结果超出接口的节点、范围、字节或计算上限。 |
| `conflict` | 存在并发或领域状态冲突，包括签发读取许可前授权切面变化。 |
| `store_fenced`、`corruption_detected` | 存储进入故障隔离，或校验发现损坏。 |

## 接口索引

接口表使用 `*ate.Store` 和 `*client.Client` 的 Go 方法名。Unix 方法列给出 wire 名称。`ApplyWork` 等复合调用按表中标出的方式封装。

### 事务、状态和回执

Unix 能力标识为 `core/storage-v1` 和 `core/publication-ref-v1`。Go 类型见 [事务契约](../api/contracts.go)、[状态契约](../api/state.go) 和 [发布引用契约](../api/publication.go)。Unix 请求体见 [wire DTO](../protocol/local.go)。

| Go 方法 | Unix 方法 | 请求参数 | 返回值 |
| --- | --- | --- | --- |
| `Commit` | `commit` | `CommitRequest`。指定 `Space`、`Principal`、`RequestID`、版本化 `Method` 和该方法的规范编码 `Body`，可附 `ExpectedFence` | `CommitResult`。提交成功时含 `TxID`、`Fence`、`RequestDigest`、`ResultDigest`、`StateRoot` 和 `RootSet` |
| `Mutate` | `mutate` | `MutateRequest`。指定 `Key` 和 `Batch`，可附 `ExpectedFence`。`Batch.ExpectedRoot` 可为领域状态加根版本条件 | `CommitResult`。`StateRoot` 是提交后的空间领域状态根 |
| `LookupRequest` | `lookup_request` | `LookupRequestRequest`。指定原始 `Space`、`Principal`、`RequestID` | `LookupRequestResult`。含 `Code`、`ObservedFence`，已持久提交时还含原 `Result` |
| `GetState` | `get_state` | `ReadStateRequest`。指定 `Space` 和字节键 `Key` | `StateValueResult`。含当前 `Fence`、`StateRoot`、`RootSet`，以 `Found` 区分键是否存在 |
| `Snapshot` | `snapshot` | Go 仅接收 `ctx`，Unix 请求体为 `{}` 并要求全局快照权限 | `StateSnapshot`。含全局 `Fence`、`RootSet` 和各状态分区的根 `Roots` |
| `ResolvePublication` | `resolve_publication` | `ResolvePublicationRequest`。提供 `Principal` 和 `SourcePublicationRef`，后者指定 `Store`、`Space`、`ProducerTx`，可附 `Cursor` 或 `Knowledge` | `SourcePublication`。返回原交易确切发布位置的 `Anchor` 和 `RootSet`，交易须属于调用主体 |

- `StateBatch.Operations` 接受 `put` 和 `delete`，每项可加 `absent` 或 `value` 条件。`Value.Kind` 可为 `null` 或 `bytes`。缺失键由 `Found=false` 表示，`null` 和空字节值分别保留。每批 1 至 1024 项，键最长 1024 字节，单值最多 1 MiB，编码后整批最多 4 MiB。

### 历史视图和节点查询

Unix 能力标识为 `core/state-view-v1`、`core/node-query-v1`、`core/node-explore-v1` 和 `core/content-range-v1`。事件层还要求启用 `event/v1`。类型见 [View 契约](../api/views.go)、[节点契约](../api/contracts.go) 和 [搜索、追溯与内容读取](../api/nodes.go)。

| Go 方法 | Unix 方法 | 请求参数 | 返回值 |
| --- | --- | --- | --- |
| `OpenView` | `open_view` | `OpenViewRequest`。指定 `Space` 和 `Principal`，可附 `Anchor`、`Knowledge`、`Mode`。省略 `Anchor` 时固定在当前已发布位置 | `ViewDescriptor`，含 `Ref`、实际 `Anchor` 和 `ViewDigest` |
| `CloseView` | `close_view` | `CloseViewRequest.Ref` | Go 返回 `error`，Unix 成功体为 `{}` |
| `GetViewState` | `get_view_state` | `ReadViewStateRequest`。指定 `View` 和字节键 `Key`，适用于领域状态视图 | `StateValueResult`，读取位置由该视图固定 |
| `GetViewEntries` | `get_view_entries` | `ReadViewEntriesRequest`。指定 `View`，可用 `Limits` 约束节点数、条目数、结果字节和读取字节 | `StateEntriesResult`。`Items` 含字节键及对应值对象的 ID，超限时整次读取失败 |
| `ListNodes` | `list_nodes` | `ListNodesRequest`。指定 `View`，可按 `Layer`、`EntityKind`、`IncludeHidden` 筛选，并指定分页参数 | `NodePage`。含节点引用、种类、标签和分页状态 |
| `GetNode` | `get_node` | `GetNodeRequest`。指定 `View`、`Ref`，可用 `Limit` 约束结果 | `NodeDetail`。含 `Summary`、`Payload`、`Sources`、类型化 `Data`、`Encoding` 和 `Anchors` |
| `GetNodeContext` | `get_node_context` | `NodeContextRequest`。指定 `View`、`Ref`，按需要设置 `Mode`、`Sections`、`Limits` 和 `Continuation` | `NodeContext`。含分区摘要、声明的 `Anchors`、证据缺口 `Gaps`、状态差异及分页状态 |
| `SearchNodes` | `search_nodes` | `SearchNodesRequest`。指定 `View`，可按层、实体种类和字面文本筛选。`TextField` 支持 `label` 或 `kind`，`Match` 支持 `contains`、`exact` 或 `prefix` | `NodeSearchResult`。含匹配项、命中原因、`Coverage` 和分页状态。事件层支持按 `EventOperation` 筛选 |
| `Trace` | `trace_nodes` | `TraceRequest`。指定 `View`、起始节点 `Start`，可附 `Direction`、`Relations`、`Limits` 和 `Continuation` | `NodeTrace`。含节点、带关系类型的边、深度限制和分页状态 |
| `ReadContent` | `read_content` | `ReadContentRequest`。指定 `View`、可见的对象引用 `Ref`，可附 `Projection`、`Offset`、`Length` 和 `MaxBytes` | `ContentRange`。含该段 `Data`、`TotalBytes`、`EOF` 和整个投影的 SHA-256 `Digest` |

#### 视图、分页和证据范围

- `OpenView.Mode` 支持 `state`（默认）、`audit_snapshot` 和 `execution_knowledge`。`execution_knowledge` 要求 `KnowledgeKey={Actor, Execution}`，读取限于该执行已采用的知识。
- 节点列表和搜索的 `Layer` 支持 `entities`（默认）、`content` 和 `events`。浏览 `entities` 层需要 Agent Profile，浏览 `events` 层需要 Event Profile。搜索在可见节点摘要上按大小写敏感的 UTF-8 字面文本匹配，`SearchVersion` 默认 `summary-text/v1`。`IncludeHidden` 允许已授权的查询纳入注记隐藏的节点。
- 节点分页须沿用同一个 View 和查询条件，并原样回传 `Continuation`。`QueryLimits` 约束节点、边、字节和深度。节点查询的默认值为 1024 节点、2048 边、4 MiB 和深度 1，上限分别为 65536、65536、8 MiB 和深度 8。 [节点限额](../engine/node_reader.go)`Truncated` 表示当前深度内仍有结果可分页，`DepthLimited` 表示还有更深的关系。 [节点限额](../engine/node_reader.go)
- `Trace.Direction` 支持 `parents`（默认）、`children` 和 `both`。`Relations` 可选 `parent`、`source`、`revision_parent`、`causal_parent`、`stream_previous`，省略时查询全部关系类型。边始终从子节点或证据使用者指向父节点或证据来源，`stream_previous` 表示流内顺序。
- `ReadContent.Projection` 支持 `payload`（默认的规范对象载荷）和 `value_bytes`（核心字节值的逻辑内容）。`Length` 默认为 64 KiB，上限 256 KiB。`MaxBytes` 约束含元数据和 base64 的整份结果，上限 1 MiB。每段读取都检查当前权限。

#### `GetNodeContext` 模式

| `Mode` | 使用方式 |
| --- | --- |
| `recorded_input` | 查看已有的历史输入记录。默认分区为 `inputs`，也可选 `node` |
| `as_known_at_anchor` | 查看指定知识锚点上该执行已采用的知识，使用 `execution_knowledge` View |
| `evidence_neighborhood` | 按明确发布的关系展开目标附近的证据，可选 `node`、`inputs`、`upstream`、`downstream`、`stateDiff`、`messages`、`activityReceipts`、`artifacts` |
| `retrospective` | 从 `audit_snapshot` View 的固定位置回顾可见关系和后续证据 |
| `neighbors`、`parents`、`children` | 兼容查询模式，返回有界的父节点和子节点摘要。`Mode` 与 `Sections` 均省略时使用此邻域行为 |

`Projection` 接受空值或 `summary`。`Gaps` 会报告 `no_recorded_input`、`unavailable_in_view` 或 `no_state_revision` 等缺口。需要原始字节时，使用返回的可见对象引用调用 `ReadContent`。 [模式实现](../engine/node_context.go)

### 任务关键轨迹

`agent/task-trace-v1` 在同时启用 Agent Profile 和 Event Profile 时可用。轨迹由指定 Query 或 Task 的已发布事件投影得出。宿主可以保存带来源的节点说明和解释关系。

| Go 方法 | Unix 方法 | 行为 |
| --- | --- | --- |
| `ApplyTaskTrace` | `apply_task_trace` | 以 `ExpectedVersion` 比较并更新该主体的节点说明和解释关系。校验历史锚点、事件归属、证据可见性和当前披露权限。 |
| `GetTaskTrace` | `get_task_trace` | 读取 Query 或 Task 的关键节点、记录中的因果边，以及宿主提交的解释关系。Query 可额外指定 `Session` 核对归属。 |
| `GetTaskTraceEvidence` | `get_task_trace_evidence` | 指定两个事件和关系类型。对 `causal` 返回所有同时处于起点下游和终点上游的范围内事件。对 `supports`、`refutes`、`explains` 返回对应的引用。 |

继续分页时带上首次返回的 `Anchor`，沿用相同查询条件和页大小。结果里的 `TraceCitation` 可用来在对应历史位置打开节点，再用 `GetNodeContext` 或 `ReadContent` 展开。`Boundary` 标记轨迹范围外的直接因果来源，例如多个任务共享的状态基线。展开该边会返回经授权的来源事件及本轨迹内的后续路径。读接口继续执行当前披露检查。解释关系由宿主提供，ATE 校验来源和访问范围。

单页最多 128 个事件。分页先在 TaskTrace 元数据索引上按 `LSN、Attribution.Ordinal、EventID` 合并并定位游标，再读取当前页事件及因果展开所需的父事件；不会为了返回第一页预加载整个轨迹。单次因果展开仍受 65,536 个对象、64 MiB 和边数上限约束，超限会返回资源限制错误。旧事件按原注册表保留，历史中可用的因果边取决于当时实际记录的关系。 [请求与结果](../api/task_trace.go)

### 修订、分支和合并

Unix 能力标识为 `graph/v1` 和 `graph/query-v1`。读操作默认查询当前发布，可用 `Anchor` 固定历史位置。带领域 `Cursor` 时受该修订的祖先范围约束。类型见 [Graph API](../api/graph.go)、[图命令](../graph/command.go) 和 [图结果类型](../graph/graph.go)。

| Go 方法 | Unix 方法 | 请求参数 | 返回值 |
| --- | --- | --- | --- |
| `ApplyGraph` | `apply_graph` | `ApplyGraphRequest`。指定 `Key` 和一个 `graph.Command`，可附 `ExpectedFence` | `CommitResult`，持久回执含本次更新的 `Fence` 和 `RootSet` |
| `GetGraphRevision` | `get_graph_revision` | `ReadGraphRevisionRequest`。指定 `Space`、修订 `ID`，可附 `Anchor` | `GraphRevisionResult`。以 `Found` 表示是否找到，`Value` 含状态父修订、合并父修订、状态根和条目 |
| `GetGraphBranch` | `get_graph_branch` | `ReadGraphBranchRequest`。指定 `Space`、分支 `ID`，可附 `Anchor` | `GraphBranchResult`。含 `Found`、分支头和版本，裁剪过的分支附 `Pruned` 和 `Prune` |
| `DiffGraph` | `diff_graph` | `DiffGraphRequest`。指定 `Space`、`From` 和 `To`，可附 `Anchor`、`Limit`、`Limits` 和 `Continuation` | `GraphDiffResult`。含按键的增、删、改及分页状态 |
| `PlanGraphJoin` | `plan_graph_join` | `PlanGraphJoinRequest`。指定 `Space`、`Left` 和 `Right`，可附 `Anchor`、`Limits` | `GraphJoinPlanResult`。只读计算合并基点、无歧义的状态和未解决冲突 |
| `TraceGraph` | `trace_graph` | `TraceGraphRequest`。指定 `Space` 和修订 `Start`，方向为 `ancestors`（默认）或 `descendants`，可附 `Anchor` 和分页参数 | `GraphTraceResult`。含修订、深度、父边和分页状态 |

`graph.Command` 每次选择一个操作。`AddRevision` 登记经过验证的状态修订，`Fork` 从指定修订创建分支，`Advance` 按分支版本推进，`Join` 按指定分支版本和 `ExpectedDomainRoot` 原子合并，`Prune` 按分支版本和头修订裁剪引用并记录原因。合并时按计划为每个冲突键提交明确的 `Resolutions`，空 `Value` 表示删除。裁剪记录和历史修订仍可在相应发布位置审计。

`Join` 会替换整个 Space 的领域状态。并行探索可以使用以下命令明确控制写入范围：

| 命令 | 发布内容 |
| --- | --- |
| `ForkLocal` | 从已存在的 `Revision` 创建局部分支。引擎记录分叉基线、认证主体和可选的执行身份。 |
| `WriteBranch` | 根据 `Target` 中的分支、头版本和序号应用 `Batch`，仅推进该分支。`Batch` 使用 `engine.EncodeStateBatch` 编码，条件针对分支状态。 |
| `JoinLocal` | 固定源和目标分支的位置，计算三方合并，仅推进目标分支。冲突需提供明确的 `Resolutions`。 |
| `PublishBranch` | 根据已发布的上一版分支状态或分叉基线计算差异，校验 `ExpectedDomainRoot` 与变更键后将差异写入 Space。其他键保留。 |

与执行关联的分支要求提交该执行当前的 `ExecutionAuthority`。引擎验证授权，并在新修订中记录 `Producer`。父执行可以将同一认证主体的子分支合并到自己的目标分支，然后明确发布。读取 `Branch.Scope.Base` 和 `Branch.PublishedHead` 可以区分分叉位置和上次进入领域状态的版本。 [局部分支命令](../graph/local.go)

分支的主体与执行约束用于写入授权。分支及修订的历史读取继续遵守 Space 读取范围与当前披露规则。仅获得分支版本不会导入执行知识，也不会取得另一执行的事件读取权限。

调用 `DiffGraph` 或 `TraceGraph` 继续分页时，沿用首次响应的 `Anchor` 与其余请求条件，只替换 `Continuation`。直接调用 `apply_graph` 时，`Command` 使用 `graph.EncodeCommand(space, command)` 的输出。 [图 wire DTO](../protocol/domain.go)

### Agent 执行与恢复

需要启用 `agent/v1`。嵌入式使用 `ate.Options{AgentProfile: true}`，服务使用 `ate serve --agent-profile`。历史读取按 `Space` 和认证主体定位。以下控制记录的 `Anchor` 只选择发布位置，省略时读取最新发布位置。

| Go 方法 | Unix 方法 | 请求关键字段 | 返回 |
| --- | --- | --- | --- |
| `ApplyAgent` | `apply_agent` | `ApplyAgentRequest` 的 `Key`、`Command`，可选 `ExpectedFence` | `CommitResult` |
| `GetAgentState` | `get_agent_state` | `ReadAgentStateRequest` 的 `Space`、`Principal`、可选 `Anchor` | `AgentStateResult`，含 `Found`、`Snapshot`、`Anchor`、`LiveControlRoot` |
| `ApplyWork` | `commit` | `ApplyWorkRequest` 的 `Key`、`Unit`，可选 `ExpectedFence` | `CommitResult` |
| `GetQueryIntake` | `get_query_intake` | `ReadQueryIntakeRequest` 的 `Space`、`Principal`、`Query`、可选 `Anchor` | `QueryIntakeResult`，含 `Found`、`State.Intake`、`State.Started`、`Anchor` |
| `GetQueryProgress` | `get_query_progress` | `ReadQueryProgressRequest` 的 `Space`、`Principal`、`Query`、可选 `Anchor` | `QueryProgressResult`，含 `Found`、`Progress`、`Anchor` |

`ApplyAgent` 的 `Command` 每次选择一种操作，覆盖任务和执行、模型与工具活动、调用尝试、结果、消息、检查点、取消和恢复。Unix 请求的命令字节使用 `agentprofile.EncodeCommand`。读取 Query、Activity 和已接纳的结果时，从 `GetAgentState.Snapshot` 或节点投影进入。 [Agent 请求与结果](../api/agent.go) · [命令清单](../agentprofile/command.go) · [执行控制操作](../agentprofile/recovery_types.go) · [Unix DTO](../protocol/agent.go)

`ApplyWork` 把一组满足同一生命周期约束的动作合在一次发布中。填写 `WorkUnit.Kind`，按该类型填写 `Query`、`Intake`、`Steps`、`Decision` 或 `QueryTurn`。最多 64 步，可用 `AgentWorkStep`、`KnowledgeWorkStep`、`EmissionWorkStep` 生成步骤。直接发 Unix 请求时使用 `commit`，命令方法为 `agent.work.apply/v1`，`Body` 使用 `api.EncodeWorkUnit` 的输出。Go Unix 客户端的 `ApplyWork` 已封装这层映射。 [WorkUnit 与编码](../api/work.go) · [Go 客户端](../client/work.go)

`query_intake` 先保存 Query、宿主路由、配置和完整的下一条请求。恢复时用 `GetQueryIntake` 查看 `Started` 并按回执规则对账 `Intake.Next`。对于已建立的 Query-only 流程，用 `GetQueryProgress` 查看 `Version`、`Phase`、`Decisions`、`Completed` 和可选的完整 `Next`。需要重试时沿用 `Next` 的请求键、命令体及发布条件。 [Query 进度字段](../api/query.go) · [受管理 Query 流程](../engine/query_turn.go)

Query 有两条不能混淆的完成路径：

- Query-only 只维护 Query、`QueryProgress` 和 `(Query, DecisionSequence)`。Query-scoped Activity 可以使用既有 Activity/Attempt 状态机，但其 `Execution` 必须为空；流程不会创建合成的 Task、Execution 或 Step。没有启动 Task 时，`query_answer` 可以在没有 `TaskOutcome` 的情况下完成。
- Task-backed Query 只有在显式启动 Task 后才进入 Task/Execution 生命周期。最终答复仍通过 Query 发布，但必须提供并绑定每个已启动 Task 的完整、已记录 `TaskOutcome`。缺少、增加或替换 Task outcome 都会被拒绝。

因此，是否创建 Task 由 Query 是否声明并提交工作启动动作决定，而不是由“是否需要持久化”决定。查询、路由和短交互可以独立完成；真正派生工作的 Query 必须进入 Task-backed 路径。

### 出站许可

需要 `agent/v1`。出站许可用于宿主对模型或工具派发前的授权与审计。发送和外部效果对账由宿主负责。

| Go 方法 | Unix 方法 | 请求关键字段 | 返回 |
| --- | --- | --- | --- |
| `ApplyEmission` | `apply_emission` | `ApplyEmissionRequest` 的 `Key`、`Command`，可选 `ExpectedFence` | `CommitResult` |
| `GetEmission` | `get_emission` | `ReadEmissionRequest` 的 `Space`、`Principal`、许可 `ID`、可选 `Anchor` | `EmissionResult`，含 `Found`、`Record.Status`、`Record.Version`、`Record.Proof` |

`EmissionCommand` 每次选择签发、领取或撤销。签发时，`Issue` 绑定内容对象、已发布的 `Anchor`、投影、字节范围、目的地及 Query 或 Execution 作用域。领取时提供 `ClaimID`、`ClaimVersion` 和执行作用域所需的授权绑定，ATE 会重新检查当前权限、版本和有效期。已绑定活动的许可须通过 `emission_dispatch` Work 与 `DispatchAttempt` 原子领取，版本过期或边界不符会被拒绝。撤销使用 `RevokeID` 和 `RevokeVersion`。Unix 命令字节使用 `api.EncodeEmissionCommand`。 [许可 DTO 与编码](../api/emission.go) · [Unix DTO](../protocol/emission.go)

### 执行知识、上下文与摘要

`knowledge/v1` 提供知识接口。按 Actor 和 Execution 选择知识时，ATE 用 Agent Profile 校验执行所有权。摘要接口属于 `agent/v1`，依赖已接纳的生成活动记录。

| Go 方法 | Unix 方法 | 请求关键字段 | 返回 |
| --- | --- | --- | --- |
| `ApplyKnowledge` | `apply_knowledge` | `ApplyKnowledgeRequest` 的 `Key`、`Command`，可选 `ExpectedFence` | `CommitResult` |
| `GetKnowledgeState` | `get_knowledge` | `ReadKnowledgeStateRequest` 的 `Space`、`Principal`、可选 `Anchor` 和 `Knowledge` | `KnowledgeStateResult`，含 `Found`、`Snapshot`、`Anchor`、所选执行的 `Revision` 和 `Frontier` |
| `ApplySummary` | `apply_summary` | `ApplySummaryRequest` 的 `Key`、`Summary`，可选 `ExpectedFence` | `CommitResult` |
| `GetSummary` | `get_summary` | `ReadSummaryRequest` 的 `Space`、`Principal`、摘要逻辑 `ID`、可选 `Anchor` | `SummaryResult`，含 `Found`、`Summary`、实际存储对象 `Record`、`Anchor` |

`ApplyKnowledge` 的命令选择 `Observation`、`Packet` 或 `Import`。跨执行导入需指定来源知识修订、目标执行、`ExpectedSequence` 以及要采用的观察 ID。读取时，`Knowledge` 由 Actor 和 Execution 组成，同时出现在请求和 `Anchor` 时必须一致。指定 `Cursor` 需要同时指定 `Knowledge`。省略二者会读取当前主体在该空间的知识分区。Unix 命令字节使用 `knowledge.EncodeCommand`。 [知识请求与结果](../api/knowledge.go) · [知识命令](../knowledge/snapshot.go) · [导入参数](../knowledge/import.go) · [Unix DTO](../protocol/domain.go)

`knowledge.SelectContext` 是 Go 本地函数。输入是已读取的知识快照、带知识前沿的来源位置、模型窗口与预留额度、必需观察、token 成本和证据组合。输出给出选中项、遗漏原因和用量。必需项超出容量时返回 `knowledge.ErrContextBudgetUnsatisfied`。宿主按结果渲染实际模型请求，可通过 `ApplyKnowledge` 发布带 `RecordedInput` 的 `ContextPacket`。 [选择参数与结果](../knowledge/selection.go) · [上下文包](../knowledge/packet.go)

跨会话复用时，同一 Space 和主体下的新 Execution 可以通过 `Import` 明确采用来源修订中的 Observation。新观察推进自己的 Knowledge Revision，后续发布的 ContextPacket 固定该次模型的选择和实际请求。派发时再次检查 Memory、ExperiencePath、Summary 及其来源是否仍可复用。 [跨会话用例](../engine/typed_import_recovery_test.go)

`ApplySummary` 提交不可变摘要，包括来源执行的知识修订和观察、生成 Activity、Attempt、Result、输入输出对象及验证记录。Unix 的 `Summary` 字段使用 `Summary.Canonical()` 的输出。`GetSummary.Anchor` 选择读取该摘要的发布位置，生成时的知识位置从 `Summary.Source` 读取。 [摘要请求与结果](../api/summary.go) · [摘要字段](../knowledge/summary.go) · [Unix DTO](../protocol/summary.go)

### 记忆与归档交换

分别通过 `memory/v1` 和 `transfer/archive-v1` 发现。

| Go 方法 | Unix 方法 | 请求关键字段 | 返回 |
| --- | --- | --- | --- |
| `ApplyMemory` | `apply_memory` | `ApplyMemoryRequest` 的 `Key`、`Command`，可选 `ExpectedFence` | `CommitResult` |
| `GetMemoryState` | `get_memory` | `ReadMemoryStateRequest` 的 `Space` | `MemoryStateResult`，含 `Found`、`Snapshot`、`StateRoot`、`Fence` |
| `ExportTransfer` | `export_transfer` | `ExportTransferRequest` 的 `Space`、`Principal`、`Roots`、`Policy`，可选 `Anchor` 和 `Omit` | `transfer.Manifest`，含证据条目、依赖和缺口 |
| `ImportTransfer` | `import_transfer` | `ImportTransferRequest` 的目标 `Key`、`Manifest`、`ApprovedDigest`，可选 `ExpectedFence` | `CommitResult` |
| `GetTransfer` | `get_transfer` | `ReadTransferRequest` 的目标 `Space`、`Principal`、清单 `ID`、可选 `Anchor` | `TransferResult`，含 `Found`、`Record.Mappings`、`Anchor` |

`ApplyMemory` 支持录入候选、验证、提升、状态迁移、撤回来源、构建和撤回经验路径。Unix 命令字节使用 `memory.EncodeCommand`。`GetMemoryState` 读取指定 Space 的最新记忆快照，请求只接收 `Space`。 [记忆请求与结果](../api/memory.go) · [记忆命令](../memory/command.go) · [Unix DTO](../protocol/domain.go)

`BuildPath` 的每个步骤关联一个可复用 Memory 和一个可在来源 Anchor 解析的节点。引擎核对节点与来源证据的匹配，并为省略节点保留证据依赖。路径记录的是宿主选定的经验和验证范围，原始事件的因果关系通过 TaskTrace 浏览。 [路径字段](../memory/types.go) · [路径校验用例](../engine/memory_path_test.go)

归档导出的根限定为当前可复用的 `memory` 或 `experience_path`，每个根都要带来源锚点。`Policy` 需要 `ID` 和 `Version`，`Omit` 用对象 ID 标记整条排除。导入时 `ApprovedDigest` 必须等于清单 `ID`，目标 Space 必须与来源 Space 不同。Unix 导入的 `manifest` 字节使用 `Manifest.Canonical()` 的输出。目标端持有只读证据归档，采纳为本地知识或记忆须另行提交领域命令。 [Transfer 请求与结果](../api/transfer.go) · [清单与映射](../transfer/manifest.go) · [Unix DTO](../protocol/transfer.go)

### 注记

能力标识为 `annotation/v1`。

| Go 方法 | Unix 方法 | 请求关键字段 | 返回 |
| --- | --- | --- | --- |
| `ApplyAnnotation` | `apply_annotation` | `ApplyAnnotationRequest` 的 `Key`、`Command`，可选 `ExpectedFence` | `CommitResult` |
| `GetAnnotations` | `get_annotations` | `ReadAnnotationsRequest` 的 `Space`、`Principal`、可选 `Anchor` | `AnnotationsResult`，含 `Snapshot.Records`、`Anchor`、`Fence`、`RootSet` |

调用 `annotation.NewRecord` 生成记录 ID。注记要求 `Target`、`Source` 和 `Reason`，可附 `Evidence`。一般注记选择 `Label`，撤回已有注记时填写 `Retracts`。ATE 检查目标和证据在来源锚点的可见性及当前披露权限。`Hide` 影响同一主体普通视图中 `ListNodes` 和 `SearchNodes` 的默认可见性。原始轨迹仍按权限可审计。Unix 命令字节使用 `annotation.EncodeRecord`。 [注记请求与结果](../api/annotation.go) · [记录与标签](../annotation/annotation.go) · [Unix DTO](../protocol/annotation.go)

### 调查检查点

能力标识为 `investigation/v1`。证据以 `Citation{Ref, Anchor}` 固定在已发布的位置，单份调查中的证据须属于同一 Store 和 Space。

| Go 方法 | Unix 方法 | 请求关键字段 | 返回 |
| --- | --- | --- | --- |
| `UpdateInvestigation` | `update_investigation` | `UpdateInvestigationRequest` 的 `Key`、`Command` | `CommitResult` |
| `GetInvestigation` | `get_investigation` | `GetInvestigationRequest` 的 `Space`、`Principal`、调查 `ID`、可选 `Anchor` | `InvestigationResult`，含 `Found`、`Checkpoint`、`Anchor`、`Fence`、`RootSet` |

- 创建时用 `ID`、`ExpectedVersion=0` 和必填的 `Subject`，成功后版本为 1。后续写入按调查版本比较，省略 `Subject`。版本不符时返回 `conflict`，原因是 `precondition_failed`。
- `Findings` 按 ID 新增或替换，每条包含 `Text` 和 `Evidence`。`RemoveFindings` 按 ID 移除。`Sources` 声明判断、问题或笔记引用的其他来源，会保留到后续版本。
- `OpenQuestions`、`NextFocus`、`Position` 为指针字段，省略时保留旧值。用指向空列表或空字符串的指针清空前两项。提供 `Position` 时替换浏览位置，其中 `Focus` 是证据引用，`Mode`、`Sections` 传给 `GetNodeContext`。
- `GetInvestigation.Anchor` 选择检查点本身的发布位置。续读证据时用 `Position.Focus.Anchor` 和 `Position.ViewMode()` 新建 View，再传入原 `Focus.Ref` 浏览。恢复从保存的节点开始，重新分页。来源权限变化时，历史检查点读取也会重新校验。
- 单次更新编码上限 128 KiB，检查点上限 256 KiB、32 条不同的锚定引用。Unix 的 `command` 字段使用 `investigation.EncodeUpdate` 的输出。

[调查请求与结果](../api/investigation.go) · [更新字段](../investigation/update.go) · [检查点与引用](../investigation/investigation.go) · [Unix DTO](../protocol/investigation.go)

## 保存并恢复一次证据调查

这个例子先提交一段宿主数据，再从已发布的状态视图取得证据 `NodeRef`。调用 `UpdateInvestigation` 保存浏览位置，关闭并重开存储后用 `GetInvestigation` 恢复，最后按限额读取节点摘要。把代码保存为 Go 文件，在 ATE 模块根目录执行 `go run 文件路径`。

```go
package main

import (
	"context"
	"fmt"
	"os"

	ate "github.com/vkviyu/agent-temporal-engine"
	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/investigation"
	"github.com/vkviyu/agent-temporal-engine/model"
)

func must[T any](value T, err error) T {
	check(err)
	return value
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func committed(result api.CommitResult, err error) api.CommitResult {
	check(err)
	if result.Code != api.CodeDurableAck {
		panic(fmt.Sprintf("commit %s: %s %s", result.RequestID, result.Code, result.Conflict))
	}
	return result
}

func main() {
	ctx := context.Background()
	dir := must(os.MkdirTemp("", "ate-investigation-"))
	defer os.RemoveAll(dir)
	store := must(ate.Create(dir, ate.Options{}))
	defer func() { _ = store.Close() }()

	const space model.SpaceID = "demo"
	const principal model.PrincipalID = "local-host"
	seed := committed(store.Mutate(ctx, api.MutateRequest{
		Key: model.RequestKey{Space: space, Principal: principal, RequestID: "write-output"},
		Batch: api.StateBatch{Operations: []api.StateMutation{{
			Key: []byte("output"), Kind: api.MutationPut,
			Value: api.Value{Kind: api.ValueBytes, Data: []byte("test completed")},
		}}},
	}))
	anchor := model.SnapshotAnchor{Space: space, Fence: seed.Fence}
	view := must(store.OpenView(ctx, api.OpenViewRequest{
		Space: space, Principal: principal, Anchor: &anchor,
	}))
	entries := must(store.GetViewEntries(ctx, api.ReadViewEntriesRequest{View: view.Ref}))
	check(store.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref}))
	if len(entries.Items) != 1 {
		panic("expected one evidence value")
	}
	citation := investigation.Citation{
		Ref:    model.NodeRef{Kind: model.NodeObject, Object: entries.Items[0].Value},
		Anchor: anchor,
	}
	nextFocus := "核对测试输出"
	committed(store.UpdateInvestigation(ctx, api.UpdateInvestigationRequest{
		Key: model.RequestKey{Space: space, Principal: principal, RequestID: "save-review"},
		Command: investigation.Update{
			ID: "review", Subject: &citation, NextFocus: &nextFocus,
			Position: &investigation.Position{
				Focus: citation, Mode: "evidence_neighborhood", Sections: []string{"node"},
			},
		},
	}))
	check(store.Close())
	reopened, _, err := ate.OpenExisting(dir, ate.Options{})
	check(err)
	store = reopened

	result := must(store.GetInvestigation(ctx, api.GetInvestigationRequest{
		Space: space, Principal: principal, ID: "review",
	}))
	if !result.Found || result.Checkpoint.Position == nil {
		panic("investigation position unavailable")
	}
	p := result.Checkpoint.Position
	view = must(store.OpenView(ctx, api.OpenViewRequest{
		Space: space, Principal: principal, Anchor: &p.Focus.Anchor, Mode: p.ViewMode(),
	}))
	defer store.CloseView(ctx, api.CloseViewRequest{Ref: view.Ref})
	page := must(store.GetNodeContext(ctx, api.NodeContextRequest{
		View: view.Ref, Ref: p.Focus.Ref, Mode: p.Mode, Sections: p.Sections,
		Limits: api.QueryLimits{MaxNodes: 4, MaxEdges: 4, MaxDepth: 1, MaxBytes: 4096},
	}))
	fmt.Printf("调查版本 %d，证据 LSN %d，节点数 %d\n",
		result.Checkpoint.Version, page.Anchor.Fence.LSN, len(page.Sections["node"]))
}
```

使用 Unix SDK 时，用 `*client.Client` 调用相同的业务方法。进程启动、连接和关闭分别由 `ate serve`、`client.Dial` 和 `Client.Close` 完成。读取已记录的原始模型请求时选 `recorded_input`，并检查返回的 `Gaps`。是否把证据片段放入下一次模型请求及如何计入 token 预算，由调用方决定。

## Go 维护接口

下列接口用于拥有存储目录或实现传输的宿主。

| 方法 | 参数 | 返回值与用途 |
| --- | --- | --- |
| `ate.Create` | 私有空目录 `path`、`ate.Options` | `*ate.Store` 和 `error`。初始化存储。 |
| `ate.OpenExisting` | 已有存储目录 `path`、`ate.Options` | `*ate.Store`、`storage.RecoveryReport` 和 `error`。验证并重放存储。 |
| `Store.Close`、`Store.CloseContext` | `Close()` 无参数，`CloseContext(ctx)` 接收调用上下文 | `error`。等待释放所有权。取消 `CloseContext` 只中止调用方等待，可再次调用完成关闭。 |
| `Store.CompactAuditFull` | `ctx` | `storage.CompactionReport` 和 `error`。校验并重写物理布局，保留完整历史和逻辑地址。CLI 对应 `ate compact-state`。 |
| `Store.ReclaimRetired` | `ctx` | `[]string`、`int` 和 `error`。分别返回已移除的文件和仍受读者保护的段数。 |
| `Store.LoadGraph` | `ctx`、`space`、头修订 `head`、上限 `maxRevisions` | `*graph.Store` 和 `error`。加载指定修订的有界祖先图。 |
| `Store.PrepareReadDelivery` | `ctx`、`ReadDeliveryBinding`、执行读取的回调 | `ReadDeliveryGrant` 和 `error`。为受信传输绑定请求、目的地和当前授权。 |
| `Store.PrepareContentRead` | `ctx`、`ReadContentRequest`、`ReadDestination` | `ReadEmissionGrant` 和 `error`。签发绑定具体字节范围的一次性读取许可。 |
| `Store.AgentProfileEnabled`、`Store.EventProfileEnabled` | 无 | `bool`。检查嵌入式实例的 Profile 开关。Unix SDK 用 `Identity().Capabilities` 查询。 |
| `ate.ClassifyError`、`Store.ClassifyError` | `error` | `api.ErrorClass`。返回嵌入式错误分类。 |

源码见 [Go 门面](../ate.go)、[维护入口](../maintenance_api.go)、[图门面](../graph_api.go) 和 [读取许可契约](../api/read_delivery.go)。设计理由及演进记录见 [ATE API 与协议设计](ATE_API_PROTOCOL_DESIGN.md)。
