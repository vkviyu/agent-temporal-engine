# ATE V1 四文档与代码交叉审计 — 2026-11-02

> 本文件保留历史核查记录。最新 public API→执行语义核查见
> [2026-11-03 独立审计](V1_INDEPENDENT_CURRENT_AUDIT_2026-11-03.md)：下文关于 Work 原子边界已强制、
> schema 3 `emission_dispatch`/Query grant 已接入的关闭声明不成立，C-01/C-02/C-03 继续阻塞 V1。
> 类型化 Summary v2 已有真实工件；最新结果见 [M6 实测](V1_REAL_MODEL_EFFECTS_2026-11-03.md)。

当前结论：**严格 V1 尚未全部验收。**此前以新增 scope 声明关闭原始要求并宣布完成的结论已撤回。
协议边界与未关闭项见 [STRICT_V1_SCOPE_DECISION_2026-11-03.md](STRICT_V1_SCOPE_DECISION_2026-11-03.md)。
本文前部保留历次审计/测试记录；其中“旧 Profile 兼容 reader”及历史完成结论不再描述当前代码。
当前公开 admission 和内部 replay 均已移除旧 Profile decoder，V1 自身的历史仍保留。

本报告审计当前代码树，不把上一轮的测试绿色或 `ATE_V1_IMPLEMENTATION_ADDENDUM_2026-11-02.md`
当成语义证明。审计对象为：

- `AGENT_TEMPORAL_ENGINE_DESIGN.md`（D，语义目标与不变量）；
- `ATE_V1_IMPLEMENTATION_ARCHITECTURE.md`（A，实施边界与阶段门槛）；
- `ATE_API_PROTOCOL_DESIGN.md`（P，库/服务/SDK API 与协议）；
- `ATE_EXECUTION_PROFILE_WALKTHROUGH.md`（W，长程多 Agent 对象流水与故障场景）。

## 审计方法

每个结论同时检查文档条款、生产代码符号、正/负例测试和库/Unix SDK 入口。结果分为：

- **一致**：文档语义、代码约束和测试证据相互支持；
- **部分**：代码覆盖了子集，但文档还要求未实现的组合；
- **冲突**：代码行为与文档要求不同，或文档声明超过代码能证明的范围；
- **缺证据**：实现可能存在，但没有对应独立回归或双形态证据。

## 已确认一致的区域

| 区域 | 代码与证据 |
| --- | --- |
| 独立基础设施边界 | 独立 Go module、无 `internal`、无 GSEA/Eino 生产依赖；`tests/architecture/dependencies_test.go` |
| 五根、单协调器、receipt-first、P1 | `engine/plan.go`、`publication_plan.go`、`replay.go`；普通/race 全量测试 |
| Graph Revision/Fork/Join/Prune | `graph/`、`engine/graph*`；独立 map/graph oracle 与恢复测试 |
| Knowledge Cursor/Import/typed Observation | `knowledge/`、`engine/knowledge.go`；schema1–4 向量、Import、重开和 compaction |
| Context selector/Packet schema3/Summary | `knowledge/selection*.go`、`knowledge/summary*.go`、`engine/summary.go`；mandatory、容量、future source、摘要 provenance 测试 |
| Query intake 和跨根 Work | `api/work.go`、`engine/work*.go`；一张 receipt、私有候选、失败回滚、reopen/compaction |
| Typed Validation | `agentprofile/validation.go`、`engine/validation.go`；旧目标、错误产物、命令、环境、工具和策略负例 |
| NodeContext 分区 | `api/contracts.go`、`engine/node_context*.go`；input/receipts/messages/artifacts/stateDiff/gaps 和历史 View 测试 |
| 本地服务基础协议 | 有界 JSON frame、session→View、Principal 覆盖和断线/未知结果基础测试 |

## 发现的高优先级冲突和缺口

本节保留首次交叉审计的基线；后续修补的当前状态见文末“本轮代码补齐与复验”。不能将下面的
初始缺口描述误读为代码仍没有任何对应字段，也不能将局部补齐误读为整个 V1 已完成。

**审计注记：本节是历史基线，不是当前代码状态。** 当前结论以“本轮代码补齐与复验”和
“后续补齐最终复验”为准。

### A1 — Invocation/DecisionRef 契约没有实现完整（P1）

D §11.1、§11.3、§11.12 以及 W C06–C11 要求工具调用绑定：

`DecisionRef + accepted model result + providerCallKey + tool definition/version + arguments + dispatchClass`。

当前 `agentprofile.Invocation` 只有 `Execution、Step、ProviderCallKey、Tool、Args、Status、Result、Attempt`；
`ActivitySpec` 没有 typed ToolDefinition、accepted-result 引用或 dispatch class。当前唯一槽仍是
`(Execution, Step, ProviderCallKey)`，见 `agentprofile/snapshot.go`。因此换 accepted result、工具定义版本或
dispatch class 仍可能被表示成同一个逻辑调用，文档要求的旧结果隔离和工具版本绑定没有落地。

**裁决：冲突。** 需要新增版本化 `InvocationContract/DecisionRef`，让 RecordInvocation、Claim、Dispatch、
Accept 和 Work `model_decision` 全部校验同一契约；历史 schema 保留原 replay 语义，新写入不得降级。

### A2 — Continuation 不能完整表达文档规定的恢复位置（P1）

D §11.10、A §12.3 要求 Continuation 绑定 owner scope、当前 Step、Knowledge Cursor、adapter 版本和下一批
PendingAction refs。当前 `agentprofile.Continuation` 只有 `Execution、Version、Incarnation、PendingKey、
PendingGeneration、InputFence、Phase`。`ResumePlan.AdapterVersion` 是接管计划字段，并没有进入 Continuation；
也没有 Step/Knowledge Cursor/owner scope。

**裁决：部分实现。** 当前恢复测试证明了 ActionKey/Request/Fence/Phase，但不能证明“从同一 adapter 与知识位置
恢复”。需要扩展 schema3，新字段追加在尾部，旧 schema1/2 replay 保持不变。

### A3 — Query-only 短交互与当前 StartWork 模型不一致（P1）

D §11.1、§11.3、§11.12 明确允许没有 Task/Execution/Step 的 Query DecisionRef；A §12.1 也把短交互列为
Profile 能力。当前可恢复的 `query_start` Work 必须提交 `StartWorkCommand`，而它必然创建 Task+Execution；
`Query` 生命周期没有 QueryDecision/DecisionSequence 形式的 Invocation/Action 归属。

**裁决：冲突。** 这不是把 Task 创建前 intake 做好就等价完成了 Query-only。应在文档和代码之间二选一：
实现 Query-scope DecisionRef/Invocation/Answer 路径，或明确把 Query-only 改列为 V1 后续范围并在 A/W/P 中删除当前承诺。

### A4 — Emission grant 的绑定字段少于 D §17.6（P1）

当前 `api.EmissionSpec` 绑定了内容、历史 Anchor、字节范围、目的地、Task/Execution/GoalEpoch 和
AuthorityVersion；`engine/emission.go` 在 Issue/Claim 时重查当前 Memory policy。这保证了撤回后的新 Claim
会被拒绝，但仍缺少：

- 依赖来源集合或其 digest；
- 签发时的 ACL/UseBan/Privacy policy version；
- grant 失效条件/期限；
- 统一的 DispatchGrant 与外部执行器确认协议。

**裁决：部分实现。** 当前是持久的一次性 range grant，不是 D §17.6 的完整 grant contract。应至少增加
`DependencyDigest` 和 `IssuedPolicyFence/Version`，并定义执行器收到 grant 后的 claim/send/reconcile 协议。
撤回不能追回已经交给外部执行器的字节，这一点符合 D 的限制，也符合 durable Activity 通常需要幂等和回执
对账的实践；本地 receipt 不等于外部 exactly-once。

### A5 — Event 归因 schema 仍只有 core command occurrence（P1）

D §4.3、§20/I-34 要求区分 Subject、InitiatingActor、Execution、Query、Session 和触发关系。当前
`eventlog.KindCommandApplied` 只记录 Principal stream、TxID、Method、Command、StateOps 和前一 stream event；
Profile facts 没有统一的 Subject/TriggeredBy/DecisionRef registry。

**裁决：部分实现。** 现有 Event 足以证明 core command 的发生顺序，但不能支撑文档中完整的 Agent 因果归因。
需要新增 profile event registry 或明确把完整归因降为 V2。

### A6 — 格式“manifest”尚未等于真正冻结（P1）

A §18.2、P §8 要求 frozen object/profile/protocol registry、兼容 corpus 和迁移规则。当前新增
`docs/formats/ate-v1-manifest-2026-11-02.md` 是兼容边界说明，但底层 registry 仍标为 experimental，
没有统一机器可读 registry、迁移器或 protocol capability golden。manifest 不能把实验格式改名为 frozen。

**裁决：缺冻结证据。** 可以保留 manifest 作为 RC 边界，但不能在四文档审计中把 G-10 视为严格 frozen V1 已完成。

### A7 — 独立 reference model 的位置和覆盖不足（P1）

A §16.1 要求 `tests/reference` 使用独立 map/集合/图，不复用生产 reducer。当前 `reference/engine.go` 是
opaque receipt oracle，并调用生产 `engine.CommandDigest`；新增 `engine/independent_oracle_test.go` 是测试内小 map，
只覆盖 Spawn/Cancel 和 Emission Issue/Claim/Revoke，不覆盖 Join/timeout、Goal/authority、Work atomicity、
Node visibility 和 policy gates。

**裁决：部分实现。** Graph/radix 局部 oracle 很好，但不能称为完整 Profile reference model。需要独立包或
明确缩小 A §16.1 的承诺。

### A8 — 新增 API 没有完成双形态 conformance（P1）

`ApplyWork/GetQueryIntake`、`ApplySummary/GetSummary`、`ApplyEmission/GetEmission` 已有 API、service 和 client
入口，但对应的现有 service/client 测试主要覆盖旧 Agent/Node/Domain 路径；新三组方法没有与 embedded 结果逐项
对照的 Unix SDK 测试。

**裁决：缺证据。** 不能用 embedded engine 测试代替 A/P 要求的库与 SDK 语义等价验证。

### A9 — 跨 Space 导出/导入仍未实现（P2）

D §17.5 要求导出闭包、来源 Space、脱敏策略、source→destination 映射和目标域 ID 重算。当前 Knowledge
Import 是同一 Space 内的 Execution→Execution seed，不是跨 Space export/import。Memory/ExperiencePath 也没有
脱敏映射协议。

**裁决：缺实现。** 如果它属于 V1，必须新增 ExportManifest/ImportMapping；如果不属于 V1，应在 D/A 的第一版目标表
明确降为后续范围。

### A10 — Summary/selector 已实现，但“模型效果”仍不是实现证明（P2）

Selector、Summary provenance 和 Node projection 已完成确定性机制，但 G-09 仍只有 deterministic host/provider。
D §15.4、§15.5 明确把记忆与跨项目收益视为研究问题。当前文档若把机制测试写成“长期效果已完成”，会超出证据。

**裁决：机制一致，效果缺证据。** 报告应继续把真实模型成功率、跨项目迁移和大规模收益列为外部评测。

## 设计与实现中的正面裁决

1. ATE 没有用内存通知冒充 durable subscription；D/P 明确把广播、Watch、push 延后，这一边界是一致的。
2. 外部 Activity 采用 attempt/acceptance/settlement/unknown 分层，符合“本地状态提交不等于外部 exactly-once”的原则。Temporal 的官方文档同样将 Activity 重试与幂等责任分开，并明确 at-least-once 处理需要稳定操作身份和幂等策略：[Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)、[Nexus operations](https://docs.temporal.io/nexus/operations)。
3. 事件源保持不可变、状态由事件/投影重建的方向与事件溯源审计原则一致：[Focusing on Events](https://martinfowler.com/eaaDev/EventNarrative.html)。但这不替代 A5 所需的 Agent 归因字段。

## 审计结论

当前代码可以称为 **V1 durable core + 部分 Agent profile closure**，不能称为“四份文档逐条完全一致的严格 V1”。
在开始大规模工程重构前，至少应先处理 A1、A2、A4、A5、A8；A3、A6、A7、A9 需要在文档和实现之间做明确
范围裁决，不能靠追加一份 addendum 将冲突隐藏。

下一步顺序：

1. 先冻结 Query-only 是否属于 V1、Invocation/Continuation schema 的最终形状；
2. 补 Invocation contract、Continuation 完整恢复信息和 Emission policy/dependency binding；
3. 补 profile Event attribution 和 Work/Summary/Emission 的 embedded↔Unix SDK conformance；
4. 再决定 V1 format freeze、reference model、跨 Space export/import 是否进入本版；
5. 重新运行四份文档交叉审计和全量/race 验证，之后才开始整体代码重构。

## 本轮代码补齐与复验（2026-11-02）

| 条目 | 当前状态 | 新证据与剩余边界 |
| --- | --- | --- |
| A1 Invocation contract | **执行级与 Query-only 契约已补齐，保留旧格式边界** | Invocation schema 2/Control schema 4 固定 accepted primary result、工具定义/版本、参数和 dispatchClass；QueryProgress 固定 `(Query, DecisionSequence)`、工具集合和精确下一请求；新 Work/Query 拒绝缺失或重复契约。旧 profile 日志继续按历史语义 replay，非 Query 的全部 legacy 新写入升级仍不承诺。 |
| A2 Continuation | **部分，typed 执行级恢复位置已补齐** | Position 固定 owner、最新 Step、确切知识 Fence/Key/Cursor、adapter 和有序下一动作 generation；检查恢复/快照关系和历史知识真实性，禁止已有 Position 降级或静默更换 adapter。旧字段仍可选；Query 使用独立 QueryProgress，不把 Query 伪造成 Execution。 |
| A3 Query-only | **已补齐 V1 主路径** | `query_open/decision/tool/answer` 使用 QueryProgress 和 DecisionSequence，不创建 Task/Execution/Step；模型结果、工具 Activity/Attempt、最终回答、原请求 reservation、重开、compaction、历史读取、隐私撤回和 SDK/Unix 对照均有测试。仍不把所有 Profile opcode 都建成 Query scope。 |
| A4 Emission | **部分，schema 3 绑定与 Work dispatch 已补齐** | schema 2 保存依赖闭包/digest、PolicyFence/root、准确投影/范围摘要；schema 3 可绑定 Query 或 Execution 的 Activity/Attempt/RequestDigest、内容类型和确定性 NotAfterLSN，并通过 `emission_dispatch` Work 原子消费 exact grant/dispatch。现有未绑定 legacy Activity dispatch 仍可直接调用；外部网络 send/reconcile 仍由执行器负责。 |
| A5 Event attribution | **主要路径已实现，非 Agent 仍保留语义边界** | 新 core event schema 2 + `ate.agent.event/v1`、registry、Subject/InitiatingActor/Query/Session/Execution、PreviousInStream、CausalParents、Work 多事实和 QueryInvocation 归因已通过混合日志/replay/compaction/Node 测试。Graph/Memory/Knowledge/Summary/Emission/Transfer 现在也产生 principal-scoped `Core.Method` typed fact，并支持 `EventOperation` 搜索；这些 facts 记录已认证方法边界，尚未把每个 domain command 的实体语义全部展开。 |
| A6 Format registry | **V1 frozen** | `formats` 公共包、机器可读 registry、真实 decoder corpus、旧字节 replay/write policy、Unix hello/capability parity、fail-closed migration planner 已实现；当前 registry 发布为 `v1.0.0` frozen，跨版本迁移必须使用新导出/导入或新版本，未知版本拒绝。 |
| A7 Independent reference | **部分，主要类别已覆盖** | `tests/reference` 独立模型覆盖目标/authority/tree fence/cancel/join/quorum/timeout、Activity unknown/reconcile/quarantine、Work atomicity、知识隔离和 policy/visibility；不复用生产 reducer/digest。仍不是每个 Profile opcode 的穷尽 oracle。 |
| A8 新 API 双形态 | **已补齐本轮新增 API 的对照证据** | Work/Summary/Emission/Query/Transfer 使用真实 Store 对照 net.Pipe 和原生 Unix socket，覆盖历史/重试/冲突/隔离/恶意成功响应/重开；Transfer 还验证跨 Space manifest、mapping、foreign evidence 和 source omission。 |
| A9 跨 Space transfer | **部分，导出/只读证据导入已实现** | Export closure、显式 omission、manifest digest、source→target mapping、目标 Space read-only archive、历史/权限/幂等/重开和显式 Observation/Memory candidate adoption 已验证；不导入本地控制权威，不自动 promote。不同 Store 但同名 Space 的导入仍明确不支持，脱敏当前是 whole-record omission，不是字段级匿名化。 |
| A10 Summary/selector 效果 | **机制完成，模型效果仍缺证据** | 确定性 selector、Summary provenance、Query/Node 投影和 policy gate 已验证；真实模型成功率、跨项目迁移和大规模长期收益仍属于外部评测。 |

### 新发现并修复的可执行错误

1. 原 `model_decision` Work 强制 FinishStep，同时登记 pending Invocation；Step 完成门禁又拒绝
   pending Invocation，导致带工具的真实决议无法提交。现在允许先原子提交 acceptance、完整调用集和
   Continuation，工具处理后再完成 Step。零工具决议仍要求 FinishStep，未知工具效果不会被当作完成。
2. 新增 Invocation 字段也位于反射 Control codec 内；若只升级独立 record，会改变旧 Control 字节。
   schema 1–3 现按原字段数编解码，schema 4 才携带新字段。历史 Work replay 也保留原契约语义，
   新 `model_decision` 则拒绝 legacy 空契约。
3. `GetEmission` 原本缺少部分读取门禁和 SDK Anchor/Fence 校验。现在 engine 检查 profile、scope、
   精确历史 Anchor、当前隐私披露策略和输出门禁；SDK 拒绝替换历史切面、错误 RootSet、主体/证明
   不一致等非法成功响应，并关闭连接。
4. Emission 原本未将 offset/length 与实际字节核对。schema 2 现复用 ReadContent 的 payload/value_bytes
   投影，拒绝越界和不支持的 Cursor/Knowledge 源，Proof 保存全量/范围 SHA-256 与总长度。
   历史 schema 1 replay 不追加新字段；SDK 与 Unix conformance 独立核对范围证据。

### 重点回归证据

- `agentprofile/invocation_contract_test.go`：独立 schema-1 golden、新契约截断/部分字段拒绝、
  accepted primary 和不可变调用契约、快照恢复拒绝伪造结果、dispatch class 不匹配。
- `engine/work_decision_contract_test.go`：五类错误绑定无部分发布、pending 工具决议一张 receipt、
  reopen/compaction 保留调用与恢复点、不同请求对象不能绕过工具定义的当前 UseBan。
- `agentprofile/continuation_position_test.go` 与 `engine/agent_continuation_test.go`：多动作、归属、
  alias 隔离、旧格式、降级/adapter/Step 拒绝及重开后取回原知识 cut。
- `api/emission_test.go`、`engine/emission_record_test.go`、`engine/emission_test.go`：旧日志独立构造、
  原 receipt、proof 损坏、同一 group 中 policy cut/root 对应关系、当前撤回和历史披露。
- `client/emission_test.go` 与 `service/work_summary_emission_test.go`：恶意成功响应和双形态一致性。

本轮结论为 **V1 durable core + Agent profile 主路径 closure，仍有明确边界**。A3 已完成主路径，
A5/A6/A7/A9 已从“未实现”推进到可验证的部分实现；A1/A2/A4 仍保留兼容/外部执行器边界。
整体工程重构尚未开始。

### A5 的实施约束（已查明，尚未实施）

> 本小节是 2026-11-02 的待实现设计记录，已被后续 Core.Method typed facts、EventOperation
> 搜索和 Profile Event 实现部分 supersede；仍保留 domain command 实体级 subject 细化的待办。

当前 `replayTransaction` 限制每笔事务只有一个 Event，`finishPlan` 也只拥有一条 core occurrence，
stream head 仅按 Space/Principal 索引，Node event 投影假定 payload 为 StateOps。这些限制不能
表达一个 Work 同时涉及 Query、父执行、子执行与知识采用的多条类型化事实。

后续应保留旧 `core.command_applied/v1` 原字节，新增静态注册的 Profile event，并由新版 core
occurrence 记录 registry 版本和有序 Profile EventID 清单。Profile event 指向 Command/TxID，
不引用当前 core EventID 或 CommitDigest，避免内容哈希自环。replay 按已记录版本选择生成规则，
旧日志不补生成新事实；新写入不允许调用方选择旧规则。物理事务/frame 不必因此另建日志系统。

必须分别校验 RecordedBy、Subject、InitiatingActor、Query/Session/Execution scope 与权限分区，
将 `PreviousInStream` 与语义 `CausalParents` 分开。Work 事件由私有候选分配各 stream 连续序号，
失败或重试不消耗序号，全部随同一 P1 发布。测试必须覆盖旧/新混合恢复、compaction、多 stream
Work、派发→结果→接受因果链、跨主体同名逻辑 ID、缺失/额外/乱序事件的 replay 拒绝。
该方案仍是待实现设计，不能算 A5 已关闭。

## 本轮最终运行记录

Go 1.27.0，Darwin/arm64。所有下列进程已结束，未把中间包输出当成全量成功。

| 检查 | 结果 |
| --- | --- |
| `GOCACHE=/tmp/ate-go-build GOTOOLCHAIN=local go test -json ./... -count=1 -timeout=300s` | exit 0；24 包结束，1906 test/subtest/seed pass 事件，0 fail，0 skip |
| `GOCACHE=/tmp/ate-go-build GOTOOLCHAIN=local go test -race -json ./... -count=1 -timeout=360s` | exit 0；24 包结束，1906 pass，0 fail，0 skip |
| 最后一处 Emission 存储错误传播修补后的相关 engine race 回归 | exit 0；`TestEmission/WorkModelDecision/ContinuationPosition` 相关测试通过 |
| `go vet ./...`、`go build ./...`（同上 GOCACHE/GOTOOLCHAIN） | exit 0 |
| `gofmt -l .` | exit 0，空输出 |

完整 JSON 日志：`/tmp/ate-cross-audit-tests-20261102.jsonl`、
`/tmp/ate-cross-audit-race-20261102.jsonl`。普通与 race 中新增原生 Unix 子测试均实际执行，无跳过。
当前非文档源树（`.go/.mod/.json/.py`，排除 docs/.git）376 个文件，按既有长度前缀算法的 SHA-256 为
`20878bae62bfa35c18f0e04ed9d2caf8bad36e11f9257767d7e781cbbb252b3d`。
仓库仍无提交，未虚构 commit SHA。后续代码修改必须重新验证对应语义。

## 后续补齐最终复验（2026-11-03）

上一节的运行记录对应补齐前的源树。Query-only、Profile Event、Transfer 和 Emission schema 3
继续实现后重新执行：

| 检查 | 结果 |
| --- | --- |
| `GOCACHE=/tmp/ate-go-build GOTOOLCHAIN=local go test -json ./... -count=1 -timeout=360s` | exit 0；27 包结束，2032 pass，0 fail，0 skip |
| `GOCACHE=/tmp/ate-go-build GOTOOLCHAIN=local go test -race -json ./... -count=1 -timeout=480s` | exit 0；27 包结束，2032 pass，0 fail，0 skip |
| `go vet ./...`、`go build ./...` | exit 0 |
| `gofmt -l .` | exit 0，空输出 |
| `GOOS=linux GOARCH=amd64 go test -run '^$' -exec /usr/bin/true ./...` | exit 0 |

最新非文档源树（`.go/.mod/.json/.py`，排除 docs/.git）423 个文件，按同一长度前缀算法的 SHA-256 为
`3ed5df3bd9b2f6a0953564f44fe38e7d2cf4e995405a9d778bfd339bc623deeb`。

### 当前仍明确保留的边界

- A1/A3 的 Query-only 主路径已完成，但旧 Profile 非 Query 新写入仍允许历史兼容模式；没有把每个
  Profile opcode 都重做成 Query scope。
- A2 typed Execution Continuation 已完成，旧 Continuation.Position 仍可为空；同一事务内初始
  未发布 Knowledge seed 的 typed position 尚未加入。
- A4 schema 3 已绑定 Activity/Attempt/RequestDigest、Query scope、ContentKind、NotAfterLSN，
  并用 Work 原子消费 exact grant/dispatch；未绑定的旧 Activity dispatch 仍可直接调用，外部
  executor send/reconcile 仍需执行器按 attempt/unknown/reconcile 处理，不能由本地 receipt 宣称
  exactly-once。
- A5 的 Agent/Work/Query 类型化事实和多 stream 已实现；Memory、Graph、Summary、Emission 等
  非 Agent 方法已产生 `Core.Method` typed fact 并支持 operation filter；每个 domain command 的
  实体级 subject 语义仍是后续细化边界。
- A6 registry 已冻结为 v1.0.0；不存在语义原地迁移，跨版本必须显式导出/导入或发布新版本。
- A7 独立模型覆盖主要 Profile 类别和 Work/visibility/policy；仍非每个 opcode 的穷尽形式模型。
- A9 Transfer 已实现 closure、显式 omission、mapping 和只读 foreign archive，并支持目标域显式
  Observation/Memory candidate；不自动导入 authority/promote，当前只支持不同 SpaceID，不同
  Store 的同名 Space transfer 明确拒绝，redaction 仍是 whole-record omission。
- A10 真实模型长期成功率、跨项目迁移收益和大规模性能仍需外部评测。

因此当前最准确的名称是 **V1 durable core + Agent profile 主路径 closure + 明确的研究/外部执行边界**，
不是“四份文档所有语义已严格冻结”。

## 当前 clean-cut 交叉审计 — 2026-11-03

原始条款继续作为验收基准，不能由本报告或主文档的追加 closure 段落自行削减。
再次检查生产门禁、夹具和原文后，当前结论如下；仍在实现的项不标为一致：

| Clause | Current code evidence | Verdict |
| --- | --- | --- |
| Public Profile protocol boundary | `agentprofile.DecodeCommand`, Control codec, Invocation codec, lifecycle opcode 8, `api.DecodeEmissionCommand`, Work boundary and typed validation | Consistent: old Profile encodings fail before mutation; current V1 bytes are canonical |
| Deterministic V1 time travel | `engine/replay.go`, StateOps/receipt/root verification, current Agent snapshot/QueryProgress/Events, reopen and compaction suites | Consistent: time travel selects published current V1 facts and does not require obsolete Profile decoders |
| Dynamic multi-Agent execution | D §11.12/W C04–C06；Invocation source、SpawnChild、FinishStep/CompleteStep | 修复中：移除了 pending 无 accepted result 和子分派跳过其余义务的放行；聚焦测试通过后仍需全量复验 |
| Continuation recovery | D §11.10/W C02,C05；`Continuation.Position`、Work 初始 seed | 部分：确实无知识与本事务新建 seed 必须分开；精确 producer 绑定和原子 seed 尚在补齐 |
| Event attribution | D §4.3/I-34；Agent/Work/Query facts 与非 Agent Core.Method | 部分：Core.Method 不能替代主要实体 Subject，闭合实体词汇和导航正在实现 |
| Graph/Memory/Knowledge/Transfer | independent domain codecs, Cursors, policy gates, read-only foreign Transfer archive and explicit adoption | Consistent with documented whole-record omission and same-store/Space limits |
| Library/service parity | embedded and Unix/net.Pipe conformance suites for Work/Summary/Emission/Query/Transfer plus hostile response checks | Consistent; native Unix socket test is skipped only when the managed test sandbox returns EPERM |
| Independent reference | D §21.1/A §16.1；`tests/reference` independent map/tree/graph/policy oracle | 部分：需要补控制/Invocation/Continuation/事件、receipt、render/selector 和裁决的可观察差分 |
| M6 actual task/effect report | D §15.5、D/A M6；`V1_EFFECT_OVERHEAD_2026-11-02.md` | 未完成：报告明确没有真实模型结果，不能凭本地机制测试关闭要求 |

四份主文档末尾已撤回错误的完成宣称；它们只记录 clean cut 和审计状态，不能用来循环证明验收。
远程 exactly-once 和订阅的原有边界保留；M6 实测仍在 V1 验收中。

### 前一次源树的验证记录（不代表本次修复已复验）

Go 1.27.0, Darwin/arm64:

- `GOCACHE=/tmp/ate-go-build GOTOOLCHAIN=local go test -json ./... -count=1 -timeout=420s`: exit 0, 2035 pass events, 0 failures, 11 environment skips.
- `GOCACHE=/tmp/ate-go-build GOTOOLCHAIN=local go test -race -json ./... -count=1 -timeout=540s`: exit 0, 2035 pass events, 0 failures, 11 environment skips.
- `go vet ./...`: exit 0.
- `go build ./...`: exit 0.
- `gofmt -l .`: empty output.
- `GOOS=linux GOARCH=amd64 go test -run '^$' -exec /usr/bin/true ./...`: exit 0.

The current non-doc source set contains 422 `.go/.mod/.json/.py` files (excluding
`.git` and `docs`). Its SHA-256 using the audit's length-prefix algorithm
`hash = SHA256( len(file_bytes)||file_bytes for sorted paths )` is
`90bfe5665c4e76059fc44cf054eff3fd5477a3bf1d2881a3a5a60c9f454b3e7d`.
The repository has no commit; this is a source fingerprint, not a commit ID.

## 当前审计结论

严格 V1 尚未全部完成；四文档交叉审计仍有实质缺口，最终工程重构未完成。先完成原定 V1 与验收，
再关闭交叉审计，最后进行工程质量和可读性重构。后续每项关闭必须提供实现、反例与实际运行结果，
不能仅靠调整范围声明或引用这份报告。
