# ATE agent/core experimental 1

本 registry 描述 Agent Profile → LiveControl 及当前 domain typed command 与 core 的集成，仍为实验格式。它们复用现有 journal、对象 envelope、command/StateOps/receipt/RootSet，不增加第二个 Journal 或 publisher。普通 `core.state.mutate/v1` 的字节格式不变。

## 启用与能力

库使用 `ate.Options{AgentProfile: true}`，CLI 使用 `serve --agent-profile` 或 `recover-state --agent-profile`。有 Agent 命令的日志必须启用该静态 profile 才能重开；缺失时在尾部隔离或发布同步前拒绝。没有 Agent 事务的旧 core 日志仍能正常打开。

本地 hello 仅在后端启用 profile 时增加 `agent/v1`。该能力包含当前任务、调用结算及 Query/Activity/Attempt/Result/Message/Continuation 命令记录子集；完整 parent/child 外部副作用恢复已通过嵌入库和 Unix socket SDK 的联合宿主验收，但仍不构成远程 provider 或设备断电证明。Knowledge 的 Observation/ContextPacket 由独立 deterministic 包定义，经 core 的 canonical 领域 reducer 发布到 ExecutionKnowledge。

## Canonical command

外层为 `ATE-COMMAND` envelope，Method 为 `agent.apply/v1`。顶层 Body 依次为 raw `ATE-AGENT-COMMAND`+NUL、uint schema=1、uint operation、bytes operation body。Lifecycle 为 opcode 12。Control 为 opcode 13，其内部依次为 raw `ATE-AGENT-CONTROL`+NUL、uint schema=5、uint operation 及 operation 的固定字段。公开解码拒绝 Control schema 1–4、Invocation schema 1 和 Lifecycle Continuation opcode 7。时间旅行重放已发布的受支持 V1 事务。完整 body 最多 1 MiB，未知版本或操作拒绝。Control 字节不使用 JSON。

| operation | 字段 |
| --- | --- |
| 1 CreateTask | canonical Task |
| 2 Spawn | canonical Execution |
| 3 StartStep | canonical Step |
| 4 RecordInvocation | canonical Invocation |
| 5 CompleteStep | Execution string、Sequence uint、GoalEpoch uint |
| 6 CancelTask | Task string |
| 7 RebaseGoal | Task string、NewGoal string、ExpectedEpoch uint |
| 8 PutPending | canonical PendingAction |
| 9 RebindPending | Key string、Generation uint |
| 11 SettleInvocation | Invocation ID string、Result bytes |
| 12 Lifecycle | canonical lifecycle command body |
| 13 Control | canonical authority/activity/goal/action/recovery control command body |

`agentprofile.Command` 必须恰好选择一个分支。外层 RequestKey 绑定 Space/Principal/RequestID；调用者不能提交替换快照或任意控制根。Task.Owner/Space 必须匹配主体和 Space；其他操作从 Task 或所属 Execution 解析所有权。Pending.Request 必须等于原外层 key。

`ActionIntent` 是核心提交的 durable reservation：新请求只有在 RequestKey、Method、ExactCommand、ExpectedFence 和当前
Execution authority 全部匹配时才会进入对应的 core reducer。Resolve/absent 后 Rebind 会使旧 RequestKey
永久失效；若旧请求已经有 receipt，协调器仍先返回原 ACK，不重新执行或被新 reservation 改写。
Control 5 的 ActionIntent 和 RebindAction 编码 optional ExpectedFence（0=无，1=后接 CommitFence）。
nil 与指定 Fence 不等价，恢复不能省略、刷新或替换它。全局 Fence 随 reservation
发布后过时会照常 CAS 冲突。日常预订写入可使用 StateBatch.ExpectedRoot 表达领域级前置条件，不能静默
放宽为无条件提交。所有历史 generation 的 RequestKey 都保持已占用；候选组内的原请求 receipt 也不能被误判 absent。
父 Execution 的 Authority fence 会递归标记所有后代 Authority、活动和 pending work；父 Resume 只恢复
父本身，子代理必须在自己的 fence/resume 流程中重新取得 authority，不能作为 detached worker 继续运行。
V1 的 fence 统一使用递归执行树语义；历史读取以已发布的当前 V1 StateOps 和 RootSet 为准，不能用今天的策略重算旧 RootSet。

纯转换先拥有 canonical 输入、克隆候选、校验 epoch/取消/唯一槽，再校验完整后置快照。错误不会修改原 Ledger。非空 Step.InputAnchor 或 Pending.InputFence 必须对应当前已认证前缀或同组已准备的前驱，不能用错误 Store、未来 LSN 或伪造摘要。

## LiveControl 投影

LiveControl 为系统 scope radix，键为三个 canonical string 的串联：`agent/v1`、Space、Principal。其值是 `ate.agent.snapshot`、version=1、Space=请求 Space 的 immutable object。

Snapshot payload 为 uint schema=1、uint flags=0、Owner string、bytes canonical Snapshot。Snapshot 的独立编码域为 `ATE-AGENT-SNAPSHOT`+NUL，version=1（无生命周期记录）或 version=2（追加 lifecycle envelope）；原五类记录以有界 count/bytes 保存。生命周期 envelope 当前使用固定字段、排序 slices 的 JSON，并由解码后精确重编码检查；这与传输 JSON 和二进制 command body 是不同表示。payload 包括 owner 与包装在内受 8 MiB object 限制。

实体 ID、命令摘要和 CommitFence 摘要不是 ObjectID 引用；此 snapshot 的 envelope.Refs 为空。它们的历史发布来源目前由完整保留的 Journal/history 认证。本阶段没有 GC，不能据此宣称已实现保留闭包或可安全删除历史。

## 单次发布与重放

Agent 命令构造：Command → profile snapshot → LiveControl radix delta → StateOps → receipt → receipt radix delta → RootSet。StateOps.BeforeRoot/AfterRoot 是该笔 LiveControl 根，change 是该分区 snapshot identity 的前后值。`CommitResult.StateRoot` 对此 Method 表示 LiveControl；对原 `core.state.mutate/v1` 仍表示 domain state 根。

所有已注册命令共用 `finishPlan`，派生同一个五根 RootSet：Agent 修改 LiveControl，Graph 修改 BranchRefs，Memory/Knowledge 修改 ExecutionKnowledge，普通宿主写入修改 DomainStates；它们都修改 CommandReceipts，其他根保持不变。RefUpdates 为旧/新 BranchRefs 根；Memory/Knowledge 的 ControlUpdates 记录旧/新 ExecutionKnowledge 根，其他命令记录旧/新 LiveControl 根。只有 coordinator 的 P1 能发布它们。

Open 从上一 RootSet 读取 profile 分区，重算同一静态转换、对象序、StateOps、receipt、ControlUpdates 和 PostRootSet。完整相符才交换对象目录和根；未知 schema、损坏对象或状态转换不符拒绝打开。未知发布结果不安装候选，公开读取会 fencing，恢复只采用实际认证前缀。

## Domain typed command

`graph.apply/v1`、`memory.apply/v1`、`knowledge.apply/v1` 的 body 分别由 graph、memory、knowledge 的 `EncodeCommand` 生成，是闭合 canonical 二进制命令。facade 在 admission 前只编码原始意图，不读取最新状态来生成 StateBatch。协调器先查 receipt，之后才运行 reducer；即使 head、Memory 状态或 Knowledge frontier 已改变，原 RequestKey 的相同请求仍返回原 ACK。

Graph 当前支持 AddRevision、Fork、Advance。Revision 的 root/entries 必须来自认证的领域状态；parent/head 必须存在，分支按 version CAS。Memory 验证新增来源和状态转换，不允许普通 Transition 绕过 Validate/Promote。Knowledge 校验 canonical ID、连续序号、来源发布与 Packet 成员采用时点。

这是对先前实验 facade 的纠正：三个受保护 method 不再接受 `EncodeStateBatch` 的 bytes，也不把替换 snapshot 当领域命令。先前使用该实验表示写入的记录不能默认为新格式；当前没有自动转换或静默重写迁移。打开失败时应保留原 store，使用匹配版本显式导出/迁移。

Knowledge 的系统 slot 先按 Space 指向 radix，再使用由 Space/Principal 规范编码摘要得到的有界分区 key。读取必须提供有效 Principal；local service 从连接绑定它。KnowledgeKey 进一步选择 `(Actor,Execution)`，并认证历史与当前所有权。不同 Principal 即使复用相同 Actor/Execution ID，也不共享观察流和序号。Frontier 只绑定所选观察集合，渲染 Packet 不会推进它。

## 查询与 convenience facade

`ApplyAgent` 返回统一 durable receipt；同 key/同 canonical 请求在 epoch 变更、取消和重启后先返回原 ACK，不重新执行转换；同 key/不同 body 冲突。`GetAgentState` 用 Space、Principal 及可选历史 Anchor 读取当时分区，禁止 Cursor/Knowledge selector 被悄悄替换成 latest。

`agentstore` 提供 typed convenience result，必须从 ACK 对应历史 Fence 解析结果，而不能从最新快照猜测。旧 domain-key snapshot 不自动当作新 LiveControl 状态；需要显式迁移，检测到旧格式时拒绝打开 facade，防止默默丢失状态。

## 明确边界

- 当前是每 Space/Principal 一幅有界完整快照；命令与读取会有 O(n) 编解码和复制，尚未实现实体级增量更新或大规模长程任务容量优化。
- Invocation settlement 会校验原调用、结果幂等性和 Pending/Unknown 屏障；重复 settlement 只能返回相同语义。Activity/Attempt/Result、消息消费和 Continuation 记录属于 lifecycle opcode 12。
- Agent snapshot 受 8 MiB object 上限约束；当前 domain snapshot 仍使用 core value，Knowledge/Memory 分区受 1 MiB value 上限约束，超限拒绝。
- 独立 Durable 适配器仍可单独使用，但不参与同一个 core store 的第二套发布。
- KnowledgeKey 历史筛选、领域/知识 Cursor、正式 Event、Trace/Search、核心联合 Fork/Join/Prune、
  SourcePublicationRef、RecordedInput 与 bounded content reader 已接入 core/library/service/client。
  AuditFull compaction 仍只回收已验证物理副本，不删除 AuditFull 权威事务；联合宿主验收已覆盖
  嵌入库和 Unix socket SDK，但不等于远程 provider、设备断电或容量基准证明。
- 库的 Principal 来自可信宿主；local service 由受控凭据映射，禁止客户端自报主体。历史读取不赋予倒带现实控制状态的执行权。

## Current V1 Effect records

Control 5 keeps the existing operation selectors and uses the following fixed layouts. Strings use canonical length framing, booleans are uint 0/1, slices begin with a uint count, and `Authority` is `Deployment string, OwnerEpoch uint, Incarnation uint, Holder string`.

| Operation | Layout |
| --- | --- |
| 6 CreateActivity | Existing `ActivitySpec` through `ResourceScope`, then `EffectContract`, then command `Authority` |
| 7 ClaimAttempt | `Attempt, Activity, Sequence, Authority, RetryUse` |
| 8 DispatchAttempt | `Attempt, Authority, Evidence, RetryUse` |
| 13 RetryActivity | `Activity, ExpectedVersion, Authority, RetryUse` |
| 32 PutIdempotencyGuarantee | `IdempotencyGuarantee` |
| 33 AuthorizeRetry | `RetryAuthorization` |

`EffectContract` fields are `Version, Definition, Recipient, AllowsMutation, VerifiedPure, HistoricalRead, KeyNamespace, RetentionNanos, DifferentRequest, ResultQuery, ResourceFence, Compensation, CompensationLimit, LateArrivalFence`. The digest hashes `ATE-EFFECT-CONTRACT` + NUL + uint 1 + these fields. The immutable Activity also fixes the exact request object/digest, recipient, resource scope and idempotency key. An idempotent Activity requires a key namespace, a positive retention period, same-key different-request rejection and a declared late-arrival fence.

`IdempotencyGuarantee` fields are `ID, Activity, Version, Status, Contract, RequestDigest, KeyNamespace, IdempotencyKey, ReceiverEpoch, CoveredAttempts, NextSequence, ValidFrom, ValidUntil, TerminalCondition, TerminalEvidence, Evidence, Authority`. The receiver proof covers the exact sorted prior Attempt IDs and next sequence. The latest version supersedes earlier versions. `Status` is `active` or `revoked`. `TerminalCondition` is `request_deadline` or `receiver_epoch` and must match the contract.

`RetryAuthorization` fields are `ID, Activity, ControlVersion, Sequence, Guarantee, CheckedAt, NotBefore, NotAfter, Evidence, Authority`. It binds the successor Activity control version produced by `RetryActivity`. Guarantee and authorization IDs are SHA-256 over their respective NUL-terminated domains `ATE-IDEMPOTENCY-GUARANTEE` and `ATE-RETRY-AUTHORIZATION`, uint 1, and the fixed record with the ID encoded as an empty string. `RetryUse` fields are `Authorization, ObservedAt, SendNotAfter, Evidence`; the empty form encodes the same four fields with zero values.

`RetryActivity`, later claims and dispatches verify the relevant prior Attempt set, current authority, sequence and retry evidence. For a finite guarantee, the check and proposed send window must fit the latest active guarantee and authorization. Times are explicit adapter-provided nanoseconds and `ValidUntil` is an exclusive arrival cutoff. Snapshot Recovery saves `ActivityControl.RetryObservedAt` when the Activity selects a finite authorization and omits the JSON field when zero. The claim's check time must be at or after it, and the dispatch's check time must be at or after the claim's. Attempt controls record their corresponding `ClaimUse` and `DispatchUse`. Without a guarantee, retry requires the `pure` classification or terminal `no_effect` reconciliation with `FutureEffectFenced` for all possibly dispatched attempts. Evidence digests attest receiver behavior. The host retains those artifacts and enforces the declared deadline or epoch fence when sending. Replay uses the recorded timing facts.

## Current V1 Continuation position and atomic seed layout

Control schema **5** and Lifecycle Continuation opcode **8** carry the full
position layout. The incomplete pre-release position shape is rejected. The independent fixture is
`agentprofile/testdata/continuation-source-v1.hex`, generated by the adjacent
Python unsigned-varint encoder; the format corpus also carries opcode-8 and
Control-5 SaveContinuation vectors.

A Continuation encodes fields in this order:

`Execution string, Version uint, Incarnation uint, PendingKey string,
PendingGeneration uint, InputFence, Phase string, Position presence uint,
Position fields`.

Position is mandatory in V1. Its fields, including the final optional source,
are ordered as follows:

| Field | Encoding |
| --- | --- |
| Space, Principal | canonical strings |
| Step | uint; zero only before a Step exists |
| KnowledgeFence | Store string, LSN uint, CommitDigest string |
| KnowledgeCursor | Store string, Space string, Revision string |
| KnowledgeKey | Actor string, Execution string |
| AdapterVersion | canonical string |
| NextActions | count uint; sorted Key string / Generation uint pairs |
| KnowledgeSource | presence uint (0/1), followed by SourcePublicationRef when present |

An embedded SourcePublicationRef is `Store string, Space string, ProducerTx
string, Cursor presence uint, [Cursor fields], Knowledge presence uint,
[KnowledgeKey fields]`. Both selectors are mandatory for a knowledge source.
The source is mutually exclusive with the direct KnowledgeFence/Cursor/Key
triple: that triple must be all zero when KnowledgeSource is present. Source
Store/Space/Execution/Actor must agree with the continuation owner. It contains
no final CommitDigest, RootSet or enclosing object identity.

A direct triple authenticates an already published cut. KnowledgeSource instead
names the exact producer transaction plus immutable knowledge Revision. The
engine resolves that transaction's own authenticated Fence and RootSet, even
when later transactions shared its physical publication group. Unknown
producers, foreign principals and a Cursor absent from that producer are rejected;
resolution never substitutes latest. An all-zero triple with no source is
accepted only while the execution has no adopted observations or packets.
Saving a known position back to that empty state is rejected.

`query_start` Work creates Root control, authority, explicit Query/Goal seed
observations and its initial continuation in one publication. Source bytes for
the Query input and `agentprofile.GoalKnowledge(Task)` are published immutable
inputs before the start intent is executed. The receiving execution need not
exist at that old source cut; this narrow exception is limited to the new
execution created by the same validated Work. Source membership and owner checks
still apply. `child_spawn` additionally imports an explicit selection from the
parent's published knowledge cut and adopts its own canonical Goal image.
Neither path imports all parent knowledge by identity.

Before checkpointing, Work deterministically derives KnowledgeSource from its
preallocated TxID and the candidate's exact knowledge Revision. The signed
canonical outer Work bytes are unchanged, so receipt-first retries and replay
reuse the same transaction and input intent. `message_consume` must use this
binding for the recipient's final knowledge Revision after adopting the
matching observation. This is the only private-source
exception; an ordinary ApplyAgent command may name published producers only.
No ContextPacket can consume the private source before publication. A failed
Work installs none of its control, knowledge, action or continuation; an unknown
publication is reconciled through its original receipt before any new action.

`agentprofile.PublicationReferences` exposes these transaction dependencies
separately from `ContentReferences`; a ProducerTx is never interpreted as a raw
ObjectID. AuditFull compaction retains every producer and its original roots.
