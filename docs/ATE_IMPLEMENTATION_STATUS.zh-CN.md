# Agent Temporal Engine（ATE）——实现状态

[English](../README.md) | [简体中文](ATE_IMPLEMENTATION_STATUS.zh-CN.md)

ATE 是一个独立、优先支持本地部署的时间执行基础设施，面向长时运行任务和多 Agent 系统。

ATE 刻意独立于 GSEA、Eino、模型供应商以及宿主应用的内部包。宿主应用通过公共 API 和版本化的 `agent/v1` Profile 接入。

当前状态：核心状态、静态 `agent/v1`、Graph、Memory、Knowledge、有界 Work 单元、Summary provenance、类型化 Validation、NodeContext 投影以及 Emission grant/revoke 共用同一个 Coordinator、Receipt authority、RootSet 和 P1 publisher。Domain command 会在 admission 阶段携带规范化意图，Coordinator 会在运行 reducer 前检查 receipt，replay 会验证相同的状态转换。当前实现还包括 Invocation settlement 和生命周期记录、按 owner 隔离的 Knowledge 历史、Domain 与 Knowledge Revision Cursor、显式的跨 Execution Import、正式 Event occurrence、有界 Node Search/Trace/Content、SourcePublicationRef、RecordedInput 以及 AuditFull 物理压缩。可复现的 V1 实现和交叉审计补充记录在 `validation/ATE_V1_IMPLEMENTATION_ADDENDUM_2026-11-02.md`；外部模型质量和平台持久性证据仍然有明确边界。所有包保持为 public package，没有 `internal/` 目录，也没有第三方依赖。

单个 Coordinator 会基于私有的增量 staging view，按 FIFO 顺序处理命令。事务可以跨越多个 segment，但单个 frame 始终保持完整，对象地址保持稳定。一次 P1 会发布整个 batch；每笔事务仍然保留自己的 receipt、root 和 commit proof。命令进入 admission 后，取消只会结束当前调用方的等待；即使所有 waiter 都离开，命令本身仍会继续执行。依赖 candidate 状态的拒绝也会延迟到发布阶段之后再返回。

根包提供以下入口：

`Create`、`OpenExisting`、`Mutate`、`Commit`、`ApplyAgent`、`GetAgentState`、`ApplyWork`、`GetQueryIntake`、`ApplyEmission`、`GetEmission`、`ApplySummary`、`GetSummary`、`ApplyGraph`、`GetGraphRevision`、`GetGraphBranch`、`ApplyMemory`、`GetMemoryState`、`ApplyKnowledge`、`GetKnowledgeState`、`ApplyTaskTrace`、`GetTaskTrace`、`GetTaskTraceEvidence`、`ListNodes`、`SearchNodes`、`Trace`、`GetNode`、`GetNodeContext`、有界的 `ReadContent`、`ResolvePublication`、历史 View、`LookupRequest`、`GetState`、`Snapshot`、`CompactAuditFull`、`ReclaimRetired`、`CloseContext` 以及可重试的 `Close`。

通过 `Options{AgentProfile: true}` 可以启用 Agent 命令和重放。任务轨迹还需要 `EventProfile: true`。Graph 更新 `BranchRefs`，Memory、Knowledge、Summary 和轨迹说明更新 `ExecutionKnowledge`，Agent、Work 与 Emission 更新 `LiveControl`。根门面共用同一个 Core。`graphstore`、`knowledge.Durable` 和 `memory.Durable` 是拥有独立存储的适配器。进程内的 InFlight 跟踪等待提交结果的调用方。Agent 控制与 Memory 仍使用有界快照投影，Knowledge 写入使用带索引的不可变记录。Unix 和嵌入式读取均使用一次性、绑定目的地的出站许可。

Knowledge 读取需要由可信宿主或经过认证的本地连接提供 Principal。未提供 KnowledgeKey 时，只会选择当前 owner 的 partition；提供 KnowledgeKey 时，会选择指定的 Actor/Execution，并同时校验历史和当前 owner。`execution_knowledge` View 只会展开已采用的内容。Domain Cursor 用于选择已发布的 Revision，同时保留所选 Fence 对应的 control state。Knowledge Cursor 用于选择指定 Key 下精确的 adopted Observation 前缀；Packet 不会推进该 Revision。跨 Execution 复用必须显式使用 `knowledge.ImportCommand`，并保留 origin/source provenance。Observation 可以显式携带类型化的 `memory` 或 `experience_path` logical ID；Core 会通过历史 Memory projection 解析它，当前模型 dispatch 会拒绝已经撤回的版本。`RecordedInput` 会把 Packet 绑定到之前已经发布的、精确的模型请求字节；只有 metadata 的旧版 Packet 不能授权模型活动。`SourcePublicationRef` 会把 producer transaction 解析到它自己的 fence/root，不会替换成 latest。持久化的 ActionIntent reservation 会在新的 Core commit 之前绑定 request key、method、完整 body 和当前 authority；已发布的 retry 仍然优先返回原 receipt。`storage.Maintain` 仍然只进行保守的 quarantine 清理；`CompactAuditFull` 会在保留完整 audit log 和 reader-pinned generation 的前提下重写物理布局。

Execution 的 primary decision 需要带 `Requirements` 的 schema-5 ContextPacket。引擎会在 Packet 的历史 source cut 上推导结构化约束，并验证这些约束的完整 adopted content。Host 提供版本化 renderer 和 semantic candidate policy。`EffectContract` 声明 receiver 能力。有限的 `IdempotencyGuarantee` 和单序列 `RetryAuthorization` 约束尚未确定的幂等 effect 的重试。可信 adapter 提供 timing 和 terminal evidence，并负责执行 receiver deadline 或 epoch fence。

受保护的 root start、child spawn、message consumption 以及 primary decision acceptance 都使用有界 Work。每次模型或工具 Activity dispatch 都通过 `emission_dispatch` 完成，它会在同一个 receipt 下发布精确的 grant claim 和 Attempt dispatch。当 Activity 绑定 ContextPacket 时，它的 grant 会选择同一个 Packet，并认证完整的 recorded request。Adapter 会在发布后发送请求，并单独对账外部结果。

## 目录结构

- `ate.go`：嵌入式 facade 和 native resource ownership。
- `model/`：公共身份、anchor 和 RootSet value type。
- `api/`：公共 command、query、error 和 capability contract。
- `engine/`：有界 admission、单 FIFO Coordinator、私有 staging、持久 receipt、状态规划和语义 replay。
- `state/`：不可变的路径压缩 Radix Tree，由 reader 提供持久化能力。
- `reference/`：并发的内存 receipt oracle，不负责领域执行和持久化。
- `codec/`：有界的 canonical primitive。
- `storage/`：frame/metadata codec、基于 root 的文件 IO、owner lock、分段 P1 publisher 和物理恢复。
- `cmd/ate/`：二进制入口。
- `protocol/`、`service/`、`client/`：有界的本地 IPC 和 connection-affine SDK。
- `graph/`、`graphstore/`、`journalstore/`：确定性的 Graph 算法和持久化 semantic-record adapter。
- `agentprofile/`：静态类型化转换、canonical current-V1 command/snapshot codec 和持久化恢复状态机。
- `agentstore/`：基于 Core LiveControl 和原始 receipt fence 的类型化 Agent convenience facade。
- `tests/architecture/`：公共包依赖和无 `internal` 目录检查。
- `testdata/journal/`：独立生成的 golden transaction。
- `docs/`：设计、实施、执行 Profile 和 API protocol 文档。

## 构建

```sh
go test ./...
go test -race ./...
go vet ./...
go run ./cmd/ate version
go run ./cmd/ate help
# DIR 必须已经存在，并且在 init 前为空
go run ./cmd/ate init-state DIR
go run ./cmd/ate recover-state DIR
# 如果该 store 包含 agent.apply/v1 transaction，则必须启用
go run ./cmd/ate recover-state --agent-profile DIR
# 重写已关闭 store 的物理布局，同时保留完整 audit history
go run ./cmd/ate compact-state --agent-profile DIR
# 本地服务：RUN_DIR 必须已经存在且为私有目录；需要设置 ATE_AUTH_TOKEN
go run ./cmd/ate serve --agent-profile --socket /absolute/private-run/ate.sock --space agent DIR
go run ./cmd/ate init-journal DIR
go run ./cmd/ate recover-journal DIR
# 显式保留并移除未发布的 tail
go run ./cmd/ate recover-journal --quarantine-tail DIR
```

该 module 目标使用 Go 1.27.0。AuditFull maintenance 的细节和限制见 [compaction registry](formats/compaction-experimental-1.md)。

二进制还支持 `check-journal --previous-digest HEX [--previous-lsn N] FILE`，用于检查实验性的 **raw frame stream**。该命令只读文件，不会修改文件，并且会报告 `publication_verified: false`。它不是 store verification 或 repair 命令，也不会读取 CURRENT/manifest。

语义恢复使用 `recover-state`；`recover-journal` 仍然是纯物理工具，并会报告 semantic verification 为 false。任何一个命令都不会静默提升一个完整但未发布的 tail transaction。

格式 registry：[V1 manifest](formats/ate-v1-manifest-2026-11-02.md) 记录了当前接受的兼容性边界；底层实验 registry 仍然提供字节级细节。Native IO 使用显式的 file/directory sync profile；当前开发阶段尚未完成设备或掉电认证。

当前 V1 candidate 使用干净的格式切分：Control 6、Invocation 2、Continuation opcode 8、Work 1、Emission 3 和 Packet 5 / Summary 2。历史审计和范围决策保留其原始发现与证据边界。Strict V1 acceptance 仍未关闭。[V1 manifest](formats/ate-v1-manifest-2026-11-02.md) · [Independent audit](validation/V1_INDEPENDENT_CURRENT_AUDIT_2026-11-03.md) · [Strict V1 scope](validation/STRICT_V1_SCOPE_DECISION_2026-11-03.md)

早期阶段的验证记录包括：[M6 continuation](validation/M6_2026-11-01.md)、[M5](validation/M5_2026-10-31.md)、[M4](validation/M4_2026-10-31.md)、[M3](validation/M3_2026-10-31.md)、[M2b](validation/M2B_2026-10-31.md)、[M2a](validation/M2A_2026-10-31.md)、[M1f](validation/M1F_2026-10-31.md)、[M1e](validation/M1E_2026-10-31.md)、[M1d](validation/M1D_2026-10-31.md)、[M1c](validation/M1C_2026-10-31.md)、[M1b](validation/M1B_2026-10-30.md) 以及 [M0 / M1a](validation/M0_M1A_2026-10-30.md)。

## 2026-11-06 当前验收

流式输出完整性与持久分块、最终模型的任务证据绑定、原始内容来源的原子发布已经实现。当前 Control 6 / Packet 5 / Summary 2 明确拒绝旧预发布格式。严格 V1 验收仍保留外部证据边界。读取侧短锁、嵌入式读取许可和 writer 锁拆分均已实现，硬件掉电与更完整模型效果证据仍单独记录，不能用当前绿色测试代替。见 [当前交叉审计](validation/V1_CROSS_AUDIT_2026-11-06.md)。

## 长程任务对象与执行闭环

- Query 或 Task 的轨迹可直接返回关键事件、因果关系与可展开的历史引用。宿主可以追加有证据的解释。历史轨迹的输出仍检查当前撤回策略。Event Registry 2 扩展了任务、答复、分支和上下文的归属或因果记录，并可重放早期已发布事件。
- Knowledge 的 Observation 与 Packet 使用增量索引，保留历史读取及恢复后的精确请求字节。跨执行和跨会话采用须有明确来源，派发前复核当前 Memory、ExperiencePath 和 Summary 的依赖是否仍可用。
- 局部分支支持基线、独立写入、三方合并、差异发布和审计。状态合并、任务结果汇聚、工作知识采用各自校验及提交。

对象关系见 [设计文档 §4.8](AGENT_TEMPORAL_ENGINE_DESIGN.md#48-概念之间的关系)，完整流程见 [执行场景走读](ATE_EXECUTION_PROFILE_WALKTHROUGH.md#长程任务闭环实例)，调用契约见 [API Reference](ATE_API_REFERENCE.md)。
