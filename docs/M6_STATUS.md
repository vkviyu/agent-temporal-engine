# M6：第一版收口状态

> 11-05 的当前源树与验证状态见 [交叉验收续记](validation/V1_CURRENT_CROSS_CHECK_2026-11-05.md)。本页以下记录对应其历史版本。

> 2026-11-02 历史状态更正：下方原文保留第一次复核时的结论。后续实现续记已按
> `validation/ATE_V1_IMPLEMENTATION_ADDENDUM_2026-11-02.md` 记录可复现机制的补齐；严格 V1 仍有缺口；
> 外部模型质量、平台耐久和断电认证仍不由本地测试证明。
> 第一次复核时严格 V1 尚未完成。A1–A5 机制测试不能覆盖四文档全部要求；
> 后续复核发现 typed Import schema4、原子消费、上下文选择/摘要、出站授权、格式冻结与
> 效果/开销验收等缺口。完整逐项映射见 `validation/V1_REQUIREMENTS_MATRIX_2026-11-02.md`。
> 工程重构只有第一阶段职责拆分，不代表整体重构完成。

本轮实际修复与最终运行记录见 [V1 复核修复与验证](validation/V1_RECHECK_2026-11-02.md)：typed Import
schema4、reservation/同组 receipt、完整 ExpectedFence、持久化上下文选择与部分当前撤回 gate 已补齐，
全量测试/race 的 24 包都已结束且 exit 0。Summary、跨根原子消费/子代理准备、Query intake、出站 grant、
typed Validation、完整 oracle 与冻结门槛继续保留，不因这轮绿色测试而自动关闭。

## 后续实现续记

后续代码已经补齐 Query intake/Work 的跨根原子边界、Summary provenance、Emission
grant/revoke、typed Validation、NodeContext 分区和一个小型独立控制 oracle。当前代码树的最终
普通/race 测试、vet、build、gofmt 与 Linux amd64 交叉编译结果见上述续记和
[V1 implementation addendum](validation/ATE_V1_IMPLEMENTATION_ADDENDUM_2026-11-02.md)。
这些是实现证据，不等于四份文档逐条严格一致。最新交叉审计仍发现 Invocation/Continuation
契约、出站策略绑定、Profile Event 归因和新 API 双形态 conformance 缺口；外部付费模型的
效果、Linux 原生文件系统耐久及断电认证也仍明确属于外部验收。

最新代码补齐了 Query-only DecisionSequence、执行级 Invocation contract、typed Continuation 位置、
Profile Event 归因、Transfer foreign archive、frozen V1 format registry、独立 reference model 和
Emission schema 3 的绑定/Work dispatch；并修复 pending 工具决议无法提交以及 Emission 读取/SDK
校验问题。新增 API 的真实 Unix 对照测试已补齐。A2/A4/A5/A6/A7/A9 仍有明确边界，A10 仍需外部
效果评测；准确状态见
[本轮交叉审计](validation/V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-02.md)。

M6 是发布收口阶段，不提前宣称所有 Agent 能力完成。当前已有：

- 独立 Go module、公开包边界、无 `internal`、Go 1.27.0；
- M1a–M1f 的 frame、P1、group commit、轮转、恢复、View/pin、local protocol/service/client；
- M2a/b 的 Revision 算法、canonical codec、graphstore/journalstore 持久适配器；
- M3 agentprofile 的纯状态机、snapshot/restore 与 DurableLedger；
- M4 knowledge 的纯语义包和 prepared Durable 投影；
- M5 memory/ExperiencePath 的纯语义包、canonical decode 与 prepared Durable 投影；
- core `agent.apply/v1`：封闭 typed union、静态纯转换、私有候选 LiveControl 根与统一 receipt/RootSet；与普通 state 写入共用 admission/group/P1 和确定性 replay。
- `agentstore` 已改为该 core 的 typed convenience facade，不再写 domain snapshot、不保留本地权威副本；重试按原 ACK.Fence 读取原 typed result，同 key 换命令冲突，Reconcile 只查 core receipt。
- 库与 service/client 支持 ApplyAgent/GetAgentState；历史查询固定原 Fence，local wire 不允许自报 Principal。CLI `serve/recover-state --agent-profile` 显式启用 profile，未启用时不能重放包含 Agent 事务的日志。
- 历史 View 增加有界、完整的 radix `StateEntries` 读取，并复用本地 service/client；返回前认证所有值对象，不返回部分结果。
- `graphstore` 已持久化 BranchRef 的 Fork/Advance、版本 CAS、重启恢复和同 RequestKey 原始 proof 重放。
- `model.RootSet.Validate` 与 core RootSet codec 共同拒绝非 canonical/全零根，结构校验与 profile 根槽位语义授权保持分离。
- graph/graphstore 的 Join/BranchRef 先纯 Preview 校验、P1 后安装；后续 head 推进及重启不改变原 typed Branch/proof。
- Agent 生命周期已扩展到 Query、Activity、Attempt、ActivityResult、Message、Consumption、Continuation，并以 snapshot v2 和 opcode 12 规范命令持久化；Invocation settlement 使用 opcode 11，保留 opcode 10 未占用。
- 根门面增加 Graph Revision/Branch、Memory snapshot/command 和有界 Node list/detail/context 查询；它们复用现有 core journal、RootSet、receipt 和 CAS 语义。
- Knowledge Observation 支持显式 typed `memory`/`experience_path` 逻辑引用；历史解析、Knowledge View
  逻辑节点、当前模型派发撤回检查和跨执行 Import 都保持 provenance，不把逻辑 ID 当 raw object。
- Agent recovery 已加入 durable ActionIntent 准入和祖先 authority fence 传播；重绑定旧请求拒绝，父恢复不自动复活子 Agent。
- `storage.Maintain` 提供取得独占 owner lock 后的 quarantine 维护；`CompactAuditFull` 在同一 publication owner 下完整验证并复制 canonical TX 到新 layout，保留全部 AuditFull 历史，Reader pin 释放后才由 `ReclaimRetired` 删除旧物理副本。

Graph、Memory、Knowledge 现已使用 canonical 领域命令进入同一个协调器，在 receipt 查找之后才构建候选；不再让调用方 StateBatch 绕过保护根。Graph 修改 BranchRefs，Knowledge/Memory 修改 ExecutionKnowledge，Agent 修改 LiveControl，普通宿主 mutation 修改 DomainStates；所有命令统一修改 CommandReceipts 并通过同一 P1/replay。Knowledge 以 Space/Principal 分区并按 Key 认证和过滤，Knowledge Revision/Import、formal Event、Node Search/Trace/Content、SourcePublicationRef 和 RecordedInput 均已接入库/服务/SDK。local service/client 提供对应方法。当前 Agent profile 使用有界整分区快照（payload ≤8 MiB、转换 O(n)）；Knowledge/Memory 分区受 core value 1 MiB 上限约束，超限拒绝，不宣称已解决大规模长程任务容量。

统一提交边界与迁移顺序见 [M6 RootSet integration plan](M6_ROOTSET_INTEGRATION_PLAN.md)。

本轮可执行验证见 [Core / Knowledge / Node 验收](validation/M6_CORE_KNOWLEDGE_NODE_2026-11-01.md)；历史阶段报告不作为 V1 完成证书。

第一轮审计见 [V1 document/code cross-audit](validation/V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-01.md)。A1–A5 已有局部实现、回归和联合宿主证据；不代表四文档全部 V1 要求已验收。容量上限、无设备断电证明、无广播订阅和不删除 AuditFull 权威历史继续明确保留。下一步先修复实际缺陷并完成逐项需求验收，后续再继续整体工程重构。

## 2026-11-05 当前源树状态

上文保留 11-02 及更早的阶段状态。当前 clean-cut V1 的实现、四文档交叉验收和工程整理以 [V1 当前交叉验收续记](validation/V1_CURRENT_CROSS_CHECK_2026-11-05.md) 为准。公开旧 Profile 写入/读取路径已在当前 registry 和 admission 中拒绝，V1 内部 Fence/View/Cursor/receipt 历史读取保留。完整普通/race、vet、build、Linux/amd64 交叉编译和真实模型 M6 工件均已记录。
