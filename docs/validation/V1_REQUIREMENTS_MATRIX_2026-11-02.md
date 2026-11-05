# V1 四文档要求与实现证据矩阵 — 2026-11-02

> 11-05 的当前源树与验证状态见 [交叉验收续记](V1_CURRENT_CROSS_CHECK_2026-11-05.md)。本页以下记录对应其历史版本。

> 本表为历史审计基线，非当前完成清单。后续关于 Work/Emission 缺口已经关闭的说法被
> [当前独立审计 C-01/C-02/C-03](V1_INDEPENDENT_CURRENT_AUDIT_2026-11-03.md) 的实际调用链反例推翻。
> 当前已有实体事件、独立领域 oracle、typed Summary v2 真实模型工件和 Annotation 实施；不能继续把
> 当时“未找到”当作今日缺实现，也不能因新增接口和绿色测试而忽略公开绕过。

本表用于替代“已有 A1–A5 报告，所以四文档全部验收”的推断。它是本轮的静态交叉审计基线，
不是运行测试记录，也不是 V1 完成证书。表中“已有实现/回归”仅表示已找到对应实现和具体测试，
最终关闭仍需记录该 revision 上实际运行的命令、退出码和产物。不能用接口存在、测试名称或历史报告
代替行为证据；不能把 V1 条款搬到后续范围以降低完成标准。

## 文档与状态含义

四份基准文档：

- D：`docs/AGENT_TEMPORAL_ENGINE_DESIGN.md`。
- A：`docs/ATE_V1_IMPLEMENTATION_ARCHITECTURE.md`。
- P：`docs/ATE_API_PROTOCOL_DESIGN.md`。
- W：`docs/ATE_EXECUTION_PROFILE_WALKTHROUGH.md`。

章节号比易变行号更适合作为要求定位；代码路径为仓库根目录相对路径。

| 状态 | 含义 |
| --- | --- |
| 已有实现/回归 | 找到实现和直接测试；不宣称该测试覆盖了条款的所有组合 |
| 部分 | 已有子集，但条款中至少一项语义或组合尚缺实现 |
| 缺证据 | 实现可能存在，但没有找到要求的独立测试、报告或冻结工件 |
| 缺实现 | 未找到可表达、提交和验证该要求的 API/状态/转换 |
| 后续明确范围 | 主文档明确列为 M7/后续，不能当作 V1 已实现 |
| 未启用条件 | 条款针对当前明确不开放的能力；必须继续拒绝，不能返回伪成功 |

## 本轮必须保留的未关闭问题

| ID | 优先级 | 要求/观察到的缺口 | 关闭条件 |
| --- | --- | --- | --- |
| G-01 | P1 | typed Memory/ExperiencePath 跨 Execution Import 的 Observation schema4 编码遗漏 PayloadKind，而 decoder 读取该字段；`knowledge/typed_payload_test.go` 原测试仅 schema3，没有实际 Import | schema1–4 独立向量与双 typed kind 的实际 Import→canonical decode→snapshot→core reopen/compaction/receipt 回归；修复由本轮实现任务处理，不能仅改完成报告 |
| G-02 | P1 | D §14.1–14.3 的 mandatory、omission、bundle 选择、容量拒绝和 Summary provenance 无对应完整契约；当前 `knowledge/packet.go` 只有 Items/GoalRef/Renderer/Tokenizer/EstimatedTokens/RecordedInput 等 | 提供有界确定性 selector、mandatory 超限错误、稳定 tie-break、完整选中/省略来源；Summary 派生版本和历史 cut 验证；模型真实输入仍逐字节保存 |
| G-03 | P1 | D I-27/§17.6 要内容+目的地+当前控制版本的 ReadEmissionGrant/DispatchGrant 与 revoke 串行化；当前只有 ExecutionAuthority、ActivitySpec/Attempt 的 dispatch evidence | 明确实现 grant 身份/版本/范围/撤回顺序/执行器绑定；包括固定 View 后撤回、分页/chunk、创建 Activity 后撤回再 Claim/Dispatch 反例 |
| G-04 | P1 | D §11.12、W C01/C02 的 Task 前 Query pending-intake / StartIntent 不存在；`Query` 只有输入、Fence、状态、版本和 Answer，ActionIntent 只归属 Execution | Query intake 原子保存下一动作与确切输入/配置/RequestKey；Task 尚不存在时可恢复；C01 后 C02 前退出和换 RequestID 唯一启动反例 |
| G-05 | P1 | D I-28/§11.13、W C04/C13 的跨根原子转换未闭合：`CheckpointCommand` 仅 Intent+Continuation；`ConsumeMessage` 仅 ledger consumption；Knowledge 在另一条命令提交 | 结果 acceptance+Invocation 集合+Step+下一动作、消息 consumption+Observation+知识根+Continuation 有一个受保护的原子 reducer；逐子步骤中断不会留下“已消费但无下一动作”的事实 |
| G-06 | P1 | W C05 的 child/SpawnResult/SeedObserved/初始知识/Continuation 原子归属仍拆成 SpawnChild、GrantAuthority、knowledge.Import 多笔 | 明确受保护原子 spawn/seed 命令或等价可证明的 durable preparation 协议；不能以现有 happy path 相邻调用视为同事务 |
| G-07 | P1 | D §15.3 要 UseBan、PrivacyErasure、ACLRevocation 不同语义；`memory.Store.Get/GetPath` 和 typed Knowledge Node 仍是历史 audit read，`nodeOutputGate` 只校验所有权；现有 typed model 检查只在 CreateActivity | 对当前禁止内容的历史输出、模型/工具再派发有一致权威 gate；FactualCorrection 不改旧 bytes；隐私撤回可明确拒绝历史读取；缺少此项时不能宣称当前授权完整 |
| G-08 | P1 | D §21.1/A §16.1 要 map/显式图/控制语义的独立参考模型；`reference/engine.go` 只处理 opaque receipt，且调用生产 `engine.CommandDigest` | 独立领域/图/可见性/控制 oracle，与真实 core 对相同小型序列差分；已有 radix/graph map oracle 可作为局部证据，不能冒充完整参考模型 |
| G-09 | P1 | D §15.5、§21.6、§22.1/M6 要约束保真、早证据召回、反例利用、跨任务/项目适用性、开销/效果实测；当前 host 为确定性 provider 输入 | 可复现对照场景、基线、指标、输出和局限报告；规则型实验只能证明规则型检索/约束结果，不宣称真实模型成功率或普遍迁移收益 |
| G-10 | P1 | A §17.3/M6、§18.2 要 V1 frozen/迁移规则/平台支持矩阵；P §8 仍写 protocol-v1 未冻结，所有 formats 标实验 | 关闭字节/语义缺陷后，冻结 manifest/object/profile/protocol registry、旧格式拒绝/迁移政策、支持平台/文件系统/耐久证据等级；不能仅改标题为 frozen |
| G-11 | P1 | D §11.12/W C17 的 ValidationRecord 应绑定 Goal、产物、命令、环境/工具版本和接受策略；当前 `CompleteTask` 只验证两个已发布内容 ID，`TaskOutcome` 没有类型化该关系 | 类型化验证证据或受注册宿主策略验证的 immutable envelope；测试别的产物/旧 Goal/其他环境的“验证通过”不能完成当前 Task |
| G-12 | P2 | D §13/18/W 导航要求多种语义 context 分区和恢复位置；`GetNodeContext` 当前只支持 parents/children/neighbors，ReadContent 仅 raw codec payload/value_bytes | 逐条确定 required projection 与宿主责任；必要输入/receipts/messages/artifacts/stateDiff 有来源和缺口状态，不能只用普通邻居数代表完整历史输入 |

以上是待实施/待验证项，不能由“全量 test 绿色”自动关闭。新代码落地后应在本表附加关闭测试与运行产物，
保留原缺口记录。schema4 修复改变此前不能被自身 decoder 读取的实验字节，需要同时写明旧异常记录策略。

## D §20：全部不变量映射

### 本轮修复进展（原缺口记录保留）

本表下方是静态审计时的基线，后续运行证据见 `V1_RECHECK_2026-11-02.md`，不能把尚缺要求从表中删除。

| 缺口 | 更新状态 | 实际新增证据与剩余部分 |
| --- | --- | --- |
| G-01 | 本轮修复并验证 | schema1–4 独立 Python 向量、两类真正 Import、canonical/snapshot/core reopen/compaction/receipt；旧异常实验字节不静默迁移，策略见 domain/core registry |
| G-02 | 部分补齐 | 生产 SelectContext、Packet schema3 完整提案与结果、core 历史重算、旧 schema1/2 原字节与双宿主路径；Summary、自动义务发现、多轮语义压缩仍缺 |
| G-07 | 部分补齐 | 当前 source policy 复查 Create/Claim/Dispatch、历史披露类型区分和新 core 请求的防降级；`engine/content_policy_test.go`；外部出站 grant、任意复制/旧缺失 provenance、全部低层诊断读取并未因此解决 |
| G-09 | 部分补齐 | `tests/acceptance` 六个机制对照与可复现 native/Knowledge/selector 小规模 benchmark；没有把这些结果冒充真实 LLM 效果、跨项目迁移或全部性能矩阵 |
| G-10 | 部分补齐 | 新 `docs/PLATFORM_SUPPORT.md` 明示本机和 Linux 交叉编译证据；格式尚未冻结，迁移工具和平台运行矩阵仍缺 |
| I-37 组合反例 | 本轮修复并验证 | 所有历史 RequestKey reservation、同组 receipt、ExpectedFence nil/变更/过时及 receipt-first、legacy schema reducer；见 action_gate/action_fence/control_fence/recovery_reservation 测试 |

原始审计时的完整测试和 race 均得到最终 exit 0，不表示当时的 G-03–G-06、G-08、G-11、G-12 已实现。

### 2026-11-02 implementation addendum

当前代码树已新增并通过针对性回归：`engine/work.go`、`engine/emission.go`、
`knowledge/summary.go`、`engine/validation.go`、`engine/node_context*.go`。对应
的关闭证据、接口边界和仍然不能证明的外部条件见
`docs/validation/ATE_V1_IMPLEMENTATION_ADDENDUM_2026-11-02.md`。

G-02、G-03、G-04、G-05、G-06、G-08、G-11、G-12 的实现缺口已关闭；G-07
的显式 provenance/use/disclosure gate 已覆盖核心路径。G-09 仍只证明确定性
host 的机制与效果，不能推导付费模型成功率或跨项目迁移收益。G-10 已有格式
manifest，但平台支持仍按 `docs/PLATFORM_SUPPORT.md` 的证据等级解释，Linux 原生
耐久和断电认证没有被虚构为已完成。

| ID | 要求与关联章节 | 状态 | 实现与直接证据 | 未关闭边界 |
| --- | --- | --- | --- | --- |
| I-01 | DurableAck 可从发布日志完整重建；A §9/10 | 已有实现/回归 | `storage/journal.go`、`engine/replay.go`；`engine/engine_integration_test.go::TestDurableStateLifecycleAndHistoricalReceipts`、`engine/recovery_fault_test.go` | 需在最终 revision 跑逐故障矩阵；不等价设备断电认证 |
| I-02 | Branch 不引用未发布对象；D §5/12 | 已有实现/回归 | `engine/graph.go`、`engine/graph_join.go`；`engine/graph_core_test.go::TestGraphCoreRejectsRawMutationAndMissingRevisionWithoutFencing`、`engine/graph_publication_test.go` | core 与 standalone adapter 分别验证 |
| I-03 | 幂等键只对应一个不可变请求与结果；A §5 | 已有实现/回归 | `engine/admission.go`、`engine/coordinator.go`、`engine/engine.go::lookup`；`engine/engine_integration_test.go::TestDurableReceiptPrecedesCASAndFullQuota`、`::TestDurableRequestNamespacesAndStateSpaces` | 所有新复合命令必须仍 receipt-first |
| I-04 | Event parent 存在且无环；D §6 | 已有实现/回归（受限 schema） | `engine/events.go::prepareAppliedEvent` 只生成前一 principal stream Event；`eventlog/event.go`；`engine/events_test.go`、`engine/node_explore_test.go::TestEventTraceFollowsOnlyPublishedCausalParents` | 当前不是任意 profile causal relation registry；见 I-34 |
| I-05 | 发生身份与 payload 去重身份分开 | 已有实现/回归 | Event 包含 TxID/Sequence，ID 为 event object identity；`engine/events_test.go::TestFormalEventsAtomicOccurrenceHistoryAndRecovery`、`eventlog/event_test.go` | 避免把同内容 ObjectID 当所有 occurrence 的身份 |
| I-06 | Branch head/version CAS 最多一个成功 | 已有实现/回归 | `graph/graph.go`、`engine/graph.go`；`graph/correctness_test.go::TestConcurrentSnapshotOwnershipAndBranchCAS`、`engine/graph_join_core_test.go` | 最终运行并发及恢复组合 |
| I-07 | 固定 StateOps replay 得同根 | 已有实现/回归 | `engine/plan.go`、`engine/replay.go`、`state/radix.go`；`engine/oracle_test.go::TestDurableStateIndependentMapOracleAcrossReopens`、`state/radix_test.go::TestRadixInsertionOrderAndGolden` | 全 profile independent oracle 仍 G-08 |
| I-08 | Join 使用记录的决议，不再问模型 | 已有实现/回归 | `graph/command.go`、`engine/graph_join.go`；`engine/graph_join_core_test.go::TestGraphJoinConflictsRequireExactResolutionsAndBothCASGuards`、`engine/graph_join_publication_test.go` | 字段间业务约束验证另属宿主/注册策略，需声明 |
| I-09 | StateParent/JoinParents/knowledge cut 分离 | 已有实现/回归 | `graph/graph.go`、`knowledge/revision.go`、`engine/cursor.go`、`engine/knowledge_cursor.go`；`engine/cursor_test.go`、`engine/knowledge_cursor_test.go` | 新 typed 导入组合需 G-01 回归 |
| I-10 | 旧 View 无未来来源泄漏 | 部分 | `engine/views.go`、`engine/node_catalog.go`、`engine/node_events.go`；`engine/knowledge_cursor_test.go`、`engine/node_queries_test.go::TestNodesCannotReadFutureMissingOrOtherPrincipalObjects` | Summary 尚缺；当前撤权输出见 G-03/G-07 |
| I-11 | RecordedReplay 不调用模型/工具 | 已有实现/回归（请求重放） | `knowledge/packet.go::RecordedInput`、`engine/node_content.go`；`engine/recorded_input_test.go::TestRecordedModelInputRetainsExactBytesAcrossPacketRecoveryAndCompaction`、`engine/agent_render_recovery_test.go` | 完整 Summary/省略输入谱系尚 G-02；不宣称重新调用模型输出确定 |
| I-12 | 旧 incarnation 不推进新执行；晚到证据可审计 | 已有实现/回归 | `agentprofile/control.go` Capture/Accept/ImportActivityResult；`agentprofile/recovery_test.go::TestRecoveryUnknownEffectsQuarantineAndNoEffectRetry`、`engine/agent_recovery_integration_test.go` | 遗留 schema replay 与祖先递归 fence 新语义兼容应另测 |
| I-13 | unknown 不伪装无效果 | 已有实现/回归 | `agentprofile/control_lookup.go::safeRetry`、`agentprofile/control.go` Reconcile/Retry；`agentprofile/recovery_test.go::TestRecoveryUnknownEffectsQuarantineAndNoEffectRetry`、`tests/host/long_task_test.go` | 外部 Evidence 为宿主 attestation，不是引擎独立证明 |
| I-14 | fork/restore 不复制现实预算/权限/令牌 | 部分 | Graph Fork 只 BranchRef；`agentprofile` Resume 需新 owner epoch/incarnation；`agentprofile/recovery_test.go` | 无正式预算/grant 账本；历史恢复不赋权只是已有隔离子集，见 G-03 |
| I-15 | 活跃 reader generation 不回收 | 已有实现/回归 | `storage/reader_pin.go`、`storage/compaction.go`、`engine/views.go`；`storage/compaction_test.go::TestCompactionPreservesExactLogAddressesAndPinnedLayout`、`engine/maintenance_test.go` | 长 pin 的容量/延迟报告仍 G-09 |
| I-16 | Prune 不删共享对象 | 已有实现/回归 | `graph/prune.go`、`engine/graph_join.go`；`engine/graph_join_core_test.go::TestGraphJoinPublishesBothRootsPruneRetainsHistoryAndRetries`、host 场景 | AuditFull 本来不删除权威事务 |
| I-17 | 缺/滞后索引不造成关键假阴性 | 已有实现/回归（扫描模式） | `engine/node_catalog.go` 从固定根扫描，`engine/node_explore.go` 完整层/有界错误；`engine/node_explore_test.go::TestNodeSearchExactSummaryCoverageHistoricalPagesAndAuthorization` | 无异步索引可测；资源超限不能被解释为未找到 |
| I-18 | durable floor 内损坏不静默回退 | 已有实现/回归 | `storage/recovery.go`、`storage/segmented_recovery.go`；`storage/journal_recovery_test.go::TestJournalCorruptionNeverRepairsPublishedPrefix`、`storage/rotation_contract_test.go::TestRotationPublishedCorruptionNeverFallsBackOrRepairs` | 实机平台声明仍 G-10 |
| I-19 | sync 失败无 ACK、writer fence | 已有实现/回归 | `storage/journal.go`、`storage/batch.go`；`storage/journal_fault_test.go::TestJournalPublishCrashAtEveryMutation`、`storage/batch_test.go::TestPreparedBatchEveryMutationReturnsUnknownWithoutPartialACK` | 逐故障模拟与设备真实证据区分 |
| I-20 | 标注/修剪不改原事件 | 部分 | `graph/prune.go` append-only PruneRecord，`engine/graph_join_core_test.go`、`engine/maintenance_test.go` | D §16.2 可撤回 Annotation contract 未找到；不能把 Prune 冒充 Hide/Annotate |
| I-21 | 受控遗忘显式降低恢复能力 | 未启用条件 | `storage/compaction.go` AuditFull、不删权威事务；`storage/maintenance.go` 仅 quarantine；`docs/formats/compaction-experimental-1.md` | BoundedRetention/Erasure 未开放，不计已实现；当前隐私访问拒绝仍 G-07 |
| I-22 | accepted、settled 分开，补结算不重跑 | 已有实现/回归 | `agentprofile/control.go` Accept/Settle；`agentprofile/recovery_test.go::TestRecoveryRawOrSettledResultCannotCompleteTaskOrStep`、`::TestRecoveryJoinCannotObserveRawSettledChildAsCompleted` | 接受→知识消费原子闭合仍 G-05 |
| I-23 | 当前控制高水位不倒带 | 部分 | `engine/anchors.go`、`engine/cursor.go`、`engine/agent_inputs.go`；`engine/cursor_view_test.go::TestDomainCursorViewReadsRevisionStateAndPreservesFence` | authority 子集已在当前根校验，ACL/预算/grant 高水位尚 G-03/G-07 |
| I-24 | 模型声明不自动授权/验证/晋升 | 部分 | 静态 closed command、可信 Principal；`engine/memory.go`、`memory/command.go`；`engine/memory_core_test.go::TestMemoryCoreRejectsRawSnapshotsForgedSourcesWithoutFencing` | 完成验证只校验已发布 ID，尚缺 Goal/产物类型化绑定 G-11；核心不能证明宿主说法为真 |
| I-25 | 因果祖先不自动已观察 | 已有实现/回归 | `knowledge/import.go`、`engine/knowledge.go`、`engine/node_events.go`；`engine/knowledge_core_test.go::TestKnowledgePacketRequiresAdoptionAtSourceFence`、`engine/node_explore_test.go::TestAdoptedEventNavigationDoesNotGrantUnadoptedEvidence` | 不能因 typed reference 导入而授予任意 raw ancestor |
| I-26 | maintenance 同 owner，重排不丢事务/floor | 已有实现/回归 | `engine/maintenance.go`、`storage/compaction.go`；`storage/compaction_test.go::TestCompactionCrashAtEveryMutationKeepsCompleteCanonicalHistory`、`engine/maintenance_test.go` | 暂停/空间开销实测 G-09；非在线无停顿 |
| I-27 | 内容/目的地/身份 grant 与 revoke 唯一顺序 | 缺实现 | 最近机制为 `agentprofile.ExecutionAuthority`、`ActivitySpec`、`AttemptControl`，`engine/node_reader.go::nodeOutputGate` 仅所有权 | G-03/G-07；现有 mechanism 不等于 ReadEmissionGrant/DispatchGrant |
| I-28 | Continuation、处理结果、下一意图原子；partial 非 final | 部分 | `agentprofile/recovery_types.go::CheckpointCommand` 只原子 Intent+Continuation；`agentprofile/recovery_test.go::TestRecoveryAtomicCheckpointAndRebindRequiresFencedGeneration` | G-04/G-05/G-06；ActivityResult 无 typed partial chunk/finalization contract |
| I-29 | published changefeed 与补读 | 后续明确范围 | D §18.4/§20、A §12.4 明确 M7 | 不建空壳/不算 V1 已实现 |
| I-30 | 广播独立游标与消费/checkpoint 原子 | 后续明确范围 | 同上 | 当前 MessageConsume 不是此订阅能力；但 I-28 的业务消费原子仍 V1 要求 |
| I-31 | Snapshot→Watch 同 fence 与过期 | 后续明确范围 | 同上 | 普通 ViewRef 不是 Watch cursor |
| I-32 | context 模式/anchor/部分状态明确 | 部分 | `api/contracts.go`、`engine/node_queries.go`、`engine/node_explore.go`；`engine/node_queries_test.go::TestNodeEntitiesLifecycleDetailContextAndBoundPagination` | G-12；仅 neighbors/parents/children，未完整实现 inputs/receipts/stateDiff 等分区 |
| I-33 | Core/Profile/宿主与版本保护双形态一致 | 已有实现/回归（当前子集） | `tests/architecture/dependencies_test.go`、`service/agent.go`、`service/domain.go`；`service/domain_test.go::TestDomainServiceRejectsPrincipalSpoofScopeAndRawMutationInjection`、`tests/host/long_task_test.go` | 新增 capability/复合命令/grant 必须对称；格式冻结仍 G-10 |
| I-34 | Event stream/主体/记录者/触发归因分离 | 部分 | `model/event.go`、`eventlog/event.go`：principal stream、RecordedBy、Tx/Method、前序因果；`engine/events_test.go` | 只有 core.command_applied，不具完整 Subject/TriggeredBy/Activity 执行流 schema；W 的各事实来源需注册 |
| I-35 | Fence 固定控制根，Cursor 不猜控制历史 | 已有实现/回归 | `engine/anchors.go`、`engine/cursor.go`、`engine/knowledge_cursor.go`；`engine/cursor_test.go::TestDomainCursorUsesRevisionAtSelectedFenceAndSurvivesReopen`、`engine/knowledge_cursor_test.go` | 当前授权覆盖范围见 I-23 |
| I-36 | 同 Actor 多 E 隔离，恢复不清知识 | 已有实现/回归 | `knowledge/revision.go`、`engine/knowledge.go`；`engine/knowledge_core_test.go::TestKnowledgeCoreHistoricalKeyIsolationRetriesAndRecovery`、`engine/knowledge_owner_test.go`、host reopen | typed Import schema4 组合 G-01 尚需关闭 |
| I-37 | ActionIntent 固定原请求；rebind 收敛旧结果/fence | 部分 | `agentprofile/dispatch.go`、`engine/action_gate.go`、`engine/agent_inputs.go`；`agentprofile/recovery_test.go::TestRecoveryAtomicCheckpointAndRebindRequiresFencedGeneration`、`engine/agent_recovery_integration_test.go` | 同私有 group 中 receipt/absence 竞态、重复 RequestKey 保留槽及 replay 版本边界需独立反例 |
| I-38 | 唯一 Query 启动/Invocation/Activity 槽 | 部分 | `agentprofile/control.go` StartWork/SpawnChild/CreateActivity；`agentprofile/control_version_test.go`；Invocation 槽当前 `(E,Step,ProviderCallKey)` | W 要 binding acceptedResult/tool definition/version/dispatch class；当前 Invocation 未存 source acceptedResult，G-05/G-06 |
| I-39 | 祖先取消与重派保留责任，无 detached | 已有实现/回归（当前控制子集） | `agentprofile/control_barriers.go`、`control_lookup.go`；`agentprofile/recovery_test.go::TestParentFencePropagatesToDescendantAuthority`、`::TestRecoveryChildCancellationRetainsUnknownEffects`、`::TestRecoveryGoalBarrierIncludesGrandchildren` | 父 resume/孙 Spawn 并发、遗留 schema replay 与真实资源 fence 仍需证据；宿主 attestation 不等价物理资源隔离证明 |
| I-40 | CP 仅此前已发布源，无 hash 环 | 已有实现/回归 | `knowledge/packet.go`、`engine/knowledge.go`、`engine/publication_refs.go`；`engine/publication_group_test.go::TestSourcePublicationResolvesItsOwnMemberWithinOnePhysicalGroup`、`engine/agent_render_recovery_test.go` | 新 Summary/复合命令不可引用自身最终摘要 |
| I-41 | raw 引用/逻辑关系/投影分开，不靠 latest | 部分 | 各 `references.go`、`engine/content_closure.go`、`engine/node_catalog.go`；`engine/content_closure_test.go`、`graph/references_test.go`、`memory/references_test.go`、`knowledge/references_test.go` | G-01 证明新组合尚不能恢复；所有新增 schema 需 codec+closure+View 组合证据 |
| I-42 | 新父目标/子继承失配 Blocked、受信 Rebase | 已有实现/回归（当前子集） | `agentprofile/control_lookup.go`、`engine/agent_goal_inputs.go`、`agentprofile/goal_knowledge.go`；`engine/agent_goal_inputs_test.go` 两个实际 adoption/孙继承反例、`agentprofile/recovery_test.go::TestRecoveryGoalBarrierIncludesGrandchildren` | GoalRef/constraints 的 immutable 定义与 Task 逻辑 currentGoal 拆分程度仍较主设计简化，格式冻结前应明确 |

## M0–M6 阶段门槛

| 阶段 | 四文档要求 | 状态 | 直接实现/证据 | 还缺什么 |
| --- | --- | --- | --- | --- |
| M0 | D §22.1/A §17.1：核心身份、根/错误/引用、首批向量与独立参考模型 | 部分 | `model/`、`codec/`、`api/`、`tests/architecture/dependencies_test.go`；`codec/*test.go`、`engine/records_test.go`、`state/radix_test.go`、`graph/correctness_test.go` | 全领域独立 oracle G-08；新增 schema 明细/invalid corpus |
| M1a | A §17.2：frame/事务规范、正反向量/fuzz | 已有实现/回归 | `storage/frame.go`、`transaction.go`；`TestFrameGolden`、`TestTransactionIndependentGolden`、截断/翻转/fuzz 测试 | 冻结/平台证据 G-10 不由此自动完成 |
| M1b/c | Create/Open/P1/receipt/CAS/RootSet/replay、逐同步故障 | 已有实现/回归 | `storage/journal*`、`engine/replay.go`、`engine/recovery_fault_test.go`、`engine/oracle_test.go` | 真实平台等级需声明；不作设备耐久断言 |
| M1d | inflight/group/取消不撤销 accepted、每笔原 proof | 已有实现/回归 | `engine/admission.go`、`coordinator.go`、`staging.go`；`engine/admission_test.go`、`coordinator_test.go`、`speculative_result_test.go` | 最终完整退出码，不能只记录部分输出 |
| M1e/f | rotation、跨段/reader pin、单 owner、最小 IPC/SDK | 已有实现/回归 | `storage/rotation*`、`segmented*`、`reader_pin.go`；`storage/rotation_contract_test.go`、`service/security_test.go`、`client/client_test.go` | 支持矩阵/协议冻结 G-10 |
| M2 | D §12/13、A §17.3：Revision/Fork/Diff/Trace/Join/Prune、复杂基点 | 已有实现/回归 | `graph/`、`engine/graph*.go`；`graph/correctness_test.go::TestExactMergeBasesCrisscrossAndCycleDetection`、`::TestDiffAndJoinAgainstIndependentMapOracle`、core joint root/fault tests | Annotation、跨字段宿主 validator、完整 profile causal schema 尚部分 |
| M3 | 标准 Agent profile、结果/消息/目标/权限/恢复原子性 | 部分 | `agentprofile/`、`engine/agent*.go`、host 场景；多项 rebind/unknown/raw-settled/祖先 fence 测试 | G-03/04/05/06/11、I-34/38；不能只凭 Task/Activity 类型存在关闭 |
| M4 | D §14、A §17.3：KnowledgeKey/Anchor/CP、selector、Summary/召回、真实输入 | 部分 | `knowledge/`、`engine/knowledge*`、`engine/recorded_input_test.go`、`agent_render_recovery_test.go` | G-01/02/07/09；RecordedInput 保存不等于 selector 和 Summary 已实现 |
| M5 | Memory/ExperiencePath/撤回/保留/归档/维护，效果独立评测 | 部分 | `memory/`、`engine/memory.go`、`storage/compaction.go`；`memory/audit_regression_test.go`、`engine/maintenance_test.go`、host 场景 | G-07/09；跨 Space 导入/脱敏 provenance mapping、Annotation/导出归档契约未见 |
| M6 | A §17.3/18.2、D §22.1：双形态真实任务报告、故障对齐、开销效果、格式支持矩阵冻结 | 部分 | `tests/host/long_task_test.go` 同一确定性 provider+本地副作用场景分别跑 embedded/Unix SDK；service SDK conformance | G-09/G-10；单场景没有覆盖全部 C01-C20 原子反例，见下表 |

## P/A 的公开 API、格式和平台要求

| 条款 | 状态 | 代码/工件/测试 | 审计结论 |
| --- | --- | --- | --- |
| A §2/P §7：独立 Go module、无 GSEA/Eino/internal/第三方生产依赖 | 已有实现/回归 | `go.mod`、`tests/architecture/dependencies_test.go` | ATE 独立，不把 GSEA agent loop 拖入内核 |
| A §3–7：封闭 transition、五根、候选、单 publisher | 已有实现/回归 | `engine/plan.go`、`publication_plan.go`、`typed_value.go`、`replay.go`；core raw bypass/group/fault tests | 仍需每个新复合 transition 的五根故障证据 |
| P §2/A §11：Anchor latest 原子捕获、View 生命周期/pin、有界错误 | 已有实现/回归 | `engine/views.go`、`engine/node_reader.go`、`storage/reader_pin.go`；`service/security_test.go::TestDisconnectCancelsWaitAndRetiresOwnedView`、`::TestConnectionOwnsViewsAndSnapshotNeedsOperator` | 历史成员关系不是永久出站授权；G-03/07 |
| P §3/§4/A §11.2：list/inspect/expand/trace/search/read_content | 部分 | `api/contracts.go`、`api/nodes.go`、`engine/node_*`、`service/nodes.go`、`client/nodes.go`；nodes/explore/content tests | Basic 和 bounded summary search 存在；GetNodeContext 分区 G-12，typed object 并非所有内容都能 raw ReadContent |
| P §4.3：拆分 SDK 方法语义等价，不要求万能 Explore union | 已有实现/回归 | `client/nodes.go`、`service/nodes.go`；`service/nodes_test.go::TestNodeExploreWireMatchesLibraryAndRejectsForeignViews`、`service/node_content_test.go` | 无 ExploreRequest 本身不是缺陷；允许拆分 capability |
| P §5.1/A §13.2：有界 length+JSON、顺序单连接 | 已有实现/回归 | `protocol/local.go`、`service/session.go`、`client/client.go`；`protocol/local_test.go`、`service/security_test.go::TestFrameReceiverLimitsBeforePayloadRead` | 禁未知字段/重复键/无效 UTF-8/溢出；不推断已做 push/多路复用 |
| P §5.2/A §13.3：可信 Principal/Space、session→View、单 owner | 已有实现/回归 | `service/service.go` Credential、`session.go`、`storage/directory_unix.go`；`service/security_test.go`、`storage/directory_test.go::TestDirectoryOwnerLockSubprocess` | token 为受控本地身份映射，非动态 per-object ACL revoke |
| P §5.3/A §13.4：断线保原 RequestKey、未知结果/View 回收 | 已有实现/回归 | `client/client.go`、`service/session.go`；`client/client_test.go`、`service/security_test.go::TestDisconnectCancelsWaitAndRetiresOwnedView` | 须将新复合命令故障对齐加入 conformance |
| P §6/§8/A §8.5/18.2：registry、版本/capability golden、冻结迁移 | 部分/缺冻结证据 | `docs/formats/*-experimental-*`、`engine/registry_test.go`、`protocol/local_test.go`、agent codec golden | P §8 明言未冻结；无统一 V1 compatibility corpus/迁移策略和格式发布 manifest，G-10 |
| A §15/16.3/18.2：支持平台、DurableIO 和证据等级 | 部分/缺冻结证据 | `storage/directory_unix.go` build tags darwin/linux；`directory_unsupported.go` 明确 unsupported；native/faultio tests | 能编译/某次本地运行不等于 Darwin+Linux 各文件系统/断电认证；需具体支持矩阵 |
| D §21.4/A §16：模型检查与独立差分 | 部分/缺证据 | radix/graph naive oracle 与 faultio 枚举存在；`reference/engine.go` 是 receipt oracle | 未找到 spawn/cancel、join/timeout、grant/revoke 等独立小状态机枚举工件，G-08 |

## W C01–C20：真实路径及原子边界

此表区分“场景顺序跑过”与“该对象组原子提交并逐边界崩溃跑过”。`tests/host/long_task_test.go`
确有 native store、真实 Unix socket 和一次 subprocess 文件效果，不是纯包 mock；其模型输出仍是确定性本地数据。

| Walkthrough 场景 | 状态 | 当前证据 | 仍缺闭合 |
| --- | --- | --- | --- |
| C01 Query intake | 部分 | `Lifecycle.PutQuery` 记录 input/fence；host 第一次 PutQuery | pending-intake/Task 前 PendingAction 不可表达；G-04 |
| C02 路由/StartIntent/Root | 部分 | `Control.StartWork` 原子 Task+Execution+启动槽；`agentprofile/control_version_test.go` | exact route/config/resource baseline/query action 未整体保存；C01→C02 退出恢复仍 G-04 |
| C03 原知识→CP→模型 Activity/Attempt | 部分 | host 的 model helper、RecordedInput chunk read；`engine/agent_render_recovery_test.go` | selector/Summary G-02；Create→Claim→Dispatch 后新 revoke gate G-03/G-07 |
| C04 主模型结果接受→Invocation→下一动作 | 部分 | Capture/Accept、RecordInvocation、FinishStep 分命令；raw/settled barriers 测试存在 | 无一个原子处理模型结果+Invocation 集合+Step/下一意图 transition，G-05 |
| C05a/b 子 Task/Execution/seed/authority/Continuation | 部分 | SpawnChild 唯一槽；host 后续 GrantAuthority、Import seed | 子 Execution 初始知识/续行动作不是 Spawn 同事务；G-06 |
| C06–C11 grep Activity/Attempt/result/acceptance/settlement | 部分 | `engine/agent_recovery_integration_test.go`、host external subprocess 写后退出/unknown/reconcile/import | Invocation 未绑定 acceptedModelResult/tool version，partial stream/end 状态未建模；G-05 |
| Observation→pending-render→CP 中间退出 | 已有实现/回归（阶段协议） | `engine/agent_render_recovery_test.go::TestCheckpointPendingRenderRecoversOriginalKnowledgeAndRecordedInput`；`engine/publication_refs_test.go` | 该测试是多个持久阶段+ActionIntent，不能据此推断结果消费/知识根的原子组已实现 |
| C12 Message/child result ready | 部分 | SendMessage、CompleteTask、accepted/settled 独立状态；host 消息 | Task result_ready/awaiting_review、type Validation/Artifact 当前只 opaque 内容/TaskOpen→Completed，G-11 |
| C13 父消费及知识/下一动作 | 部分 | ConsumeMessage 唯一 `(Message,Recipient)`；host 后续 Import selected observations | 消费、Observation/Knowledge、Continuation 分多笔；G-05 |
| C14–C18 领域 merge、Workspace integration、验证和 TaskOutcome | 部分 | 核心 Joint root merge、双 CAS；host 本地文件结果；`engine/graph_join_publication_test.go` | engine 不负责真实工作区，此责任可由 host 提供；但 typed Validation 与 Goal/产物/命令绑定仍 G-11 |
| C19–C20 final Answer/投递 | 部分 | AnswerQuery 独立 response slot；host 最后 AnswerQuery | 无独立投递/重发状态契约与“完成后答复失败只补答复”端到端反例 |
| Fork/严格过去/复盘/Prune/Memory/Compaction | 已有实现/回归（基础子集） | host 明确两个分支、失败反例、Memory/Path、Prune 保祖先、reader 活跃 compaction | 效果对照与跨项目/Space 复用 G-09；Privacy/ACL 和 Summary G-02/G-07 |

## 收口顺序与当前结论

1. 先关闭可复现 codec/恢复问题 G-01，以及 action/replay/祖先 fencing 的组合反例；不要开始格式冻结。
2. 设计并实现当前控制与原子消费/处理边界 G-03–G-07、G-11，补真实故障路径；单独控制和只读数据 API 的既有语义保留。
3. 完成 Context selector/Summary/provenance G-02，才能将长期效果评测绑定到真实实现，而非测试内硬编码选择器。
4. 补独立 oracle、性能/效果对照和平台/迁移/冻结工件 G-08–G-10，分别记录实测范围与未证明范围。
5. 每项附最终测试退出码与报告后再讨论 V1 完成。整体工程重构目前只有第一阶段同包拆分，不能与上面的语义收口混为同一完成状态。

**静态交叉审计的结论：目前不能宣布严格 V1 或四文档逐项验收已全部完成。** 底层日志/发布/恢复、
图合并、知识隔离、原输入保存及 AuditFull 的实现与回归证据较扎实；原子 Agent 工作单元、出站撤回、
完整 Context/Summary、效果与发布冻结存在明确未关闭条目。这些条目必须实际实现或通过有依据的设计审计
给出更合理的替代方案，并保留变更理由，不能直接删掉要求。
