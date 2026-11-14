# V1 四文档与当前实现交叉审计 · 2026-11-06

严格 V1 尚未全部验收。本轮按四份原始设计逐条验证，补齐了流式结果完整性、最终答复来源和原始内容撤回检查，也沿原条款完成了嵌入式读取许可、读取切面和物理存储短锁拆分。严格验收仍保留平台硬件掉电与真实模型效果的证据边界，不能把受限实验外推为这些外部证明。

基线为 `0417ac1` 加本轮工作区变更。D/A/P/W 分别指总体设计、实施架构、API 协议与执行场景。历史报告保留其日期和源树条件；当前状态以本报告为准，不将既定 V1 条款重新归类到后续版本。

## 本轮修复与条款对应

| 条款 | 真实缺口与当前实现 | 行为验证 |
| --- | --- | --- |
| D §11.10、I-28；W §9.3 | 原先只存终结 Payload，不能表示已持久分块和结果完整性。新增 `OutputChunk`、Control opcode 34，结果必须显式为 full/truncated/partial/unknown。分块绑定 Activity、Attempt、精确请求、Incarnation、响应及连续序号，单 Attempt 累计 1 MiB，终结结果逐个核对有序清单和连接后的字节。 | `agentprofile/output_test.go` 独立向量、乱序/重复/跨响应/累计限额；`engine/output_stream_test.go` 未终结前缀重开、partial/unknown 拒收、CURRENT 替换前后全有或全无。模型结果要求 full；工具可显式接受 truncated，Outcome 与 Capture 正交。 |
| D §11.12–11.13；W C19/C20 与 §13 | 原先 QueryAnswer 不能证明来自最终模型或已完成任务。现在 Body 必须逐字节等于 accepted、full、success 的 Query 模型结果；最终决策不能再带工具调用。已启动任务要求精确的全部 TaskOutcome 与预先发布的 `FinalAnswerInput`。 | `agentprofile/answer_test.go`、`engine/answer_test.go`、`engine/query_turn_test.go`、`tests/reference/query*_test.go`；`tests/host/answer_test.go` 同一场景跑嵌入式/Unix SDK：Task 完成，最终模型失败，重开后只重试答复，保持原成果和不可变回执。 |
| D §14–§15、§17.6；P 当前读取授权 | Summary 的逻辑 ID 有来源门禁，但它的独立 Generation.Output 原始对象曾绕过来源撤回。现在 Packet/Summary 与有界来源邻接索引同事务发布；索引覆盖 logical ID、RecordedInput、Generation.Output、嵌套来源与重复原始对象的全部明确声明。 | `TestSummaryCoreCurrentWithdrawalGatesRawGenerationOutput`、`engine/derivation_integration_test.go`：原始 State、固定 View、ReadContent、传递 Packet 链、共享原始输入、重开、AuditFull、CURRENT 故障。事实修正/UseBan 保留审计读；隐私删除/ACL 撤回禁止披露。 |
| D §17.6；W 最终答复证据 | 最终模型的 Query Packet 不含执行私有 Items，仅校验 typed Activity 曾允许从原始请求/证据键绕过成果撤回。创建最终 Activity 时，将 witness→Outcomes、request/packet→witness 与 Agent 状态原子写入 LiveControl。当前闭包同时查询 ExecutionKnowledge 和 LiveControl。 | `engine/answer_disclosure_test.go`：撤回产物或验收证据后，原始请求、原始 witness、固定 View 与重开均拒绝。CURRENT 前后 Activity、receipt 与来源邻接全部一起出现或一起不存在。 |
| D §11、W Step 完成 | 旧公开 CompleteStep 曾对无 authority 执行生效。Engine 现在统一拒绝该入口，以受管理 Work 完成步骤。纯 Ledger 是独立状态机制，不提供 Engine 持久化绕过。 | `engine/work_step_completion_test.go`、`agentstore/live_control_test.go` 及对应公共入口回归。 |
| D §21–§22、A §18.2、P 平台矩阵 | 已补 Linux/arm64 实际运行、ext4 真正 ENOSPC 与 guest 重启后 ACK 前缀恢复。 | [原生平台报告](V1_NATIVE_PLATFORM_2026-11-06.md) 和 [支持矩阵](../PLATFORM_SUPPORT.md)。有序 guest 重启与物理设备掉电分开记录，不作未测试的硬件保证。 |

## 最终答复的边界

任务先通过带类型验收的 CompleteTask 发布 TaskOutcome。随后创建 Query-scoped `query_decision`，它的 ContextPacket 与实际 provider 请求均已发布；`ActivitySpec.FinalAnswerInput` 指向 schema 1 的不可变宿主渲染见证，绑定 Query、按 Task 排序的全部 Outcomes、Packet、Request、精确 RequestDigest 与 HostPolicyVersion。HostPolicyVersion 必须等于 Packet.Renderer。

引擎认证身份、任务归属、结果与验收来源、发布 cut、完整结果接受和当前撤回策略。命名的宿主 renderer 负责把证据确实渲染到供应商请求中；引擎不解析任意供应商 JSON 的语义，也不改写原请求去嵌入见证。最终模型失败不会撤销已完成任务，也不会制造已经完成的 QueryAnswer。

## 格式切分与重放

当前格式为 Control 6、Packet 5、Summary 2，新增通用来源邻接 schema 1。Control 1–5、Packet 1–4、Summary 1 不属于当前预发布解码与重放边界。保留旧向量只验证明确拒绝，不增加兼容 reader，不删除现有目录，也不静默重签历史身份。

实际跨实现探针证明：旧 HEAD 写出的 Packet 4 请求在旧实现中可正常重开；若只加入邻接多写而不换协议，当前 reducer 会与历史 StateOps 不一致。Packet 5 / Summary 2 因此同时版本化了发布语义。新 reader 以 unsupported knowledge schema 明确拒绝旧实验格式；原目录仍能由原 reader 验证。当前版本内部的历史 Fence、Root、Cursor、receipt 和 AuditFull 保持原样。

此裁决遵循已记录重放必须与决策序列一致的原则。[Temporal 的安全变更文档](https://github.com/temporalio/documentation/blob/main/docs/develop/safe-deployments.mdx)要求会改变重放行为的代码变更采用明确版本策略。ATE 当前尚未发布且用户要求不保留实验兼容路径，因此采用显式拒绝旧预发布格式；未来正式版本的迁移仍需独立协议。

来源索引的编码为 `ATE-DERIVATION` + NUL、schema、ObjectID、source count、严格有序去重的来源 ID。ExecutionKnowledge 使用 Space 内 key `ate/derivation/v1/<ObjectID>`；LiveControl 使用带显式 Space 的 canonical tuple key，存储对象仍属于该 Space。内容 ID 被多次明确声明时来源集合单调合并。该保守规则可能扩大共享字节的拒绝范围，但不会通过后来的声明丢掉旧来源。闭包限制 16,384 个节点、65,536 条访问边，磁盘读取另有累计预算，不扫描整个知识分区。

## 实际运行证据

当前 schema 6/5/2 基线的 Darwin 普通测试、race、build、vet 和 Linux/arm64 原生全量均已运行通过，28 个测试包及 1 个无测试文件的 Transfer 包。最终源树在来源 witness、View 锁序、读取切面、storage lease 和 P1 writer 拆分之后重新运行这些检查。

真实模型只预先选择了 V2 `paired-1` 作为新协议烟雾验证，使用既有授权的 `gpt-6.1-sol` Responses 端点。4 次请求均返回 completed；第二轮摘要被独立事实复核拒绝，流程正常停止，没有采用该摘要、没有执行最终配对答题，因此本轮没有可比较的最终得分。保留拒绝结果，不重试到获得更好的分数，也不把实验通过解释为压缩效果提升。

## 仍未关闭的原始要求

| ID | 原始条款与实际代码 | 完成条件 |
| --- | --- | --- |
| L-01 · 当前实现已覆盖 | A §6.2/§6.3/§11.3：coordinator 的私有计算和 P1、Verify、Compact、Reclaim、Close 的实际 I/O 都在 e.mu/j.mu 外。writerMu 与 publicationPending 负责发布、Fence 和 unknown 收敛。readCut 捕获不可变根、物理 Pin 与授权策略，来源闭包、Graph/Node 遍历和对象校验在锁外完成，最终仅短锁重核 Fence、roots、生命周期和物理状态。View 与 readCut 的代际 pin 在 Close/退休时锁外释放。 | 已有 planner/P1、storage lease、View pin、typed/Graph/Node 交错测试，普通、race、Linux/arm64 全量复验通过。仍需把未来新增读取入口接入同一 readCut，并保持 map/segment 生命周期规则。 |
| L-02 · 已实现，复验见下文 | D §17.6：普通嵌入式读取已复用 PrepareReadDelivery 与单次 Consume，绑定精确请求、目的地、固定 cut 和确切授权响应。27 个普通 DTO 接口及 LoadGraph 走同一协议；ReadContent 保留原有范围许可。无 capture 的内部输出默认拒绝。没有生产调用的公开 typed-root 原始读取逃逸入口已删除，历史状态改用固定 View。 | `engine/embedded_read_delivery_test.go` 枚举公开读方法、防漏门禁、序列化锁外、撤回交错、结果副本与 OpenView 失败清理；Engine/Unix/SDK 对照及最终完整复验。LoadGraph 授权 Revision DTO 后才重建独立拥有的 graph.Store。 |

当前代码与四份文档的主要实现条款已完成交叉复验。严格 V1 的剩余状态只涉及外部证据边界，尤其是物理设备掉电和更完整的付费模型效果评测。整体代码重构仍未开始，下一阶段按 [短锁与读取交付实施方案](V1_SHORT_LOCK_REFACTOR_PLAN_2026-11-06.md) 的并发不变量继续维护。

## 第一阶段并发与交付调整

新 `writer_lock_test.go` 在真实 P1 目录 Sync 已完成但调用尚未返回的窗口暂停：e.mu 可取得，Snapshot、GetState、确定缺失 Lookup 和输出门禁及时 fail closed，不等待该 Sync，也不泄漏旧授权。私有 planner 的读取暂停时，已发布 cut 的 Snapshot/GetState 仍可读取；同时发生语义 Fence 时不得再发布 CURRENT。完成 flight 与 admission 的操作不嵌套 e.mu。维护、Close 与 writer 的所有权及 unknown 结算另有定向回归。

嵌入式库的普通读取现在先在锁外计算请求摘要，进入与 Unix 相同的 capture，按当前 cut 生成许可并消费一次，再从确切授权字节重建拥有的 DTO。许可前的序列化发生策略变化时明确返回 AuthorizationChanged，调用者不能将其当作缺失或已授权结果。OpenView 未成功交付时释放注册句柄；本轮也修复了原先 registry/core 锁序倒置和 capture 失败残留。

本轮没有把 writerMu 变成所有读者都必须等待的全局 I/O 锁。storage 的实际文件读写、当前策略投影和来源闭包都在固定 cut 与代际 pin 上运行。
