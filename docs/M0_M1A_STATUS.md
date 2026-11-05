# M0 / M1a 实现状态

> 这是此前阶段的历史记录；当前能力与保证见 [M1c 状态](M1C_STATUS.md)。


本文件区分“设计目标”和“当前代码保证”。当前版本 `0.1.0-dev.m1a`，Go 1.27.0，无第三方依赖，无 `internal` 目录；所有包为公开的 v0 experimental API。ATE 与 GSEA、Eino、模型供应商无生产代码依赖。

## 已实现

| 范围 | 当前保证 |
| --- | --- |
| 身份 | EntityKey 含稳定 ProfileID/Kind/LocalID；NodeRef 严格区分 Entity、Event、Revision、Object；内容 ID 使用小写 SHA-256 文本 |
| Anchor | 全 store CommitFence 与 Space 分离；检查 Cursor 的 store/space、KnowledgeKey 完整性；Clone 不共享可变 selector 指针 |
| 命令规范编码 | 完整 RequestKey、Method、Body 和 ExpectedFence 进入带域分隔的无歧义编码/摘要；不含连接/等待 deadline |
| 参考账本 | (Space, stable Principal, RequestID) 隔离；并发同键只记一次；相同 receipt 在新 CAS 检查前返回；不同摘要冲突；拒绝和取消不占用键 |
| 易失结果 | 参考模型只返回 VolatileAck/VolatileNotFound；缺失查询附当时捕获的 volatile fence；不同实例身份独立；不承诺重启后的对账 |
| 帧 | 固定 header、CRC32C、零 padding、尾标记；校验版本/kind/flags/ordinal/TxID；分配前检查长度和剩余事务预算 |
| 事务 | Begin → 混排 typed body → Ref/Control/Receipt → Commit；前置物理帧摘要、LSN/前摘要链和完整 CommitDigest；失败不返回部分事务、不推进已完成位置 |
| 错误 | clean EOF、结构性不完整、格式不支持、损坏、资源上限和底层 I/O 错误可区分；Scanner 首个错误后停止，不自行跳过或截断 |
| 二进制 | version/help；check-journal 对原始帧流只读结构检查，输出 publication_verified=false |

`reference` 是 receipt oracle，Method/Body 是不执行的 opaque command。它不是完整的图/领域状态参考模型。`SnapshotAnchor.Validate` 只校验结构，不证明 Cursor 已发布或调用者有权限。

## 验收工件

- `codec` 和 `engine` 的固定 golden、整数溢出、非最短编码、UTF-8、长度/边界反例。
- 独立 Python 编码器生成的完整 frame/transaction golden，不靠运行时调用被测 Go 编码器制造 expected。
- 每字节截断、合法 CRC 的非法结构、完整事务摘要不匹配、顺序/LSN 错误、失败后的原子位置不变。
- 三个 parser fuzz target：FuzzDecoder、FuzzFrame、FuzzTransactionScanner。
- 并发幂等和 race 检查；生产包依赖方向和禁止 internal/第三方依赖的架构检查。

测试通过是本次实现范围的证据，不是完整 ATE 的正确性证明。

## 尚未实现的关键保证

1. M0 的完整 Event/Revision/Profile、受保护动作槽、PendingAction、权限与授权验证、所有错误/根契约尚未落地或冻结。
2. M1b 的 DurableIO、独占 owner、IDENTITY/genesis、manifest/CURRENT、同步与原子替换、P1、Fenced 状态、故障注入尚未实现。
3. M1c 的对象 schema/引用与拓扑、真正 RootSet/StateOps replay、持久 receipts、CAS 和 Reopen 尚未实现。当前 PostRootSet/Receipt 仅是 envelope 中的非零引用，Scanner 不证明它们存在或状态转换正确。
4. M1d–M1f 的 inflight/group commit、段轮转/跨段事务、reader pins、服务/SDK 尚未实现。
5. Fork/Join、时间旅行查询、Agent 运行控制、上下文和记忆等上层能力仍按完整设计文档分阶段实现。

CURRENT/manifest 认证前缀之前，不能把 ErrIncomplete 当作允许修复的尾部。即使最后一个完整事务也可能未发布。Scanner 返回的 Position 只表示结构扫描位置，不能当作 durable CommitFence 或授权派发依据。

## 下一纵切面

M1b 从单段、batch=1 开始，先实现可注入的文件 I/O 和单 owner，建立 CURRENT/manifest 的发布证据，再接 Create/P1/Reopen 和逐点中断测试。公开门面的 Create/Open 将在这些保证真实存在后提供；不会先包装一个内存实现冒充磁盘引擎。
