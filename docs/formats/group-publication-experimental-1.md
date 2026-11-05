# ATE-GROUP-PUBLICATION-EXPERIMENTAL-1

> 本文保留原阶段定义；M1e 的多段扩展与逻辑地址说明见 [segmented-store-experimental-2](segmented-store-experimental-2.md)。旧单段字节保持不变。

状态：experimental。此协议扩展单 segment 的 P1 发布调度，不改变事务帧、摘要链、manifest 或 CURRENT 的磁盘编码。故障模型测试验证 `FileSyncExperimental` 接口约定，不构成操作系统、文件系统、设备或断电硬件认证。

`PrepareTransaction(tx, limits)` 只执行 `EncodeTransaction`，持有其独立拥有的规范字节和发布所需的值元数据，不执行 IO。返回后，修改原始 Transaction、Records payload、RefUpdates 或 ControlUpdates 不会改变准备结果。所有准备字段私有；`Position()` 和 `Size()` 只返回值。准备期间不得并发修改输入。

`PublishPreparedBatch` 接受 1 至 256 笔事务，准备字节合计最多 64 MiB；单笔仍受 64 MiB 以及 journal 配置的更低限制约束。批内 LSN 每笔递增一，摘要必须连续衔接当前已发布位置。发布前验证全部链、已发布及批内重复 TxID、累计事务/字节配额、各笔 frame/payload 限制和实际 segment 长度；任何输入错误都不会写入前缀。

P1 持有 journal 锁，按顺序连续追加整批事务，执行一次 segment Sync，计算全部新前缀摘要，生成并安装一个 manifest 和一个 CURRENT。沿用已有的 manifest 文件同步、manifest 目录同步和 CURRENT 文件同步步骤，最终只执行一次 CURRENT 所在根目录 SyncDir。此同步成功后才交换内存 Publication 并一次性返回全部结果。单笔 `Publish`、`PublishWithRecords` 也进入同一条 P1 实现。

LSN 每笔增加，Generation 每批增加。`PublishedBatch.Publication` 是整批最后一笔的 tip。每笔 `PublishedTransaction.Publication` 具有相同的新 Generation，同时保存自己的 LSN、CommitDigest、PostRootSet 和 PublishedEnd；RecordLocations 是从旧已发布末尾逐笔累计的绝对 payload 位置。每笔位置都是最终发布前缀中的已提交成员，不会生成额外的中间 manifest 或 CURRENT。

第一次 append 尝试前可响应 caller cancellation；P1 开始后不再因 cancellation 停止。第一次 Write 开始后的任何失败均返回严格零值 `PublishedBatch` 和 `ErrCommitUnknown`，并使 journal 进入 Fenced。不会对批内前缀返回部分 ACK。`PublicationError.TxID` 保留单笔兼容字段，`TxIDs` 提供整个批次的诊断 ID；旧 Publication 只保留为 fenced 状态诊断。

恢复仍只服从经过验证的 CURRENT/manifest。即使磁盘尾部包含多笔完整、摘要连续的 Commit，也不能提升未发布尾部。恢复得到旧 tip 或完整新 tip；默认保留并拒绝未发布尾部，显式 TailQuarantine 才能隔离并截断。隔离尾部的整体上限为 64 MiB，即使 journal 将单笔事务预算配置得更低也不变；已发布前缀的每笔事务仍遵守其单笔限制。此扩展不改变回放的事务验证规则。

`NormalizeJournalOptions` 是无 IO 的纯验证与默认值补齐入口，允许上层在准备及调度前使用与 Journal 相同的配额配置。
