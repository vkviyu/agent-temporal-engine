# M1b：单段物理发布与恢复

> 这是此前阶段的历史记录；当前能力与保证见 [M1c 状态](M1C_STATUS.md)。

当前版本 `0.1.0-dev.m1b`，Go 1.27.0。独立 module、无第三方依赖、无 `internal` 目录；全部包仍为 v0 experimental。完整目标设计不代表已经实现的能力，本文件记录当前保证。

## 已实现的纵切面

| 部分 | 代码与保证 |
| --- | --- |
| 文件边界 | `storage.DurableIO`、ReadFile、DurableFile、OwnerLock；相对路径、独立文件/目录同步、可注入故障 |
| 平台适配 | `OpenDirectory` 使用 os.Root 限域；Darwin/Linux 的非阻塞 flock；持久锁文件不移除；未知平台拒绝 native 打开 |
| 受信目录 | root/managed 目录和已有 regular 文件必须归 euid 所有且无 group/world write；拒绝 managed symlink；新目录 0700、新文件 0600 |
| 初始化 | `CreateJournal` 只接受现有空目录（允许已有 owner 锁文件）；写身份、genesis header、不可变 manifest 与 CURRENT，全部按发布顺序同步 |
| 发布 | `Journal.Publish` 接受已准备的 M1a Transaction；batch=1、单 segment、单同步协调器；数据同步、manifest 安装/目录同步、CURRENT 原子替换/目录同步后才交换内存位置并返回 |
| 未知结果 | append 尝试以后任何 I/O 错误均 CommitUnknown + Fenced；后续 Publish/Published 被拒绝，Status 只保留上次位置供诊断 |
| 物理重开 | `OpenJournal` 严格读取 CURRENT 指定 manifest；验证 identity/header/完整前缀/链/tip/root，重新同步发布入口，再返回 ready；不扫描“最大代际”猜最新状态 |
| 尾部 | 默认返回 verified report + RecoveryRequired，保留文件；显式 TailQuarantine 先同步原始尾部副本及名字，再截短并同步 active segment |
| 生命周期 | Close 先停止新操作，确认 segment 关闭后释放 owner；关闭失败保持 Closing，保留句柄逐步重试，已退休句柄幂等关闭 |
| 边界 | 每笔事务已有帧/字节限额；另加总 published 字节和事务数上限，避免历史 TxID 去重索引隐性内存增长 |

M0/M1a 的规范命令编码、参考 receipt oracle、NodeRef/Anchor 校验、frame/transaction scanner 保留。`reference` 仍明确易失，只有物理 Journal 完成 P1 才产生 Publication。

## 对外入口

库的当前入口位于 `storage`，没有提供冒充完整语义引擎的根包 Create/Open：

- `OpenDirectory(existingPath)`：获得 native I/O；调用方持有并管理其生命周期。
- `CreateJournal(io, JournalOptions)`：初始化空目录，返回持有 owner 的 Journal。
- `OpenJournal(io, JournalOptions)`：恢复已存在的物理 journal，返回 Journal、RecoveryReport、error。
- `Journal.Publish(ctx, preparedTransaction)`：发布一笔已准备物理事务；调用者取消只在写入前被采纳，不中断正在进行的 P1。
- `Published()` 返回当前可用的物理 Publication；`Status()` 仅诊断；`Close()` 关闭失败时应保留引用重试，成功后再关闭 DirectoryIO。

Create/Open 失败时也可能仍有尚未确认关闭的资源。若 error 可通过 `errors.As` 取得 `*storage.CleanupError`，保留 DirectoryIO 并重试其 Close；它按文件→segment→owner 顺序完成释放。临时 metadata 文件关闭失败会由 Fenced Journal 的 Close 统一收尾，不丢失句柄。acquisition 方法在报错时若已获得句柄，须同时返回该句柄，协调器也会清理这一情况。

Publication 包含 Store、Generation、Position、PostRootSet、PublishedEnd、Profile；Fence() 可表达其物理提交链位置。它本身不验证域根存在性、不执行命令、不分配 Agent authority。

RecoveryReport 的 `Verified` 表示 CURRENT 引用链和前缀字节已校验；`PublicationSynced` 表示本次打开又完成发布入口同步。默认 tail 拒绝、或 witness 同步失败时，不返回可用 Journal，即使 Verified=true。报告也包含已验证范围、扫描事务数、尾部大小/处置和隔离文件。

二进制：

```sh
go run ./cmd/ate version
go run ./cmd/ate init-journal /path/to/existing-empty-directory
go run ./cmd/ate recover-journal /path/to/store
go run ./cmd/ate recover-journal --quarantine-tail /path/to/store
```

输出明确区分 `publication_verified`、`publication_synced` 和 `semantic_replay_verified`。最后一项当前始终 false。`check-journal` 仍只读取原始帧流，不是 store 恢复入口。

## 当前资源与平台范围

| 配置 | 默认 | 硬上限 |
| --- | --- | --- |
| 单 frame payload | 16 MiB | 16 MiB |
| 单事务物理字节 | 64 MiB | 64 MiB |
| 单事务帧数 | 65,536 | 65,536 |
| JournalOptions.MaxPublishedBytes | 1 GiB | 64 GiB |
| JournalOptions.MaxPublishedTransactions | 100,000 | 1,048,576 |

选项为 0 使用默认；可以降低。超过资源限制明确拒绝，不截断历史、不牺牲校验。隔离 tail 的大小不超过所选单事务字节上限，过大则保留并返回 ErrLimit。历史 TxID 索引当前保存在受上述数量限制的内存 map；持久状态索引在 M1c 接入。

Durability profile 为 `file-sync-experimental-1`，确实执行文件与目录同步；它不是硬件/文件系统掉电认证。Darwin arm64 已实际执行 native、race、子进程退出测试；Linux 适配已交叉编译，尚未在 Linux 文件系统实际验收；其他平台只支持纯 codec/模型，native 打开返回 Unsupported。

根目录必须已经存在，其名字/父目录的持久性由调用方保证。owner 锁协调合作的 writer；同 UID 的外部程序不得绕过锁修改文件。当前权限检查使用 POSIX owner/mode bits，不是完整 ACL、加密或对抗恶意宿主的认证方案。

## 本次恢复验收

- 故障模型分别保存 volatile 文件、已同步 inode、volatile 与已同步目录项，崩溃只保留其定义的 durable 状态；模型自身也有独立测试。
- 初始化、发布、尾部隔离的每个变更 I/O 均覆盖 before/after 失败再 crash；记录旧 ACK、unknown 和恢复前缀，验证没有半笔提升或 ACK 丢失。
- 隔离截短已持久化时，必须已经存在完整持久诊断副本；允许保留部分 orphan 证据，不靠清理证据“修复”。
- 进程重启看到新 CURRENT 后重新同步，再 model crash，仍恢复相同新位置。
- 真实 native 子进程分别在 CURRENT 替换前、CURRENT 目录同步后直接退出，验证锁释放及旧/新前缀恢复。
- published prefix 损坏、缺失、CRC 正确但 tip 改写、CURRENT 缺失、只剩 orphan manifest 时均拒绝；不回退旧 manifest，不在 floor 内截断。
- 未发布尾部覆盖完整事务、残帧、残事务、CRC 错误和任意垃圾字节，统一保留原始证据，不从其内容推断发布。
- 覆盖短写、零进展、n>0 附错误、ReaderAt 完整读取附 EOF、短 EOF、关闭重试及 witness 同步失败。
- 覆盖多个并发已准备事务争抢同一前缀时只有一个成功，其他明确 Precondition，崩溃后仍只有一笔；覆盖构造失败的 cleanup 可继续释放 owner。

故障模型不模拟所有实际存储设备的扇区撕裂、缓存策略或拜占庭修改；真实进程退出测试也不能代替掉电测试。

## 下一阶段

M1c 接入规范对象/引用验证、真正 RootSet 与最小持久 radix、StateOps replay、持久 command receipts 和域 CAS。之后才可提供完整语义引擎的 Create/Open/Commit/LookupRequest。当前尚不能声称 Agent 任务/权限/记忆恢复已经实现。

group commit、段轮转/跨段事务、reader pins、服务/SDK、时间旅行/Fork/Join、ContextPacket 和记忆仍按架构文档继续推进。

本次实际命令与故障工件见 [M1b 验收记录](validation/M1B_2026-10-30.md)。
