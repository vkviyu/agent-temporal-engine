# M1e：日志封存、轮转与跨段事务

当前版本 `0.1.0-dev.m1e`，Go 1.27.0。独立 ATE module 保持无第三方依赖、无 `internal` 目录。本批扩展物理存储生命周期，核心 StateOps、RootSet、RequestKey 与 receipt 语义不变；完整 Agent profile 仍未实现。

实际检查与故障证据见 [M1e 验收记录](validation/M1E_2026-10-31.md)，字节协议见 [segmented-store-experimental-2](formats/segmented-store-experimental-2.md)。

## 本批完成

| 能力 | 当前保证 |
| --- | --- |
| 私有布局规划 | 写入前计算整批 frame 边界、段数、header/footer、认证范围与摘要；超限和取消不写盘，不修改已发布目录或 hash |
| 自动轮转 | SegmentTargetBytes 是软目标；单个 frame 不拆分，过大 frame 在空段完整存储；事务可从一段继续到下一段 |
| 封存链 | footer 固定 160 bytes；同步已封存前段后才创建 successor；后继 header 绑定前段包含 footer 的完整物理摘要 |
| 整批发布 | 所有触及段和新目录项同步后一次 manifest/CURRENT P1；每笔事务保留原 LSN、CommitDigest、RootSet 和 Ack |
| 稳定地址 | Journal.ReadAt、RecordLocation、ScannedTransaction 与 PublishedEnd 使用逻辑日志地址；不包含后续 header/footer，不因轮转改变 |
| 有序目录 | manifest schema2 认证全部 sealed/active descriptor、逻辑范围、物理前缀、完整事务位置与前驱链；不能忽略必需目录 |
| 完整恢复 | CURRENT 唯一权威；校验身份、每段前缀/封存、frame 不跨段和完整事务链，再重放核心状态；拒绝已发布损坏，不回退旧 manifest |
| 显式修复 | 默认保存尾部和孤立新段并返回 RecoveryRequired；明确 TailQuarantine 时先持久保存全部证据，再截断 active/remove orphan；修复被再次打断仍可重试 |
| 未发布布局 | 完整新 header/footer/事务均不构成提交；零字节孤立段也需要显式处理；不识别的目录项保留并拒绝，不自动删除 |
| 资源所有权 | 获取句柄同时报错也保留清理责任；所有 published/pending 文件确认关闭后才释放 owner；关闭失败可重试 |
| 兼容性 | 旧 schema1 单段库与 golden 不变；未轮转可继续 schema1，首次轮转发布 schema2；旧 reader 不得写不认识的 schema2 |

## 两个地址空间

物理文件由 `header || complete frames || optional footer` 构成。逻辑流为最初的 `[0,96)` header 加上顺序拼接的全部 canonical TX frames。后续 header 和 footer 只属于物理布局。

因此同一笔跨段事务在逻辑流仍是连续字节；Scanner、对象索引和核心状态恢复可使用原有地址和摘要。逻辑读通过有序 descriptor 二分定位段，只读取已发布 data interval。封存段文件长度必须恰好等于 DataEnd+footer；active 的额外尾部不影响旧 CURRENT 认证的前缀。

段边界 Position 指最后一笔完整事务。跨段时若事务还在继续，多个边界可持有相同 Position；这不能被当成边界处另有一次提交。全局 CURRENT 必须止于完整事务，不能发布半笔事务。

`Publication.PublishedEnd` 和 RecoveryReport.PublishedBytes 是逻辑字节计数，保留既有配额语义；descriptor.DataEnd 才是各物理文件截断边界。不能把全局 PublishedEnd 用作最后段的物理偏移。

## 配置与上限

```go
options := ate.Options{
    Journal: storage.JournalOptions{
        SegmentTargetBytes: 64 << 20,
        TailPolicy: storage.TailReject,
    },
}
```

以上使用公开根包 `ate` 与 `storage` 包。SegmentTargetBytes=0 使用默认 64 MiB，允许 1024 bytes..1 GiB。目标包含该段 header、不包含 footer；frame 大于目标时允许整体超过，避免拆帧或无限创建空段。

| 资源 | 上限 |
| --- | --- |
| 已发布段数 | 1024 |
| schema2 manifest | 1 MiB，分配前检查 |
| schema1 / IDENTITY / CURRENT | 原 4096 bytes 不变 |
| 一个 prepared batch | 原 256 笔 / 64 MiB 不变 |
| 显式恢复的未发布布局总字节 | 128 MiB，覆盖 active tail 与孤立段 |
| 本次检查的孤立段数 | 1024；目录项总数最多 2048 |

SegmentTargetBytes 是 writer 策略，不是已存格式的一部分；重开可调整它，已有封存段保持不变。读者当前保留全部段文件句柄直到 Close，还没有独立 reader pins 或后台回收。

## 发布与失败

布局规划不修改持久数据。第一次布局 IO 开始后，不再响应该事务等待者的取消来撤销工作；任何写、同步、目录或 P1 失败都令整批 CommitUnknown 并 Fenced，无部分 Ack。尚未写入的队列成员仍按 M1d 的边界返回 StoreFenced。

一次 batch 可以横跨多个新段，但物理轮转不是独立的逻辑事务或发布。直到 CURRENT 最后目录同步成功，候选对象、收据和拒绝结论都不能绕过 M1d 的结果隔离规则。

## 恢复与证据

1. 跟随 CURRENT 验证 IDENTITY、manifest、全部引用段和完整逻辑事务链，并完成语义 replay。
2. 未引用的合法 `SEGMENT.<ID>` 文件是孤立证据，不采用其内容，也不根据新旧文件时间猜测提交状态。
3. TailReject 不改变文件；返回已验证 prefix 的诊断，以及 preserved 尾部/孤立段。
4. TailQuarantine 先复制全部原始数据、Sync 文件并同步 quarantine 目录，再截断并同步 active、关闭并移除孤立段、同步 segments 目录。
5. 重建 CURRENT 发布见证后才返回 Ready。中途失败保留原发布前缀和可重试资源，不提供可用 Store。

RecoveryReport 保留原 QuarantineFile，并增加 OrphanSegments、QuarantineFiles；CLI 同步输出 `orphan_segments`、`quarantine_files`，TailBytes 汇总未发布原始字节。只有零字节孤立段时 TailBytes 可以是 0，仍返回 RecoveryRequired；是否需要修复不能只检查字节数。

## 代码组织与范围

- `storage/segment_metadata.go`：schema2 descriptor、successor/header/footer 纯 codec。
- `storage/rotation.go`：私有布局规划和 P1 文件写入，所有持久发布仍走现有 coordinator。
- `storage/segmented_reader.go`：逻辑地址到物理区间映射。
- `storage/segmented_recovery.go`：跨段认证、frame 边界和全局事务 replay。
- `storage/orphan_recovery.go`：孤立段发现、证据先于修复、修复资源上限。

下一阶段 M1f 已在 [M1f 状态](M1F_STATUS.md) 实现 reader pins、最小本地服务/SDK 与双形态生命周期契约。checkpoint/compaction、Revision/图、Fork/Diff/Join、Agent authority、任务恢复、ContextPacket 和记忆仍属于后续阶段。本批未测吞吐/恢复性能，也没有设备断电认证。
