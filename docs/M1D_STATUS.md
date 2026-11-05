# M1d：有界 admission、等待取消与 group commit

> 这是 M1d 阶段记录；当前分段与恢复能力见 [M1e 状态](M1E_STATUS.md)。

当前版本 `0.1.0-dev.m1d`，Go 1.27.0，公开的 v0 experimental module；无第三方依赖、无 `internal` 目录。M1c 的 radix/RootSet/StateOps/receipt 语义保留，磁盘对象和事务格式没有因批次发布改变。

全量检查、故障/并发边界和库/CLI 恢复往返见 [M1d 验收记录](validation/M1D_2026-10-31.md)。

## 本批完成

| 能力 | 当前保证 |
| --- | --- |
| 单协调器 | 只有一个提交 goroutine 处理 FIFO 命令，调用者 goroutine 不做持久发布 |
| 有界队列 | 请求数、owned request 字节、总 waiter 和每 RequestKey waiter 独立限额，超限不占用新的 key |
| 在途共享 | 相同 RequestKey+digest 共享同一次执行；不同 digest 立即 IdempotencyConflict；一个 caller 取消不影响其余 waiter |
| 取消 | admission 前取消是本次 NotSubmitted；之后只停止等待，命令仍按原 body 执行/拒绝，即使最后一个 waiter 也离开 |
| 输入所有权 | admission 原子复制 Body/Fence；WaitInterrupted 返回后调用者复用或修改原 slice 不改变已接受命令 |
| Receipt 优先 | 已提交 key 的 membership hint 只触发完整持久树校验；旧 receipt 在 admission/写配额前返回；hint 绝不直接返回 Ack 或 absence |
| 私有 staging | 只保留批内增量 overlay，父位置/proof 索引共享只读；不每批复制整份历史索引，也不把候选 payload 当磁盘地址 |
| 批内顺序 | 后继在前驱候选根上校验；前驱拒绝不进入视图；按 Count/encoded Bytes/Delay 有界组批和 FIFO 拆批 |
| P1 | 多个完整事务追加后一次 segment sync、一次 manifest/CURRENT 安装，整批成功才发布内存根与收据 |
| 结果隔离 | 基于候选前驱的 CAS/配额拒绝也要等 P1；前驱 unknown 时不发送候选冲突或资源结论 |
| Unknown | P1 开始后失败，整批已选择写入的事务均 CommitUnknown；新排队及依赖候选的未写命令为 StoreFenced；无部分 Ack |
| Query | 已接受 key 的 Lookup 为 InFlight，不假 NotFound；Get/Lookup/Snapshot 等 core 锁支持 context 取消 |
| Drain | CloseContext 先停止 admission/新读取，再排空已接受工作；取消只结束关闭等待，不提前释放 owner，保留对象重试 Close |

## 明确的取消与结果边界

`Commit`/`Mutate` 是接受命令并等待结果的同步 API，但等待者不拥有工作生命周期：

1. 入队前 ctx 已结束：CodeNotSubmitted + ctx error。
2. 请求数/字节/waiter 限额拒绝：CodeNotSubmitted + ErrAdmissionLimit/ErrLimit。它只说明**本次调用没有加入**，不能证明该 RequestKey 在其他 waiter 或历史里不存在。
3. 已接受后 ctx 结束：CodeWaitInterrupted + ErrWaitInterrupted，errors.Is 仍匹配原 ctx error。不得自动换 RequestID/改 body 重发，应 Lookup 原 key。
4. 已提交：所有 waiter 返回原 Ack；已经结束等待的 caller 后续也能 Lookup/原请求重试。
5. P1 unknown：所有新读取拒绝，包括旧值/快照/receipt 与 absence；重开后按 CURRENT 验证结果。

InFlight 和队列都是进程内事实，不是 durable admission token。进程在发布前退出时，队列不被假装持久化；重开后通过 receipt 或 NotFoundAtFence 对账。业务 PendingAction/outbox/authority 仍属于后续 profile 工作。

## 批次的两个位置概念

一个 batch 共享一次物理 CURRENT 发布，Generation 只加 1；每个事务 LSN 仍单独递增，保留自己的 CommitDigest、StateRoot、RootSet 和 Receipt。

每个 Ack/历史 Lookup 返回该成员自己的 Fence 和 RootSet；全局 Snapshot/Lookup.ObservedFence 指向最终 batch tip。CURRENT 认证完整前缀，因此同批中间成员的 proof 也是已发布前缀，不必假造“CURRENT 曾经独立指向每一成员”。SourcePublicationRef 后续也必须使用 producer 自己的 proof，不能换成 batch 最末根。

批内拒绝结果不得暴露 speculative state。若 A 的候选 put 使 B 的 ConditionAbsent 失败，B 在 A 持久化前仍为 InFlight；A 成功后 B 才收到冲突。若整批写失败，A=CommitUnknown，未写入的 B=StoreFenced，不输出原候选冲突。已确认写入前的非 Fenced 发布拒绝则将这类依赖结果放回 FIFO 重验。

## 资源上限与 API

根包提供 Options、AdmissionOptions、BatchOptions 别名：

```go
options := ate.Options{
    Admission: ate.AdmissionOptions{MaxPending: 64, MaxPendingBytes: 64 << 20},
    Batch: ate.BatchOptions{MaxCount: 16, MaxBytes: 64 << 20, MaxDelay: time.Millisecond},
}
```

| 配置 | 默认 | 硬上限 |
| --- | --- | --- |
| MaxPending | 64 | 4096 |
| MaxPendingBytes | 64 MiB | 256 MiB |
| MaxWaiters | 4096 | 65,536 |
| MaxWaitersPerRequest | 64 | 1024 |
| MaxCount | 16 | 256 |
| MaxBytes | 64 MiB | 64 MiB |
| MaxDelay | 1 ms | 1 s |

0 使用默认，负值/高于硬上限拒绝；MaxCount=1 不等待组批延迟。已选择、正在写和没有 waiter 的 flight 也占 pending 容量，直到完成。字节计费覆盖 owned Body/身份/方法/Fence 和固定 request 开销，不只算原正文。

storage 的 PrepareTransaction 产生拥有规范字节的 opaque 值，PublishPreparedBatch 与旧单笔 Publish 共用同一 P1 实现。预检所有长度/链/重复 TxID/配额后才写，调用者事后修改原 Transaction 不影响 prepared bytes。

`Store.CloseContext(ctx)` 与 `Engine.CloseContext(ctx)` 取消的是关闭等待；已接受工作继续排空。成功/失败后的资源退休仍有序可重试。纯查询等待 core owner 时也可取消，不需要等当前 fsync 结束才观察 ctx。

## 实现组织

- `engine/admission.go`：flight/key hint、资源计费、owned 输入、waiter 生命周期；admission lock 不等待 core。
- `engine/mutex.go`：可取消、零值可用的 core mutex，无 polling。
- `engine/coordinator.go`：FIFO 收集、验证、延迟结果、批次发布与原子完成。
- `engine/staging.go`、`object_index.go`：只含增量对象的只读父视图；多成员共享去重/对象预算。
- `storage/batch.go`：准备与一次 P1；每成员位置/正文定位，任何失败返回严格零值。

锁顺序为 core→admission。入队、停止或 alias 只短暂持有 admission；等待 core、done 或 ctx 时均已释放 admission。成功批次在一个 admission 临界区内更新 hint/fence 并完成全部 flights，避免已提交 key 仍因 alias quota 被误拒。

## 范围与下一阶段

保留单 segment、完整 prefix replay 和 M1b file-sync 实验 profile；没有新增掉电认证。总未发布 batch tail 上限为 64 MiB，不能再以自定义单事务上限错误拒绝合法多成员尾部隔离。

下一阶段是 M1e 封存/轮转/跨段事务。Reader pins、checkpoint/compaction、服务/SDK，以及 agent/v1 权限、任务、ContextPacket、知识和记忆仍未实现。本批不把可信宿主 core 的排队能力当成完整多 Agent authority 或持久业务 admission。
