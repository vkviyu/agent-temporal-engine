# V1 机制效果与开销证据 — 2026-11-02

本文件保留当时无 provider 的机制实验。2026-11-03 新增了真实 Responses 调用，见
[真实模型效果对照](V1_REAL_MODEL_EFFECTS_2026-11-03.md)；下文“本次没有远程模型调用”仅指本历史工件。

本报告区分三类问题：记录和恢复是否正确、确定性选择/检索机制是否满足固定样例、真实模型是否能在
长程任务中获益。前两类可以在本地无 provider 条件下复现；本次没有远程模型调用，第三类没有数据。
这些结果补充主设计 §14.4、§15.5、§21.6、§22.1，不能单独作为这些条款全部通过的证明。

## 确定性机制对照

代码位于 `tests/acceptance/`，只调用生产公开包，不在测试中实现另一套上下文选择器。

| 实验 | 明确的比较对象 | 实测结果 | 能证明的范围 |
| --- | --- | --- | --- |
| Memory 适用性 | 忽略 Scope/Goal/Environment、只看相同 `restart` 关键词的四条候选，与 `memory.Store.Search` 对照 | claim-only 样例相关项 1/4；指定 project-a/repair/go1.27 后 1/1；project-b 只命中自身记录；project-c 自动复用 0 | 当前检索过滤和 Space 隔离，不是跨 Space 迁移收益 |
| 失败反例与路径缺口 | 已晋升决策及 ExperiencePath 保存一个有用反例和一个省略分支引用，随后对反例来源 UseBan | 反例引用 1/1、缺口引用 1/1 保留；撤回后可复用路径 0、可检索派生 Memory 0；原审计路径 canonical bytes 不变 | 失败来源可追溯及撤回传播，不证明模型主动利用反例 |
| 早期证据回查 | 同一 64/128 历史知识位置上，最近八条窗口与按两个已记录 ID 回查比较 | 最近窗口 0/2；显式 ID 回查 2/2；未来泄漏 0；三次 canonical 编解码后身份保持 | 历史可寻址和明确召回，不代表自主发现相关性或固定 token 选择 |
| 固定输入容量 | `knowledge.SelectContext` 对照最近五条窗口；双方都是 10 个 synthetic estimated token，原始候选 32 条，每条 2 tokens | recency 关键项 0/5；selector 5/5：mandatory 2/2、结论+支撑+反例 3/3；27 条省略有原因；输入顺序反转后选择和 ProposalDigest 不变 | 生产纯选择器在已认证快照上的容量/分组机制，不证明 token 估计等于真实 provider 计数 |
| 容量与严格过去错误 | mandatory 需 4 tokens 但输入只剩 3；另以 16/32 旧 Cursor 提交未来候选 | 分别返回 `ErrContextBudgetUnsatisfied` 与 `ErrUnknownObservation`，不偷偷削弱目标或回退 latest | 明确失败、历史成员关系和固定版本选择 |

`TestEarlyEvidenceRecallUsesExactKnowledgePosition` 的“三次重建”是 canonical snapshot 编解码，
不是摘要压缩；不能写成“多轮 LLM 压缩无遗忘”。这些样例的相关性由夹具预先声明，不是由检索结果
反向定义。跨 Space 没有导出/导入生产协议时，测试只确认隔离，不私自复制 claim 冒充经验迁移。

## 测试环境与开销参数

- Go `go1.27.0 darwin/arm64`；macOS 26.3.1 (25D2128)，Darwin 25.3.0。
- CPU Apple M1，内存 16 GiB；`/private/tmp` 位于 `/System/Volumes/Data`，测量时数据卷使用率约 86%。
- `-cpu=1` 将 Go 的 GOMAXPROCS 设为 1，不是物理 CPU 绑核。系统未隔离其他进程，冷热缓存未分离。
- 每项 20 次操作，独立运行 3 组。下表为三组 **ns/op 的中位数**，内存为相应组的 B/op。
- native 数据集：32 个已提交事务、32 个键、每值 1 KiB；使用真实 `ate.Create/OpenExisting` 和本地目录 IO，
  不换成内存磁盘。单请求顺序执行，`Batch.MaxCount=1`，不把该结果称为并行 group throughput。
- Knowledge 数据集：32/256 条观察，单 Key、每条独立 SourceAnchor；测试真实 canonical 编解码和
  `SelectRevision`，不包含模型 tokenizer、renderer、网络或 provider 延迟。
- Context selector 数据集：同样 32/256 条观察，两个 mandatory，其余按每组三条组成结论/支撑/反例；
  Window=128、OutputReserve=32、FramingReserve=16，输入上限 80；每段 synthetic estimated cost=2。

| 开销项目 | 三组中位耗时 | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| native Commit，1 KiB，32 条种子事务后追加 | 23.144 ms | 110,496 | 1,084 |
| native GetState，1 KiB，32 条事务 | 74.481 µs | 23,821 | 296 |
| native OpenExisting，32 条事务重放 | 9.726 ms | 6,164,499 | 77,910 |
| native AuditFull + ReclaimRetired，32 条事务 | 51.200 ms | 7,161,305 | 84,405 |
| Knowledge EncodeSnapshot，32 条 | 120.392 µs | 103,919 | 726 |
| Knowledge DecodeSnapshot，32 条 | 174.190 µs | 122,670 | 801 |
| Knowledge SelectRevision，32 条，选择中间位置 | 151.888 µs | 183,982 | 1,550 |
| Knowledge EncodeSnapshot，256 条 | 1.078 ms | 863,092 | 5,919 |
| Knowledge DecodeSnapshot，256 条 | 1.438 ms | 1,045,856 | 6,939 |
| Knowledge SelectRevision，256 条，选择中间位置 | 1.302 ms | 1,543,706 | 12,154 |
| SelectContext，32 条、10 个 bundle | 405.388 µs | 387,558 | 3,176 |
| SelectContext，256 条、84 个 bundle | 3.047 ms | 3,348,018 | 25,007 |

native AuditFull 每次复制 canonical log 182,568 bytes，确认 Fence 前后完全相同；20 次布局重写并回收
后目录内普通文件 length 合计 201,396 bytes。这个总量包括保留 metadata，**不是物理块占用**、峰值
磁盘使用或写放大；32×1 KiB 逻辑值大小也不能替代完整审计历史大小。

每组 native commit 的样本 p50/p95/p99 (ms) 分别为：

| 组 | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| 1 | 23.008 | 25.982 | 26.097 |
| 2 | 23.967 | 28.005 | 30.468 |
| 3 | 20.874 | 26.943 | 26.993 |

20 次样本的 p99 就是该组最大值，仅用于保留原始样本观测，不是可外推的生产尾延迟保证。
reopen 的计时不含之前的 Close；AuditFull 计时包含完整验证、布局安装与旧段回收。没有宣称对其他
引擎更快，没有同语义基线对手，不在这个小规模样例外作容量或性能推断。

## 复现命令与完成状态

```sh
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test ./tests/acceptance -count=1 -v
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test -race ./tests/acceptance ./tests/benchmarks -count=1
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test ./tests/benchmarks -run '^$' -bench 'Benchmark(NativeStore|Knowledge)$' -benchtime=20x -count=3 -cpu=1 -benchmem
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test ./tests/benchmarks -run '^$' -bench BenchmarkContextSelection -benchtime=20x -count=3 -cpu=1 -benchmem
```

上述六个确定性机制测试、race 和两条开销命令均在本机以 exit code 0 完成。SelectContext 是生产纯函数，
测试没有在本地造一个 selector 冒充实现。宿主仍负责认证 Source、提供 mandatory/benefit、保存选择及实际
framing 请求，并在新派发前独立查权限和撤回；其 engine/Packet/service 集成未因纯函数通过而完成。

## 尚缺的 V1 证据

- 固定容量选择器与 engine ContextPacket、当前撤回 gate、service/SDK 的完整集成及故障证据。
- 真正的多轮摘要派生、约束保真和 provenance 机制；snapshot 编解码不能替代摘要评测。
- 跨 Space 的脱敏、来源映射与 ID 重算导入协议；现有隔离不能等同迁移成功。
- 固定 provider/model、固定任务、固定 token/费用预算及相同评分标准下的长程成功率、记忆收益、
  过时/污染风险和跨项目迁移收益；本报告没有模型结果。
- 大规模/多 Actor、冷热缓存、并发 group、近满磁盘压力、fsync 次数、写放大、pin 延迟和完整性能矩阵。

以上缺口继续保留为未完成，不能因为新增几个绿色样例或一个时间表就关闭。
