# 严格 V1 短锁与嵌入式读取许可实施方案 · 2026-11-06

本轮开始时发现两项架构差距。嵌入式统一读取交付、writer 锁拆分、readCut、物理 layout lease 和读取侧短锁现已实施。本文保留实现不变量和后续维护门槛，保留 D §17.6、A §6.3/§11.3
的既定要求。OpenView 小修复本身只关闭已复现的锁序与失败清理问题；后续切片单独验证。

## 本轮开始时可定位的缺口

1. `coordinator.processBatch` 从开始到结束持有 `e.mu`，跨 reducer 的读取/编码/哈希和
   `Journal.PublishPreparedBatch` 的 P1 文件同步，并在结束时嵌套 `admission.mu`。
   `issueContentRead`、`finishReadDelivery` 等输出路径亦在此锁内调用 `loadMemory` 和
   provenance closure，后者执行 radix lookup 和对象读取。A §6.3 要求全局短锁内无 I/O、
   大对象哈希或图遍历，admission 与 publication 锁不嵌套。A §11.3 要求预编译授权快照。
2. `finishReadDelivery` 在 `capture == nil` 时直接返回值。Unix 使用
   `PrepareReadDelivery`，嵌入式普通 Getter 只做当前策略检查。`ReadContent` 已有单独 grant，
   其他嵌入式读取尚未普遍形成 D §17.6 所要求的目的地、请求摘要和单次交付许可。

跨层阻塞也必须处理。`storage.PinnedReader.ReadAt` 和 `Journal.ReadAt` 目前持有 `j.mu`
完成文件读取，P1 publisher 也持有该锁。仅释放 `e.mu` 后继续共享此 I/O 门禁，仍会让
固定 View 读取等待整批同步。`PinnedReader` 当前复制 `[]*journalSegment`，直接在锁外使用
这些指针又会与 active descriptor 的更新产生竞争。

本轮的确定性 OpenView 回归在修复前同时复现了两种问题。capture 最后一次取 `e.mu` 时
仍持 `views.mu`，与普通 OpenView 开始阶段的 `e.mu → views.mu` 相反。capture 的锁等待
取消或 binding 校验拒绝后，未返回给调用者的注册 entry 仍保留到 TTL。修复释放 registry
锁后再取 core 锁，并在 capture 失败时关闭未返回的 View。

## 状态所有权与数据竞争边界

保留单个 coordinator goroutine 作为逻辑 writer。增加只供 writer、maintenance 和最终
Close 使用的串行门。普通读者不获取这个门。它可以跨磁盘操作，承担单 writer 和物理维护
互斥，不承担公开读授权或 View 生命周期保护。

`publishMu` 只保护下面的短操作。

- 当前 `PublishedBundle` 指针、Ready/Draining/Fenced/Closed、publication phase 和改变通知。
- View registry、provisional pins、activeOps、closing 和代际引用计数。
- 输出许可的最终版本比较、生命周期检查和单次授权状态安装。

已发布 bundle 包含 Fence、RootSet、rootSet ObjectID、物理 generation、只读 catalog cut
和预编译授权快照。发布后这些值及其中拥有的集合不可修改。private reducer、staged bytes、
eventHeads 和待提交 LSN 由 writer 独占，不能经公开 reader 暴露。不要复制带 mutex 的 Engine
充当读取快照，使用独立 `readCut` 保存 bundle、固定 reader 与 pin。

现有 `objectIndex` 的位置 map 已有短 `RWMutex`。可以保留这个只追加的重建索引，每次
只在小锁内查询或安装位置，实际 ReadAt、校验与分配在锁外。读取能力固定 publishedEnd，
拒绝超出该 cut 的位置，源 reader 绑定对应 layout pin。`staged` map 继续仅属于私有
writer/replay，公开 reader 不访问它。不要在已共享的 index 上切换 `source`。

`history`、`envelopes`、`events` 等普通 Go map 不能继续由 coordinator 写入、读者无锁
查询。可按其只追加特性使用独立的短元数据锁保护单键操作，值带 LSN，查询必须核对 bundle
cut。枚举使用按 batch 封存的不可变 block，bundle 固定 block 链头，扫描在所有全局短锁
之外进行。writer 使用的最新 `eventHeads` 保持私有。当前 AuditFull 保留全部逻辑历史，
这一步无需逻辑删除索引条目，也无需每批复制全部对象/历史 map。

## coordinator 与 P1

1. writer 取得串行门，短暂捕获当前 bundle 和输入 layout pin，然后释放 `publishMu`。
   admission 收集、flight 查询/完成分别使用其自身锁，始终与 `publishMu` 分开。
2. 在私有 candidate 中完成 reducer、历史读取、StateOps、对象编码/哈希、batch 准备与
   新授权快照编译。Memory policy 和受保护 Knowledge/owner authority 从候选状态建立
   拥有副本，未改变的 Space 可以共享旧不可变快照。
3. P1 前在短锁中复核基线 bundle/lifecycle，并登记 publication 正在进行。P1 本身在
   `publishMu` 外执行。读取可继续捕获固定 cut、读取文件、展开和序列化。
4. P1 成功后，在锁外完成新位置 catalog 和 bundle 的构造，短锁内整体安装。随后分别
   更新 admission hints/完成 flights，先安装 hint 再移除 flight，保留重复键无空窗语义。
   P1 成功以前不安装公开根、receipt 或新对象可达入口。
5. 不确定结果在短锁中进入 sticky Fenced 并唤醒等待者，再在锁外停止 admission、完成
   CommitUnknown。已有 pin 可保留清理能力，不能据旧 bundle 签发新的内容许可。

publication phase 需要明确的失败排序。P1 尚未确定时，输出签发可以在短锁外等待该 phase
完成，再核对当前 cut，或返回现有的 `AuthorizationChanged`。不得持锁等待、依赖一个会
跨 P1 的 `Journal.Published()` 调用，或从旧内存给出确定 NotFound。此阶段读者的文件读取
和候选计算不受 writer 串行门阻塞。

## layout pin、maintenance 与关闭

存储 reader 必须获得不可变 layout snapshot。snapshot 的 segment descriptor 按值保存，
持有被 pin 的文件读取能力，并固定逻辑 end。`ReadAt` 在短元数据锁内确认未关闭/未 fenced
并增加 activeOps，释放锁后做文件读取，完成后递减。发布只追加已发布前缀之外的字节，旧
snapshot 的描述符与范围保持固定。禁止继续借用 writer 可变的 `journalSegment` 描述符。

OpenView 在 `publishMu` 内捕获 bundle 并增加 provisional pin，锁外解析历史 Anchor，
再在短锁内将同一个 pin 转交给 entry。失败、取消、停止或 capture 拒绝均释放该 provisional
引用。每次操作在短锁内增加 activeOps 后才得到读取能力，关闭/过期关闭新 acquire，最后
一个 activeOps 释放后仅退休一次。物理 handle 的关闭在短锁外完成。

CompactAuditFull 和 ReclaimRetired 走 writer 串行门，验证/复制/同步在 `publishMu` 外。
安装新 layout 时关闭旧代际的新 pin 入口，已有 pin 继续读取旧 layout。旧文件仅在 pin 与
activeOps 均为零后进入待删除清单，实际 close/remove/目录同步由维护 owner 在锁外执行。
逻辑对象地址、历史 Fence、receipt 和 canonical transaction bytes 保持现有契约。

Close 先在短锁下设置 sticky Draining、关闭新 View/输出准入，再分别停止 admission。
它在锁外等待已接受 coordinator 工作、进行中的 maintenance、provisional pins 和所有
active reads，最后由串行门的 owner 在锁外关闭 journal/owner lock。取消或超时只结束
调用者等待。失败 handle 继续归同一 owner 管理，重试完成资源退休后才标记 Closed。
并发 Close 共享同一次关闭过程，不能由多个调用者同时关闭底层资源。

## 读取授权与嵌入式交付

所有读取分成捕获、计算和签发。捕获产生固定 readCut，计算在锁外完成对象读取、依赖闭包、
当前授权快照检查、编码与内容/请求摘要。签发只在短锁内比较捕获的当前 policy/control cut
和生命周期，发生变化则返回 `AuthorizationChanged`，不在锁内重载 Memory 或遍历来源。
Unix JSON 序列化后的再次比较沿用这一规则。

建立公用嵌入式交付 wrapper，使现有公开 Getter 在未处于 capture context 时也进入同一
捕获/签发路径，并立即消费只属于此次调用的 typed grant。grant 拥有结果副本，绑定
Principal、embedded session、一次 Request ID、Method、规范 RequestDigest、View/Anchor、
内容和依赖摘要，消费采用单次原子状态。typed 消费可直接交付拥有的结果，无须在锁内进行
JSON 编码、反序列化或大对象哈希。Unix 和 embedded 共用 policy/proof 生成逻辑。

需要显式处理当前不带 Principal 的 core State Getter。由受信嵌入式会话/创建者绑定身份，
facade 将其传给公用 wrapper，不能接受未经认证的外部自报身份。已有公开入口也必须接入
wrapper，只增加一个可选的新 grant API 仍不能关闭原条款。`ReadContent` 的现有单次能力
和五秒 grant 时限在这次整合中保留。

## 实施顺序与关闭证据

先落实不可变 layout reader 和代际生命周期，再引入 bundle/readCut 与元数据 map 的并发
安全边界，然后拆 coordinator/maintenance/Close，最后移动输出授权计算并统一 embedded
交付。每步独立保持现有语义、格式、receipt/replay 与普通/race/native 基线通过。

必须新增能拦住具体并发回归的确定性交错。

- 在 reducer 读取、P1 Sync、输出 JSON/hash 和 ReadAt 各阶段暂停，证明 `publishMu` 可被
  其他操作取得，固定 cut 的读取计算继续执行，未发布候选无法被查询。
- ACK 不确定、并发语义读失败、撤权和输出签发交错，证明无旧授权内容或确定 NotFound 泄漏。
- OpenView provisional pin、注册失败、过期、Close 超时、active read、compaction 和 reclaim
  交错，证明没有提前关闭、重复释放、永久 pin 或 owner lock 提前释放。
- 元数据 map/descriptor 的高并发读写运行 race，核对固定历史 Anchor 和原 receipt。
- 对每个 embedded/Unix 读取入口覆盖错误目的地、错误请求摘要、错误 View、重复消费、许可
  生成期间撤权与结果可变切片，确认两条接入路径都只交付已绑定的拥有副本。

关闭依据应来自最终代码与这些交错结果。当前普通/race/native 绿色基线以及本轮 OpenView
小修复不代替上述两项架构验收。

## 本轮已实施的切片

- coordinator 与维护/最终 Close 增加独立 writerMu，普通读者不获取它；私有 reducer 与 P1 移至 e.mu 外。公开根只在 P1 成功后短锁安装，admission 完成另取自身锁。
- publicationPending 覆盖 durable CURRENT 到内存安装的整个窗口；该窗口不签发旧授权，也不返回确定缺失。P1 失败保持 unknown/fenced 的既定分类。
- writer 失败的物理 Fence、verify/compact/reclaim/close、普通 getter 的 readFailure 和读取授权 IO 都已移到全局 core 锁外。最终短段只重核 Fence、roots、生命周期和物理状态。
- 嵌入式普通读方法复用请求绑定的一次 capture/Consume。结构化回归覆盖普通接口清单、锁外序列化、变更策略、拥有副本、OpenView 失败清理与 LoadGraph。没有 capture 的内部交付默认拒绝，未使用的公开 typed-root 原始逃逸方法删除。

确定性交错已覆盖 planner 与 P1 的锁释放、未知结果、并发 Fence、维护/Close、非嵌套 admission 完成、View capture 注册锁与失败清理、readCut 物理读取、Graph/Node 遍历和 embedded/Unix 交付。后续新读取入口必须继续接入同一 readCut 和 layout pin。物理设备掉电与更完整的真实模型效果仍属于外部证据，不由这些代码测试替代。
