# ATE-STORE-EXPERIMENTAL-1

> 本文保留原阶段定义；M1e 的多段扩展与逻辑地址说明见 [segmented-store-experimental-2](segmented-store-experimental-2.md)。打开与 tail 章节补充当前 R08 恢复契约，末节 R07 描述 publication metadata 维护契约；既有格式编码与旧单段字节保持不变。

状态：experimental，模块 v0。数值 schema/version=1 不表示磁盘格式或 API 已冻结。此文件定义 M1b 单 segment store 的物理 metadata；事务帧与链摘要见 [journal-experimental-1.md](journal-experimental-1.md)，canonical 基元见 [canonical-experimental-1.md](canonical-experimental-1.md)。物理发布边界不是语义 replay、授权验证或业务状态恢复的证明。

## Metadata envelope 与 canonical 字段

IDENTITY、manifest、CURRENT 均为完整文件：`magic[8] || canonical payload || SHA-256(magic || payload)[32]`。checksum 不包含自身。metadata 总长度硬上限为 4,096 字节；解码在分配、解析任何字段之前拒绝超限输入，不根据不可信长度分配缓冲区。文件没有隐式 padding，payload 必须恰好消费，追加字段即使重算 checksum 也被拒绝。

payload 整数为最短 unsigned varint；bytes/string 以最短 varint 字节长度开头，string 必须合法 UTF-8。表格从上到下是固定字段顺序，所有字段必填。schema 和 flags 均是 canonical uint；当前 schema=1、flags=0。

StoreID、SegmentID、ManifestID 的 Go 表示统一为 32 位小写十六进制、非全零；metadata payload 使用 `bytes(16)` 原始值，不保存十六进制字符串。摘要统一为 `bytes(32)` 原始 SHA-256 值。LSN、generation 和 offset 均为 uint64；codec 不强制 generation 与 LSN 相等，跨文件的一致性由 store 协调层验证。

### IDENTITY

magic 为 ASCII `ATEID001`。

| 顺序 | 字段            | 限制                       |
| ---- | --------------- | -------------------------- |
| 1    | schema uint     | 1                          |
| 2    | flags uint      | 0                          |
| 3    | Store bytes(16) | 非零                       |
| 4    | Profile string  | `file-sync-experimental-1` |

Profile 定义同步契约，不认证设备、文件系统或操作系统的故障行为。未知 profile 拒绝，不回退为其他模式。

`IdentityDigest = SHA-256(完整 IDENTITY 文件)`，范围包含 magic、payload 和文件 checksum。

`GenesisPosition.LSN = 0`；`GenesisPosition.Digest = SHA-256(raw ASCII "ATE-STORE-GENESIS" || NUL || IdentityDigest 原始 32 字节)`。域分隔字符串没有 canonical 长度前缀。

### Manifest

magic 为 ASCII `ATEMAN01`。

| 顺序 | 字段                          | 限制                                         |
| ---- | ----------------------------- | -------------------------------------------- |
| 1    | schema uint                   | 1                                            |
| 2    | flags uint                    | 0                                            |
| 3    | Store bytes(16)               | 非零                                         |
| 4    | Generation uint               | uint64                                       |
| 5    | ManifestID bytes(16)          | 非零                                         |
| 6    | SegmentID bytes(16)           | 非零                                         |
| 7    | SegmentHeaderDigest bytes(32) | 非零；完整 96 字节 segment header 的 SHA-256 |
| 8    | PublishedEnd uint             | >=96；相对 segment 起点的 exclusive offset   |
| 9    | PrefixDigest bytes(32)        | 非零；segment `[0, PublishedEnd)` 的 SHA-256 |
| 10   | Position.LSN uint             | uint64                                       |
| 11   | Position.Digest bytes(32)     | 非零；genesis 或最后已发布 CommitDigest      |
| 12   | PostRootSet bytes(32)         | LSN=0 时必须为零，LSN>0 时必须非零           |

metadata codec 校验字段长度和值域。PublishedEnd 是否处于事务边界、prefix/header 摘要是否匹配实际 segment、Position 是否来自 IDENTITY genesis 和被验证的事务链，以及 PostRootSet 是否与末笔 Commit 相同，均由恢复协调层验证。PostRootSet 不在本阶段证明业务根对象存在或语义合法。

### CURRENT

magic 为 ASCII `ATECUR01`。

| 顺序 | 字段                     | 限制                                                |
| ---- | ------------------------ | --------------------------------------------------- |
| 1    | schema uint              | 1                                                   |
| 2    | flags uint               | 0                                                   |
| 3    | Store bytes(16)          | 非零                                                |
| 4    | Generation uint          | uint64                                              |
| 5    | ManifestID bytes(16)     | 非零                                                |
| 6    | ManifestDigest bytes(32) | 非零；完整 manifest 文件的 SHA-256，包含其 checksum |
| 7    | IdentityDigest bytes(32) | 非零；完整 IDENTITY 文件的 SHA-256                  |

CURRENT 自身 checksum 证明字节完整性；它指向的 manifest、IDENTITY 和 segment 必须进一步匹配，不能根据单个 metadata 文件独立提升发布位置。

## Segment header

Header 固定 96 字节。表内固定整数字段使用 little-endian，与 payload 中 canonical 整数不同。schema 1 仅支持初始 segment，FirstLSN 必须为 1；跨 segment 协议尚未定义。

| Offset | 字节 | 字段                                                            |
| ------ | ---- | --------------------------------------------------------------- |
| 0      | 8    | ASCII `ATESEG01`                                                |
| 8      | 2    | version=1                                                       |
| 10     | 2    | flags=0                                                         |
| 12     | 4    | reserved=0                                                      |
| 16     | 16   | Store，非全零原始 ID                                            |
| 32     | 16   | SegmentID，非全零原始 ID                                        |
| 48     | 8    | FirstLSN=1                                                      |
| 56     | 32   | PreviousDigest，非零；初始段应为 IDENTITY 导出的 genesis digest |
| 88     | 4    | CRC32C，仅覆盖 `[0,88)`                                         |
| 92     | 4    | 零 padding                                                      |

CRC32C 使用 Castagnoli，反射多项式 `0x82f63b78`，标准 initial/final XOR，与 frame CRC32C 相同。reserved 和末尾 padding 必须为零，即使 CRC 正确也拒绝非零值。首个事务 frame 从 offset 96 开始；没有 footer。

## Codec 错误与验证向量

| 错误           | 含义                                                                                     |
| -------------- | ---------------------------------------------------------------------------------------- |
| ErrUnsupported | 未知 schema/version、profile 或 flags                                                    |
| ErrMalformed   | 已知 schema 下的非法长度、ID、零摘要、位置值、保留字节、padding、magic 或 canonical 结构 |
| ErrIntegrity   | metadata SHA-256 checksum 或 segment header CRC32C 不匹配                                |
| ErrLimit       | metadata 输入大于 4,096 字节；在解析与分配之前检查                                       |

canonical 解码错误以 ErrMalformed 包装，同时保留 `errors.Is` 对原 codec 错误的匹配，包括非最短 varint、整数溢出、长度超限、截断、非法 UTF-8 和尾随数据。checksum 先于 payload 解释；未重算 checksum 的 payload 损坏先表现为 ErrIntegrity。物理 codec 不执行文件操作。

`storage/metadata_test.go` 包含 IDENTITY、segment header、manifest、CURRENT 的固定完整 golden，以及固定 IdentityDigest/genesis digest。这些预期值由独立 Python 的 varint/长度前缀、hashlib SHA-256、按位 CRC32C 实现推导。invalid corpus 覆盖每字节截断和翻转、重算 checksum 后的未知 schema/flags、非最短整数、ID/digest 长度、零值、UTF-8、尾随数据、reserved/padding，以及超限输入在零分配下被拒绝。FuzzMetadataCodecs 验证无 panic、输入不变和所有成功解码结果的重编码字节一致性。

## 文件布局与唯一发布者

```text
existing-store-directory/
    .owner.lock                      持久 flock 文件，不删除
    IDENTITY
    CURRENT                          唯一可替换发布入口
    segments/SEGMENT.<SegmentID>      单个 active segment
    manifests/MANIFEST.<ManifestID>   不可变元数据
    manifests/.STAGE.<ManifestID>     尚未安装的候选，可能遗留
    .CURRENT.<ManifestID>             尚未替换的候选，可能遗留
    quarantine/TAIL.<randomID>        显式尾部隔离证据，可选
```

ID 都是上述 32 位小写十六进制，路径由实现生成/验证，不接受 manifest 中任意文件系统路径。ManifestID 每次发布随机分配，Generation 从 0 开始递增；不重用失败候选文件覆盖不可变元数据。恢复只按 CURRENT 指定 ID 打开，不以目录排序、最大代际、文件大小或完整 TX_COMMIT 推断已发布状态。

所有发布和恢复写操作都在同一 owner 内串行执行。native IO 使用受信目录的 os.Root，并拒绝 managed 符号链接；普通文件须属于 euid 且无 group/world write。合作 writer 使用持久锁文件的非阻塞 flock，不通过删除锁文件“解锁”。根目录本身已经存在，其父目录持久性是调用方前置条件。

## P1：单段、batch=1

初始化在独占 owner 下检查空目录（允许锁文件），建立并同步子目录、IDENTITY 和 genesis segment，随后按同一元数据发布协议写 Generation 0。genesis manifest 的 PrefixDigest 是完整 header 摘要、PublishedEnd=96、LSN=0、PostRootSet=0。

每次 Publish 按以下顺序执行：

1. 在任何写入前验证 transaction、下一 LSN/前摘要、重复 TxID、frame/事务/总历史资源上限；取消和拒绝不占用事务位置。
2. 完整追加事务，循环处理短写/零进展/n>0 附 error；同步 active segment。
3. 写 `manifests/.STAGE.<id>`，同步并关闭；InstallNew 原子安装不可变 final name，不覆盖已有名字；同步 manifests 目录。
4. 写 `.CURRENT.<id>`，同步并关闭；原子替换 CURRENT；同步 store 根目录。
5. 在协调器锁内一次交换内存 Publication，然后返回成功。

native InstallNew 使用同目录 hard link + unlink staging，不使用会覆盖已有 final 文件的 rename。若链接成功但后续步骤失败，允许遗留两个候选名字；没有 CURRENT 就不能提升为发布。CURRENT 替换使用 rename，不允许先删旧文件再改名。

append 尝试后的任意 I/O/安装错误均返回 PublicationError（errors.Is 匹配 ErrCommitUnknown 与原错误），携 stage、TxID 和上次诊断 Publication，同时进入 Fenced。Published 和后续 Publish 均拒绝；Status 不授权新读取或派发。恢复结果未知时不能自动换 RequestID 重发业务命令；域 receipt 对账在 M1c 实现。

## 打开、验证与重新建立发布入口

OpenJournal 获得 owner 后依次验证：CURRENT envelope → IDENTITY digest/Store/profile → CURRENT 指定 manifest 的完整摘要和绑定 → segment header → `[0, PublishedEnd)` 的完整字节摘要与连续事务链 → 最后 Position/PostRootSet/事务边界。LSN0 必须恰好是 identity 导出的 genesis 和空域根。

段文件可以比 PublishedEnd 更大；未认证 tail 不进入 prefix 哈希。prefix 内任何结构/校验失败都 fail-stop，缺少 CURRENT 则 IncompleteStore，不猜旧 manifest。未知格式和资源上限分别报告 Unsupported/Limit，不伪称“数据已被修复”。

进程退出后可见的新 CURRENT 可能来自未完成目录同步的 rename。存在 compaction intent 或选中 manifest 的 retired segment descriptor 时，Open 在完整验证与 `collectOrphans` 成功后、任何布局清理前，先 `SyncDir(".")` 稳定当前 CURRENT 选择。处理完布局和尾部后，Open 仍必须再次同步 store 根目录，才能返回 Ready。`RecoveryReport.Verified` 在完整 prefix、所提供的 replay 和 compaction source 验证通过后才置 true；不提供 replay 的 `OpenJournal` 不证明领域语义。提前失败时 `Verified` 仍为 false。`PublicationSynced` 只在返回 Ready 前的最终 publication witness 同步成功后置 true；清理前的 stabilization 成功并不设置它。false 不证明此前未执行过 `SyncDir`，stabilization 或布局清理可能已同步 store 根目录或子目录。不返回可用 Journal 的报告不能作为派发 grant。

## 未发布 tail

默认 `TailReject` 拒绝 active tail 和普通 orphan segment 的修复，保留这些字节；它不把 Open 变成只读检查。R08 保持当前顺序：

1. 认证 CURRENT/IDENTITY/manifest，检查 compaction intent 路径。未知名称或非法 intent ID 在 metadata 阶段提前失败，不进入 prefix 验证或布局清理。
2. 验证完整 published prefix、所提供的 replay 和 compaction source；checkpoint 路径同样要求完整物理验证及 checkpoint/suffix replay 或 fallback 的完整 replay。
3. 收集并检查普通 orphan（`collectOrphans`），排除当前段、已认证退休段与已认证 intent 的 candidate；此步骤失败时不进入 stabilization 或布局清理。
4. 若存在 compaction intent 或选中 manifest 的 retired segment descriptor，先 `SyncDir(".")` 稳定已验证的 CURRENT 选择；同步成功后才清理 compaction intent，再移除已认证 retired segment 并同步相关目录。
5. 仍有 active tail 或普通 orphan 时才判断 tail policy。`TailReject` 报告尾部大小、普通 orphan 和 `TailDisposition="preserved"`，返回 `storage.ErrRecoveryRequired`，不返回可用 Journal。即使 tail 包含完整、校验正确的下笔事务，也不提升。
6. 无需拒绝或显式隔离修复完成后，执行最终根目录 witness 同步，才返回 Ready。

第 4 步不受 `TailReject` 禁止：经过认证的 intent 可授权删除冗余 compaction candidate；选中 manifest 认证的 retired segment 可被移除。合法 `compactions/INTENT.<id>` 路径中无法解码的内容也触发清理前的 stabilization，随后先完整复制并同步到 `quarantine/COMPACTION.*`，再关闭并移除 intent。malformed 内容本身不授权删除 candidate，未知路径也不会被猜测为可隔离 intent。

清理前的 barrier 防止以下崩溃窗口：compaction 已替换可见 CURRENT 但根目录尚未同步，恢复删除并同步旧段后因 `TailReject` 退出，随后崩溃恢复旧 CURRENT 却无法再找到旧段。发布协议已在替换 CURRENT 前同步 manifest 文件和 manifests 目录，因此此 barrier 只需同步根目录。没有 compaction intent 或 retired descriptor 的普通 active-tail `TailReject` 路径仍不执行目录同步。

清理前的 stabilization 同步失败返回 `storage.ErrRecoveryIncomplete` 并保留原始 I/O 错误；尚未开始布局清理或 tail refusal，报告中 `Verified=true`、`PublicationSynced=false`，清理列表为空。布局清理或其目录同步失败也可在 tail refusal 之前返回 `storage.ErrRecoveryIncomplete`；此时报告只有部分进度，`TailDisposition` 可能尚未设为 `preserved`。即使 stabilization 成功，随后 tail refusal 或最终 witness 同步失败时 `PublicationSynced` 仍为 false。`ReclaimedSegments`、`DiscardedCompactionSegments`、`QuarantineFiles` 是进度记录，不证明所有删除、关闭和后续目录同步均已完成。资源关闭失败通过 `CleanupError` 保留可重试 handle 和原错误，按 file → owner 顺序退休；完成前不能提前关闭调用方的 `DurableIO` 或重新取得 owner。CURRENT、选中 manifest、published prefix、逻辑 Fence 与 RootSet 不因这些布局清理被改写，但整个目录并非无写入。

显式 `TailQuarantine` 在上述验证与布局清理之后处理未发布字节。仅有 active tail 时，先创建并同步 quarantine 目录名字；流式复制**所有原始 tail 字节**至新证据文件，完成文件同步/关闭、quarantine 目录同步；然后只在单段 PublishedEnd（多段 active descriptor 的 DataEnd）处截短并同步 active segment。存在普通 orphan 时，同样先保存并同步完整布局证据，再截短 active tail 和移除普通 orphan。最后重建 CURRENT 目录同步并返回 Ready。

任何隔离 I/O 失败都返回 `storage.ErrRecoveryIncomplete`，不把它当成功恢复。若截短已持久化，协议保证此前完整证据文件及名字已同步。重复恢复可能留下 partial/orphan 证据文件；上述认证布局清理不等于自动删除这些证据，也不回滚已发布历史。

## 当前限制与寿命

JournalOptions 对总 published 字节和事务数设独立资源上限，默认 1 GiB/100,000 笔、硬上限 64 GiB/1,048,576 笔；单笔与 payload 限制沿用 frame registry。隔离 tail 不超过选定的单事务字节上限。降低选项可能使已有 store 明确返回 Limit，但不能改写或丢弃历史。

Close 停止新操作，先关闭 segment，再释放 owner。失败保持 Closing，重试只处理尚未确认退休的资源；只有成功关闭 Journal 后才能关闭调用方持有的 DirectoryIO。Close 不调用 SyncDir 或提升未确认事务。

构造、metadata 读取或候选写入失败时，关闭本身也可能失败。CleanupError 保留待退休资源的引用并支持 Close 重试；外层构造错误按临时文件→active segment→owner 聚合这些资源。Publish 的 cleanup 则保留在 Fenced Journal 中，由 Close 先处理。错误 acquisition 若已取得句柄，应返回非 nil 句柄与 error，调用方仍须清理；不能丢失 lease 后让锁永久滞留。

本格式没有封存/footer、轮转、跨段事务、group commit、checkpoint、compaction 或读者 pins。所有元数据和根都由单协调器安装，不允许未来后台维护者绕过 CURRENT 的唯一发布路径。

## R07：publication metadata 的显式维护

当前 `storage.Maintain(ctx, d, policy)` 对调用方指定的 `storage.DurableIO` 执行一次有界维护。`storage.RetentionPolicy{}` 默认仅检查：取得独占 owner、读取 metadata 并返回 inventory，不删除或改写发布数据，也不执行目录同步。已有 Store/Journal 必须先完成关闭并释放 owner；无法取得独占所有权时返回 `storage.ErrMaintenanceBusy`。

只有调用方显式选择以下策略才请求 publication metadata 删除；`DropQuarantine` 是独立的显式选项，默认也为 false：

```go
report, err := storage.Maintain(ctx, d, storage.RetentionPolicy{
	DropPublicationStaging: true,
})
```

这不是后台 GC，不会自动清理或删除用户的 store 目录。`Maintain` 不创建新的 publication、不改写 CURRENT/IDENTITY/manifest 的字节、不做恢复或 tail repair；任何策略均不删除 segment 或 checkpoint stream，也不消费 compaction intent。既有 metadata、checkpoint 和 compaction 编码均不变。

### 验证与有序分类

所有检查完成后才允许删除或目录同步。检查验证 CURRENT 的 canonical envelope、IDENTITY 摘要/Store/profile、选中 manifest 的完整摘要/ID/generation 绑定，以及递归 source 绑定和退休 descriptor 的来源。缺失 CURRENT、选中 authority 或其 source closure 无效会返回错误，且在删除任何 publication metadata 或 quarantine 之前停止。目录排序、最大 generation 和候选文件名都不构成发布 authority。

这是 **metadata 验证，不是 semantic replay**。`Maintain` 不打开 Journal，不读取或验证 segment 历史和 checkpoint stream 的内容；成功报告不证明事务链、checkpoint projection 或业务根合法，也不授予 Ready/派发资格。完整恢复仍由正常 Open 路径负责。

`MaintenanceReport` 的四类 inventory 互斥，使用按字典序排序的 store-relative 路径：

| 字段                    | 含义                                                                                                                                                                                                     |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SelectedMetadata`      | CURRENT、IDENTITY、选中的 final manifest，以及其引用的 checkpoint 路径。                                                                                                                                 |
| `RetainedProofMetadata` | 未列入 selected 的 source proof closure：经验证的 `LayoutSource`、`RecoveryCheckpoint.Source` 所需 final manifests、引用的 checkpoint 路径，以及有效 pending compaction intents 和它们的 source proofs。 |
| `StagedMetadata`        | 已认证、未选中且不在 proof closure 中的 final manifests（包括过时或未完成发布的 final）、`manifests/.STAGE.<id>` 和根目录 `.CURRENT.<id>`。这是候选 inventory，不是独立删除授权。                        |
| `UnknownMetadata`       | 无法按已知名称/内容/绑定认证的 metadata 或未知条目；保留，不猜测归属。包括 manifests、compactions、根目录中的未知条目，以及没有 selected/proof 引用的 loose checkpoint 文件或 staging 条目。             |

Manifest 的 source 边要求 generation 严格递减，且完整摘要、generation、PublishedEnd、Position 和 PostRootSet 必须匹配被引用的 final manifest。closure 不只保护 CURRENT 的直接 source：**本轮所有有效 manifests（包括准备删除的 obsolete/staged manifests）的 sources 都保留**，并验证传递依赖。有效 intent 自身及其 source closure 也保留。表中的 checkpoint 路径只是引用，可能对应不存在的文件，不表示 stream 已验证。

候选 `.CURRENT.<id>` 必须具有 canonical 编码、正确的 Store/IDENTITY 绑定和文件名 ID；存在对应 final/staged manifest 时还须匹配其有效内容、摘要和 generation。没有对应 manifest 的完整候选仍可被认证为非 authority；部分写入或冲突候选则保留为 unknown。

只要存在 unknown，`PublicationReclamationBlocked=true`，`ReclamationBlockers` 就列出排序后的 unknown 路径。此时 **全部** publication metadata 删除被阻止，即使 `StagedMetadata` 非空且请求了 `DropPublicationStaging`；blocked 本身可伴随 `err == nil`，调用方必须检查该 flag。它不阻止单独显式请求的 `DropQuarantine`。非 authority 候选的缺失、截断或单文件超限等输入可列为 unknown；其他 I/O 故障、取消、聚合预算耗尽或 authority 检查错误则会使检查失败，不降级为可继续清理 quarantine 的 unknown。

`QuarantineBefore`、`RemovedQuarantine` 是排序后的 quarantine 内 basename；`RemovedStagedMetadata` 是排序后的 store-relative 路径。`PublishedSegments`、`PublishedManifests` 只是检查时目录中 `SEGMENT.`、`MANIFEST.` 前缀的物理计数，后者也包含 obsolete 或 malformed final 文件，不能当作 authoritative publication 数。

### 删除顺序、重试与多轮回收

1. 在独占 owner 下完成全量有界检查。默认 inspection 到此结束，不同步目录。
2. 请求 `DropPublicationStaging` 且未 blocked 时，**先** `SyncDir("manifests")`，再 `SyncDir(".")`，然后才开始删除。中断的 publisher 可能已经替换 CURRENT 却尚未同步根目录；此步骤会稳定当前观察到的 CURRENT namespace，避免旧 CURRENT 在崩溃后恢复却已失去它的 manifest。它不改写任何文件字节，也不依据候选提升发布位置，但不能把它误称为纯只读或保证保留此前未同步的 namespace 状态。
3. 按请求删除 quarantine 和未 blocked 的 `StagedMetadata`，同步相应目录，最后同步根目录。请求 publication 清理时，即使 blocked 或本轮没有候选，成功路径仍同步 manifests 和根目录；quarantine 的空重试也会同步其目录（若存在）和根目录。这使上次已移除但未完成目录同步的条目能够在重试中持久化。

删除不构成跨文件原子事务。取消、Remove 或 SyncDir 失败可返回部分 `Removed*` 报告；该列表不代替成功的目录同步。释放遗留资源后，用新的有效 context 显式重试维护，不能把失败报告视为清理已经持久完成。

过时依赖链可能需要多轮：本轮删除 obsolete manifest 后，其 source 仍保留到后续重新检查确认不再被引用时才可能回收，确保任意中断删除边界都不会遗留缺少 proof 的 surviving manifest。若删除该 manifest 后留下实际存在的 loose checkpoint stream，下一轮会把它列为 unknown 并阻止全部 publication 删除；`Maintain` 不会自动删除该 stream，也不保证反复调用最终清空所有候选。

### 预算与 CleanupError

每次检查的聚合预算为 `MaxMaintenanceEntries = 16_384` 个列举条目和 `MaxMaintenanceMetadataBytes = 64 << 20`（64 MiB）metadata 读取字节。条目预算覆盖根目录、segments、manifests、quarantine、compactions、checkpoints 的所有列举结果，包括非 metadata 条目；各文件既有的 codec/读取上限仍适用。聚合预算耗尽返回 `ErrLimit`，在删除之前停止。`DurableIO.List` **不分页**：预算是在单次 List 返回之后检查，不能限制该调用本身的分配，也不能中断单次底层 I/O；此 API 不承诺严格的总内存或即时取消上限。

读取文件或 owner 的 Close 失败时，返回错误保留可重试的 `*storage.CleanupError`，同时保留原始错误原因。用 `errors.As` 取得该 handle，保留 `d`，按 **file → owner** 顺序重试其 `Close()` 直到成功；文件尚未确认关闭时不能提前释放 owner。获取操作同时返回 live handle 和 error 的路径同样保留清理责任。关闭失败不能降级为 unknown、丢弃 handle 或先关闭调用方的 DirectoryIO；完成 cleanup 后才可重新取得 owner 或关闭 `d`。

实现见 [maintenance.go](../../storage/maintenance.go)、[publication_maintenance.go](../../storage/publication_maintenance.go) 和 [cleanup.go](../../storage/cleanup.go)；checkpoint 与 compaction 的补充边界见 [recovery-checkpoint-v1](recovery-checkpoint-v1.md)、[compaction-experimental-1](compaction-experimental-1.md)。
