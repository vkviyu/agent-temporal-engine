# ATE-STORE-EXPERIMENTAL-1

> 本文保留原阶段定义；M1e 的多段扩展与逻辑地址说明见 [segmented-store-experimental-2](segmented-store-experimental-2.md)。旧单段字节保持不变。

状态：experimental，模块 v0。数值 schema/version=1 不表示磁盘格式或 API 已冻结。此文件定义 M1b 单 segment store 的物理 metadata；事务帧与链摘要见 [journal-experimental-1.md](journal-experimental-1.md)，canonical 基元见 [canonical-experimental-1.md](canonical-experimental-1.md)。物理发布边界不是语义 replay、授权验证或业务状态恢复的证明。

## Metadata envelope 与 canonical 字段

IDENTITY、manifest、CURRENT 均为完整文件：`magic[8] || canonical payload || SHA-256(magic || payload)[32]`。checksum 不包含自身。metadata 总长度硬上限为 4,096 字节；解码在分配、解析任何字段之前拒绝超限输入，不根据不可信长度分配缓冲区。文件没有隐式 padding，payload 必须恰好消费，追加字段即使重算 checksum 也被拒绝。

payload 整数为最短 unsigned varint；bytes/string 以最短 varint 字节长度开头，string 必须合法 UTF-8。表格从上到下是固定字段顺序，所有字段必填。schema 和 flags 均是 canonical uint；当前 schema=1、flags=0。

StoreID、SegmentID、ManifestID 的 Go 表示统一为 32 位小写十六进制、非全零；metadata payload 使用 `bytes(16)` 原始值，不保存十六进制字符串。摘要统一为 `bytes(32)` 原始 SHA-256 值。LSN、generation 和 offset 均为 uint64；codec 不强制 generation 与 LSN 相等，跨文件的一致性由 store 协调层验证。

### IDENTITY

magic 为 ASCII `ATEID001`。

| 顺序 | 字段 | 限制 |
| --- | --- | --- |
| 1 | schema uint | 1 |
| 2 | flags uint | 0 |
| 3 | Store bytes(16) | 非零 |
| 4 | Profile string | `file-sync-experimental-1` |

Profile 定义同步契约，不认证设备、文件系统或操作系统的故障行为。未知 profile 拒绝，不回退为其他模式。

`IdentityDigest = SHA-256(完整 IDENTITY 文件)`，范围包含 magic、payload 和文件 checksum。

`GenesisPosition.LSN = 0`；`GenesisPosition.Digest = SHA-256(raw ASCII "ATE-STORE-GENESIS" || NUL || IdentityDigest 原始 32 字节)`。域分隔字符串没有 canonical 长度前缀。

### Manifest

magic 为 ASCII `ATEMAN01`。

| 顺序 | 字段 | 限制 |
| --- | --- | --- |
| 1 | schema uint | 1 |
| 2 | flags uint | 0 |
| 3 | Store bytes(16) | 非零 |
| 4 | Generation uint | uint64 |
| 5 | ManifestID bytes(16) | 非零 |
| 6 | SegmentID bytes(16) | 非零 |
| 7 | SegmentHeaderDigest bytes(32) | 非零；完整 96 字节 segment header 的 SHA-256 |
| 8 | PublishedEnd uint | >=96；相对 segment 起点的 exclusive offset |
| 9 | PrefixDigest bytes(32) | 非零；segment `[0, PublishedEnd)` 的 SHA-256 |
| 10 | Position.LSN uint | uint64 |
| 11 | Position.Digest bytes(32) | 非零；genesis 或最后已发布 CommitDigest |
| 12 | PostRootSet bytes(32) | LSN=0 时必须为零，LSN>0 时必须非零 |

metadata codec 校验字段长度和值域。PublishedEnd 是否处于事务边界、prefix/header 摘要是否匹配实际 segment、Position 是否来自 IDENTITY genesis 和被验证的事务链，以及 PostRootSet 是否与末笔 Commit 相同，均由恢复协调层验证。PostRootSet 不在本阶段证明业务根对象存在或语义合法。

### CURRENT

magic 为 ASCII `ATECUR01`。

| 顺序 | 字段 | 限制 |
| --- | --- | --- |
| 1 | schema uint | 1 |
| 2 | flags uint | 0 |
| 3 | Store bytes(16) | 非零 |
| 4 | Generation uint | uint64 |
| 5 | ManifestID bytes(16) | 非零 |
| 6 | ManifestDigest bytes(32) | 非零；完整 manifest 文件的 SHA-256，包含其 checksum |
| 7 | IdentityDigest bytes(32) | 非零；完整 IDENTITY 文件的 SHA-256 |

CURRENT 自身 checksum 证明字节完整性；它指向的 manifest、IDENTITY 和 segment 必须进一步匹配，不能根据单个 metadata 文件独立提升发布位置。

## Segment header

Header 固定 96 字节。表内固定整数字段使用 little-endian，与 payload 中 canonical 整数不同。schema 1 仅支持初始 segment，FirstLSN 必须为 1；跨 segment 协议尚未定义。

| Offset | 字节 | 字段 |
| --- | --- | --- |
| 0 | 8 | ASCII `ATESEG01` |
| 8 | 2 | version=1 |
| 10 | 2 | flags=0 |
| 12 | 4 | reserved=0 |
| 16 | 16 | Store，非全零原始 ID |
| 32 | 16 | SegmentID，非全零原始 ID |
| 48 | 8 | FirstLSN=1 |
| 56 | 32 | PreviousDigest，非零；初始段应为 IDENTITY 导出的 genesis digest |
| 88 | 4 | CRC32C，仅覆盖 `[0,88)` |
| 92 | 4 | 零 padding |

CRC32C 使用 Castagnoli，反射多项式 `0x82f63b78`，标准 initial/final XOR，与 frame CRC32C 相同。reserved 和末尾 padding 必须为零，即使 CRC 正确也拒绝非零值。首个事务 frame 从 offset 96 开始；没有 footer。

## Codec 错误与验证向量

| 错误 | 含义 |
| --- | --- |
| ErrUnsupported | 未知 schema/version、profile 或 flags |
| ErrMalformed | 已知 schema 下的非法长度、ID、零摘要、位置值、保留字节、padding、magic 或 canonical 结构 |
| ErrIntegrity | metadata SHA-256 checksum 或 segment header CRC32C 不匹配 |
| ErrLimit | metadata 输入大于 4,096 字节；在解析与分配之前检查 |

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

进程退出后可见的新 CURRENT 可能来自未完成目录同步的 rename，因此验证完且处理完尾部后，Open 必须再次同步 store 根目录，才能返回 Ready。RecoveryReport.Verified 只表示引用链与字节校验；PublicationSynced 表示本次已重建同步入口；不返回可用 Journal 的报告不能作为派发 grant。

## 未发布 tail

默认 TailReject：验证 published prefix，报告尾部大小和 `preserved`，返回 RecoveryRequired，不修改 active segment。即使 tail 包含完整、校验正确的下笔事务，也不提升。

显式 TailQuarantine：先创建并同步 quarantine 目录名字；流式复制**所有原始 tail 字节**至新证据文件，完成文件同步/关闭、quarantine 目录同步；然后只在 PublishedEnd 处截短并同步 active segment，最后重建 CURRENT 目录同步并返回 Ready。

任何隔离 I/O 失败都返回 RecoveryIncomplete，不把它当成功恢复。若截短已持久化，协议保证此前完整证据文件及名字已同步。重复恢复可能留下 partial/orphan 文件，当前没有自动清理或回滚已发布历史。

## 当前限制与寿命

JournalOptions 对总 published 字节和事务数设独立资源上限，默认 1 GiB/100,000 笔、硬上限 64 GiB/1,048,576 笔；单笔与 payload 限制沿用 frame registry。隔离 tail 不超过选定的单事务字节上限。降低选项可能使已有 store 明确返回 Limit，但不能改写或丢弃历史。

Close 停止新操作，先关闭 segment，再释放 owner。失败保持 Closing，重试只处理尚未确认退休的资源；只有成功关闭 Journal 后才能关闭调用方持有的 DirectoryIO。Close 不调用 SyncDir 或提升未确认事务。

构造、metadata 读取或候选写入失败时，关闭本身也可能失败。CleanupError 保留待退休资源的引用并支持 Close 重试；外层构造错误按临时文件→active segment→owner 聚合这些资源。Publish 的 cleanup 则保留在 Fenced Journal 中，由 Close 先处理。错误 acquisition 若已取得句柄，应返回非 nil 句柄与 error，调用方仍须清理；不能丢失 lease 后让锁永久滞留。

本格式没有封存/footer、轮转、跨段事务、group commit、checkpoint、compaction 或读者 pins。所有元数据和根都由单协调器安装，不允许未来后台维护者绕过 CURRENT 的唯一发布路径。
