# ATE segmented store metadata — experimental 2

M1e 的实验格式扩展；不是冻结的 v1 格式，也不是设备掉电认证。IDENTITY、CURRENT、初始 96-byte segment header、canonical 事务帧与提交摘要均保持原定义。新 manifest schema2 认证有序的 sealed/active 段目录；successor header 和 seal footer 使用以下新布局。

旧 schema1 单段 manifest 字节保持不变，仍可打开。没有轮转的提交可以继续写 schema1；首次轮转后的 manifest 使用 schema2。不能用旧 reader 忽略必需的分段目录。CURRENT 始终是唯一发布 authority；完整 seal、新段或完整未发布 TX 都不能单独提升提交位置。

## 地址与范围

`Publication.PublishedEnd`、`ScannedTransaction` offsets、`RecordLocation` 和 `Journal.ReadAt` 使用一个稳定的逻辑字节流：初始 header 的 `[0,96)`，随后顺序连接所有段的 canonical TX frames。后续 header 和所有 footer 均不进入逻辑地址。这个逻辑视图不是磁盘连续文件，调用者不能将它当作某个段的物理 offset。

一个事务允许跨多个段；一个 frame 必须完整位于同一个段的 data interval。布局改变不修改 canonical frame 字节、事务 LSN、摘要、Receipt 或 RootSet。段边界记录的 Position 只指最后一笔**完整事务**；一个大事务继续通过多个段时，这些边界的 Position 可以相同。

段 header 和 manifest 位置中的前事务摘要是逻辑提交链摘要；successor 的 PreviousSegmentDigest 则是前段包含 seal footer 的完整物理 SHA-256。这两种摘要不能互换。

## Manifest schema2

Magic 仍为 `ATEMAN01`。Envelope 为 `magic || canonical payload || SHA256(magic || payload)`，schema 为 uint 2，flags 必须为 0。schema1 的总长度上限保持 4096 bytes；schema2 的硬上限为 `MaxManifestBytes=1,048,576`，在解析和分配前拒绝超限。

schema2 payload 先按原顺序保存以下字段：

1. Schema uint=2、Flags uint=0。
2. Store ID bytes(16)、Generation uint、ManifestID bytes(16)。
3. SegmentID bytes(16)、SegmentHeaderDigest bytes(32)、PublishedEnd uint、PrefixDigest bytes(32)。
4. Position.LSN uint、Position.Digest bytes(32)、PostRootSet bytes(32)。
5. SegmentCount uint，然后是 SegmentCount 个 descriptor。

第 3 项的 SegmentID/HeaderDigest/PrefixDigest 必须与**最后 active descriptor**完全相同，不是可忽略的兼容缓存。PublishedEnd 是最终逻辑末端；Position/PostRootSet 是最后完整事务。

每个 descriptor 按以下顺序 canonical 编码。bytes(N) 自带最短长度前缀，uint 为最短 unsigned varint。

| 字段 | 编码 / 语义 |
| --- | --- |
| SegmentID | bytes(16)，非零 ID |
| HeaderSize | uint，初始 96，后续 160 |
| HeaderDigest | bytes(32)，完整物理 header SHA-256 |
| Sealed | uint，0=active、1=sealed；其他必需状态 Unsupported |
| DataEnd | uint，物理 exclusive end，不含 footer |
| PrefixDigest | bytes(32)，物理 `[0,DataEnd)` SHA-256 |
| SealedDigest | bytes(32)，sealed 时为含 footer 的完整物理 SHA-256；active 必须全零 |
| LogicalStart | uint，本段第一个 frame 的逻辑位置 |
| LogicalEnd | uint，本段 frame interval 的逻辑 exclusive end |
| StartPosition.LSN / Digest | uint / bytes(32)，第一 frame 前最后完整事务 |
| EndPosition.LSN / Digest | uint / bytes(32)，DataEnd 前最后完整事务 |

目录包含 2..1024 个唯一段；除最后 active 段外，全部必须 sealed。sealed 段不得没有 frame 数据。每段必须满足 `LogicalEnd-LogicalStart == DataEnd-HeaderSize`，相邻逻辑范围和完整事务 Position 连续。第一段 LogicalStart=96、StartPosition 为 IDENTITY 导出的 genesis；最后范围和 Position 必须与 manifest 的最终字段相同。物理和逻辑地址不能超出 ReaderAt 的有符号范围，footer 长度也必须留出空间。

这些 DTO 关系检查不代替恢复：真实 header、footer、prefix/full digest、frame 边界、事务链、语义根与源文件长度都必须进一步验证。

## Successor header（160 bytes）

固定整数均为 little-endian，不是 canonical varint。

| Offset | 长度 | 字段 |
| --- | --- | --- |
| 0 | 8 | Magic `ATESEG02` |
| 8 | 2 | Schema uint16=2 |
| 10 | 2 | Flags=0 |
| 12 | 4 | Reserved=0 |
| 16 | 16 | Store ID |
| 32 | 16 | SegmentID |
| 48 | 8 | LogicalStart uint64 |
| 56 | 16 | PreviousSegmentID |
| 72 | 32 | PreviousSegmentDigest：前段含 footer 的物理 SHA-256 |
| 104 | 8 | Position.LSN uint64 |
| 112 | 32 | Position.Digest |
| 144 | 12 | Reserved=0 |
| 156 | 4 | CRC32C，覆盖 `[0,156)` |

Position 是最后完整事务；下一笔事务的 LSN 隐含为 Position.LSN+1，可能是从前段继续过来的事务。LogicalStart 必须大于 96，前段与本段 ID 必须不同，ID/摘要均非零。CRC 验证后再解释 schema/flags/reserved。

## Seal footer（160 bytes）

| Offset | 长度 | 字段 |
| --- | --- | --- |
| 0 | 8 | Magic `ATESEAL1` |
| 8 | 2 | Schema uint16=1 |
| 10 | 2 | Flags=0 |
| 12 | 4 | Reserved=0 |
| 16 | 16 | Store ID |
| 32 | 16 | SegmentID |
| 48 | 8 | DataEnd uint64，不含 footer |
| 56 | 32 | PrefixDigest：本段 `[0,DataEnd)` SHA-256 |
| 88 | 8 | LogicalEnd uint64 |
| 96 | 8 | Position.LSN uint64 |
| 104 | 32 | Position.Digest |
| 136 | 20 | Reserved=0 |
| 156 | 4 | CRC32C，覆盖 `[0,156)` |

footer 在物理 DataEnd 处。封存段的完整长度必须为 `DataEnd+160`；后续不允许追加或原地更改。footer 的 PrefixDigest 不包含自身，避免循环；descriptor 的 SealedDigest 是 `SHA256(header || frames || footer)`，用于后继绑定。

## 发布与恢复顺序

发布者在任何文件写入前检查整批 frame 布局和上限。需要轮转时，先追加并同步前段 footer，再构造绑定该完整摘要的新 header。所有触及的数据段和新目录项同步后，整批通过**一次** manifest/CURRENT P1 发布。中间段和事务不单独 ACK；同批事务仍保留各自原始 receipt 和逻辑提交 proof。

恢复只跟随 CURRENT：验证其有序目录、每段认证范围和完整事务链，完成语义 replay 后，才可以隔离未发布数据。active 只认证 `[0,DataEnd)`，额外 tail 不使旧发布 prefix 失效；sealed 必须认证全长和 footer。未引用的新段属于未发布布局，绝不能因为 header/seal/transaction 完整而被采用。显式修复必须先持久保留 active tail 和全部孤立段证据，再截短或移除；默认拒绝并保留。

## 规格工件

`storage/segment_metadata_test.go` 保存 successor/header、footer、schema2 manifest 的独立 Python 完整字节向量；这些向量用 `hashlib.sha256`、最短 varint 和按位 CRC32C 推导。manifest fixture 使用合成的非零 prefix digest，仅用于纯 codec 规范，不伪装成可恢复完整 store。

测试包括 legacy golden 不变、全部字节截断/翻转、重签后未知 schema/flags/reserved、非法 descriptor 关系、稀少输入声明大目录、超过 4KiB 的合法 schema2 目录、超限零分配拒绝、owned 解码，以及 fuzz 成功解码后的规范重编码等价。
