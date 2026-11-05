# ATE-JOURNAL-EXPERIMENTAL-1

状态：experimental，模块 v0。header 的数值版本为 1，但不是“ATE V1 磁盘格式已冻结”的声明。当前编码对象是无 segment header/footer 的原始帧流；没有 CURRENT/manifest 和可打开的磁盘 store。

## Frame

整数字段使用 little-endian，payload 内 canonical 字段使用 ATE-CANON-1。

| Offset | 字节 | 字段 |
| --- | --- | --- |
| 0 | 8 | ASCII `ATEFRM01` |
| 8 | 2 | version=1 |
| 10 | 2 | kind，下表编号 |
| 12 | 4 | flags=0；任何非零值拒绝 |
| 16 | 4 | headerLength=48 |
| 20 | 4 | payloadLength |
| 24 | 16 | TxID，非全零原始字节；Go DTO 为 32 位小写十六进制 |
| 40 | 4 | ordinal，事务内从 0 连续递增 |
| 44 | 4 | header CRC32C，仅覆盖 [0,44) |
| 48 | payloadLength | payload |
| 随后 | 0..7 | 零 padding，使 payload+padding 为 8 的倍数 |
| 随后 | 4 | record CRC32C，覆盖完整 header+payload+padding，不含 trailer |
| 随后 | 4 | ASCII `END!` |

CRC 使用 Castagnoli（反射多项式 0x82f63b78，标准 initial/final XOR）。Header CRC 与 record CRC 各自独立；非零 padding 即使重算 CRC 也拒绝。

| kind | 编号 | 当前 payload |
| --- | --- | --- |
| TX_BEGIN | 1 | schema(1)、Space string、Principal string、RequestID string、RequestDigest bytes(32) |
| OBJECT | 2 | opaque bytes |
| EVENT | 3 | opaque bytes |
| STATE_OPS | 4 | opaque bytes |
| REVISION | 5 | opaque bytes |
| REF_UPDATES | 6 | opaque bytes |
| CONTROL_UPDATES | 7 | opaque bytes |
| COMMAND_RECEIPT | 8 | schema(1)、Receipt bytes(32) |
| TX_COMMIT | 9 | 下节 |

实体 key、事件因果、StateOps、根和引用的语义校验不属于此物理 parser。body 可按依赖拓扑混排，不按类型固定分区；真正内容拓扑验证在语义 replay 阶段补齐。

## 事务顺序与摘要

严格顺序：`TX_BEGIN → (OBJECT|EVENT|STATE_OPS|REVISION)* → REF_UPDATES → CONTROL_UPDATES → COMMAND_RECEIPT → TX_COMMIT`。三个尾部帧各一次，即使其 updates 为空；TxID 全程一致，ordinal 从 0 无缺口。最少 5 帧。

PreCommitFramesDigest = SHA-256(raw ASCII `ATE-TX-FRAMES` + NUL + 所有 TX_COMMIT 前物理帧的完整字节)。范围包括 Begin、body 和三个尾部帧的 header/payload/padding/trailer，不含 TX_COMMIT。

TX_COMMIT payload 依次为：

1. schema uint(1)
2. LSN uint，非零
3. PreviousCommitDigest bytes(32)，非零
4. PreCommitFrameCount uint（所有 commit 前帧数，等于 commit ordinal）
5. PreCommitFramesDigest bytes(32)
6. PostRootSet bytes(32)，非零
7. Receipt bytes(32)，非零且与 COMMAND_RECEIPT 相同
8. RequestDigest bytes(32)，与 Begin 相同

CommitDigest = SHA-256(raw ASCII `ATE-TX-COMMIT` + NUL + TxID 原始 16 字节 + 完整 canonical TX_COMMIT payload)。因此 TxID、LSN、前摘要、完整前置帧顺序、根、receipt 和请求摘要都受到绑定；不嵌入自身摘要，避免自引用。

RequestKey/Begin 被 PreCommitFramesDigest 绑定；内容引用当前只校验结构，不证明存在性。PostRootSet 将指向后续规范 RootSet 对象，不在此阶段假定某个固定 root 列表。

## 扫描与限制

`NewScanner(reader, previous Position, Limits)` 接受调用者提供的上一 LSN/摘要，要求下一笔 LSN=previous+1，前摘要相等，不允许 uint64 回绕。Scanner 单 owner，不支持并发 Next。

默认/硬上限：payload 16 MiB；事务物理总字节 64 MiB；每事务帧数 65,536。Limits 各字段 0 使用默认，可降低、不可提高硬上限。读取 payload 前检查总长度和剩余事务预算；不足一个 header 的预算不消费输入。

成功只返回完整事务，更新 Position/LastCompleteOffset；失败保留上一完整位置，返回含物理 offset 和事务起点的 ScanError，后续调用返回同一终止错误。Offset 是实际已消费字节，可能包含失败帧。没有索引、回调 publication 或磁盘修改。

| 结果 | 含义 |
| --- | --- |
| io.EOF | 事务边界的干净结束 |
| ErrIncomplete + io.ErrUnexpectedEOF | 结构性截断，或事务尚未遇到 Commit；不推断能否丢弃 |
| ErrIntegrity | CRC、LSN/前摘要链、前置帧摘要或 Commit 绑定不匹配 |
| ErrMalformed | magic/TxID/顺序/ordinal/padding/已知 schema 的结构错误 |
| ErrUnsupported | 未知版本、kind、flags 或 payload schema |
| ErrLimit | 分配/读取前拒绝超限输入 |
| 其他 I/O error | 原错误通过 errors.Is 保留，不改称 torn tail |

## 发布证据边界

结构完整不等于 durable。恶意或错误代码可以重新计算 CRC 和最终 CommitDigest；只有与可信的 CURRENT/manifest 认证 tip 对比才能证明整个已发布前缀。最后一笔重新封装的合法根可能被裸 Scanner 接受，但必然产生不同 digest，后续恢复层必须拒绝与发布证据不一致的 tip。

Scanner 不截断、跳过损坏或提升未发布的完整 Commit。已发布前缀内的缺失/损坏应 fail-stop；尾部隔离权限只能由未来恢复层基于已验证发布边界决定。

## Golden 与 invalid corpus

`storage/frame_test.go` 固定完整 frame 向量，`testdata/journal/transaction-v0.json` 固定完整混排事务向量和 CommitDigest。两者由独立 Python 按位 CRC32C/编码器推导，不调用 Go 被测 encoder 生成预期值。

`storage/transaction_test.go` 覆盖每字节截断、CRC 合法但顺序/摘要错误、metadata 修改、预算边界、失效后的原子位置和错误锁存；FuzzFrame 与 FuzzTransactionScanner 验证 parser 健壮性与成功重编码性质。
