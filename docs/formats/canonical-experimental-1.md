# ATE-CANON-1：当前实验编码

状态：experimental，模块 v0；用于当前测试与实现评审，尚未冻结长期 ABI。此注册表仅覆盖已实现字段；不代表完整 Object/Event/Revision/Profile registry 已完成。

## 基础规则

- 无符号整数：unsigned LEB128，必须为最短编码；uint64 至多 10 字节，第十字节只能为 0 或 1，且最终必须满足最短性。溢出与截断分开报告。
- 字节串：最短 varint 字节长度 + 原字节；单值上限 16 MiB。nil 与空 byte slice 都编码为 `00`，不靠它们表达 optional。
- 文本：字节串形式，严格 UTF-8；不做 Unicode/换行/空白规范化，NUL 是合法字节。
- optional：schema 显式写 presence=0/1，再写值；未定义的值由相应 schema 拒绝。
- 固定字段顺序；不进行反射 struct 编码；IPC JSON 不决定摘要。不提供隐式 map 排序、浮点或动态类型。
- Decoder.Bytes 借用输入内存；持有/修改需自行复制。所有失败保持 decoder offset 不变；Done 拒绝剩余字节。

示例：uint(128)=`80 01`；bytes(`00 ff 01`)=`03 00 ff 01`；string(`中`)=`03 e4 b8 ad`。`80 00` 不是合法的零。

## 命令 envelope

`engine.CanonicalCommand` 的字段依次为：

| 字段 | 编码 |
| --- | --- |
| 域 | 原始 ASCII `ATE-COMMAND` + NUL，无长度前缀 |
| schema | uint(1) |
| Space、stable Principal、RequestID、Method | 各为 string，依次排列；各 1..256 UTF-8 字节 |
| Body | bytes，最多 8 MiB |
| ExpectedFence presence | uint(0) 或 uint(1) |
| presence=1 时 | Store string（1..256 字节）、LSN uint、CommitDigest string（64 小写十六进制字节） |

CommandDigest = SHA-256(上述完整字节)，公开 DTO 使用 64 位小写十六进制文本。RequestID 属于该 envelope；临时 CorrelationID、连接、调用者等待 deadline、服务器分配的 TxID/LSN 不属于它。

Body 是上层 schema 已规范编码的域命令字节。当前核心只保留其字节，不解析任意业务 JSON，也不保证 JSON 字段重排后摘要相同；后续 profile 命令有自己的显式编码器。

ExpectedFence 的缺失与存在明确区分；它是幂等请求内容的一部分。相同 RequestKey 更换前置条件会产生 IdempotencyConflict。已经存在的同摘要 receipt 优先于新命令 CAS 检查；临时拒绝不被持久记作 receipt。

## 当前资源上限

所有大小按字节计。`model.MaxIdentityBytes=256`，`api.MaxCommandBody=8 MiB`，`codec.MaxByteString=16 MiB`。可配置资源限制的后续版本不得静默改变已编码身份；改变这些 schema 限制需明确版本决策。

Golden 位于 `codec/canonical_test.go`、`engine/command_test.go`；包括 NUL 字段边界碰撞反例与非法前置条件。
