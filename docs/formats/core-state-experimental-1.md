# ATE-CORE-STATE-1：当前实验 core schema

状态：experimental，模块 v0；以下字节是当前实现与 golden 的评审契约，尚未冻结长期 ABI。所有对象均使用 ATE-OBJECT-1 envelope。普通 scope 必须为 1..256 UTF-8 字节，且不得以保留前缀 `ate:` 开头；下表所列 core 记录只有 RootSet 使用系统 scope `ate:system`，系统 catalog/receipt radix 节点也使用该 scope。

## 固定 registry 与基本字段

| Type | Version | Space |
| --- | --- | --- |
| `ate.value` | 1 | 值所属请求 space |
| `ate.command` | 1 | 命令的请求 space |
| `ate.stateops` | 1 | 变更所属请求 space |
| `ate.receipt` | 1 | receipt RequestKey.Space |
| `ate.rootset` | 1 | `ate:system` |

除 Command 外，每个 Payload 均以 uint schema=1、uint flags=0 开头。Command 直接复用现有 M0 `CanonicalCommand`，有自己的原始域和 schema，没有 flags 字段。未知必需 Type、Version、payload schema 或非零 flags 返回 `ErrUnsupportedSchema`。非法字段、排序、scope、Refs、截断、非规范编码或尾随数据返回 `ErrInvalidStateData`，并保留原始 codec 错误，支持 `errors.Is`。

uint、bytes、string 均使用 ATE-CANON-1。普通 ObjectID 为 bytes(32)；optional root/value identity 为 presence uint(0) 或 uint(1)+bytes(32)。公开空 ObjectID `""` 表示空树或缺失值；32 个全零字节不是其编码，必须拒绝。普通 ID 不允许缺失、全零、非小写十六进制或错误长度。

对象 Refs 不是任意声明的边：每个 reader 从已解码字段重新提取依赖，去除空 sentinel，升序去重，与 envelope.Refs 精确比较。新增、缺失或替换依赖均被拒绝，重新计算外部 ObjectID 也不能绕过这些验证。

## Value

Payload 为 schema、flags、value kind 和对应内容：

| kind | 内容 |
| --- | --- |
| uint(0)，null | 无后续字段；公开 Value.Data 必须为空 |
| uint(1)，bytes | bytes，最多 1 MiB |

null、空 bytes 和 key 不存在是三种语义。Value 对象无 Refs。Reader 返回独立持有的 Data。

## StateBatch

StateBatch 是命令 Body，不是额外的 Object。顺序为：schema、flags、ExpectedRoot 的 pointer presence、存在时的 optional root identity、operation count、每个 operation。

ExpectedRoot=nil 编码 pointer presence=0；指向空树编码 `01 00`；指向非空根编码 `01 01 20 <32 digest bytes>`。因此未提供根前提与显式要求空根不同。

每个 operation 依次为：

1. Key bytes，0..1024 字节；任意二进制字节合法。
2. mutation kind：put=uint(1)，随后一个 inline Value；delete=uint(2)，无 Value 字段。
3. condition kind：any=uint(0)、absent=uint(1)，均无后续内容；value=uint(2)，随后一个 inline Value。

inline Value 与上节相同，仅有 kind 和对应内容，不重复 schema/flags。delete 的公开 Value 必须为零值；any/absent condition 的公开 Value 也必须为零值。多余 Data 不会被静默丢弃。

batch 必须包含 1..1024 个 operation，完整编码最多 4 MiB。Encoder 按 Key 的原始字节升序输出，不改调用者 slice，并拒绝重复 Key；Decoder 拒绝乱序或重复，不自动归一化，重新编码必须逐字节相同。Key、Value.Data、condition.Value.Data 及 ExpectedRoot pointer 均独立持有。

## Command

Payload 为原 M0 格式，按顺序：raw ASCII `ATE-COMMAND`+NUL、uint schema=1、Space string、Principal string、RequestID string、Method string、Body bytes、ExpectedFence presence，以及存在时的 Store string、LSN uint、CommitDigest string。没有新增 flags。

reader 严格检查原始前缀、最短 varint、完整消费与原请求合法性；对象 Space 必须等于命令内部 Space。Refs 必须为空。Standalone decoder 继续支持 M0 的 8 MiB Body 上限；作为 Command 对象时，完整 Payload 还受通用对象的 8 MiB 上限约束。

## Operations

Payload 按顺序为：schema、flags、BeforeRoot optional ID、AfterRoot optional ID、change count、每个 change 的 Key bytes、Before optional value ID、After optional value ID。Space 只由 envelope 指定，不在 Payload 重复。

change count 为 1..1024；Key 为 0..1024 字节，必须已严格按原始字节升序且唯一。每条输入命令均有一个 change，即使 Before=After；双空或同一非空 ID 的 no-op 合法。Refs 为 BeforeRoot、AfterRoot 及所有 Before/After 中的非空 ID 的升序去重集合。Reader 返回独立的 Key 字节。

## Receipt

Payload 按顺序为：schema、flags、RequestKey.Space string、Principal string、RequestID string、RequestDigest bytes(32)、Command bytes(32)、Operations bytes(32)、TxID bytes(16)、LSN uint、StateRoot optional ID。

RequestKey 遵循共享身份合法性，内部 Space 必须等于 envelope.Space。TxID 在公开模型中为 32 个小写十六进制字符，非全零；编码带长度前缀 `10`，不是 raw16。LSN 必须大于零。Refs 只包含 Command、Operations 和非空 StateRoot；RequestDigest 是命令摘要，不能当作 Object 边。

Receipt 不包含其所属 RootSet 或最终 CommitDigest，因此不会与 RootSet 的 receipt tree 产生摘要环。最终 fence 由已验证事务 envelope 提供。

## RootSet

系统 scope 的 Payload 依次为：schema、flags、DomainStates optional ID、ExecutionKnowledge optional ID、BranchRefs optional ID、LiveControl optional ID、CommandReceipts optional ID。五个字段均可为空。Refs 是这五个字段的非空升序去重集合。

## 独立 golden 与验证

`engine/statecodec_test.go` 与 `engine/records_test.go` 固定了由独立 Python unsigned-LEB128 编码与 hashlib 计算的 Value、StateBatch、RootSet、Receipt 向量。示例 Value：Space=`s`，bytes 内容 `61 00`，完整对象编码为：

```text
4154454f424a30310100096174652e76616c75650173010006010001026100
ObjectID=c98469c7be596a4b87ab53b6646e10181e9dba94108c89d80729966b5881967c
```

批、记录及 command 的测试覆盖边界、二进制 key、空根前提、null/empty bytes、原始内存所有权、未知 schema/flags、错误 Refs/scope/type/version、非规范及截断输入。Fuzz 同时验证可解码值的重新编码一致性，并为恶意 Payload 重算 generic ObjectID 后再执行 core 校验。

## Runtime 与回放约束

Core host state 使用 `core.state.mutate/v1`；静态 Graph/Memory/Knowledge 命令使用各自 canonical typed body，受保护 root/schema 见 [domain/core registry](domain-core-experimental-1.md)。显式启用 Agent Profile 时还有 `agent.apply/v1`，其 LiveControl snapshot/操作 schema 见 [agent/core registry](agent-core-experimental-1.md)。Method 的版本属于静态 registry。Unknown Type/Version/Method/payload schema 或缺失所需 profile 均拒绝重开，不按现版本猜测含义。

每笔 accepted command 存储其 canonical bytes；StateOps 的 Space/BeforeRoot、所有 Before/After 和 AfterRoot 均与历史根及原命令逐项相符。回放用记录的变化作为 Apply 输入，再核对完整派生对象序、receipt、Ref/Control updates 与 PostRootSet。它不重新调用外部模型、策略插件或用今日余额/权限裁定历史。

`core.state.mutate/v1` 的原 layout 固定顺序仍是 Command→必要新 Values→domain nodes→StateOps→domain catalog nodes→Receipt→receipt nodes→RootSet；Agent layout 单独注册。两者共用 `finishPlan` 的 receipt/RootSet 封装；每组 tree delta 为 child-before-parent，已有对象不重复存储，内容相同的 StateOps 可引用旧记录。实验 layout 及 registry 版本不能被后续 writer 静默重解释。

DomainStates/CommandReceipts 为系统树；启用的 agent.apply/v1 还更新系统 LiveControl 树，ExecutionKnowledge/BranchRefs 尚保留。通用 domain batch 不能写控制根。RootSet 由 engine 派生，调用者不能提交任意根。Receipt.Key/TxID/LSN/RequestDigest 绑定 envelope；receipt 的最终 Fence/RootSet 从日志重建的 TxID proof 索引取得，不在 receipt payload 中反向嵌入。

Open 的语义回放钩子在任何尾部隔离/截短和发布入口 sync 之前完成。已发布对象的 hash/schema/长度损坏是内容故障；新请求超限是 admission 资源拒绝；普通 EIO 是 IO 失败。内容校验失败和普通 IO 读取失败都必须停止输出，不能返回旧 snapshot 或假 NotFound；EIO 不伪称有确定的内容损坏。新请求的正常资源拒绝不使健康 store 进入 Fenced。
