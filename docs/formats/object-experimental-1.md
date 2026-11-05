# ATE-OBJECT-1：当前实验不可变对象 envelope

状态：experimental，模块 v0；供当前实现与 golden 评审，未冻结长期磁盘 ABI。通用对象编码位于 `codec/object.go`，仅依赖 `model` 与 ATE-CANON-1 primitives。

## 字段与摘要

完整编码按以下顺序连接，禁止剩余字节：

| 字段 | 编码与约束 |
| --- | --- |
| magic | 原始 8 字节 ASCII `ATEOBJ01`，无长度前缀 |
| envelope schema | uint(1) |
| flags | uint(0) |
| Type | string，1..128 UTF-8 字节 |
| Space | string，1..`model.MaxIdentityBytes`（256）UTF-8 字节 |
| Version | uint，非零 |
| reference count | uint，0..4096 |
| 每项 reference | bytes，长度必须恰好为 32；内容为 ObjectID 的原始 SHA-256 字节 |
| Payload | bytes，0..8 MiB |

其中 uint、string、bytes 遵循 ATE-CANON-1：无符号最短 LEB128；string 与 bytes 带最短字节长度前缀。reference 的编码为 `20` + 32 个摘要字节，不是 64 字节十六进制文本，也不是无前缀的 raw32。

公开 Go 值使用 `Object{Type string, Space model.SpaceID, Version uint64, Refs []model.ObjectID, Payload []byte}`。每个 Ref 必须为 64 个小写十六进制字符、非全零；输入必须已严格升序且唯一。编码器拒绝乱序与重复，不自动排序或去重。全零的树空根 sentinel 不得进入 Refs。空 Refs 和空 Payload 合法；nil 与空 slice 没有不同的编码含义。

`ObjectID = lowercase_hex(SHA-256(raw("ATE-OBJECT") || 00 || 完整编码))`。

域为原始 ASCII `ATE-OBJECT` 加单个 NUL，无长度前缀。摘要覆盖 magic、schema、flags、Type、Space、Version、Refs 和 Payload。编码没有自校验尾；`VerifyObject(id, encoded)` 接受外部 ObjectID，先完整验证规范编码，再检查摘要是否匹配。重算一个非法 envelope 的摘要不能使它通过验证。

文本不做 Unicode、换行、空白或大小写归一化；NUL 是合法 UTF-8 字符。长度按 UTF-8 字节计算。

## 资源、所有权与错误

导出上限为 `MaxObjectRefs=4096`、`MaxObjectPayload=8<<20`、`MaxEncodedObject=16<<20`。解码器先检查整个 envelope 大小，读取 reference count 后先检查上限及可用编码长度，再分配 reference slice。解码返回独立持有的字符串、Refs 和 Payload；修改或释放输入不影响结果，修改结果也不影响输入或另一次解码结果。

| sentinel | 含义 |
| --- | --- |
| `ErrObjectUnsupported` | 未知 envelope schema 或非零 flags |
| `ErrInvalidObject` | 非法字段、摘要 reference、非规范整数、截断、非法 UTF-8 或尾随数据 |
| `ErrObjectLimit` | 整体编码、Type、Space、Refs 数量、reference 字节长度或 Payload 超限 |
| `ErrObjectDigest` | 规范 envelope 与外部 ObjectID 不匹配，包括空或非法拼写的外部 ID |

primitive 原因保留，可用 `errors.Is` 检测 `ErrTruncated`、`ErrNonCanonical`、`ErrIntegerOverflow`、`ErrLengthOverflow`、`ErrInvalidUTF8`、`ErrTrailingData`。失败不返回部分 Object。

通用 codec 接受任何合法 Type 和任意非零 Version，不维护业务 schema registry，也不解释 Payload。上层静态 registry 负责拒绝未知的必需 Type/Version，并验证 Payload 的语义及 Refs 与实际依赖相符；合法 generic envelope 不代表已通过这些上层检查。

## 独立 golden

`codec/object_test.go` 的向量使用独立 Python base-128 编码与 `hashlib.sha256` 生成。字段为 Type=`example.kind`、Space=`space-中`、Version=300，Refs 为各 32 字节 `01`、`02`，Payload=`00 ff 41 00`。

```text
encoded:
4154454f424a303101000c6578616d706c652e6b696e640973706163652de4b8adac02022001010101010101010101010101010101010101010101010101010101010101012002020202020202020202020202020202020202020202020202020202020202020400ff4100
ObjectID:
2e71f5c6b11ff1bc17b58c8015aea0a002c7109295493edc1c253f74d2836def
```

测试分别改变 Type、Space、Version、Refs、Payload 验证摘要绑定；包含边界、所有权、未知 payload schema、UTF-8 不归一化、非法输入与带重算外部摘要的 fuzz 路径。
