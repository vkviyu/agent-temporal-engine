# ATE-RADIX-1：持久路径压缩 byte radix

状态：experimental，模块 v0；M1c 的纯算法与对象格式，不承诺长期 ABI。`state` 仅依赖 `codec`、`model` 与标准库，不导入 storage、engine 或 api。保存对象、校验外部 value schema、提交 root/fence 由上层负责。

## Root 与对象

`state.EmptyRoot = model.ObjectID("")` 只表示空树，不是合法 ObjectID，也不能放入对象 envelope 的 Refs。非空树 root 是一个 `codec.Object` 的内容摘要；旧 root 永远可读，Apply 不修改、删除或写入 reader 的对象。

每个节点使用通用实验 object envelope，Type=`ate.radix`、Version=1、Space=所属 space。对象身份由通用 codec 的 `SHA-256("ATE-OBJECT" + NUL + canonical envelope)` 计算；节点本身没有额外 checksum 或自引用字段。空 digest、全零 digest、非小写或非 64 位十六进制 digest 均不合法。

Payload 按以下顺序使用 ATE-CANON-1 编码；`bytes32` 表示长度前缀为 32 的 byte string，不是无前缀的 raw digest：

| 字段 | 编码与约束 |
| --- | --- |
| Prefix | bytes，0..1024 字节；任意 byte，允许 NUL |
| Value presence | uint(0) 或 uint(1) |
| presence=1 时 Value | bytes32，外部 value object 的 digest |
| ChildCount | uint，0..256 |
| 每个 child 的 Label | uint，0..255，严格升序 |
| 每个 child 的 Ref | bytes32，child node 的 digest |

Payload 必须恰好结束；非最短整数、未知 presence、错误 digest 长度、零 digest、重复/逆序 Label、未消费的字节均拒绝。节点必须有 Value 或 children；无 Value 的节点必须至少有两个 children，一个 child 的节点要压缩合并。没有 Value 和 children 的树使用 EmptyRoot，不存空节点。

Child 的 Prefix 包含边的 Label；被访问时其 Prefix 必须非空且第一字节等于 Label。任一路径所有 Prefix 的总长度至多 1024 字节。根可以有空 Prefix，因此可以保存空 key，并同时保存其他 key。节点 Value 不消耗额外 key 字节。

Envelope Refs 必须**恰好**等于 Value（若存在）与所有 child Ref 的严格升序去重集合；排序采用 ObjectID 小写十六进制字节序，与 digest 字节序一致。Value 与 child 引用相同 ID 时，Refs 只出现一次。Value 必须是合法非零 ObjectID；其实际存在性、type 与同 space 要求由上层 object registry 校验，state 不读取 value 内容。

## 读取与验证

`ObjectReader.ReadObject(ctx, id)` 返回通用 encoded object。Get/Apply 对每个实际加载的节点先用 `codec.VerifyObject` 校验 envelope、schema、flags 与外部内容摘要，再校验节点 type、space、version、payload、Refs 集合与压缩结构。向 child 走访时还验证 Label/Prefix 关系与累计路径长度。读取失败、上下文取消与 hash/schema 错误直接作为错误返回。

`ValidateNodeObject(ctx, obj, reader)` 校验传入 object 的 envelope/payload，以及所有直接 children 的身份、节点 schema、space、Label/Prefix 与相对路径长度。它不递归扫描整棵树，不读取 Value；上层可用 overlay reader 校验尚未持久化的 delta。它没有外部 claimed ID 参数，若对象来自不可信存储，调用方应先用 `codec.VerifyObject(claimedID, raw)` 校验该 ID。

## Copy-on-write 与 delta

Apply 按传入顺序执行最多 1024 次 Mutation，重复 key 为 last-write-wins。Delete 忽略 Value，缺失 key 的 Delete 是 no-op；未变化的 set 保持 root。每次修改只复制所访问路径：插入在公共前缀处拆分，删除后将无 Value 且只有一个 child 的节点合并。它不把整棵树转换为 map，也不读取无关 subtree。节点按 Prefix、Value、排序 children 唯一编码，因此同一逻辑映射不依赖插入次序，得到相同 root。

TreeDelta 包含最终 Root 与最终 Root 可达的本次构造节点；不会输出已被后续 Mutation 覆盖的中间节点。对象按 Label 升序 DFS、child 在 parent 前的顺序返回，按内容摘要去重。无变化的调用返回原 root 和空 Objects。输入 key、reader encoded buffers、旧 root 与输出对象的 slice 不共享可变内存。对象是不可变值；调用者仍不可与进行中的调用并发修改输入。

## 硬资源上限

| 常量 | 上限 |
| --- | --- |
| MaxKeyBytes | 1024 bytes，包括空 key 与任意二进制 key |
| MaxMutations | 1024，每次 Apply |
| MaxReadNodes | 65536，每个 Get/Apply/ValidateNodeObject 的 reader 加载次数；本次已缓存对象不重复计数 |
| MaxNewNodes | 65536，每次 Apply 的不同新构造内容摘要数，包括后来被覆盖的中间节点 |
| MaxDeltaBytes | 16 MiB，每次 Apply 构造的 encoded object 字节总数，包括中间节点 |

空间、对象 envelope 与 refs 还受通用 codec/model 上限约束。构造预算计入中间对象，确保一批重复覆盖也不会无限积累内存；最终输出一定不超过同一预算。前缀长度与每条 child 必须消耗至少一个 byte，使走访深度也有 1024-byte 硬上界。取消在读取前后、每次路径递归、节点构造与 delta 收集时检查。达到上限时失败，不返回部分 delta。

`state/radix_test.go` 包含固定 root golden、独立 map oracle、不同插入次序、空 key/NUL、前缀拆分/合并、相同后缀不同前缀、旧 root、no-op、输出去重/可达性/顺序、输入输出 ownership、不可信 hash、错误节点 schema/Refs/child 关系、长度/资源限制及取消测试。完整 Diff/GC/engine replay 不属于此格式的 M1c 纯算法部分。
