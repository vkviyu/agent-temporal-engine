# M2a：Revision 图算法纵切面

M1f 之后的第一条 M2 纵切面已加入独立公开 `graph` 包。它不依赖 GSEA、Eino、engine 或 storage，也不把临时 map 投影冒充持久权威；持久适配器尚未接入 Journal。

## 已实现

- `Revision`：内容身份由 Space、StateParent、完整 JoinParents、StateRoot、Events 和规范排序的 state entries 决定；ID 不从调用者自报。
- `Branch`：Fork 只创建 O(1) head/version 引用；AdvanceBranch 使用 version CAS。
- `Diff`：在相同绝对 key 上排序比较，区分 add/update/remove；不按压缩路径后缀跳过。
- `MergeBase`：遍历 StateParent 与 JoinParents 的祖先集合；多个最佳基点返回 `ErrAmbiguousMergeBase`，不猜一个。
- `PlanJoin`：三路状态合并；左右都改同键且结果不同形成显式 Conflict；一侧未改动时采用另一侧，包括删除。
- `CommitJoin`：未解决冲突禁止生成 Revision；JoinParents 完整记录左右来源，StateParent 是左侧工作基线。
- canonical codec：Revision/Branch 使用版本化 canonical bytes；Decode 后重算 RevisionID，拒绝非规范顺序、错误摘要、未知 schema 和超限目录。

`graph.Store` 是确定性算法容器，不是存储引擎。下一步将把 Revision/Branch 的规范对象接入 ATE 的强引用 registry、事务 body 和 RootSet；接入前不向现有 core mutation API 暴露伪造的通用 graph write。

## 验证

`graph/graph_test.go` 覆盖 deterministic ID、Fork/branch CAS、Diff 排序、无环 parent、三路 join、冲突显式解决、删除语义和 Revision/Branch 编解码，并通过 `go test -race ./graph`。架构依赖测试明确 `graph → codec/model`，没有 `internal` 或第三方依赖。

## 未完成

持久 Revision/Branch schema、Event/Relation 强引用提取、Journal replay、历史 View 的 Cursor 选择、持久 Fork/Join 命令、图索引与合并提交尚未完成。M2a 不改变 M1f 对外服务协议的能力声明。
