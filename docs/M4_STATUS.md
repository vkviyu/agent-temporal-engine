# M4：KnowledgeKey、Observation 与 ContextPacket

M4 的第一条独立纵切面已加入公开 `knowledge` 包。它不导入 engine、storage、GSEA 或模型供应商；调用者必须显式提供已发布来源和内容身份。

## 已实现

- `SourceAnchor`：绑定 `SnapshotAnchor`、可选 ViewDigest 和知识 frontier；校验 Store、Space、Fence、Cursor、KnowledgeKey 结构。
- 严格 historical cut：`ValidateBefore(consumerFence)` 要求 source Store 一致且 `source.LSN < consumer.LSN`，禁止 ContextPacket/Observation 引用自身或未来批次。
- `Observation`：绑定 `KnowledgeKey=(Actor,Execution)`、SourceAnchor、Payload、单调 Sequence 和来源标签；source KnowledgeKey 与 Observation key 不一致时拒绝。
- `ContextPacket`：绑定 Space、可选 KnowledgeKey、SourceAnchor、GoalRef、按 Ordinal 排序的 Observation/Content 项、Renderer/Tokenizer 版本和 token 计数；重复、跳号、空标签和非法内容身份拒绝。
- canonical 编解码：Observation/ContextPacket 的 ID 从域分隔的 canonical bytes 派生；Decode 后重新计算 ID，拒绝非规范编码、未知 schema、未来来源和截断输入。

这些类型记录“实际提供给模型的输入来源”，不会把查询本身自动变成知识，也不会把摘要或当前 latest 偷换成历史事实。ContextPacket 的内容对象由宿主/持久层另行保存，包内只保存确切 ObjectID，避免凭空复制或重渲染。

## 验证

`knowledge/knowledge_test.go` 覆盖 Observation/Packet canonical roundtrip、Cursor/KnowledgeKey source anchor、未来 Fence 拒绝、同 Actor 不同 Execution 隔离、非连续/重复输入拒绝。架构依赖 guard 明确 `knowledge → codec/model`，无 `internal` 与第三方依赖。

下一步是将 KnowledgeStateRoot、Observation CAS、ContextPacket pending-render 与 M3 Agent Profile 接入持久事务；本包本身不假装已经提供 Journal authority 或模型上下文选择器。
