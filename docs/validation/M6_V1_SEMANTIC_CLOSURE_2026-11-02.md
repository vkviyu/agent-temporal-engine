# V1 语义收口验收 — 2026-11-02

本轮目标是严格补齐既定 V1，而非把剩余功能改成后续范围。更正：此前根据下表局部证据声称
四份主文档逐项全部通过并不准确。schema4 导入缺陷和额外 V1 要求已在复核中发现；本报告
只记录机制级阶段证据。完整需求状态见 `V1_REQUIREMENTS_MATRIX_2026-11-02.md`，严格 V1 尚未完成。

后续修复、代码树哈希、24 包最终退出码与明确剩余项见 `V1_RECHECK_2026-11-02.md`；本文件下表
保留此前机制证据，不替代该最新运行记录。

## 实现与证据

| 目标 | 实现和可执行证据 |
| --- | --- |
| 精确知识时间旅行 | `knowledge/revision.go`，`engine/knowledge_cursor_test.go`；独立知识 Revision 链、空版本、旧 Key+Cursor、未来/其他执行拒绝、Packet 不推进知识 |
| 显式分支种子 | `knowledge/import.go`，`engine/knowledge_import_test.go`；源 Cursor/Fence/成员和目标 sequence CAS；ImportedFrom 保留来源；普通 Observation 不能跨执行共享 |
| 记忆与知识桥接 | `knowledge.Observation.PayloadKind` schema3/4、`engine/memory.go`、`engine/knowledge_owner_test.go`；memory/experience_path 只按历史 Memory 根解析，逻辑 ID 不进入 raw object 引用；当前模型派发检查撤回/替代 |
| 动作意图准入 | `engine/action_gate.go`、`engine/agent_recovery_integration_test.go`；新请求需匹配 durable ActionIntent 的 RequestKey/Method/ExactCommand/authority，重绑定后的旧请求拒绝，已有收据仍优先重放 |
| 祖先权威屏障 | `agentprofile/control.go`、`agentprofile/recovery_test.go`；父 Authority fence 递归标记后代执行和活动，父恢复不会自动复活子代理；每个子执行必须单独 resume |
| 正式发生事实 | `eventlog/event_test.go`、`engine/events_test.go`；Event 与原命令/StateOps 同 P1，sequence/owner/causal refs、历史及主体边界、receipt retry 不重复 occurrence |
| 核心联合合并与修剪 | `engine/graph_join_core_test.go`、`graph_join_publication_test.go`；DomainStates 与 BranchRefs 原子推进，显式冲突 resolution、两个 CAS、旧 receipt、unknown publication、Prune 保留祖先 |
| 统一导航 | `engine/node_explore_test.go`、`node_recovery_test.go`、`node_content_test.go`、`service/nodes_test.go`、`client/nodes_test.go`；固定 View 的 Search/Trace/Control nodes、内容范围、过滤与排序分页、跨主体/未来内容拒绝 |
| 真实模型输入记录 | `knowledge/recorded_input_test.go`、`engine/recorded_input_test.go`；schema2 RecordedInput 保存已发布完整请求 bytes 的 ID/Anchor/encoding；覆盖当前值、reopen、compaction 后仍逐块还原同一字节 |
| 多 Agent 恢复 | `agentprofile/recovery_test.go`、`engine/agent_recovery_integration_test.go`；真实文件副作用后丢失响应，parent/child checkpoint、fence/resume、unknown 禁止重试、迟到结果隔离、occurred 对账与显式导入、原命令和 receipt 复用 |
| 精确源发布解析 | `model/publication.go`、`engine/publication_refs_test.go`；ProducerTx 解析自己的 Fence/RootSet，不能换 latest/group tip 或跨主体；SourcePublicationRef 不预嵌自己的 CommitDigest |
| AuditFull 物理回收 | `storage/compaction_test.go`、`compaction_metadata_test.go`、`engine/maintenance_test.go`、`cmd/ate/maintenance_test.go`；每个持久化故障点、旧 readers、closing sticky、引用验证、独立 Python schema3 golden、五根/历史/receipts/新写入/reopen |

## 设计上的明确选择

Knowledge Revision 是同一 `(Space,Actor,Execution)` 的不可变采用序列，不复用领域图的 StateRoot。
继续另一路探索使用新 Execution 和显式 Import 种子；旧执行、旧输入和被剪枝的失败事实仍保留。

实际模型请求先发布为 ValueBytes，再由 Packet.RecordedInput 引用精确 Anchor，模型 Activity
必须绑定同一已记录请求。旧 schema1 Packet 可做历史元数据，但缺少真实输入的 Packet 不授权
模型 Activity，不在恢复时临时重渲染填补记录。

AuditFull 以完整 canonical 事务历史为保留根。compaction 只替换物理布局：LSN、TxID、事务摘要、
StateOps、receipt、五根、逻辑对象地址全部保持，generation 增加。只有新的 CURRENT 已发布且旧
reader pins 释放后才删除旧物理副本。此选择符合主设计的 AuditFull 默认保留模式，未承诺删减
权威历史；schema 引用提取不被当作选择性 sweep 的许可。

## 本轮全量验证

在 Go 1.27.0 本地环境完成：

```sh
GOCACHE=/tmp/ate-go-build go test ./...
GOCACHE=/tmp/ate-go-build go test -race ./...
GOCACHE=/tmp/ate-go-build go vet ./...
GOCACHE=/tmp/ate-go-build go build ./...
```

以上均通过。后续新增测试与代码变更仍须重新运行对应门槛；不能引用本报告掩盖其后的未验证改动。

## 仍保留的边界

- 仍是实验 v0 格式；不声明设备断电认证或超过已测容量的性能。
- Agent profile 使用有界完整分区 snapshot；Knowledge/Memory 仍受 core 单值 1 MiB 上限约束。
- Evidence/CompatibilityEvidence/ResourceFenceEvidence 是可信宿主的 attestation digest；引擎保存与
  校验控制关系，但不会把摘要当成对真实外部世界的独立证明。
- 广播订阅、在线无停顿 compaction、BoundedRetention、隐私遗忘与权威 payload 稀疏化仍属于主设计
  明确的后续协议；本轮没有以它们作为 V1 完成标准。
- 外部多 Agent 验收使用真实本地文件副作用和确定性宿主适配器；没有调用远程模型，也没有以
  主流产品表现或单包绿色测试作为恢复正确性的证明。
