# V1 文档—代码交叉审计

> 2026-11-02 更正：本报告 A1–A5 是第一轮局部审计，不能作为四份文档全部 V1 要求的完成证书。
> 后续复核已发现 schema4 typed Import 编解码不一致、上下文选择/摘要、原子消费与下一动作、
> 出站授权、格式冻结及效果证据等缺口。完整逐项状态以 `V1_REQUIREMENTS_MATRIX_2026-11-02.md` 为准；
> 严格 V1 与全量交叉验收尚未完成，整体重构暂停在已实施的第一阶段。

以四份主文档的目标和正确性约束为基准：`AGENT_TEMPORAL_ENGINE_DESIGN.md`、
`ATE_V1_IMPLEMENTATION_ARCHITECTURE.md`、`ATE_API_PROTOCOL_DESIGN.md` 和
`ATE_EXECUTION_PROFILE_WALKTHROUGH.md`。接口存在、包测试通过、根槽位改变，都不能单独证明该能力已完成；
不得把未实现的语义改写为后续范围以宣称 V1 完成。四份文档中明确标为 M7/后续协议的广播订阅、在线
无停顿压缩、BoundedRetention 和隐私遗忘不属于本轮完成标准；下文仅保留已检查的 A1–A5 局部证据，
并不覆盖其余所有 M0–M6 要求。

## A1：统一提交与领域校验（本轮修复）

原实现虽然把 Graph 放入 BranchRefs、Memory/Knowledge 放入 ExecutionKnowledge，
但仍接收调用方编译的 StateBatch。通用 Commit 可以绕过领域校验，facade 在最新状态上重新
计算 mutation 也会破坏同一 RequestKey 的重试。

现在三个入口都在 admission 前编码原始 canonical 领域意图，协调器先检查 receipt，之后才运行
确定性 reducer。私有候选、StateOps、根目录、receipt 和五根 RootSet 共用同一个 P1 与 replay。
原始 StateBatch 不能提交到三个受保护的 method。

对应实现：`engine/{graph,memory,knowledge,typed_value}.go`。
回归包括原 key 在 head/状态改变及重启后的重试、变更 body 冲突、伪造根和源对象拒绝、CURRENT
发布前后故障、候选不泄漏，以及 group 的单次发布。此前“仅改变 root slot 就已修复 A1”的结论被本轮证据替代。

## A2：KnowledgeKey 与历史 Cursor（已完成本轮 V1 语义）

- Knowledge 按 Space + Principal 分区，分区中按 (Actor,Execution) 保持连续观察序号；
  不同主体即使使用相同逻辑 ID 也不共享序号或内容。
- GetKnowledgeState 需要有效 Principal；服务端只使用连接身份。Anchor.Fence 固定历史根，
  KnowledgeKey 在历史及当前控制面认证所有权，然后只选择该执行的观察与 Packet。
- OpenView 支持 `execution_knowledge`。ViewDigest 绑定 Key、Fence、根和主体；Frontier
  只绑定已采用的观察，不因渲染 Packet 而推进。该 View 不能降级读取整个领域状态。
- Packet 的每个 Observation 必须已存在于 Source.Fence 的同主体快照；当前已存在不够。
  新来源必须属于真实发布前缀，不能引用同批次私有候选或猜测的对象摘要。
- 领域 Cursor 从 Anchor.Fence 的 BranchRefs 中认证 Revision，按 Revision.StateRoot 读取旧
  领域状态，同时保留 Fence 的控制面。旧 Fence 中不存在的 Revision 不会回退到最新图查询。

Knowledge 包新增独立的 content-addressed execution-knowledge Revision 链。Revision 由同一
KnowledgeKey 的连续 Observation 派生，Cursor 只选择该链的精确前缀；Packet/渲染不会推进
Revision。跨 Execution 复用必须走显式 ImportCommand，保存 ImportedFrom 与源 KnowledgeKey/
Cursor/Fence；普通 Observation 不能通过伪造 Anchor 跨执行共享。Domain Cursor 与 Knowledge
Cursor 是两个不同命名空间，均保留 Anchor.Fence 的控制面。

回归：`knowledge_core_test.go`、`knowledge_owner_test.go`、`knowledge_publication_test.go`、
`cursor_test.go`、`cursor_view_test.go`，另有真实本地 SDK/服务测试。

## A3：Node 与渐进上下文（已完成本轮 V1 语义）

- 默认实体层支持 Task/Execution/Step/Invocation/PendingAction 及 Query/Activity/Attempt/
  ActivityResult/Message/Consumption/Continuation 的摘要、类型化详情和父子关系。
- content 层在普通 View 中认证领域对象和历史 Revision；在 Knowledge View 中仅展开该 Key
  已采用的载荷、Observation 和 ContextPacket，不导出整个主体的 Agent ledger。
- 读取 ObjectID 之前先检查 View 成员关系。不存在、未来或其他主体的对象返回 not_found，
  不把普通用户请求当损坏并封禁 store。
- typed memory/experience_path 载荷在 Knowledge View 中投影为受认证的逻辑 Entity 节点；不把逻辑
  ID 塞进 raw Object catalog，也不让 ReadContent 以对象地址读取它。
- 分页绑定 View/filter/sort；节点、边、深度、响应字节和累计读取均有界。local service/SDK
  提供 Node 方法并验证 session 对 View 的所有权。
- 普通 Cursor View 只展开选中 Revision 的祖先；`audit_snapshot` 才显式浏览该 Fence 的其他分支。

Event profile 已有独立 canonical `ate.event` occurrence object：principal stream、sequence、
causal parent、command/StateOps payload refs 与同一 P1 原子发布；Event Node 只在固定 View 的
主体和 Fence 内可见。NodeExplorer 提供有界、固定 View 的 literal Search、Trace；Recovery
Snapshot 的 authority/activity/attempt/acceptance/reconciliation/import/resume/join 等控制记录
也有类型化 Node 投影。ContentReader 提供 authenticated object/value_bytes 的 bounded range，
不能把 synthetic Observation/Packet/Revision ID 当作 raw object 权限。

回归：`node_queries_test.go`、`node_revision_test.go`、`service/nodes_test.go`。

## A4：引用闭包回收（AuditFull compaction 已完成；选择性历史 GC 仍明确不启用）

`storage.Maintain` 仍只清理 quarantine。`Journal.CompactAuditFull`/`Engine.CompactAuditFull`
在唯一 publication owner 下先完整验证 canonical TX、typed replay、五根和 schema content
closure，再把完整逻辑事务流复制到新 segment，使用 schema3 `LayoutSource`/`RetiredSegments`
和同一 CURRENT 安装协议发布。已有 PinnedReader 继续读旧 layout；释放后 `ReclaimRetired`
才删除旧物理副本。中断前旧 CURRENT 保持权威，恢复会验证 source/intent 并清理 staging。
Graph/Knowledge/Memory/Agent 已区分 raw codec content、logical IDs 和 source anchors；AuditFull
不删除任何权威事务、失败事实、receipt 或未知副作用，因此没有借“闭包已提取”启用选择性历史
sweep。BoundedRetention/隐私遗忘仍是独立后续协议。

## A5：最终联合验收（已完成）

`tests/host/long_task_test.go` 的 `TestLongTaskPublicHostAcceptance` 已用同一确定性长程场景分别通过
嵌入库和真实 Unix socket/SDK：Query、动态 parent/child、两条分支、Knowledge Import、模型输入、
工具调用、消息消费、失败分支、Graph Join/Prune、Memory/ExperiencePath、AuditFull、重启、精确
RecordedInput 读取和真实本地文件副作用均在同一场景验证。`engine/agent_render_recovery_test.go`
另外覆盖 Observation→pending-render→RecordedInput→Packet 的跨阶段退出、重启、重试和收据复用；
`engine/agent_recovery_integration_test.go` 覆盖 parent/child 非幂等外部副作用的 fence/resume、迟到
结果对账、祖先权威递归 fence 与 ActionIntent 重绑定后的旧请求拒绝。两种宿主路径均通过 race，结果保持同一逻辑 Fence、
receipt 和副作用次数。

## 验收结论

以上是本轮代码与四份设计文档的第一轮交叉审计。原 A1–A4 的若干实现缺口以及 A5 联合机制场景已有修复：统一 typed reducer、
Knowledge Revision、Event/Search/Trace/Content、核心 Join/Prune、SourcePublicationRef、
RecordedInput、typed memory/experience_path bridge、durable ActionIntent reservation、schema 引用分类
和 AuditFull compaction 均有实现与回归证据。剩余限制是有界
snapshot/value 容量、外部模型/设备断电不在本仓库可证明范围、广播订阅仍是后续版本，以及
AuditFull 明确不承诺权威历史删除。这些局部证据不能证明严格 V1 全部完成；后续已做第一阶段职责拆分，
在完整需求映射和剩余验收闭合前，不继续扩大重构范围。
