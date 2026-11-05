# Domain core experimental 1

这是当前已实现的 Graph/Memory/Knowledge core registry，不是完整 V1 的完成声明。
原始领域意图在 admission 前冻结；协调器先检查 RequestKey receipt，再做前置条件及纯转换。
宿主不能提交替换 snapshot、StateOps 或目标根。三个 method 都拒绝 core StateBatch body。

## Method 与根归属

| Method | Canonical body | 修改的领域根 |
| --- | --- | --- |
| graph.apply/v1 | graph.EncodeCommand(Space,Command) | BranchRefs |
| memory.apply/v1 | memory.EncodeCommand(Command) | ExecutionKnowledge |
| knowledge.apply/v1 | knowledge.EncodeCommand(Command) | ExecutionKnowledge |

每个根是系统 Space 中的 Space→领域 radix 目录。受保护的领域 key/value 由核心 reducer 计算，
再调用 prepareTypedValue。StateOps 的 BeforeRoot/AfterRoot 和 receipt.StateRoot 指向该 Space 的
领域 radix。RootSet 绑定完整五根目录。Agent 的 StateRoot 仍是整个 LiveControl，不能混用。

RefUpdates 记录旧/新 BranchRefs；ControlUpdates 对 Memory/Knowledge 记录旧/新 ExecutionKnowledge，
对其他 method 记录旧/新 LiveControl。回放依据原 method 重新计算所有对象、操作及根，逐字节核对。
不能依据 ControlUpdates 的形状猜测 root slot。

## Graph

闭合 union 为 AddRevision/Fork/Advance/Join/Prune。AddRevision 编码时规范化不可变 Revision；核心认证
parent/joins 均存在、StateRoot 曾属于该 Space 的领域历史，并完整核对 radix Entries 与值对象。
Fork 拒绝重复 Branch，Advance 校验当前版本并要求目标 Revision 存在。EventID 只有在 EventProfile
的已发布 occurrence stream 中才可进入 Revision；Join 同时 CAS branch/domain roots 并校验完整
resolutions，Prune 只改变导航 tombstone，不删除共享祖先。

## Memory

七操作命令复用 memory 的有界二进制 codec。候选不能注入已经验证/晋升的状态；普通 Transition
不能绕过 Validate/Promote。新来源须匹配真实发布 Fence、Space 和来源成员关系。来源含 KnowledgeKey
时还必须认证 Execution 所有权，并且内容已经被该 Key 采用。回放不调用宿主或模型。

## Knowledge

闭合 union 为 Observation/ContextPacket/Import；正文使用既有 canonical record，并置于有版本、操作号的
二进制命令 envelope。Snapshot 的有效 JSON 编码仍受规范重编码和界限校验；不能以 wire JSON 身份
替代领域 command digest。

Knowledge partition key 为 UTF-8 ASCII `ate/knowledge/v1/` 加 lowercase hex SHA-256。
哈希输入为 codec.AppendString(Space) || codec.AppendString(Principal)。该 key 属于上述 Space radix；
同 Space 的不同 Principal 即使使用相同 Actor/Execution ID，也不会共享观察序号或内容。

每个 partition 中以 (Space,Actor,Execution) 保持连续 Observation sequence。合法摘要还必须等于
ObservationID/PacketID 的实际 canonical 身份。Packet 的成员、载荷和 Key 必须一致；核心进一步认证
每个成员在 Source.Fence 时已经被采用。Source 不得引用私有 group candidate；不能从当前快照推断过去。

Keyed View 的 ViewDigest 绑定主体、Key、Fence 和 RootSet；Frontier 绑定有序观察集，不含 Packet，
渲染不会推进知识。无 Key 的查询 Packet 不隐式导入执行知识。Keyed Packet 的 GoalRef 也必须已在
来源知识中采用，不能偷用同 Fence 的全局未观察内容。

Observation 的 schema=2 只由显式 Import 产生，保存 ImportedFrom、源 KnowledgeKey/Cursor/Fence 和
目标执行的连续序号；普通 Observation 禁止伪造跨执行来源。schema=3 为同执行的 typed
memory/experience_path 逻辑引用，schema=4 同时保存 typed 引用和 ImportedFrom。typed ID 只
能由核心在对应历史 Memory 根中解析，不能当作 raw codec object；撤回或替代后新的模型
派发拒绝使用。

ContextPacket 当前仅接受 schema 4。既有字段依次为 Space、可选 KnowledgeKey、SourceAnchor、GoalRef、
有序 Items、Renderer、Tokenizer 和 EstimatedTokens。随后依次写 RecordedInput、Selection、Requirements
三个 uint presence（0/1），每个 presence 为 1 时紧随对应记录，最后写 PacketID。
RecordedInput 依次保存 Object、精确历史 Anchor（以无 ViewDigest/Frontier 的 SourceAnchor 编码）和
Encoding。Object 是已发布的 ValueBytes，保存实际模型请求的完整 framing。

Selection 为有界 bytes 字段。其 ContextSelectionRecord 包含固定 SourceAnchor/Key/Cursor、模型窗口/输出/framing 预留、原 mandatory、
完整候选 Costs、结论/支撑/反例 Bundles、收益、tokenizer/计数质量/策略版本，以及确定性结果和省略原因。
core 从该 Source 的已认证历史观察重新计算整个选择，核对 Items 顺序、GoalRef 的 mandatory 成员关系和
EstimatedTokens=UsedTokens+FramingReserve。

Requirements 为有界 bytes 字段。内部依次编码 uint schema=1、RulesVersion string、HostPolicyVersion string、
Facts count，以及按 `(Kind, ID)` 严格排序的 `Kind, ID, Observation, Content` string 四元组。每条事实必须有
同 Observation 和 Content 的 Packet Item。若同时有 Selection，所有事实的 Observation 也必须在提案的 Mandatory 中。
执行级 `primary_decision` 要求 `agent_mandatory_context/v1`，引擎从 packet 的历史控制切面重新导出完整结构化集合，
并逐项核对该 KnowledgeKey 已采用的 raw ValueBytes。`GoalRef` 必须匹配当前执行所属 Task 的目标事实的 Content。
Requirements 在一般 Packet 记录中可选。HostPolicyVersion
标识宿主的渲染、语义候选和计数策略。PacketID 由 `ATE-CONTEXT-PACKET` + NUL 和不含 ID 的完整 schema-4 字节哈希得出。
预发布 Packet schemas 1–3 不在当前解码边界内。

`knowledge.SelectContext` 使用有界边际收益/成本启发式和稳定 BundleID tie-break；相同证据只计一次，
必要结论的支撑/反例闭包不能拆除。mandatory 超输入容量返回 ErrContextBudgetUnsatisfied，没有半份计划。
计数质量明确为 estimated 或 exact_framed_segments，后者要求宿主提供可加总的已 framing 段计数，
不代替对完整 RecordedInput/provider framing 的实际计数。费用预算与选择无关。Omitted 只覆盖 Costs
明确列出的有界候选集。结构化 Requirements 的发现规则与选择启发式独立，语义候选仍由宿主提供。

schema4 的字段顺序为既有 Observation 字段、PayloadKind、ImportedFrom、ID；PayloadKind 不可省略。
`testdata/generate_knowledge_vectors.py` 使用独立 Python varint/SHA-256 构建 schema1–4 的规范字节与 ID，
并同时覆盖 memory 和 experience_path。此前实验实现错误地在 schema4 中漏写 PayloadKind，
其输出不符合本格式且无法被原 decoder 读取。此修复不自动重签旧异常 snapshot/Observation ID，
不允许静默修改权威日志；包含旧异常记录的 store 可能因身份不匹配拒绝打开，必须保留原件，
用产生记录的原实现进行只读提取并显式迁移。当前没有自动迁移工具。

## 历史、查询与容量

- GetKnowledgeState 必须指定合法 Principal（本地服务使用连接身份）；无 Key 读该主体自己的分区。
- 历史 Fence 定位当时 RootSet，Key 通过历史和当前控制面认证后筛选；genesis 为空快照，不访问空 ObjectID。
- 领域 Cursor 通过历史 BranchRefs 选择 Revision.StateRoot，同时保留 Fence 控制面；Knowledge Cursor 通过所选 Key 的 Observation 链选择不可变知识 Revision，并且不读取未来 Packet。
- core 单值目前最多 1 MiB；Knowledge/Memory 整分区 snapshot 超限返回资源拒绝。纯包 8 MiB 上限不代表 core 能接收 8 MiB 分区。
- 所有日志历史仍完整保留。Graph/Memory/Knowledge/Agent 已提供 schema 级内容引用提取和完整重放校验；AuditFull compaction 只回收已验证的新物理副本与 staging，不启用权威历史 sweep。

## 实验兼容边界

先前实验曾让三个受保护 method 携带 StateBatch；这种编码不再接受，也不会自动解释或迁移为
新 canonical intent。当前机器 registry 记录 V1 候选格式边界。旧实验 store 需要使用产生它的版本检查/导出，再显式
迁移到新 store。不能通过关闭校验把旧 mutation 当成受保护的 typed command。
