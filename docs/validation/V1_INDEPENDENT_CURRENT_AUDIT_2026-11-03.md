# 四份主设计文档与当前代码独立审计 — 2026-11-03

结论：**当前不能关闭严格 V1 验收。** 本轮实际发现的主要问题不是旧报告所说的“还没有
Query-only、初始 seed、实体事件或独立参考模型”，而是新机制的公开入口仍可绕过、Emission
schema 3 的字段/辅助函数没有接入生产转换，以及 Effect 重试契约仍把单个分类当作永久保证。
历史完成声明不能覆盖这些可定位的代码行为。

## 基准、方法与范围

- D：`docs/AGENT_TEMPORAL_ENGINE_DESIGN.md`，原语、Profile 语义、不变量。
- A：`docs/ATE_V1_IMPLEMENTATION_ARCHITECTURE.md`，代码/提交边界与阶段门槛。
- P：`docs/ATE_API_PROTOCOL_DESIGN.md`，库/服务/SDK 契约。
- W：`docs/ATE_EXECUTION_PROFILE_WALKTHROUGH.md`，C01–C20 对象流水与反例。

读取了需求矩阵、11-02 交叉审计及 addendum、11-03 strict-scope、真实模型与 native-scale
报告，再独立追踪当前 public API → admission → reducer → publication → output gate；没有将
“函数/字段存在”“旧测试通过”或追加 closure 段落当作语义证明。仓库在本轮并行修改，且没有
可引用的提交号；下面按具体文件和符号给出证据。报告不是最终源树全量运行证书。

Annotation 由另一项工作实施，本报告按明确分工不将其作为已知缺口重复列出。没有将以下能力
擅自加入 V1：远程 exactly-once、durable subscriptions、字段级匿名化、同名 Space 跨 Store
迁移、自动语义相关性发现、每个 opcode 的穷尽形式证明、所有候选性能规模全部达标。

## 必须保留的当前问题

| ID | 优先级 | 条款 | 当前裁决 |
| --- | --- | --- | --- |
| C-01 | P1 | D §17.6/I-27，P Emission schema 3，W bounded emission | **未关闭：schema 3 admission 存在，执行语义未接线** |
| C-02 | P1 | D §11.7/§11.13/I-28/I-38，A §12/§16.5.1，W C02/C04/C05/C13 | **未关闭：Work 的原子边界可由公开 ApplyAgent 绕过** |
| C-03 | P1 | D §11.3/§11.5/§11.9/§19.1/I-13，W §9.1–9.2 | **未关闭：Effect 能力/期限/时序证据不足，idempotent 被永久视作安全重试** |
| C-04 | P1 | D §13.4/§15.3/§17.6，P 当前授权输出语义 | **已复现，修复已落地，待最终回归：GetAgentState 的显式来源披露绕过** |
| C-05 | P1 | 同上，D §11.10，W C01 恢复 | **修复已落地，独立针对性回归通过，待最终回归：GetQueryIntake 的原始输入/确切下一命令披露绕过** |
| C-06 | P2 | D §14.1–14.2，A §15.1 | **部分：selector 验证提案，但必要约束发现和责任绑定仍需明确验收** |
| C-07 | P2 | D §14.4/§15.5/§21.6，A M6/§18.2 | **已有真实实测，剩余最终工件/源树复验；不能沿用“没有真实模型”的旧结论** |

### C-01：Emission schema 3 不是已经实现的 exact dispatch 协议

**生产证据。** `api/emission.go::EmissionSpec` 接受 Query、Activity、Attempt、RequestDigest、
ContentKind、NotAfterLSN；但是 `engine/emission.go::prepareEmission` 的 Issue 和 Claim 仍直接
调用 `emissionAuthority(ledger, execution)`，按 Execution 路径处理。Issue 直接 `readObject(Content)`
及 `objectContentProjection`；没有按 packet/summary 解析实际请求/摘要输出。

`engine/emission_contract.go` 已写出 `checkEmissionOwner`、`checkEmissionActivity`、
`resolveTypedEmission`，但全仓调用搜索显示这三个函数均没有生产调用点。NotAfterLSN 在 engine
中没有检查点。`api/work.go::validateWorkUnit` 的 Kind 集合没有 `emission_dispatch`，WorkStep
也只接受 Agent/Knowledge reducer；不存在旧报告所称“原子 claim exact grant + DispatchAttempt”的
Work 路径。

**具体反例。**

1. 合法 Query-only Activity/Attempt 的 EmissionSpec 可通过 schema 3 编码，Issue 随即对空
   Execution 调用 `GetExecution("")` 而失败；这不是 Query scope 的授权实现。
2. Execution grant 指定不存在/不相符的 Activity、Attempt、RequestDigest 或已过期 NotAfterLSN，
   当前 prepareEmission 没有相应比较。只要其普通 raw content、Execution authority 和范围合法，
   这些 schema 3 限制不会成为准入条件。
3. `ApplyAgent(Control.DispatchAttempt)` 仍可独立执行，无需引用/消费相应 Emission。

**关闭条件。** 将 schema 3 的 owner、typed content、exact Activity/Attempt/request、目的地、
expiry、当前 policy/authority 检查接入实际 reducer；给 claim/dispatch 一个原子且受保护的入口。
测试必须实际调用 engine 和 Unix SDK，覆盖 Query-only、packet/summary、错误 Attempt/内容/目的地、
expiry、撤回与重试/重开；不能只测 Encode/Decode。若直接 DispatchAttempt 被解释为隐式 grant，
必须给出满足同一条款的完整等价契约，不能用现有 opaque DispatchEvidence 代替这些检查。

**旧报告更正。** `V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-02.md` 等关于 schema 3 已支持
`emission_dispatch` Work、Query scope 和 exact grant/dispatch 的关闭结论应撤回。当前代码不支持该
结论；未调用的辅助函数不能作为完成证据。

### C-02：公开 ApplyAgent 可以绕过 Work 必须原子提交的事实组

**生产证据。** `engine/work_boundary.go::validateWorkBoundary` 和
`engine/work_seed.go::verifyInitialWorkSeed` 已对 query_start/child_spawn/model_decision/
message_consume Work 做检查；但 `engine/agent.go::prepareAgent` 对公开 ApplyAgent 仍直接运行
`candidate.ApplyCommand`。`engine/agent_inputs.go::checkAgentControlInputs` 没有禁止公开
StartWork、SpawnChild、ConsumeMessage；AcceptActivity 仅对 managed Query 的 query_decision
有 direct-call 拒绝，并未对 Execution primary_decision 做同样限制。

`agentprofile/control.go::applyControl` 的这些转换是局部 reducer：StartWork 写 Task/Execution/
Starts；SpawnChild 写 child/TaskLink/Invocation 结果；ConsumeMessage 写消费记录；AcceptActivity
写 acceptance。它们单独执行不会更新 Knowledge 根或建立完整下一行动/Continuation。这些局部
转换可以作为 Work 私有子步骤，但当前也通过 public ApplyAgent 暴露。

**具体反例。**

- 将合法 intake.Next 内 StartWork 子步骤单独作为 ApplyAgent 提交，可先发布 Task/Root，却没有
  R0、Query/Goal seed 和初始 Continuation；Work seed 的负例不会运行。
- 对合法 pending spawn Invocation 单独提交 SpawnChild，可先发布 child 和 fulfillment，再在
  authority/Import/checkpoint 前退出；child seed 与恢复位置不在同一发布内。
- 单独 ConsumeMessage 后退出，durable `(Message,Recipient)` 消费槽已占用，而 Observation、知识
  Revision 和下一动作尚未建立。
- 单独接受 Execution primary 结果，再分笔登记 Invocation/FinishStep，可以绕过 model_decision
  必须整批记录调用集和下一位置的边界。

**关闭条件。** 在 public engine admission 强制这些语义归属于相应 Work，或提供可证明等价的
durable preparation 协议。仅在纯 Ledger 中保留受保护的局部子转换并不构成问题；问题是 public
engine/service 仍把它们作为独立成功事务。负例应检查无 fence/receipt/根变化，并覆盖 direct
Commit(MethodApplyAgent)、typed ApplyAgent、Unix SDK，防止只封一个便利入口。

**旧报告更正。** “新增 Work 后 G-04–G-06/I-28 已关闭”过强。当前代码证明选择 Work 的调用者
可以得到原子组，不证明官方公开协议强制该组；C02/C05 seed 的正面回归也不能关闭公开绕过。

### C-03：Effect 仍只有三类字符串，缺失有界幂等保证与重试时序依据

D §11.3 要求接收方/effect 契约与版本、幂等作用域及期限；§11.5 明确要求正交能力元数据，
包括幂等键作用域、保留期、同键异参行为、结果查询、资源端 fence 和补偿限制。W §9.1 也明确
ToolDefinition 固定工具/effect 契约，名称或“只读”不足以证明可重试。因此这是原 V1 条款，不是
本轮额外要求引擎独立证明外部系统事实。

**生产证据。** `agentprofile/recovery_types.go::ActivitySpec` 只有 Effect（pure/idempotent/
non_idempotent）、IdempotencyKey、Recipient 和 ResourceScope；没有版本化 effect contract、
key namespace/retention/collision policy 或保证有效的时间/代际范围。
`control_validation.go` 对 idempotent 只要求非空 key；
`agentprofile/control_lookup.go::safeRetry` 对 pure/idempotent 无条件返回 true。
ClaimAttempt（第二次及之后）和 RetryActivity 都使用该判断。

**具体反例。** 接收方仅保留 key 24 小时，第一次派发后 outcome unknown。几天后换 incarnation
恢复，spec 仍为 idempotent，RetryActivity/下一 Attempt 被接受；当前 schema 既不能表达这份
保证已经过期，也不能要求宿主提供覆盖旧 attempts 与本次重发窗口的有效证明。接收方清除 key
后再次处理可产生重复业务效果。对有限保证过期的判断不能靠修改历史 Effect 或请求 bytes 补救。

**责任界限。** 远端能力真伪、时钟与资源隔离由受信宿主/资源适配器证明；引擎无需自己探测
远端。但引擎必须能够持久表达契约、认证宿主的判断、绑定确切请求和所有相关 attempts，并在
证明缺失/过期时拒绝重试。当前 opaque reconciliation 仅在 non_idempotent 分支使用，不能修补
idempotent 的无条件放行。

**关闭条件。** 不把 idempotent 改名或直接全禁作为完成。至少实现版本化正交 contract、稳定
操作/key/request 绑定、有界保证、受信时序证据与严格 retry predicate；现有 non_idempotent 的
全 attempts no_effect + FutureEffectFenced 路径应保留。方案另由实现任务审议，本报告不改 schema。

**外部一手资料复核（2026-11-03）。** 这不是仅凭本项目文档推导的假设：Stripe API v1 明确说明
key 保留至少24小时后可以清理，清理后重用会成为新请求，并比较同键参数以拒绝异参。
[Stripe 官方幂等请求文档](https://docs.stripe.com/api/idempotent_requests)
AWS 的设计说明分别讨论迟到请求与同一 client request ID 的不同意图，强调接收方契约中的请求身份。
[AWS Builders' Library](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)
据此，本项目将 Effect 分类和可消费的重试保证分开是设计推论，不代表 ATE 能独立证明远端能力。

下一阶段的实现方向：不可变正交 EffectContract 绑定工具定义/Activity 的确切 request、recipient、
幂等 scope/key、异参拒绝和查询/fence/补偿能力；有限保证单独记录有效窗口及终结条件。
RetryAuthorization 由受信宿主提交，绑定当前 authority、ActivityControlVersion、全部相关 Attempt
集合和接收方证明，重试/claim/dispatch 复用同一谓词。墙钟与时序证据作为显式输入保留，replay
不读取今天机器时钟；仅“检查时尚未过 TTL”也不能证明旧包不会在过期后迟到。缺少有效保证时
保留 unknown/blocked，不能换 key 或改历史 Effect 冒充安全。

### C-04 / C-05：typed 控制查询也必须检查显式来源的当前披露权限

**C-04 已运行反例。** 新增 `engine/agent_state_policy_test.go::
TestAgentStateHistoricalDisclosureChecksKnownModelProvenance`。合法 aux_summary Activity 通过
ContextPacket → Observation → source 明确绑定来源，已 capture/accept 输出。随后对 source
PrivacyErasure 或 ACLRevocation，GetAgentState 的 latest 和旧 Anchor 四次读取均返回
`found=true, result_count=1, err=nil`，包括原始敏感结果。FactualCorrection/UseBan 对照允许审计
原 bytes。该反例使用已记录来源，不依赖引擎识别任意无来源文本复制。

复现命令（原实现 exit 1）：

```sh
GOCACHE=/tmp/ate-audit-gocache GOTOOLCHAIN=local go test ./engine \
  -run '^TestAgentStateHistoricalDisclosureChecksKnownModelProvenance$' \
  -count=1 -timeout=90s
```

本轮负责人随后新增 `engine/agent_disclosure.go::checkAgentSnapshotDisclosure`，将 Activity 输入
packet、typed Summary 依赖及 Query-only definitions 的来源检查接入 GetAgentState 和 View 输出。
针对性回归已由修复任务运行通过；这关闭该已复现路径的实现错误，最终普通/race 全量验证仍须
对应最终源树记录，不能沿用修补前的绿色日志。

**C-05 静态及新反例。** `engine/work.go::GetQueryIntake` 原直接返回持久 Intake.Query.Input、
Configuration、Next.Body，缺少 policy/output gate。已存 Next Work 包含初始 Query Observation
的精确 Payload/Source，所以 Task 尚未创建时也有可检查 provenance，不能以“没有 Task”豁免。
新增 `engine/query_intake_policy_test.go::TestQueryIntakeRawInputWithdrawalBeforeAndAfterStart`
覆盖 pending/started、latest/旧 Anchor、四种撤回、重开，以及拒绝不改变/fence store。负责人已将
门禁接入生产代码；本审计随后独立运行两项测试，实际进程 exit 0：

```sh
GOCACHE=/tmp/ate-audit-gocache GOTOOLCHAIN=local go test ./engine \
  -run '^(TestQueryIntakeRawInputWithdrawalBeforeAndAfterStart|TestAgentStateHistoricalDisclosureChecksKnownModelProvenance)$' \
  -count=1 -timeout=90s
```

包结果为 `ok .../engine 3.345s`。这一证据覆盖两项读取修复；最终普通/race 全量结果仍需另记。

### C-06：selector 正确性不等于必要约束已被发现并强制进入 packet

`knowledge/selection.go::SelectContext` 明确只处理已给定、已认证的候选集合，不做 hidden
discovery；这是合理的纯算法边界。当前 Mandatory 完全来自调用者提案，
`engine/knowledge.go` 重算相同提案，不从 Task/Step/Invocation/unknown ledger 独立产生必须项。
`knowledge.ContextPacket.Selection` 可为空，新模型 Activity 也接受未带 Selection 的合法 packet。
`engine/agent_goal_inputs.go::checkModelGoalPacket` 主要强制变化后（epoch > 1）的目标与继承约束。

这不要求引擎自动理解自然语言相关性；D §14.2 已知目标/验收/未完成义务/unknown 的确定性提取
与宿主 controller 责任仍须落到可审计契约和测试。否则宿主可提交空 Mandatory 或缩小 Costs，
selector 会正确证明一个不完整提案，而无法证明重要义务没有被漏掉。应明确哪些结构化要求由
引擎必选、哪些由版本化宿主策略提案，并保存提案来源；不能把 heuristic 本身通过测试当作这一
整条要求已关闭。此处列为责任/验收缺口，不声称引擎必须自主发现全部语义相关事实。

### C-07：M6 已有真实结果，仍需按工件和边界逐项关闭

`V1_REAL_MODEL_EFFECTS_2026-11-03.md` 确有已完成的真实模型记录：三轮递归摘要后证据回查
19/24 对 24/24；跨项目 applicability 小样例两组均 6/6，未观察到模型得分收益。native-scale
报告也确有 Darwin 本机持久 IO、并发、重开、AuditFull 和 reader pin 成本。继续使用“全部是
deterministic provider”“未做任何真实模型/性能评测”作为当前结论不正确。

v1 摘要后续以 raw Observation 使用，不能证明 typed Summary 复用效果。本轮随后实际完成
typed Summary v2 的3组预定试验：组1和3分别7/8对8/8，组2第一轮被模型事实复核拒绝，未采用也未执行
最终调用；18次Responses均completed。两完成组只在来源标签可获得性一项有差异，不能宣称事实召回收益。
原请求/回执、review、typed adoption和重开/压缩工件在 `results/model-effects-v2-2026-11-03-run1/`，
报告见 `V1_REAL_MODEL_EFFECTS_2026-11-03.md`；最终代码复验仍须另记。
候选输入规模表不是承诺所有行均达标，不应把未跑 10^7 Events 当新语义缺陷；性能报告
应继续保留数据形状、平台、缓存和样本数限制。

`docs/PLATFORM_SUPPORT.md` 明确 Linux 只有交叉编译证据，没有原生耐久/掉电证据。本报告不要求
凭空宣称支持更多平台；A M6 的支持矩阵和发布声明必须与这些证据等级一致。现有 frozen registry
与仍在发生的 V1 预发布字段修正也必须统一到最终字节/golden/capability 工件后再出最终验收。

## 不应重复判为当前“缺实现”的区域

| 区域 | 本轮找到的当前证据 | 审计裁决 |
| --- | --- | --- |
| Root/child Work 的初始知识与 producer position | `engine/work_seed.go`、`work_seed_test.go`；canonical Query/Goal seed、parent Import、SourcePublicationRef、unknown/reopen | 机制已实现；公开绕过另见 C-02 |
| 非 Agent 主要实体事件 | `engine/domain_events.go`；Graph Branch/Revision、Memory/Path/Source、Summary、Emission、Transfer 的 Subject；`domain_events_integration_test.go` | 不能再概括为只有 Core.Method；本轮未发现需重开整个 A5 的证据 |
| 独立参考语义 | `tests/reference/{control,action,activity,events,receipts,selection,merge_graph,query_tools}.go` 及相应差分 | 已覆盖 Invocation/Continuation、原请求对账、可见事件、receipts、selector 顺序/省略、图合并和 Query tools；旧“只有 opaque receipt oracle”失效 |
| Query-only | `engine/query_turn.go`、`query_action.go`、`query_progress.go` 及 Unix conformance | 不造 Task/E 的主路径存在；不能要求所有 Agent opcode 都变成 Query scope |
| Transfer | export closure、whole-record omission、target mapping、read-only foreign evidence、显式 adoption | 没有证据把字段级匿名化/同名 Space 跨 Store 当原 V1 必须能力 |
| 基础 durable core/graph/knowledge | sole P1 publisher、receipt-first、固定 Anchor/KnowledgeKey、AuditFull、故障/引用/历史回归 | 本轮没有证据撤回其已找到的局部保证；不等价四稿全部验收 |

## 本轮验证记录与下一步

本轮独立新增了两项有行为意义的披露反例测试，没有修改生产代码；生产修复由负责人实施。
GetAgentState 原失败输出和修复进度已反馈。GetQueryIntake 的独立测试与负责人修复并行，合并后
两项测试已实际结束并 exit 0，命令和结果见上。

优先关闭 C-01/C-02/C-03 这三个协议/执行语义问题，再运行相应负例与 embedded↔Unix 对照；
将 C-04/C-05、Annotation 和真实模型 v2 合并后的最终源树普通/race/vet/build/golden 结果记录为
新证据。旧报告关于 Work、Emission、Effect 的关闭结论不再作为当前验收依据。最终工程重构不能
替代这些语义收口，严格 V1 仍保持未完成状态。
