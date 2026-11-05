# ATE 执行对象与多 Agent 场景推演

> 本文保留完整 V1 目标。当前候选格式为 Control 5、Invocation 2、Continuation opcode 8、Work 1、Emission 3 和 Packet 4。2026-11-03 独立审计保留当时的反例和证据边界。本文末尾保留当时审计状态，当前实现收口和验证以文末 2026-11-05 段落为准。 [独立审计](validation/V1_INDEPENDENT_CURRENT_AUDIT_2026-11-03.md) · [格式清单](formats/ate-v1-manifest-2026-11-02.md)
>
> 2026-11-01 的代码对照见 [文档—代码审计](validation/V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-01.md)。本场景仍是完整验收目标，包级回归和部分 Node/Key/Cursor 支持不表示该真实多 Agent 流水已经全部执行通过。

> 日期：2026-10-30。版本：0.2.0，联合审计执行流水。  
> 关联：[语义设计](AGENT_TEMPORAL_ENGINE_DESIGN.md) v0.4.0、[实施架构](ATE_V1_IMPLEMENTATION_ARCHITECTURE.md) v0.2.0、[API 协议](ATE_API_PROTOCOL_DESIGN.md) v0.1.0。  
> 本文是官方 agent/v1 规范的对象实例与恢复验收流水，不是另一套尚未合入的候选模型；不表示已实现或已跑出这些日志，字段编号/磁盘格式仍在相应阶段冻结。  
> E0、OQ1、C01 等均为阅读用符号，真实逻辑 ID、内容摘要与 TxID 按规范生成。  
> Core/Profile 语义以语义稿为准，代码/同步以实施稿为准；本例必须使用同一对象、事件、Anchor、动作和查询契约，变更同步三稿。

## 1. 场景和执行边界

用户在已有项目 Session S0 中提交：

> 订单服务偶发超时。定位根因、修复代码、补回归测试；保持公共 API 不变，最终验证通过。

外部宿主（例如 GSEA）接收输入、运行 Agent Loop、调用模型和工具；它通过 ATE 协议接入，ATE 不依赖其包或 schema。ATE Core 接受并发布通用事务，随 ATE 交付的静态 agent/v1 校验标准 Query/Task/Execution/Step 等受保护转换，宿主负责业务策略和外部 Executor。独立运行 ate serve 也不会自动取代宿主的 Agent Loop。

初始只有一个总任务和 Root 执行。Root 后来创建代码调查、测试两个子执行；调查 Agent 又创建连接池调查子执行。这个结构是在运行中长出来的，不是预先生成完整执行 DAG。

本例假定 store、Space、Session、工具定义和项目工作区绑定已经建立。工作区 W0 若有快照，保存确切 manifest；若只绑定 live 目录，明确未捕获部分，不假称其内容可精确重现。Root 不因为有工作区引用就已经知道全部文件内容。

第一版不需要广播或订阅：宿主通过普通命令/查询执行下列流程。

## 2. “对象”的几种不同含义

### 2.1 逻辑实体、版本和事件

~~~text
稳定逻辑身份：TaskID = T0

不可变内容：
  OT0-1 = {task:T0, goal:D0, phase:running}
  OT0-2 = {task:T0, goal:D0, phase:completed, outcome:OOutcome0}

发生事件：
  ev-task-started   → OT0-1
  ev-task-completed → OT0-2

某次根里的映射：
  control.tasks[T0] → OT0-2
~~~

T0 始终是同一任务。OT0-1/OT0-2 是内容摘要寻址的不同记录，旧记录不覆盖。Event 说明变化的来源和发生身份。状态树根只是选择“在这个版本中，T0 对应哪份记录”。

不是每个字段都有独立实体 ID。Observation 可以是 typed Event + payload；ContextPacket、结果、产物、摘要通常只需 ObjectID，不再制造第二套业务身份。

### 2.2 原子提交、恢复点和临时能力

- Commit 原子接受一组对象、事件、根更新与命令 receipt。
- Revision 描述一致的领域/知识版本；不等于每一个 Commit。
- CommitFence 锚定全 store 的某个发布前缀，其 PostRootSet 固定当时控制面和多 Branch 状态；恢复须核对这些根。
- SnapshotAnchor={Fence,Space,Cursor?,KnowledgeKey?} 是持久读取锚点；KnowledgeKey=(Actor,E)，Cursor 必须此前已在 Fence 中发布。
- Cursor 指向 Revision，不单独代表当时全局控制账本。
- ViewRef 是进程内/服务内临时读取资源，不是持久记忆对象。
- frame、状态树内部节点和索引项是存储实现，不默认显示成 Agent 工作节点。

同一 Revision 后可能有多个只更新控制面或回执的 Commit。因此，历史接口使用统一 SnapshotAnchor，不能只拿一个 Revision 猜测所有控制状态。当前 ACL/grant 仍决定能否读取，历史权限不复活。

### 2.3 关系不必全部另建对象

EntityKey=(ProfileID=agent, EntityKind, CanonicalLocalID)，映射 NodeRef.Entity；Step key 为 (ExecutionID,StepSequence)。agent/v1 的版本属于契约/记录元数据，不改变稳定实体 key。GetNode(Entity,Anchor) 根据注册根读取当时 spec/state，ObjectRef 直接读取固定 bytes。

ExecutionSpec.task=T1、LoopStep.execution=E1 等固定关系直接用字段引用表达。反向邻接由这些字段/事件建立索引，不再保存另一份可独立漂移的“真相”。

独立产生、可撤销的支持/反驳/采用声明使用 RelationAssertion + Event。每个 schema 声明强内容引用、逻辑实体关系、事件来源与投影版本。逻辑 ID 不自动保留最新/全部版本；历史来源引用确切 Record/Event 或保留 Anchor/SourcePublicationRef，并登记历史保留依赖。索引、导航分组、计数和当前状态是投影，不能取代原始记录。

## 3. 最小执行 Profile 的身份与基数

| 单位 | 身份 | 关键关系 |
| --- | --- | --- |
| Session | S0 | 多次用户输入的交互容器，不隐式共享全部知识 |
| QueryRequest | U1 | 固定 Session、用户输入内容和认证来源；不包住所有后代执行 |
| Task | T0、T1、T2、T3 | 目标实例；当前接受的目标和真实运行状态受保护 |
| Actor | A0、A1、A2、A3 | 逻辑参与者身份；角色/模型/工具配置另有不可变版本 |
| Execution | E0、E1、E2、E3 | 一次承担，固定一个 Task、一个 Actor、至多一个父 Execution |
| LoopStep | E1/11 等复合身份 | 固定所属 Execution、实际目标版本和输入锚点 |
| ToolInvocation | I01、I11 等 | 哪个已接受模型结果请求哪个工具、参数是什么 |
| Activity | M01、G11、V0 等 | 模型、外部工具、资源操作等一项逻辑外部操作 |
| Attempt | G11/1、G11/2 | 某 Activity 的一次宿主可观测派发尝试 |
| Message | MSG1 | 固定发送执行、接收执行与不可变 payload；消费独立记录 |
| Branch | B0、B1、B2 | 状态探索路线，与 Agent/Task 不强制一一对应 |

TaskDefinition、TaskControl、ExecutionSpec、ExecutionControl 是记录类型，不各自再分配一套同义实体 ID。Task 的 parent/origin 在创建时固定，后续控制版本保留且不可重写；更换目标定义不会偷偷改变任务父关系。

第一版暂不增加 AssignmentID 或模糊 RunID。ExecutionSpec 固定 task/actor/parent 与初始来源，足以表示一次承担；任务重新指派创建新 Execution。一个 Task 可有多个 Execution，一个 Actor 也可承担多个 Execution。以后需要独立承担生命周期或活体移交时再拆 Assignment，不先含糊支持。

进程恢复保留 ExecutionID，增加 incarnation；Actor/Task 重新指派产生新 Execution，记录 supersedesExecution 来源。默认先给旧 E 子树建立取消屏障，再可恢复地传播取消/隔离；活子执行不改挂，旧 unknown 继续由当前 Supervisor/账本负责，新 E 使用重叠资源前须满足 fence/隔离条件。

工作知识至少按 (ActorID, ExecutionID) 隔离，历史再绑定 Revision/Branch 锚点。相同 Actor 在另一任务的 Execution 不自动继承这份工作记忆；跨执行复用通过显式 Seed、消息、KnowledgeImport 或 Memory 晋升。

### 3.1 用户交互与 Agent 循环分名

QueryRequest 替代含糊的“整个任务的 UserTurn”。Answer.respondsTo=U1 表示回答哪条输入。一次输入可启动多个任务，多次输入也可指向同一任务；询问进度不必新建 Task。无 Task 的短交互若需模型/工具，用 (QueryID,DecisionSequence) 的 DecisionRef 和 Query scope 记录；执行中的调用用 LoopStep DecisionRef，两者都绑定 accepted result 的响应内调用键。Query 披露不伪造 Execution 知识根。

一次 LoopStep 是一个主模型决策及其动作/观察批次，默认同一 Execution 串行推进主步骤。可有辅助摘要 Activity，但必须标 auxiliary，不能当成主模型请求的重试。

网络重试是同一 Activity 的新 Attempt；修改 prompt、要求修复无效决议等是新的逻辑模型操作，必须新 Step；辅助活动不得替换本 Step 的 primary accepted result。spawn 返回子执行身份后，该 Step 可以完成，不必等子执行结束。

## 4. 对象放在哪里

所有历史内容最终都是不可变对象/事件；下表说明当前生效入口，而不是允许调用者任意 Put 控制字段。

| 入口 | 内容 |
| --- | --- |
| 不可变对象区 | 原始 bytes、各版本记录、TaskDefinition、配置快照、ContextPacket、结果、消息、产物、验证、摘要 |
| LiveControlRoot 的 Profile 范围 | Query 响应状态、TaskControl、ExecutionControl、Step/Invocation/Activity/Attempt 状态、消息 inbox、Continuation |
| LiveControlRoot 的系统范围 | authority、epoch、现实预算、资源/效果账本；Profile 不能越权覆盖 |
| Branch / Revision.StateRoot | 计划、假设、候选领域状态和选定产物引用 |
| ExecutionKnowledgeRoots | 某 Actor/Execution 已正式采用的观察、知识与工作假设 |
| BranchRefRoot | Branch → 当前 Revision 与 CAS version |
| CommandReceiptRoot | 同一幂等键的已提交命令结果 |
| 已发布事务/事件链 | 所有变化、来源和提交证明，包括没有 Revision 的独立事件 |

Profile 的 schema/纯状态转换/引用和导航提取静态编入库与二进制，经 kernel 受保护分派；store 显式启用并声明版本。宿主负责业务策略，不能临时任意 Put 标准控制状态。通用存储不硬编码任何外部宿主 schema，也不加载任意 reducer。受信 Profile controller 只能更新被授权的命名空间，模型不能借 Profile 名义写 epoch/预算或伪造验证通过。

Task 的定义与执行控制必须分开：

~~~text
TaskDefinition D0：
  goal / constraints / acceptance / sourceQuery = U1

TaskControl OT0-1：
  task = T0
  currentGoalRef = D0
  phase = running
  acceptedOutcome = none
~~~

旧探索分支可保留 D0 和旧计划，但不能把现实中已取消的 T0 改回运行中。

## 5. C01：用户输入被接受

宿主先认证用户，以稳定接收命令键 K01 去重，然后提交：

| 新对象/记录 | 内容和关系 |
| --- | --- |
| OQ1 | 用户实际提交的文本/附件引用；大附件单独 blob |
| QueryRequest U1 的记录 OU1 | session=S0、input=OQ1、sourcePrincipal=用户；没有额外 QueryID/MessageID |
| QueryAccepted 事件 ev-q1 | subject=U1、payload=OU1、记录者为宿主接入组件 |
| QueryControl OU1-state1 | phase=accepted/pending_intake，下一动作的稳定键和确切输入引用 |
| receipt K01 | 已接收的 U1 身份与提交位置 |

U1 与 K01 不同：U1 是持久交互身份，K01 是这次接收命令的幂等身份。传输 correlation ID 又是第三件事。重发同一输入不能重复启动总任务。

Query 被接收不代表 Task 已完成建立，更不代表已经派出 Agent。普通聊天可以在交互范围内处理，不必为所有问候创建目标任务。

如果宿主已确定本例为工作请求，保存 StartIntent 对象 OStart1：接受的路由结果、U1、目标输入、Root 配置版本、资源基线、固定命令键 K02 和 expected QueryControl 版本。若路由要调用模型，其 Activity owner=Query U1、role=routing，以已发布 Query SnapshotAnchor 为来源（无 Execution/Cursor 也合法）；先保存 accepted result，再持久生成 StartIntent。不能恢复时重新猜路由或重新选择最新配置。

所有 PendingAction 保存 ActionKey/generation、完整原 RequestKey、exact canonical command/digest、预期版本、输入 Anchor/refs、authority 和结果绑定；Continuation 指向这些版本记录，不只是 NextAction 枚举。

恢复先 Lookup 原键；已提交复用原结果，未知先收敛。只有证明未提交且旧 authority 已 fence，才由 RebindPendingAction CAS 生成新代际/新 RequestID，保留 supersedes。接管者要有原幂等命名空间权限；不能换 body 或用 rebind 重跑 unknown 外部效果。

## 6. C02：接受工作目标，建立 Root 执行

宿主读取 OStart1，以固定 K02 提交启动命令，并 CAS QueryControl 的 pending 状态。下列对象与 U1→T0/E0 的启动绑定、phase=started、Root Continuation 一起原子建立：

| 对象 | 具体内容 |
| --- | --- |
| D0 | 不可变目标，sourceQuery=U1，约束=公共 API 不变，验收=修复与回归验证 |
| Task T0 的控制记录 OT0-1 | currentGoal=D0、phase=running |
| ActorSpec OA0 | A0 的角色/模型/工具配置版本；已有配置可复用，不含 API key |
| ExecutionSpec OES0 | execution=E0、actor=A0、task=T0、parent=none、initialGoal=D0、sourceQuery=U1 |
| Query/Goal Observation | E0 正式采用 OQ1/D0 中哪些输入与约束 |
| R0 | Root 的初始一致知识/领域版本，包含上述可见观察的来源 |
| ExecutionControl OEC0-1 | incarnation=1、acceptedGoal=D0、phase=ready、knowledgeCursor=R0、nextAction=开始步骤 |
| Branch B0 | 指向 R0；必要的 authority/admission 与资源策略一并建立 |

构造顺序为目标/执行描述 → 创建与观察事件 → R0 → 引用 R0 的后置控制记录/Continuation。后置控制事件不能反向被包含在 R0 中。后续渲染动作固定 SourcePublicationRef(producerTx=C02的预分配TxID,Cursor=R0,K0)，不含 C02 最终摘要。发布后按该 producer 事务自己的 LSN/digest/PostRootSet 解析为 Anchor(F02,R0,K0)，不取批次结束/latest；随后才能建 CP。

这些可以在一个物理 Commit 内按依赖顺序构造；同一 Commit 里的每个事件不要求全属于同一个 Revision。

C02 已提交但 ACK 丢失时，用 K02 对账并返回原 T0/E0。即便恢复处理器错误地携新命令键重复请求，QueryControl 的已启动绑定也必须阻止再次创建同一启动动作。多任务输入允许多个明确的 StartIntent/绑定，每个有独立稳定键，不靠“所有 Query 只准一个 Task”规避问题。

## 7. C03–C04：Root 的第一个模型决策

### 7.1 构造输入与登记派发

| 对象 | 关键关系 |
| --- | --- |
| LoopStep L01 | execution=E0、stepSequence=1、goal=D0、inputAnchor=(F02,R0,K0) |
| ContextPacket CP01 | sourceAnchor=(F02,R0,K0)、选定 Query/Goal/证据的确切顺序、输入 bytes 和 renderer 版本 |
| RequestEnvelope ORQ01 | 模型、参数、工具定义/输出预留等公开请求内容；凭据仅通过安全引用处理 |
| Model Activity M01 | belongsTo=L01、uses=CP01/ORQ01 |
| Attempt M01/1 | 当前 authority、接收方、请求摘要、预约与可能派发状态 |
| 派发事件与 Continuation | nextAction=等待 M01 的合法结果 |

这组意图持久提交后，宿主才发模型请求。CP01 只引用此前已发布 Anchor，不引用当前 Commit 或同 batch 未发布 staging 的 Fence/Revision。中间崩溃由 pending-render 重建输入，不重新生成模型决议。

### 7.2 保存响应和待处理动作

模型返回“派出代码调查 Agent 和测试 Agent”。宿主保存：

- 原始响应 OMR01；
- AcceptedModelResult OAM01：选中了哪个 Attempt 的哪个响应，不表示其建议必然正确；
- 两个 ToolInvocation：I01=spawn 代码调查，I02=spawn 测试；
- 每个 Invocation 的唯一来源键=(L01,OAM01,providerCallKey)，工具定义/确切参数和结果绑定；重复解释 OAM01 不再次创建相同调用；
- 解释/接受事件、步骤状态与待处理动作 Continuation。

不额外制造一个无法定义的“思考节点”。公开的计划文本和结构化动作可保存；未暴露的模型内部思维不属于可恢复事实。

原始响应/accepted result 不包含后来才生成的 Invocation 版本摘要，避免 OAM01 与 InvocationSpec 互相 hash 引用。后者引用前者，反向查找由索引提供。

## 8. C05a/C05b：动态创建两个子 Agent

I01/I02 是调用内核/宿主命令的工具，不需要假装成不可判定的远程外部效果。各自通过稳定 RequestID 提交 Spawn，并 CAS InvocationControl 的 pending→fulfilled。命令 receipt 与 child/SpawnResult/父子/admission/Continuation、唯一 child 绑定同事务；换 RequestID 或并发调度也不得对同一 I01 生第二个 E。已完成则返回原绑定或明确冲突。

以 I01 为例，一笔 Spawn 命令原子保存：

~~~text
TaskDefinition D1
  goal = 定位超时根因并提出修复
  inheritedConstraintsFrom = D0
  inheritedGoalBinding = {sourceTask:T0, goalRef:D0, goalEpoch:1, constraints:[公共 API 不变]}
  sourceInvocation = I01

TaskControl T1@v1
  task = T1
  parentTask = T0
  createdFromInvocation = I01
  currentGoal = D1
  phase = running

Seed OSeed1
  sourceAnchor = 已发布的父输入快照（Fence + Cursor + KnowledgeKey）
  selectedInputs = [D1, 允许继承的用户约束/证据]
  workspaceBaseline = W0

ExecutionSpec OES1
  execution = E1
  actor = A1
  task = T1
  parentExecution = E0
  createdFromInvocation = I01
  initialGoal = D1
  seed = OSeed1

ChildCreated / SeedObserved 事件
  → 子知识版本 R1-seed
  → Branch B1 指向该版本

ExecutionControl E1@v1
  incarnation = 1
  acceptedGoal = D1
  inheritedGoalBindings = [OInheritedGoalBinding1]
  knowledgeCursor = R1-seed
  admission / authority /资源策略
  continuation = 指向固定 RequestKey/输入/授权的资源准备 PendingAction，再开始步骤

SpawnResult
  invocation = I01
  childExecution = E1
  childTask = T1
~~~

T2/A2/E2 同理，目标是补回归测试。两个 Spawn 不必强制绑在一个事务；每个已成功子执行的一组记录必须完整原子提交。一个成功另一个失败时，Continuation 只补做未完成动作，不再请求模型重生同一计划。

Spawn 检查 Task/E/祖先当前取消屏障、目标版本与资源条件；父 Step 可结束但父 E 仍对未结束子树负责，终止前须 join 或明确取消/交接。宿主在 Spawn 持久完成后才启动 Worker。工作区物化本身是受记录的外部资源准备活动，不能因有 Branch 就假定物理目录存在。引擎分支与 Workspace/Git 分支用显式绑定，不视为同一个对象。

E1 后来通过相同流程创建 T3/A3/E3，检查连接池。parentExecution=E1 固定不变；任务树与执行树可以对应，但它们是不同关系。

子 Seed 只包含获准材料，不复制 Root 全部私有上下文。Root→子执行的父子关系也不授予任意数据权限。

## 9. C06–C11：一个子 Agent 的两次 grep

### 9.1 第一步，模型提出工具调用

E1 的 L11 使用目标 D1 与知识 R1-seed，经历自己的 CP11/M11/Attempt/响应。接受结果后登记：

~~~text
InvocationSpec OI11
  invocation = I11
  execution = E1
  task = T1
  goalVersion = D1
  step = L11
  sourceAcceptedModelResult = OAM11
  toolDefinition = OT-grep-v1
  arguments = OArgs11
  dispatchClass = external

InvocationSpec OI12：另一组搜索参数
~~~

ToolDefinition 固定实际工具版本、schema 与执行/effect 契约。名称叫 grep 不足以证明纯读或可重试；契约由宿主验证，不由模型自行声明。

### 9.2 第二步，登记 Activity 与 Attempt

I11 归类 ExternalActivity；InvocationControl 原子绑定唯一 G11，重复处理/新 RequestID 不再造另一个逻辑操作，重试只增加 Attempt。G11 是 I11 对应的外部 Activity。其不可变请求记录绑定工具定义、参数、工作区基线/资源、来源步骤与请求摘要。ActivityControl 保存当前阶段、合法 Attempt 和 acceptedResult。

Attempt G11/1 保存当前 owner/执行 incarnation、授权/预约、接收方与派发证据。提交可能派发意图后才运行工具。G12 可并行。

适配器不能私下重试而不登记可观测 Attempt；接收方内部不可观测的重试不能被虚构成精确已知事实。

### 9.3 第三步，保存结果，不直接更新模型知识

G12 先完成时，保存输出 bytes 与结果记录：

~~~text
ResultRecord OResult12
  activity = G12
  attempt = G12/1
  stdout = OStdout12
  stderr = OStderr12
  exit / error / completion / timing
  capture = full / truncated / partial / unknown
  workspaceEvidence = 对实际资源/快照的可证明引用
~~~

ResultRecorded 事件引用 OResult12，并因果依赖相应派发记录。G11 后完成时保存独立的 OResult11。相同 stdout 可以共享 blob，两次实际发生不能合并。

接受结果是另一个明确转换：ActivityControl.acceptedResult 指向一个合法回执。工具报错或没有匹配也可能是合法 accepted result；accepted 不代表任务成功或输出陈述为真。

raw 记录、Activity acceptance、settlement、按 KnowledgeKey 的 consumption 是正交状态。settled 表示费用、资源等后续记账完成，不能表示模型已看到结果，可以早于或晚于消费。

默认 Observation 引用合法 acceptance 证据，不直接消费任意 raw/旧 epoch 回执。晚到证据可经当前 owner 的明确 Reconcile/EvidenceImport 验证，不能自动满足新 E/新 Activity。Fenced 不接受新 RecordResult，未持久回执不能承诺不丢；恢复时保留 unknown 对账。

### 9.4 第四步，消费观察与下一份输入

宿主选择将哪些内容放进 E1 的工作记忆，保存 Observation payload：

~~~text
OObservation11
  recipientExecution = E1
  sources = [合法 acceptance11→OResult11, 合法 acceptance12→OResult12]
  visiblePayloads = [OVisible11, OVisible12]
  sourceRanges / transformVersion / presentationOrder
~~~

OVisible 可以是原文，也可以是带标题、截断和格式化的确切文本。原文保存完整但展示截断，与原始采集已经截断，是不同状态。

ObservationAccepted 事件与 Actor 知识根推进原子提交，得到 R1-observed。本事务同时留下带 SourcePublicationRef 的 pending-render；先发布，再解析该 producer 事务自己的 Anchor，构造 CP12(sourceAnchor=该Fence+R1-observed+K1) 和下一 Model Activity；不在同一个未发布提交里让 CP 反指自身快照。

Observation 证明宿主 Agent 工作记忆已采用这些材料；CP12 证明这次模型输入到底选择了什么。派发证据和模型回执说明请求是否可能发送/产生结果。不能把这些事实写成“远端模型必然阅读并理解了全部材料”。

### 9.5 一个调用的完整查找路径

~~~text
U1 → T0 → 子 Task T1 → Execution E1 → LoopStep L11
   → ToolInvocation I11 → Activity G11 → Attempt G11/1
   → Result OResult11 → Observation → 知识 R1-observed
   → ContextPacket CP12 → 下一模型活动 M12
~~~

每条箭头分别表示来源/归属/派发/产出/消费/采用，不是同一种 parent。

## 10. 大量工具与上下文压缩

80 次 grep 是 80 项调用，各自保留来源、尝试与结果。列表可以按 Execution→Step→Invocation 折叠；“检索阶段”分组必须有明确成员/规则版本，不能仅按时间接近猜测。

输出一万行不意味着一万个事件；字节分块不自动成为一万个用户节点。Activity 转换是否同事务，需要按执行边界决定；派发意图不能在工具已经运行后补写成事前事实。

需要摘要时：

~~~text
知识 R1-k
 → 摘要输入 Packet（此前已发布 SourceAnchor）
 → 辅助 Summary Activity / Attempt / Result
 → SummaryRecord OSummary1（SourceSet、原输出、模型/规则版本）
 → 接受为派生观察，知识 R1-k1 并发布
 → CPnext 用已发布 Anchor 选择 OSummary1 与必要原始锚点
~~~

原始工具证据不删，旧 CP 不改。Summary 可被复核或撤回，不自动成为长期真理。不需要因为压缩就创建新 Actor/Execution。

## 11. C12–C13：子结果和父消费

E1 形成：

- Artifact OPatch1：基线 W0、实际 patch/文件 manifest、来源执行与证据；
- Outcome OOutcome1：结论、产物和被采用的证据；
- Message MSG1 的不可变记录：sender=E1、recipient=E0、body/允许披露内容、OOutcome1/OPatch1 引用；
- MSG1 的投递状态与 E0 inbox entry；
- E1 的结果就绪/完成事件和相应控制版本。

这时 Root 还未消费。宿主查询 MSG1 后，消费命令 CAS (MSG1,recipient E0) 的首次消费槽，同事务记录合法来源 Observation、inbox、Root 知识 R0-msg1 与下一动作。换 RequestID 不重复首次消费；以后重读是显式证据访问。查询自身只读。

Message 的证据链可以连接子 Agent 私有活动，但 Root 不因此得到所有 payload。只消费总结就只采用获准总结；继续查原文仍经权限、披露记录和新知识版本。

子 Execution 已交付，不代表 Task 已验收。TaskControl 可处 result_ready/awaiting_review，由 agent/v1 状态机明确区分；不拿同一个 completed 表示所有层次完成。

## 12. C14–C18：接受产物、集成、验证和目标完成

Root 明确接受 E1 的修复和 E2 的测试。输入是精确 Branch heads、Artifact 版本与来源，不是“拿子 Agent 最新东西”。

| 阶段对象 | 必须绑定 |
| --- | --- |
| MergeProposal / MergeResolution | 输入 Cursor、基点/冲突、采用/拒绝项、规则/决议与目标 CAS |
| 集成 Activity | 确切 patch、基线、隔离工作区与实际回执 |
| 候选/最终 Workspace artifact | 实际内容 manifest，而不是仅目录名 |
| 发布意图与结果 | 当前资源 authority、基线 CAS、外部效果回执 |
| ValidationRecord OValidation0 | 被验证的 Wfinal、目标 D0、命令/环境/工具版本、输出与接受规则 |
| TaskOutcome OOutcome0 | 被接受产物、验证证据、当前目标版本、采用的子结果 |

Task join、领域 Branch merge、文件 patch 应用、Workspace 发布与最终验证是不同动作。它们可以组合调度，不能合成一个没有来源的“成功”。

最终测试应针对实际发布版本 Wfinal；或证明测试对象与发布对象摘要一致。任务完成前检查当前目标/资源版本仍相容，不能拿另一个版本的测试通过作为证明。

TaskControl T0 的 completed 版本引用 OOutcome0，完成事件引用该新版本。OT0-completed 不能反向包含这个当前完成 EventID，反向 createdBy 由索引提供。

需要 Root 以模型生成最终解释时，再把验证/完成结果作为 Observation，得到 R0-final 并先发布。CPfinal 引用对应已发布 SnapshotAnchor，避免偷读尚未进入其知识视图的控制状态。

## 13. C19–C20：最终答复与投递

最后一个模型 Activity 使用 CPfinal，产生原始响应和 accepted result。宿主形成不可变 Answer 记录：

~~~text
OAnswer1
  respondsTo = U1
  tasks = [T0]
  body = OAnswerText
  generatedFrom = accepted final model result
  evidence = OOutcome0 / OValidation0
  kind = final
~~~

Query 的当前响应状态指向 OAnswer1，Root Execution/Step 按完成条件结束。客户端取得该 Answer，不需要引擎主动推送；响应丢失可以按 U1 重取，不重跑任务。

若有可靠客户端 ACK，记录 client_received；不能把“写入 socket 成功”或“宿主准备好答复”说成用户已阅读。

Task 目标已经完成但最终模型请求失败时，保留完成证据与待答复状态。恢复只补答复，不重新修代码/跑全部任务。

### 13.10 API 在本例中的渐进查看

模型不直接拿到一棵“全部节点树”，也不按每一种内部对象拥有一套写接口。宿主给它一个固定 View/Anchor 和少量读取工具：

~~~text
list(Task/Execution 摘要)
 → inspect(E1)
 → expand(L11 的 inputs/upstream/downstream)
 → inspect(I11/G11)
 → expand Attempt/Result/Artifact
 → read_content(OResult11 或 CP12 的获准范围)
 → trace(Event/Commit)（进入审计模式）
~~~

每一步都携带同一 View/Anchor、KnowledgeKey（若是执行知识）、投影和资源上限。模型可以选择展开层级，但不能通过原始 ObjectID、随意加深 depth 或直接调用 Commit/StateOps 逃避权限与唯一动作槽。

读取层级和写入层级分开：list/inspect/expand/trace/read_content/search 只读；Spawn、MessageConsume、AcceptResult、GoalRebase、MergeProposal 等通过受保护的 agent/v1 命令提出并由宿主/Profile controller 校验。内部 Event、Revision、Commit、Blob 和 frame 都可以在审计模式查看，但不是默认模型工具的直接写入单位。

因此，API 的统一性体现在请求/响应、Anchor、NodeRef、分页和权限语义统一；颗粒度体现在 `layer + projection + contextMode + limits`，而不是让一个万能 API 暴露所有底层结构。

## 14. 一眼看到的三种组织视图

### 14.1 导航关系

~~~text
Session S0
  Query U1 ──启动/关联── Task T0
    Task T1 ──由执行── E1(A1)
      LoopStep L11
        ModelActivity M11
        ToolInvocation I11 / I12
          Activity G11 / G12
            Attempts → Results → Events / blobs
      产物 OPatch1 / Message MSG1
    Task T2 ──由执行── E2(A2)
  Answer OAnswer1 ──respondsTo── U1
~~~

ListNodes 显式选 layer=entities/events/content，SDK 默认 entities；原始 events 审计始终可展开。Entity 当前/历史版本按 Anchor 解析，ExecutionKnowledge 仅按 K/知识 Cursor 解释，不回退到全局最新。

这是浏览投影，不是唯一存储树。Execution 的 parent、Task 的 parent、causal parent 和 Revision StateParent 必须分类型。

### 14.2 因果与知识关系

~~~text
用户输入
 → Root 接受目标 / 模型决议
 → Spawn I01
 → 子执行观察 / 产物
 → MessagePublished
 → Root MessageConsumed
 → Root 后续模型输入
 → 验证与目标完成
~~~

子 Agent 提交较早、Root 提交较晚，不自动构成知识消费；只有显式关系才能证明。

### 14.3 物理存储与状态入口

~~~text
CURRENT → manifest → 已认证 segment 范围
  TX C01 → TX C02 → TX C03 → ...
    拓扑混排 OBJECT / EVENT / STATE_OPS / REVISION
    尾部 REF_UPDATES / CONTROL_UPDATES / receipt确认 / TX_COMMIT

发布的 RootSet
  LiveControlRoot → 逻辑实体当前版本
  BranchRefRoot → B0 / B1 / B2 heads
  Revision.StateRoot → 计划/候选领域值
  ExecutionKnowledgeRoots → 观察/知识状态
  CommandReceiptRoot → 幂等结果

catalog / 邻接 / timeline 索引：辅助查找，可验证覆盖并重建
~~~

每个 Commit 可以写多个对象、事件和多个根，部分 Commit 没有新领域 Revision。两个子执行的 Commit 可以交错，物理总序不替代业务因果。

## 15. 事件是谁记录的

统一使用语义稿 §4.3 的 Event envelope。以 grep 结果为例：

~~~text
stream.owner = Execution E1
stream.executionIncarnation = 1
streamSequence = 协调器分配的本 stream 顺序
recordedBy = 认证宿主 Executor
subject = NodeRef.Entity(agent, Activity, G11)
initiatingActor = A1
kind = ToolResultRecorded
origin = L11 / I11 / G11 / G11-1
payload = OResult11
causalParents = [相应派发事实]
~~~

RecordedBy 不来自模型自报；Subject、stream 归属、触发 Actor 和安全 scope 分开。宿主记录回执不是模型证明测试通过。字段编号尚未冻结，但这些含义已经跨稿统一。

QueryAccepted 属 Session/Query stream，尚无 E 也可存在；系统事件使用系统 scope。路由 Activity 以 Query 为 owner、使用 Fence-only SourceAnchor，不伪造 Agent。Execution stream 的 incarnation 跟随恢复代际；同 E 工作知识不因此清空。

CausalParents 证明来源依赖，StreamSequence/LSN 证明记录顺序，都不证明其他执行已消费 payload。

## 16. 中途变化与恢复的对象含义

| 情况 | 对象变化 |
| --- | --- |
| 用户 U2 只询问进度 | 新 QueryRequest/Answer，关联 T0，不必新建 Task/Execution |
| 用户 U2 改目标 | 新 TaskDefinition D0-v2；TaskControl.currentGoalRef/goalEpoch 更新，旧记录保留；后代只按显式 InheritedGoalBindings 校验实际继承约束 |
| 同一 E 采纳新目标 | ExecutionControl.acceptedGoalRef 经 CAS/GoalAdopted 更新；每 Step 仍固定实际使用版本 |
| 旧模型结果晚到 | 原始结果保存，标记旧来源；采用或派发其计划前重新验证当前约束 |
| 进程故障恢复 | 同 E、新 incarnation/owner，当前 Continuation 驱动，不重生已接受计划 |
| 重新派给另一 Actor | 同 Task、新 Execution；记录替代关系，不更改旧 E 的 actor/parent |
| 旧 E 还有活子执行 | 默认取消旧子树；祖先屏障阻止新 Spawn/Claim，当前 Supervisor 持续跟踪回收/隔离/unknown，不改挂新父；新 E 无隔离证明不得复用重叠资源 |
| 从旧点反事实续跑 | 新 Task/Execution/Branch，保留 forkedFrom 和精确旧 Goal/Anchor；不倒带原 TaskControl，采用当前权限/预算，已执行效果只作为来源证据 |
| 有派发无可靠结果 | Attempt/Activity 保留 unknown；不因恢复或取消消失 |

GoalAdopted 只说明宿主的当前绑定更新，不证明模型已经看到新目标；新的 Observation/已发布知识 Anchor/ContextPacket 才能说明输入。父 Task 更新单调提升 goalEpoch；子 Execution 保存实际 InheritedGoalBindings，并在下一主 Step、Claim、grant、完成前校验 sourceTask/goalEpoch/constraints。失配或未观察新约束时 Blocked；GoalRebase 产生不可变新子目标并用受信兼容证据 CAS 更新。下一主 Step/派发还要校验 Task 当前目标、E acceptedGoal、Step/CP 目标相容。旧模型结果可保存，但不能自动执行过时计划。

## 17. 联合基线的验收与冻结

本例是官方 agent/v1 的首批契约场景，须落实成 schema、golden 和测试 fixture；不是仍与上位稿冲突的候选建议。规范责任在语义稿，代码/同步与阶段门槛在实施稿。

| 反例 | 预期 |
| --- | --- |
| C01 已接收但未启动即退出 | 原 pending-intake/route 输入和 K02 恢复，不换最新配置 |
| C02/Spawn 已提交但 ACK 丢失，故意换命令键 | 逻辑唯一槽返回原绑定，不重复 Task/E/Activity |
| Observation 提交后、CP 前退出 | pending-render 由 ProducerTx 解析那笔事务 Anchor，不取 latest/batch-end；无自引用 |
| 同 A 的另一 E 已有私有发现 | 当前 E/历史查询不泄漏，必须显式 Seed/import |
| raw 或 settled 有记录、未合法 accepted/consumed | 不推进下一知识版本或父 join |
| GoalAdopted 后未观察新目标即派发 | blocked，旧 Step/CP 不改写 |
| 父 Task goalEpoch 更新而子 InheritedGoalBinding 未 rebase | Claim/grant/完成门禁 blocked；GoalRebase 需要具体兼容证据 |
| 换 incarnation 后恢复旧 PendingAction | 先原 RequestKey 对账，再按未提交/fence 条件显式 rebind |
| 取消/重派与孙执行 Spawn 竞态 | 祖先屏障拒绝新工作，旧子树与 unknown 有负责人 |
| Query 工具读完但披露前退出 | 原 Anchor/参数/索引版本补读，不能换 latest |
| 筛选 ExperiencePath 后回看失败证据 | 保留来源/省略锚点，不把筛选路径冒充完整原始轨迹 |

仍需阶段工件冻结的是字段编号、canonical/wire bytes、平台支持和实测效果；不会再由不同实现各自定义 Task/Turn/事件作者/恢复动作的含义。

上位规范的原子身份/版本/保留机制、官方 Profile 和 InheritedGoal/GoalEpoch 约束共同组成 Agent 原生时间执行内核。以本例逐点验证长任务、多 Agent、回溯/分叉、知识召回和经验筛选；既不能退化为宿主任意日志，也不能把存储正确当成模型记忆效果已经解决。

## V1 recovery walkthrough addendum (2026-11-02)

C01/C02 are represented by a durable Query intake Work unit before a Task exists.
C04/C13 and C05 use one bounded Work candidate for acceptance/next action, message
consumption/knowledge/continuation, and child seed/authority/continuation. A failed
substep publishes none of those facts; a lost response retries the original receipt.
C17 uses the typed Validation envelope, and an external delivery uses a versioned
Emission grant whose current policy is checked again at Claim.

### C06–C11 的工具决议补强（2026-11-02，历史）

primary 结果捕获后，`model_decision` Work 先接受该结果，创建完整 Invocation 集合并保存
Continuation；每个 Invocation 固定 accepted result、providerCallKey、工具定义/版本、参数和
dispatchClass。调用仍 pending 时不强制 FinishStep，否则会与完成门禁冲突。后续工具 Activity
通过 Invocation ID 使用固定的 dispatchClass；结果处理完成后才 FinishStep。重开和 compaction
不改变原始 receipt、调用定义或恢复点，见 `engine/work_decision_contract_test.go`。

typed Continuation 用已发布的知识 cut 和下一动作 generation 恢复；Emitter 的 Issue 保存依赖和
签发策略 cut，Claim 再检查当前策略。上述路径已有针对性证据；Query-only 主路径和 Agent/Profile
Event 归因已在后续补充中验收，非 Agent domain event 搜索和旧格式兼容边界仍按交叉审计报告保留。

出站 chunk 在 schema 2 中还固定实际投影、全量/范围摘要和总字节数；越界 chunk 不会被默默
截断后授权。原生 Unix conformance 独立计算 `value_bytes` 原文及子范围的 SHA-256，核对 SDK
收到的 Proof，而非只比较生产端生成的两个相同结构。节点和领域查询的响应同样经过一次性目的地许可；读取一个被 Packet 记录为输入的派生对象时，引擎沿 Packet、Observation 和 Summary 来源闭包重新检查当前撤回。

### Query-only 和跨 Space 证据流水（2026-11-03）

Query-only 的对象流水为 `Query intake → QueryProgress(v=1, seq=0) → Query Activity/Attempt →
accepted model result → QueryProgress Decision(seq=1) → exact tool request → tool Activity/Attempt
→ QueryProgress tool completion → QueryAnswer`。每个中间请求都有 reservation；恢复首先对账原
RequestKey，再按 source Fence 和 adapter version 继续。任何一步失败都不会新建 Task/Execution，
也不会把旧 accepted result 重用成新的 DecisionSequence。

Transfer 流水为 `source policy cut → canonical closure Manifest → target ImportRecord + read-only
ArchiveEntry/mapping → explicit local Observation/Memory candidate`。Foreign Anchor 只作为证据，
不提供目标域读取授权或执行权限；Omit 是完整记录省略并产生 Gap，不修改原始 canonical bytes。

所有模型与工具派发遵循 `Issue(schema3) → exact Activity/Attempt grant → emission_dispatch Work →
local dispatch receipt → external send/reconcile evidence`。有 ContextPacket 的模型调用授权该 Packet 的完整
RecordedInput。两个 Packet 即使记录相同字节也仍有不同的来源边界。本地提交后进程退出时保持 unknown，
由 Attempt reconciliation 决定 occurred/no_effect/compensated，不能把网络超时解释成成功或无效果。

## 18. Strict V1 场景审计状态（2026-11-04）

已发布的当前 V1 候选格式继续重放 command、StateOps、receipt、event 和 snapshot。Invocation 绑定真实
accepted primary result。`model_decision` 先发布完整 Invocation 清单。`child_spawn` 使用已登记的
Invocation，接受的调用结果为已建立的子执行身份，子任务完成由独立 TaskLink/Join 跟踪。

C02 的 Query/Goal Observation、R0、Continuation，以及 C05 的 child seed、知识位置、authority
与 Continuation 各自由同一 Work 原子建立，知识位置绑定该 Work 的 ProducerTx 和精确 Revision。
C04/C13 的主模型结果接受及消息消费分别走 `model_decision` 和 `message_consume`。C13 的接收方
Continuation 指向本次采用消息 Observation 后的精确最终 Revision 及其生产事务。同一组提交回执丢失后，
宿主先对账原 RequestKey。先前 incarnation 的主模型结果经 `occurred` 对账后，可在同一
`model_decision` 用 `ImportActivityResult` 接受并保存完整调用与恢复位置。受保护的单条 Control 调用被拒绝。

下一个执行级主决策先从其历史 Packet Source 导出 Requirements，再把每条 canonical 内容作为同一
KnowledgeKey 的 Observation 采用。首个 Packet 已包含目标；后续 Packet 同时列出当时的待验收条件、未处理
调用和行动、未消费消息、待 Join 与 unknown Attempt。若使用 selector，将这些 Observation 标为 Mandatory。
宿主保存 exact RecordedInput 和 HostPolicyVersion，引擎核对历史要求、采用记录及 Packet Items。

接收方允许短期去重时，旧 Attempt 的网络中断保留 unknown。恢复先核对原请求，再用相同 Activity、
精确请求、recipient/key 取得覆盖所有旧 Attempt 与下一 sequence 的有限 `IdempotencyGuarantee`。
宿主持久化本次 `RetryAuthorization`，在 Retry、Claim、Dispatch 分别提交仍有效的 `RetryUse`。
窗口过期、保证撤销或 authority 变化会阻止继续使用该授权。已有真实终结 `no_effect` 且能够 fence 晚到
效果时，可走全 Attempt 对账路径。外部 send 和终结证明由接收方与宿主执行。

该句属于 11-04 历史状态。当前场景、四文档交叉验收和 M6 源树证据见本文末的 11-05 段落和 [当前交叉验收续记](validation/V1_CURRENT_CROSS_CHECK_2026-11-05.md)。

## 2026-11-05 当前场景收口

C01–C20 中涉及的受保护事实组使用当前 V1 Work。模型/工具 Attempt 使用 exact Emission grant，FinishStep 使用同执行 checkpoint、ActionIntent 和最终 Knowledge cut，消息消费把 Observation 与接收方 Continuation 绑定到同一生产事务。读取 Packet RecordedInput 的派生对象会重新检查来源撤回。当前源树证据与测试退出码见 [V1 当前交叉验收续记](validation/V1_CURRENT_CROSS_CHECK_2026-11-05.md)。
