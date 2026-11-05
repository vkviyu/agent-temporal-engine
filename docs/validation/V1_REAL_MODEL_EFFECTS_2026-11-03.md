# V1 真实模型效果对照 — 2026-11-03

这是原 M6 的实测工件，**不是 V1 全部完成证明**。本轮已实际调用用户提供的 Responses 中转站，
`GET /v1/models` 包含 `gpt-6.1-sol`；`POST https://2xa.cc.cd/v1/responses` 成功。密钥来自环境，
没有写入请求工件、日志或仓库。

## 实验一：三轮递归摘要后的证据回查（v1）

代码：`tests/evaluation/{provider,host,effects}_test.go`。三组配对试验，每组用同一条三轮摘要轨迹，
随后各执行一次最近摘要输入和 ATE 原始证据回查输入。总计 15 次真实模型调用，全部 completed。

每轮增加 24 条受控运维记录，要求模型生成不超过 160 词的摘要。任务包含原始文件保留约束、
non-idempotent unknown 不重试约束、不可猜测的对账键/checksum，以及成功/失败分支及反例。
模型只做受控工程证据决策，没有执行自主编程或真实生产运维。

两组共用 20,000-byte 请求 framing 上限、最终 `max_output_tokens=1024`，摘要输出上限 768。
ATE 使用生产 `RecordContextSelection`，window/output/framing 为 4096/1024/256，segment 成本为
UTF-8 四字节近似一个 token，明确是 estimated。模型实际窗口没有被填满；该实验模拟应用层多轮
压缩，并非测试 provider 自动 compaction。相同上限不代表实际使用 token 相同。

八项固定评分：保留确切文件、拒绝 unknown 重试、回忆确切对账键、回忆确切 checksum、选择分支、
识别失败分支、解释重复追加反例、列出五类来源标签。评分在运行前固定，不因结果调整。

| 配对 | 最近摘要 + 最近八条记录 | 同一摘要 + ATE 显式原始证据 |
| --- | ---: | ---: |
| 1 | 6/8 | 8/8 |
| 2 | 7/8 | 8/8 |
| 3 | 6/8 | 8/8 |
| 总计 | 19/24 | 24/24 |

最近摘要组的来源覆盖均未通过，其中两组还没有保留确切成功分支名。其余约束、对账键、checksum
及反例解释均通过。这支持“固定相关 ID 的原始证据回查在此样例补回了摘要省略的细节”，不支持
泛化为自主相关性发现、通用长期记忆收益、统计显著性或数小时自主 Agent 成功率。

原始工件：`results/model-effects-2026-11-03-run1/`。包含每次 request/response、传输元数据、
ContextPacket/Activity/accepted Result、Summary、评分及试验 journal。15 次响应累计
input/output/total 为 **18,519 / 3,036 / 21,555 tokens**；以代理实际返回的 usage 为准。
代理的上游内部 framing 不受此实验控制，未按官方标价推算中转站费用。

v1 的明确局限：Summary 真实保存了 generation/source，随后生成笔记以普通 raw Observation 供下轮使用，
没有通过 `PayloadKind=summary` 进行 typed Summary 的验证后复用。因此 v1 不能作为该 typed 路径的
效果验收；下节 v2 另有真实 factual review 和 typed 引用。v1 评分只校验标签覆盖和有限字段，
不证明每条自然语言解释都被正确引用；词片反例检查也不是通用语义 judge。

## 实验一补充：类型化 Summary 与事实复核（v2）

精确命令为 `go test -v ./tests/evaluation -run '^TestRealModelTypedSummaryReviewAndEvidenceRecallV2$' -count=1 -timeout=25m`，
环境同上，工件目录为 `results/model-effects-v2-2026-11-03-run1/`。进程 exit 0，耗时约 170 秒。
18 次真实 Responses 调用均 completed，usage 合计 **27,179 / 2,810 / 29,989 input/output/total tokens**。
Go 测试通过只表示试验按预定策略运行，包括拒绝摘要的分支；不表示三组均完成最终任务。

每轮摘要由隔离的第二次模型调用进行事实复核。复核允许省略，只判断所给来源是否支持候选声明；
宿主以 `factual-summary-review/v1` 严格解析 verdict，保存 generation、review request/result 与
Validation.Evidence。接受后以 `PayloadKind=summary` 采用真实 SummaryID，下一轮通过其类型化
来源读取。每轮重开核对原 Summary、Observation 和源 cut，最终完成组执行 AuditFull 并再次重开。

| 预定组 | 三轮摘要状态 | 最终摘要 + 最近八条 | 同一摘要 + 显式原证据 |
| --- | --- | ---: | ---: |
| 1 | 三轮均接受 | 7/8 | 8/8 |
| 2 | 第一轮复核拒绝，未采用、未执行最终调用 | 未评分 | 未评分 |
| 3 | 三轮均接受 | 7/8 | 8/8 |

两组完成试验都只在“列出五类原始来源标签”这一项存在差异。摘要组正确引用其实际收到的
Summary 标签；它没有收到原始来源标签。其余七项（约束、早期精确值、分支和失败解释）两臂均通过。
因此 **v2 没有观察到事实回忆或任务决策的得分收益**；8/8 与 7/8 的差异反映此评分定义下原始来源
标签的可获得性，不能据此断言模型记忆质量提升。只对完成组计算为 14/16 与 16/16，不能把被拒绝组
从试验总数中删去并报告成 100% 任务完成率。

组 2 的 reviewer 认为“对账仍未解决”没有被源记录直接证实：来源只要求替代操作前先对账。
候选因此以 rejected Summary 保留审计，禁止复用。这是一次真实的**模型复核判断**，不证明该判断
本身一定正确，也不把拒绝伪装成网络错误或另跑一组替代。摘要/复核使用同一模型版本，非独立人审。

本实验的工程事实由夹具提供（包括模拟运维数字、分支及反例说明）；不是实际生产故障或数小时
自主编码任务。它证明了类型化链路在真实模型输出、拒绝分支、重开和压缩下的可运行性。未知相关性
检索、通用摘要事实准确率、普遍跨项目收益与数小时自主执行效果均未由这个小样例证明。

该链路首先通过离线传输夹具暴露并修复了一个生产引用闭包错误：packet GoalRef 可为已采用的
逻辑 SummaryID，`knowledge.ContentReferences` 不应把它当作原始 codec ObjectID。离线两轮递归
回归与 `TestTypedSummaryGoalRetainsRawInputWithoutInventingObjectAddress` 分别覆盖重开/压缩和引用分类。

## 实验二：跨项目适用性（v1）

代码：`tests/evaluation/transfer_effect_test.go`。源项目三条固定 procedure 经过真实 Memory
candidate/validate/promote API，再由真实 Export/Import 生成目标域只读 ArchiveEntry。目标域显式
Observation 采用 archive，没有导入 authority 或自动晋升本地 Memory。

分别使用匹配的 `append-v3/linux-fsync` 和不匹配的 `append-v4/read-only-audit` 环境；每种环境
比较全部三条候选与生产 `memory.Search` 的精确 applicability 过滤结果。模型需判断是否适用、确切
procedure ID 或 null、以及 unknown 是否可重试。共四次模型调用。

两组在两个环境的三个判断项都正确（各 **6/6**）。生产过滤把候选分别从 3 条减为 1 条和 0 条，
但**没有观察到模型得分收益**。不能由更少候选推导更高成功率。源候选、环境和相关性预先由夹具
声明，v1 的 source validation 文本不是实际生产部署验证。后续运行将另保存实际本地 protocol
fixture 的文件追加观测，不覆盖 v1 工件。

工件：`results/model-transfer-2026-11-03-run1/`。单次小样例只证明该固定输入上的应用/拒绝结果，
没有证明跨项目自动发现、经验学习、推广安全性或迁移的普遍收益。

### 本地接收方观测补充（run2）

当前 `checkSourceProtocol` 在临时文件中实际调用两次相同 request key 的追加：无去重接收方得到
两条记录，`append-v2/idempotent` 的进程内去重接收方得到一条。原始 `source-effect-*` 文件与
包含观测值的 validation 来源一起保存在 `results/model-transfer-fixture-2026-11-03-run2/`，
没有覆盖 run1。它验证本地夹具的重复效果和进程内去重差异，**不证明接收方去重跨崩溃持久化、
有限 TTL 之后的安全重试或 procedure 全部自然语言声明为真**；尤其不是部署重启检查。

该次四个真实模型调用均 completed，整个进程 exit 0（约 17 秒）。两环境下两臂仍各 6/6，候选
数量为 3→1、3→0，没有观察到模型得分改善。usage 共 **1,406 / 106 / 1,512 input/output/total tokens**。
模型任务与评分仍采用原 `cross-project-applicability/v1`；变化的是 source validation 增加了真实
本地夹具观测，不把这次补充伪称为新基准或更大样本。

## 恢复与性能证据的边界

真实调用的 exact request 在发送前由同一 ATE publisher 持久化；请求和模型输出进入真实
Activity/Attempt/accepted Result，摘要进入真实 Summary。每轮丢弃内存、重开并检查同一 authoritative
cut；最终执行 AuditFull。raw response 单独保存，incomplete/网络失败不能改记为任务得分失败或重试成功。
这些试验不是断电认证。预发布协议修正后的最终源树还须重新核验，旧工件不证明新源树已全部通过。

本地并发/规模开销见 [native scale 报告](../../tests/benchmarks/results/native-scale-2026-11-03.md)，
不得把该小规模本机结果扩展为所有平台/规模的性能承诺。

## 复现

显式指定新的空 artifact 目录，避免覆盖或误接旧试验；默认 `go test ./...` 不触发付费调用。

```sh
ATE_RUN_MODEL_EVAL=1 \
ATE_EVAL_BASE_URL=https://2xa.cc.cd/v1 \
ATE_EVAL_MODEL=gpt-6.1-sol \
ATE_EVAL_ARTIFACTS=/absolute/new/experiment-directory \
GOTOOLCHAIN=local go test -v ./tests/evaluation -run TestRealModel -count=1 -timeout=20m
```

typed Summary v2 已完成上述三组预定试验；最终代码复验和完整四文档验收仍须单独完成。
上述真实数据不能关闭其他未实现的 V1 条款。

## 2026-11-04 clean V1 复验

用同一 `gpt-6.1-sol` Responses 端点重跑 `TestRealModelTypedSummaryReviewAndEvidenceRecallV2`
和 `TestRealModelCrossProjectMemoryApplicability`。Go 进程退出码为 0，测试包耗时约 130 秒。
十二次 Responses 调用均返回 `completed`，合计输入 15,185、输出 1,579、总计 16,764 tokens。

| 实验 | 实际结果 | 计分范围 |
| --- | --- | --- |
| 类型化摘要 v2 | 三组分别在第 1、2、1 轮被事实复核拒绝；第二组第 1 轮通过并验证了重开后的 typed adoption | 三组均未进入最终两臂比较，0/3 完成，无事实召回得分 |
| 跨项目适用性 | 两环境下两臂各 6/6；候选分别由 3 条降至 1 条、0 条 | 本次小样例未观察到模型得分提升 |

两次首轮拒绝涉及候选摘要把实验提示里的“未复核笔记”写成了来源事实。另一组的拒绝涉及
对前轮数据的归因，以及复核器是否接受从逐条记录计算出的数值范围。以上均是复核器给出的理由，
复核结果本身需要结合原始记录解释。试验保留原 v2 提示、复核规则和预定组数，因此可以与上一次的
2/3 完成率并列比较。下次若调整摘要提示或事实复核口径，应作为新试验版本单独记录。

[原始工件](results/model-clean-v1-2026-11-04-run1/)包含运行日志、请求、响应、传输统计、持久对象投影、
本地接收方观测及每组评分状态。可重新生成的完整测试日志库留在临时试验目录，仓库保留上述可审计工件。

## 2026-11-05 全模型派发授权复验

在所有模型派发都要求同组 `Claim + DispatchAttempt` 后，按相同用例和预定三组摘要输入复验。
Go 测试进程退出码为 0，包耗时 272.483 秒。24 次 Responses 均 `completed`，合计输入 128,521、
输出 7,247、总计 135,768 tokens。对照工件逐条核验了 24 份完整请求的 SHA-256、字节数、
ContextPacket、Activity、目的地及 durable dispatch receipt。

| 实验 | 基线 | 显式 ATE 证据 | 观察范围 |
| --- | --- | --- | --- |
| 摘要 v2 · 第 1 组 | 7/8 | 8/8 | 通过三轮事实复核，进入最终评分 |
| 摘要 v2 · 第 2 组 | 未评分 | 未评分 | 第 2 轮事实复核拒绝，未采用被拒摘要，未调用最终两臂 |
| 摘要 v2 · 第 3 组 | 5/8 | 6/8 | 通过三轮事实复核，进入最终评分 |
| 跨项目 · 两个目标环境 | 均为 6/6 | 均为 6/6 | 候选分别由 3 缩到 1、由 3 缩到 0，未观察到最终得分差异 |

在进入最终评分的两组，显式证据各多答对一项。三组完成率为 2/3，应与这两个配对分数一同报告。
11-04 的三组拒绝结果继续保留，不能用本次分数覆盖。该样本可验证具体输入的回忆与出站路径，
无法推导普遍增益。 [请求、回执、输出与评分](results/model-dispatch-2026-11-05-run1/)
