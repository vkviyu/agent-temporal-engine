# Strict V1 交叉验收续记 · 2026-11-05

此页逐项追踪 [四文档基线矩阵](V1_REQUIREMENTS_MATRIX_2026-11-02.md) 和
[11-03 独立审计](V1_INDEPENDENT_CURRENT_AUDIT_2026-11-03.md) 的复核证据。D 是总体设计，A 是实施架构，P 是
API 协议，W 是执行场景。保留旧报告在其源树上的结论。这里的已验证项目有明确的行为和证据范围。

| 基线 | 当前观察和代码入口 | 已运行证据和剩余边界 |
| --- | --- | --- |
| P §25 · clean cut 与 V1 历史 | Control 5、Invocation 2、Continuation 8、Emission 3 和 Packet 4 由当前 codec 解析。历史 API 继续固定原 Fence/Root/Cursor。语义收紧后，旧预发布写法没有特殊 replay 分支。 | 格式 registry 与正反向量；`engine/work_test.go` 在重开、压缩与 receipt replay 中核对 producer。旧试验的结果文件是证据，旧实验 Store 不是本次协议输入。 |
| C-01 · D §17.6、I-27、P §25、W §17 · 派发 | `checkEmissionActivity` 绑定 owner、Attempt、recipient、完整请求字节及 ContextPacket 身份。`validateWorkBoundary` 只允许同组 Claim 和 Dispatch。公开 Control 派发被拒绝，包括 pure。 | Engine 覆盖 Query 与 Execution、错误 range/recipient/packet、expiry、authority 和撤回；Unix SDK 验证同组回执与无独立派发；内嵌和 Unix 长任务达到 LSN 116。 |
| G-03 / G-07 · D §17.6、A §11.3 · 对外读取 | `PrepareContentRead` 在当前策略下为同一 View、projection、range 和目的地签发单次临时许可。Unix 服务用认证 Principal、连接身份及请求身份消费，再返回字节。节点、图、状态、知识、记忆、Agent、Query、Summary、Emission、Annotation 和 Transfer 响应复用同一 capture/consume 边界。所选 Knowledge View 的 Packet/Observation 来源进入当前 ACL 核对。 | `TestHistoricalKnowledgeViewHonorsPacketSourceWithdrawal`、ReadContent、Node/Graph wire 和 service 范围回归已通过。一个被 Packet 记录为输入的派生对象，在原始 State/固定 View 读取时也会沿 Packet/Observation/Summary 关系重新检查当前撤回。没有 grant-capable backend 时，OpenView 和读取路由均 fail closed。 |
| C-02 · D §11.7/§11.13、I-28/I-38、W C02/C04/C05/C13 · Work | public Agent 提交不能单独启动受管理根、派生子执行、接受主模型结果、添加决议工具调用或消费消息。`message_consume` 将接收方 Continuation 绑定观察后的确切最终 Revision 和 ProducerTx。 | C02/C05/C13 的 CURRENT 替换前后故障验证全有或全无；C13 亦检查错误旧 cut、不正确步骤顺序、重开和压缩。带工具 Step 的后续完成及工具契约仍单独审计。 |
| C-03 · D §11.3/§11.5/§11.9、I-13 · 有限重试 | `safeRetry` 与 `RetryUse` 联合验证当下可用的证明、作用域、全部 Attempt、authority、精确请求和有界时序。已记录 Retry 时刻也约束下一次 Claim。 | Engine 集成覆盖缺失/过期/撤销/authority 切换、旧时刻及重开；独立参考 oracle 比对过期、撤销、替代及成功 retry/claim/dispatch。工具 Definition 现在绑定 canonical 名称、版本、schema、dispatch mode/class 和完整 EffectContract；实际 provider 请求的语义解码仍由受信适配器承担。 |
| C-04/C-05 · D §13.4/§15.3 · typed 输出 | `GetAgentState`、`GetQueryIntake` 校验明确来源；`ReadContent` 与固定 Knowledge View 的查阅遵循同一 Packet/Observation 依赖。 | `engine/agent_state_policy_test.go`、`engine/query_intake_policy_test.go` 与上述历史 Packet 回归。精确出站许可范围见 G-03。 |
| C-06 · D §14、A §15 · 必要上下文 | 主决策由历史 Agent/Knowledge cut 推导结构化 Requirements，必须存在于选中的已采用 Observation。RecordedInput 保存确切实际请求，HostPolicyVersion 确认渲染与候选责任。 | 引擎拒绝缺失或伪造的 manifest，拒绝不变化 fence。模型候选能力与已知结构化事实是两项不同的验收。任意供应商请求的实际渲染正确性仍由适配器承担。 |
| C-07 · M6 效果与开销 | 使用真实 Responses 端点复测精确出站路径，两种本地宿主跑同一长任务。 | 24 次真实请求及配对授权/receipt 核验通过。两组分别 7→8/8、5→6/8，一组在第二轮被事实复核拦截。跨项目两臂均为 6/6。详见 [真实模型证据](V1_REAL_MODEL_EFFECTS_2026-11-03.md)。 |
| G-10 · A §18.2、P §8 · 平台与发布 | Darwin/arm64 原生，Linux/amd64 交叉编译。 | Go 1.27.0 当前出站派发候选全量普通测试及 Linux/amd64 交叉编译退出 0；Linux 原生文件系统运行与设备掉电没有本机证据。持续修正期间须重验最终格式与源码。 |

`go test -count=1 ./...` 对出站派发候选通过。加入临时读许可、Packet ACL 与 C13 CURRENT 故障后，
Engine、service 与独立参考包的相关回归通过。类型化响应覆盖、模型调用与外部工具契约已经在当前源树
完成针对性验证。Linux 原生文件系统、设备掉电、远端 exactly-once 和真实供应商请求语义仍属于证据边界。
最终工程整理已完成职责拆分，最终 race 与静态检查已经通过。

## 最终源树验证补充

派生来源扫描从 `content_policy.go` 拆到 `engine/derived_disclosure.go`，读取许可和服务分发仍保持同一接口。当前源树已运行：

- `GOCACHE=/tmp/ate-gocache GOTOOLCHAIN=local go test -count=1 -timeout=240s ./...`，退出 0。最终 Agent 文件职责拆分后的完整 24 个包通过。
- `GOCACHE=/tmp/ate-gocache GOTOOLCHAIN=local go vet ./...`，退出 0。
- `GOCACHE=/tmp/ate-gocache GOTOOLCHAIN=local go build ./...`，退出 0。
- `GOOS=linux GOARCH=amd64 GOCACHE=/tmp/ate-gocache GOTOOLCHAIN=local go test -run '^$' -exec /usr/bin/true ./...`，退出 0。该命令只证明交叉编译。
- `GOCACHE=/tmp/ate-gocache GOTOOLCHAIN=local go test -race -count=1 -timeout=300s ./...`，退出 0。最终 Agent 文件职责拆分后的完整 24 个包通过，包含 Engine、Unix service、host 和 reference 场景。
- `GOCACHE=/tmp/ate-gocache GOTOOLCHAIN=local go test -race -count=1 -timeout=180s ./service`，退出 0，覆盖无许可后端的 fail-closed 路由回归。
- `GOCACHE=/tmp/ate-gocache GOTOOLCHAIN=local go test ./tests/benchmarks -run '^$' -bench 'Benchmark(NativeStore|Knowledge|ContextSelection)$' -benchtime=1x -count=1 -cpu=1 -benchmem`，退出 0，当前源树的 Darwin/arm64 小样本结果写入本机临时日志。

格式清单包含 ToolDefinition schema 1。真实模型复验的 24 个请求、准确请求摘要、Packet 身份、目的地和 durable dispatch receipt 已归档到 `results/model-dispatch-2026-11-05-run1/`。
