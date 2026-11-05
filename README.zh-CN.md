# Agent Temporal Engine（ATE）

[English](README.md) | [简体中文](README.zh-CN.md)

ATE 是一个面向 Agent 的时间引擎。

它为外部 Agent 系统提供一种持久化方式，用来记录：发生了什么、发生在什么时候、某个 Agent 在当时知道什么、哪些决策依赖这些信息，以及发生故障后可以从哪里继续执行。

ATE 刻意不承担 Agent Loop。它不会替 Agent 思考，不会自己调用模型，不会执行工具，也不会管理工作区。这些职责由宿主 Agent 系统负责；ATE 负责记录、校验和回溯由这些行为形成的时间轨迹。

## 为什么需要时间引擎？

对话历史和向量记忆只能解决 Agent 记忆问题的一部分。一个长程、多 Agent 任务还需要回答：

- 这个 Execution 在做出决策之前究竟知道什么？
- 某个结果来自哪个子 Agent？
- 一个工具调用是计划过、派发过、重试过，还是结果未知？
- 父 Agent 是否真的消费了子 Agent 的结果？
- 能否查看任务在昨天某个时间点的状态？
- 能否从历史状态继续出一条新的分支，同时保留原始历史？
- 一条记忆被撤回后，能否禁止未来继续使用，同时保留审计轨迹？

ATE 将这些事实表示为不可变、版本化并且带来源的记录。时间不是事后附加的元数据，而是记录含义的一部分。

## 一个长程多 Agent 任务

外部 Agent 系统可以运行这样的任务：

```text
任务：修复订单服务偶发超时问题
└── Root Execution
    ├── 代码调查 Execution
    ├── 回归测试 Execution
    └── 连接池分析 Execution
```

这些子 Agent 可以以任意顺序完成。它们的模型调用和工具调用可能发生重试。Root Agent 可以只消费某个子 Agent 的总结，同时保持另一个子 Agent 的私有发现隔离。

ATE 记录围绕这项工作的时间轨迹：

```text
Task
  → Execution
  → Step
  → Activity / Attempt
  → Result
  → Observation
  → Knowledge Revision
  → Context Packet
  → 下一次决策
```

宿主系统运行 Agent Loop；ATE 提供持久化的时间轨迹、历史读取、显式知识传递、恢复位置以及可分叉的状态。

## 四条相互连接的轨迹

理解 ATE 最简单的方式，是把它看成四条相互连接的轨迹：

```text
执行轨迹
Task → Execution → Step → Activity → Attempt → Result

知识轨迹
Result / Message → Observation → Knowledge Revision → Context Packet

领域状态轨迹
Revision → Branch → Fork / Diff / Join

恢复轨迹
Pending Action → Authority → Continuation → Resume
```

它们描述的是不同事实：

- 执行轨迹说明 Agent 做了什么；
- 知识轨迹说明 Agent 正式采用了什么，以及后续实际使用了什么；
- 领域状态轨迹说明状态如何被探索、比较和合并；
- 恢复轨迹说明长程执行可以从哪里安全继续。

这些轨迹通过显式引用和事件连接起来。子 Agent 完成，不代表父 Agent 已经消费了它的结果；Graph 合并，不代表 Git 工作区已经合并；一个 Execution 中保存的记忆，也不会自动对另一个 Execution 可见。

## 核心概念

| 概念                   | 含义                                                          |
| ---------------------- | ------------------------------------------------------------- |
| `Task`                 | 一个持久化的目标实例                                          |
| `Execution`            | 某个 Actor 对一个 Task 的一次执行，可选地属于某个父 Execution |
| `Step`                 | Execution 中的一次逻辑决策步骤                                |
| `Activity` / `Attempt` | 一项外部操作及其每次派发尝试                                  |
| `Observation`          | 被明确采用到某个 Execution 知识中的内容                       |
| `Knowledge Revision`   | 某个 Actor/Execution 知识流的一个版本                         |
| `ContextPacket`        | 某次决策实际选择的、带历史来源的上下文                        |
| `SnapshotAnchor`       | 稳定的历史读取位置                                            |
| `Revision` / `Branch`  | 版本化的领域状态及其导航引用                                  |
| `Continuation`         | 持久化的恢复位置                                              |
| `Memory`               | 有来源、经过验证并可复用的事实或经验                          |

核心身份和时间类型位于 [`model/`](model/)，公共 API 契约位于 [`api/`](api/)。

## ATE 当前提供什么

ATE 当前已经提供以下核心机制：

- 不可变、内容寻址的对象；
- 历史快照和基于 Anchor 的读取；
- 按 Actor/Execution 隔离的知识；
- 显式的跨 Execution 知识导入；
- 并行子 Execution 的记录和执行 Join；
- 模型与工具 Activity/Attempt 生命周期记录；
- 有界重试以及未知外部效果的对账；
- 用于故障恢复的持久化 Continuation；
- 领域状态的分支、差异比较和显式合并；
- 带来源的 Memory 和 ExperiencePath 生命周期管理；
- 上下文选择和完整模型输入记录；
- 轨迹、节点、内容和来源查询；
- 嵌入式调用和 Unix 本地服务；
- 持久化 Receipt、语义 Replay 以及保留审计历史的物理压缩。

当前实现状态、命令示例、格式边界和验证证据见 [`docs/ATE_IMPLEMENTATION_STATUS.zh-CN.md`](docs/ATE_IMPLEMENTATION_STATUS.zh-CN.md)。

## ATE 刻意交给宿主系统的部分

外部宿主仍然负责：

- Agent Loop 和模型供应商接入；
- 解释模型输出；
- 选择并执行工具；
- 工作区、Git 和文件系统操作；
- 业务相关的调度策略；
- 外部副作用对账；
- 决定哪些结果应该成为 Observation 或 Memory。

这种边界让 ATE 可以独立于 GSEA、Eino、模型供应商和宿主应用内部包。宿主通过公共 API 和版本化的 `agent/v1` Profile 接入。

## 架构总览

```text
外部 Agent 系统
  ├── 模型调用
  ├── 工具执行
  ├── 调度
  └── 工作区操作
          │
          │ commands、results、observations 和 decisions
          ▼
ATE
  ├── 不可变 Object 和 Event
  ├── Copy-on-Write Radix 状态树
  ├── 五个权威 Root
  ├── 追加式 Journal
  ├── 历史 Anchor 和 View
  ├── Agent 执行 Profile
  ├── Knowledge 和 Memory 投影
  └── 语义 Replay 和恢复
```

五个权威 Root 分别隔离领域状态、执行知识、Branch 引用、在线控制状态和命令 Receipt。单个 Coordinator 会校验 candidate 状态转换，并通过统一的持久化提交路径发布。

## 推荐阅读顺序

如果你刚开始理解这个项目，建议按下面的顺序阅读：

1. [`docs/AGENT_TEMPORAL_ENGINE_DESIGN.md`](docs/AGENT_TEMPORAL_ENGINE_DESIGN.md)：整体设计和语义。
2. [`docs/ATE_EXECUTION_PROFILE_WALKTHROUGH.md`](docs/ATE_EXECUTION_PROFILE_WALKTHROUGH.md)：一个完整的长程多 Agent 场景。
3. [`api/`](api/) 和 [`model/`](model/)：公共契约以及时间相关身份。
4. [`engine/`](engine/)、[`state/`](state/) 和 [`storage/`](storage/)：模型如何被持久化和 Replay。
5. [`agentprofile/`](agentprofile/)、[`knowledge/`](knowledge/)、[`graph/`](graph/) 和 [`memory/`](memory/)：四条轨迹在代码中的实现。
6. [`docs/ATE_IMPLEMENTATION_STATUS.zh-CN.md`](docs/ATE_IMPLEMENTATION_STATUS.zh-CN.md)：实现细节、CLI 命令和当前验证证据。

## 快速开始

项目目标使用 Go 1.27.0：

```sh
go test ./...
go test -race ./...
go vet ./...
```

状态存储初始化、恢复、本地服务和格式细节见 [`docs/ATE_IMPLEMENTATION_STATUS.zh-CN.md`](docs/ATE_IMPLEMENTATION_STATUS.zh-CN.md)。

## 项目状态

ATE 当前是一个实验性的 V1 实现。核心状态、Agent Profile、Graph、Knowledge、Memory、查询、恢复和本地服务路径已经实现，并有包级和集成测试覆盖。Strict V1 acceptance、Linux 原生持久性、设备掉电认证以及广泛意义上的外部 exactly-once 保证，仍然属于明确的证据边界。
