# ATE 工程重构基线 — 2026-11-02

更正：此前的“严格 V1 已完成”结论过早。当前已具备联合宿主等机制测试和第一阶段职责拆分，
但完整需求审计仍发现缺口；本文件只记录已实施的局部重构，整体重构并未完成。先补 V1 和验收，
再继续后续工程重构。

## 不可改变的边界

- 不改变公开包路径、公共 API、RequestKey/receipt、CommitFence、RootSet 和已发布 canonical bytes。
- 不改变 Journal CURRENT、P1、replay、compaction、reader pin 的发布语义。
- 不把恢复中的 V1 历史命令重新解释为今天的策略；clean cut 不保留旧 Profile decoder。
- 每个重构阶段都必须保留完整测试、race、vet、build 和 gofmt 门槛；阶段之间保持可回滚。

## 复杂度集中点

当前 Go 源码约 57k 行。主要实现复杂度集中在：

- `agentprofile/control.go`：控制命令 reducer、authority、lifecycle、恢复和终态屏障混在一个文件。
- `agentprofile/command.go` 与 `control_codec.go`：顶层 union、scope、canonical 编码和状态前置条件交叉。
- `engine/node_queries.go`：View 投影、Knowledge 投影、逻辑节点和普通对象读取混在一起。
- `engine/graph_queries.go`、`engine/knowledge.go`、`engine/views.go`：历史选择、权限和投影规则交叉。
- `memory/memory.go`、`knowledge/knowledge.go`：纯语义 reducer、生命周期索引和 canonical 细节集中。
- `service/service.go`：协议 dispatch、身份绑定、生命周期和各领域 DTO 路由集中。

## 分阶段顺序

1. 先拆分纯函数和 reducer 文件，保持同包、同符号、同测试；不改变数据结构或 wire bytes。
2. 再提取 engine 的认证/历史选择/投影端口，减少 `engine` 对领域实现细节的直接耦合。
3. 再拆分 service dispatch 与协议编解码，保持 socket frame 和 DTO 完全兼容。
4. 最后评估增量 snapshot、索引和性能优化；任何存储格式变化必须另立 schema 和迁移设计。

第一阶段的验收重点是：文件职责清晰、纯 reducer 可以独立测试、错误分类不再依赖调用方猜测，且所有
V1 语义测试结果与重构前一致。

## 第一阶段进度

已完成同包纯移动：

- `agentprofile`：控制校验、恢复查找、终态屏障和 Join 分到独立文件；顶层 reducer/API 不变。
- `knowledge`：Source、Observation、ContextPacket/canonical codec 分到独立文件；schema 1–4 字节保持不变。
- `service`：server lifecycle 与 session/dispatch 分到独立文件；socket frame、身份绑定和 DTO 路由不变。
- `engine`：Node catalog/Knowledge View 投影与分页查询 API 分到独立文件；typed memory/path 仍使用逻辑 Entity，
  普通 Object catalog 和 ReadContent 边界不变。
- `memory`：公共领域类型和 Store API 从生命周期/检索实现中分离；锁、状态转换和 canonical 逻辑保持原位。

随后已做一个受控的 engine 小步：`domain_methods.go` 集中 root slot ownership 和受保护 method 白名单；
prepare/replay/rejection 仍保留显式分支，没有引入会隐藏业务错误分类的 registry。

这些移动已通过全量测试、race、vet、build 和 gofmt。第二阶段再评估 engine method metadata 与投影端口抽取，
不在第一阶段引入 domain registry 或存储格式变更。
