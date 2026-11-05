# M5：Memory 与 ExperiencePath 语义纵切面

M5 已加入独立公开 `memory` 包。纯语义 `Store` 不依赖 GSEA、Eino 或 engine；`memory.Durable` 通过 prepared `journalstore` 提供自己的恢复/幂等投影。它仍明确不是 ATE core 的五根 RootSet 权威。

## 已实现

- `Memory`：claim、kind、source anchor、evidence/反证、适用 Goal/Environment、策略版本、confidence、状态和撤回信息。
- 生命周期：`candidate → validated → promoted → superseded/retracted`；非法跃迁拒绝，状态版本产生新的不可变 ID。
- 来源撤回：FactualCorrection、UseBan、PrivacyErasure、ACLRevocation 记录为权威 source ban；已晋升记录产生 retracted 派生记录，检索不能通过旧缓存复活。
- 检索：按 Space、Anchor cut、Goal/Environment、source ban 过滤；token posting 只是可丢弃加速结构，结果按 score/ID 确定排序。
- `ExperiencePath`：明确 SourceAnchor、选中的有序 NodeRef/Memory steps、Omitted 缺口、验证证据、适用范围和派生来源；成员必须已 promoted 且不超出 source cut。
- Path retract：撤回产生新不可变路径记录，原路径仍可审计；后续复用检查状态和来源。
- canonical identity：Memory/ExperiencePath 身份使用有界 canonical bytes + SHA-256，不使用 Go map 或 JSON 排序。
- `Durable`：Put/Validate/Promote/Transition/Retract/BuildPath 都先在私有候选上验证，P1 成功后才安装；Open 在物理尾部修复前完整重放语义前缀，读操作经过 Ready/fence 门。

## 验证

`memory/memory_test.go`、`memory/audit_regression_test.go` 和 `memory/durable_test.go` 覆盖生命周期状态、来源撤回、防止旧 promoted 记录检索、Anchor cut、ExperiencePath 成员资格、冲突来源和撤回版本，以及 durable create→append→close→open→replay。架构测试登记 `memory → codec/model/journalstore/storage`，无 `internal` 与第三方依赖。全量 race/vet/build 仍须在每次跨包集成后重跑。

## 边界

这是记忆语义与可持久化转换层，不是学习算法，也不保证模型主动召回或经验迁移效果。`Durable` 的日志和投影仍是独立适配器；下一步才是把 Memory/ExperiencePath schema 接入 core 的单一 RootSet 发布边界、保留根和 M5 maintenance/GC。审计历史、已发送 ContextPacket 和外部服务字节不能由本包假装可撤回。
