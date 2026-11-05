# ATE Strict V1 协议边界与验收状态 — 2026-11-03

> 11-05 的当前源树与验证状态见 [交叉验收续记](V1_CURRENT_CROSS_CHECK_2026-11-05.md)。本页以下记录对应其历史版本。

此前本文件的“within this frozen scope, V1 is complete”结论撤回。它把原 V1 的实体事件归因、
独立差分和 M6 实测要求重新归为 future/external，再据此宣称完成，没有满足既定验收标准。
四份主文档的原始要求继续有效；本文件不能作为削减范围或完成验收的授权。

旧协议兼容入口与 V1 内部历史读取是两件事。删除旧 Profile decoder 不影响选择当前 V1 日志中的
已发布 Fence/Root、receipt、事件、Cursor 和 Branch。V1 预发布字段形状的修正仍需同步 codec、
golden、registry 与协议规格，不增加旧 reader。

## 当前 V1 候选协议边界

- Agent command envelope: outer `agent/v1` schema 1; Control schema 4 only.
- Invocation record: schema 2 only; model decisions and typed tool dispatch carry
  accepted result, tool definition digest, tool version and dispatch class.
- Lifecycle continuation: opcode 8 only; the position is required. A position
  may explicitly represent a no-knowledge cut with zero KnowledgeFence,
  KnowledgeCursor and KnowledgeKey; it still binds space, principal, step,
  adapter and exact pending action generations.
- Emission command: schema 3 only. Grants bind the current policy cut and exact
  activity/attempt/request/range dispatch when used by `emission_dispatch`.
- Validation completion: the typed requirement/envelope is required for current
  task completion and replay.
- Profile Events: registry version 1. Core events use schema 1 when no profile
  facts are enabled and schema 2 when the same transaction publishes typed
  profile facts. Agent/Work/Query facts are typed; non-Agent domain methods have
  authenticated `Core.Method` facts, major domain entity subjects and operation search.
- Work, local protocol, core state, Transfer Manifest/Archive and domain codecs
  use their current V1 schemas. Domain historical versions that are part of the
  V1 knowledge/memory evidence model remain explicit read-only evidence formats;
  they are not old Agent Profile admission paths.

The public boundary rejects Control schemas 1–3, Invocation schema 1,
Continuation opcode 7, Emission schemas 1–2, partial Invocation contracts and
untyped model-decision Work. The rejection is before a state mutation or fence.

## What “time travel” means in this clean cut

A V1 anchor selects a published fence/root and the current V1 decoder replays
its authenticated command, StateOps, receipt, event and snapshot bytes. Views,
Cursors, Branches, Knowledge cuts, QueryProgress and Transfer foreign evidence
are immutable historical positions. They do not mean that a pre-V1 profile
protocol is accepted as a new request, and they do not require an obsolete
profile decoder.

## 原规范已有的边界

- External network delivery is at-least-once. The engine durably records
  Activity/Attempt/Result/Emission dispatch and reconciliation evidence; the
  host executor owns the remote send and cannot be made exactly-once by a local
  receipt.
- Transfer supports canonical closure, explicit whole-record omissions, mapping
  and read-only foreign evidence; authority is never imported. 原文未要求字段级匿名化或
  同名 Space 跨 Store；不能凭这两项额外要求判定缺口，也不能据只读归档证明跨项目收益。
- Durable subscriptions, broadcast routing, consumer offsets and live feeds are
  outside V1; the event and transaction identity design leaves room for them.

## 必须关闭的验收项

| 原始要求 | 当前审计结论 | 关闭条件 |
| --- | --- | --- |
| D §11.12、W C04/C06 的 Invocation 来源与完成门禁 | pending 来源和逐项完成门禁已修复；仍有 public ApplyAgent 绕过 Work 的问题 | 既有正反例通过不等于原子事务强制；还须关闭独立审计 C-02 |
| D §11.10、W C02/C05 初始知识与 Continuation | Work 内 seed、精确 producer/Revision 绑定与重开恢复已实现 | 必须同时拒绝 public StartWork/SpawnChild 绕过；见 C-02 |
| D §4.3/I-34 实体事件归因 | 已补主要 domain entity Subject、闭合事件词汇和导航 | 本轮普通/race 已覆盖此前源树；后续修改后须重新跑对应事件/重放回归 |
| D §21.1、A §16.1 独立参考差分 | 已补 Invocation/Continuation、事件、receipt、selector/渲染、Query tools、图裁决独立预期 | 继续随语义修复添加反例，不宣称每个 opcode 的穷尽形式证明 |
| D §15.5、D/A M6 效果与开销报告 | 已有真实 Responses v1、typed Summary v2、跨项目与本机规模工件 | 见 `V1_REAL_MODEL_EFFECTS_2026-11-03.md`；v2 预定3组中1组被事实复核拒绝，完成组两臂只在来源标签得分上有差异，不能夸大收益 |
| D §16.1–16.2 Hide/Annotate | 本轮正在集成带来源/理由、追加撤回的 Annotation，默认导航隐藏 | 必须验证固定View、IncludeHidden、权限、重试、重开、压缩及知识cut不受未采用标签影响 |
| D §17.6/I-27 Emission schema 3 | 未关闭：若干字段和辅助函数没有接入实际转换 | 独立审计 C-01：Query/typed内容、exact dispatch、expiry、当前policy和原子claim+dispatch必须真实强制 |
| D §11.3/11.5 Effect 能力 | 未关闭：idempotent 仍被永久当作 safe retry | 独立审计 C-03：正交contract、确切key/request/attempt绑定、有效保证和受信时序证据 |
| D §14.2 必要上下文义务 | 部分：selector 验证宿主提案，不能独立证明提案完整 | 独立审计 C-06：结构化必需项与版本化宿主提案责任须明确并验证 |
| 文档/代码交叉验收与最终工程重构 | 未完成 | 按原始条款补齐证据、重新审计，再做完整重构与阶段验证 |

全量测试通过只能证明已运行的断言，不能代替上述需求验收。历史运行日志不代表修复后的源树已复验。
当前结论：**严格 V1 尚未全部验收，最终工程重构也未完成。**

本轮重新沿 public API→reducer→publication→output 路径核查的裁决见
[独立当前审计](V1_INDEPENDENT_CURRENT_AUDIT_2026-11-03.md)。它撤回旧报告中过强的 Work/Emission
完成声明，同时保留当前真正已实现的 seed、实体事件、独立参考模型和真实模型实测证据。

## 2026-11-05 当前裁决更新

本文件前文保留 11-03 的未完成审计快照。当前实现已按 clean-cut 收口 Work 原子边界、有限重试、ToolDefinition、Emission dispatch、ReadDeliveryGrant、Packet 派生来源检查和 fail-closed Unix 路由。最终证据、测试退出码、真实模型结果和外部平台边界见 [V1 当前交叉验收续记](V1_CURRENT_CROSS_CHECK_2026-11-05.md)。
