# M6 RootSet 统一提交边界

当前 core 已将 Agent、Graph、Memory、Knowledge 的 canonical 领域命令接入同一协调器、receipt 与五根 RootSet 提交路径。Agent 修改 LiveControl，Graph 修改 BranchRefs，Knowledge/Memory 修改 ExecutionKnowledge，普通宿主 mutation 修改 DomainStates；CommandReceipts 统一更新。各 root facade 只调用 core，不在 admission 前读取最新状态并编译 StateBatch。graphstore、knowledge.Durable、memory.Durable 仍可用于独立 store，不能成为同一 core 的第二个 Journal 权威。

当前实现与原始 V1 目标的逐项证据见 [文档—代码审计](validation/V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-01.md)。本文件仍保留完整目标，同时明确 AuditFull、容量和订阅边界。

## 目标

第一版的统一入口必须只有 core coordinator。一个 typed profile command 在 admission 后进入私有 `CandidateBundle`，至少包含：

```text
DomainStates
ExecutionKnowledge
BranchRefs
LiveControl
CommandReceipts
object catalog additions
```

Profile、Knowledge、BranchRef、Memory 的纯转换只能读取该候选 bundle 和已认证的历史对象，不能在提交锁内调用模型、网络、插件或另一个 Journal。候选通过后，由现有 group/P1 一次发布；成功后才交换内存 bundle。

## 命令与恢复

命令使用封闭 union，canonical body 与外层 RequestKey、输入 Fence、schema/transition 版本共同形成请求身份。协调器先查原 receipt，再执行领域转换；同 key 换 body 冲突，旧请求不因最新状态改变而重新求值。每个 transition 返回预先计算的对象集合和五根新值；RootSet object 不包含自己的最终 CommitDigest，避免 hash 自引用。Graph/Memory/Knowledge/Agent 已提供 schema 级内容引用分类和完整重放校验；AuditFull compaction 保留权威事务，不因此启用选择性历史 GC。

恢复按日志前缀逐事务执行相同的纯转换：

1. 读取并验证 command、receipt、body object 和引用闭包。
2. 将上一个已认证 bundle 复制成私有 candidate。
3. 用记录中的 canonical command 重算 candidate 和五根新值。
4. 比较事务中的 RefUpdates、ControlUpdates、Receipt、PostRootSet 与重算结果。
5. 只有整笔事务通过后，才把 object locations 和 candidate bundle 交换到公开状态。

任何语义失败都拒绝打开并保持未发布尾部的物理证据；不能把当前 latest、另一个适配器的根或模型陈述当作恢复证明。

## 分阶段实施

- 已完成：以现有 `commitPlan`/staging object index 表示私有 bundle，抽取共同 `finishPlan`；普通 core.state.mutate/v1 的字节格式保持不变。
- 已完成：静态 `agentprofile.Command` → core.prepareAgent → LiveControl radix → 统一 receipt/RootSet → 既有 P1；replay 重算同一转换。`agentstore` 已移除旧 domain snapshot 写入和本地 ledger 权威，旧快照需显式迁移。
- 已完成当前子集：Graph、Memory、Knowledge 的 canonical command → core reducer → 受保护 root slot → 统一 receipt/RootSet/P1。回归覆盖原始 StateBatch 注入拒绝、状态变化后原 key 重试、恢复、CURRENT 前后故障与 group publication。
- 已完成查询能力：Knowledge 按 Space/Principal 分区，Key 选择检查 Actor/Execution 所属；领域 Cursor 与知识 Cursor 都从固定 Fence 解析精确 Revision。Node/Context/Search/Trace/Content 按历史 View 成员关系、有界资源和当前主体权限输出。
- 已完成传输能力：service/client 的 Agent/Graph/Memory/Knowledge、Node Explorer、ContentReader、SourcePublicationRef 绑定同一内核，身份由连接映射；formal Event occurrence 与 Recovery control nodes 也可在固定 View 导航。
- 已完成维护与恢复：explicit Import、RecordedInput、完整外部 parent/child unknown-effect recovery、schema 引用提取、AuditFull compaction、reader pin/retire、重启和逐持久化故障点回归均已加入。

`agentstore` 和根包的 domain facade 均调用 core；standalone 适配器不等于 core RootSet authority。Agent profile 仍为有界整分区 snapshot；Knowledge/Memory 在受保护 slot 中保存有界 snapshot，转换成本为 O(n)。这不等于已经解决长程任务容量与回收。确切已实现 registry 见 [agent/core experimental 1](formats/agent-core-experimental-1.md) 和 [本地协议](formats/local-protocol-experimental-1.md)。
