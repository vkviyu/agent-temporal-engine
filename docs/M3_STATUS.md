# M3：agent/v1 状态机纵切面

M3 的公开 `agentprofile` 纯状态机和 codec 不依赖 GSEA、Eino 或 engine。core 已静态接入 `agent.apply/v1`：Agent LiveControl 和统一 receipt 在同一 Journal/P1 发布；`agentstore` 是该路径的 convenience facade。同包 `DurableLedger` 保留为独立 Journal 适配器，不能同时作为该 core 的另一权威。

## 已完成

- TaskDefinition/TaskControl：GoalEpoch 单调递增，Task 取消递增 CancelEpoch。
- Execution：固定 Task/Actor/Parent/Incarnation，Spawn 校验当前目标代际和取消屏障。
- Step：序号跨恢复延续；第 N 步要求第 N-1 步完成；Step/Execution 目标代际必须一致。
- Invocation：按 `(Execution, Step, ProviderCallKey)` 逻辑唯一槽，重试不能创建第二个逻辑调用；Attempt 保留发生次数。
- PendingAction：保存原 RequestKey、命令摘要、输入 Fence、generation 和 owner；unknown 禁止无证据重跑，只有显式 Rebind 才能生成下一代。
- 取消/目标变更：取消传播到执行并 Fenced 相关 pending action；GoalRebase 提升 Task GoalEpoch，旧 Step/Execution 约束变为 stale。
- canonical codec：Task/Execution 的版本化确定性编码与非规范重编码拒绝。
- `agentprofile.Command` 九分支封闭 union：输入所有权、Task Space/Owner、epoch/取消和唯一槽经校验；私有候选完整验证后才安装。
- core LiveControl snapshot registry、统一 finishPlan/P1/replay、原始 receipt-first 幂等和历史 GetAgentState；service/client 同路径，CLI 显式开启 profile。
- `agentstore` 原 typed result 从 ACK.Fence 读取，避免后来 Goal/取消/Step 状态覆盖原结果；Agent profile 持久化只使用 record root/radix，开发数据如需切换格式直接重建 store。

## 验证

纯转换测试覆盖唯一槽、未结 Invocation 禁止完成 Step、GoalEpoch/取消屏障、PendingAction Rebind、canonical codec 和并发候选。core 集成/故障测试覆盖混合 domain/profile 同组 P1、CURRENT 前后 unknown、等待取消、候选拒绝延迟、资源拒绝、已存快照损坏、历史 Fence 和重启幂等；service/client 与 agentstore 复用同一权威。依赖 guard 明确登记纯转换与独立适配器的实际依赖。

## 未完成

当前 Agent core 已增加 Invocation settlement，以及 Query/Activity/Attempt/Result/Message/Continuation 生命周期；Knowledge 仍由独立 deterministic 包负责。Agent 准备统一使用每 Space/Principal 的 record root 与持久 radix，复用未变化记录。Record 状态受每条 4 MiB、所有集合合计 16,384 条、canonical 字节总和 256 MiB，以及分配预检、JSON、radix 节点/字节、事务和总对象数限制，详见 [record registry](formats/agent-record-state-v1.md)。

完整状态加载、克隆、编码、校验和 radix key 扫描仍随状态增长，后续还需增量内存准备。显式恢复 checkpoint 减少已认证前缀的 reducer 重放，仍扫描全部物理历史，不能作为容量无限或长程任务性能已完成的证明。测量范围见 [存储与 checkpoint 实测](validation/AGENT_RECORD_STORAGE_2026-11-14.md)。完整阶段边界见 [M6](M6_STATUS.md) 和 [已实现 registry](formats/agent-core-experimental-1.md)。
