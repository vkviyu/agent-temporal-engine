# DeepSeek Harness 与 ATE 集成设计

## 目标

使用官方 DeepSeek Harness 执行真实长程 Agent 任务，同时把已经提交的 SessionEvent 异步复制到 ATE。DSH 继续负责 Agent Loop、模型请求、工具执行、子 Agent 和会话恢复。ATE 负责持久轨迹、因果关系、时间旅行、来源证明和查询。ATE 不替换 DSH Session 日志，也不修改 DSH Loop 语义。

DSH Web 中增加一个 Session-scoped ATE 面板。左侧显示 Session、Run 和子 Agent 树，中间显示时间线和因果 DAG，右侧显示节点详情、父子关系、原始事件、请求、结果和历史 Anchor。节点可以展开上下游关系，也可以发起针对节点的新 Query。

## 三种身份

系统保留三种身份。

1. DSH `SessionEvent` 是执行事实，拥有 SessionID、seq、type、data 和提交时间。
2. ATE TraceNode 是可查询的时间旅行节点，拥有稳定 NodeID、ParentID、CausedBy、状态和内容对象引用。
3. UI 节点是当前 Web 视图的投影，可以合并多个流式事件。

时间顺序使用 DSH seq，树关系使用 ParentID，真实因果使用 CausedBy。一个工具结果的 CausedBy 指向对应 ToolCall。子 Agent 节点通过 Spawn 事件指向 child Session 根节点。

## 适配器

适配器作为 DSH Cordis 插件或独立 Python SDK 回调运行。TypeScript 路径订阅 `ctx.on('session/event', (session, event) => ...)`。Python 路径使用 `Session.run` 的通知订阅。适配器只复制已经提交的 SessionEvent。

事件进入有界队列，再写入本地 spool，最后批量提交 ATE。DSH 模型请求和工具执行不等待 ATE。每个 DSH 事件使用稳定幂等键 `dsh/<SessionID>/<seq>`。重试使用相同 RequestID，未知提交通过 spool 对账。

### 标准化节点

| DSH 事实 | ATE 节点 |
| --- | --- |
| Session 创建 | `session` |
| 用户请求 | `query` |
| `turn/start` | `turn` |
| 模型请求 | `model_request` |
| `assistant/message` | `model_response` |
| `assistant/chunk` | 同一响应的 OutputChunk 清单 |
| `tool/call` | `tool_call` |
| `tool/result` | `tool_result` |
| `subagent.started` | `subagent_spawn` |
| `subagent.finished` | `subagent_terminal` |
| `agent/inbox/spliced` | `message` |
| `turn/end` 和 repair | `checkpoint` |
| 异常和中断 | `error` |

默认不为每个文本增量创建独立图节点。增量保存在同一 `model_response` 的有序分块清单中。原始事件、模型请求、响应、工具参数和结果先作为 ATE ValueBytes 保存，TraceNode 只引用 ObjectID 和 Encoding。

每个节点至少保存以下字段。

```text
TraceID, NodeID, SessionID, AgentID, Kind, Status
ParentID, CausedBy[], SourceEventSeq
Turn, Step, StartedAt, EndedAt
RequestDigest, PayloadObject, Metadata
```

## ATE 写入协议

第一阶段使用独立 `trace/v1` Profile。适配器不能直接写 ATE StateOps、RootSet 或通用 Event 根，也不能把任意 DSH 事件伪装成 Agent Activity。

一个 TraceAppend 命令在同一事务中提交内容对象引用、TraceNode、ParentID/CausedBy 关系、状态更新、SourceEventSeq 和来源锚点。节点、边和状态不能部分提交。DSH event ID 对应唯一 RequestID。

## 实时更新

ATE V1 是本地请求/响应协议，没有广播或 Server Push。第一阶段使用 DSH 已有的 SessionEvent 通知和 Web `remote.mux` 的 `session/follow` 流作为实时源，ATE durable fence 和 ATE NodeID 作为持久确认状态。

DSH Host 提供 ATE 查询代理。

```text
GET  /api/ate/runs/:sessionId
GET  /api/ate/runs/:sessionId/timeline
GET  /api/ate/nodes/:nodeId
GET  /api/ate/nodes/:nodeId/context
GET  /api/ate/nodes/:nodeId/trace
POST /api/ate/nodes/:nodeId/ask
GET  /api/ate/runs/:sessionId/events
```

`events` 可以由适配器轮询最新 durable fence，并通过 Host SSE 推送。SSE 只负责状态更新，断线后按最后确认的 seq 和 Anchor 从 ATE 补齐。后续如果轮询不足，再增加 ATE Subscribe。双向控制再考虑 WebSocket。

通知类型至少包括 `node_appended`、`node_status_changed`、`edge_appended`、`ate_fence_advanced`、`run_recovered` 和 `ingest_degraded`。UI 明确区分“DSH 已收到事件”和“ATE 已 durable 写入”。

## 查询和节点问答

ATE 的 `ListNodes`、`GetNode`、`GetNodeContext`、`Trace`、`SearchNodes` 和 `ReadContent` 支撑时间线、图关系、详情和原始内容查看。首次打开面板时建立固定 View/Anchor，点击节点后按该 Anchor 查询，避免把实时状态和历史状态混合。

节点问答由 DSH 发起新的 Query。请求引用 ATE NodeID、SnapshotAnchor、Trace 摘要和用户问题，实际请求继续写入 DSH SessionEvent。回答完成后，适配器把回答、引用的 NodeID、Anchor 和生成 Activity 写回 ATE。历史节点不会被修改。

## 子 Agent

DSH 父 Session 默认只保存子 Agent 的外部任务身份和结果。要展示完整子 Agent 轨迹，适配器必须同时订阅 child Session 的 SessionEvent，并在 ATE 中保存 child Session 根和跨 Session 的 Spawn/Terminal 边。只复制父日志会丢失子 Agent 内部模型、工具和恢复节点。

## 真实模型评测

用同一 DSH 任务运行两组实验。一组不启用 ATE 适配器，另一组启用 ATE 适配器和面板。两组固定模型、工具、workspace、输入和恢复扰动。

评测记录最终成功率、验收结果、模型请求和输出字节、ATE 节点覆盖率、边覆盖率、payload 完整率、子 Agent 覆盖率、适配器延迟、重启后的节点连续性，以及节点问答引用的 NodeID/Anchor 可复核性。

实验产物必须同时保留 DSH Session 日志、ATE receipt/fence、DSH event 到 ATE NodeID 的映射表和最终评分。模型效果分数来自 DSH 任务验收，ATE 提供轨迹、来源和恢复证据。

## 实施顺序

1. 固化 `trace/v1` Profile 和 DSH event 到 TraceNode 的映射测试。
2. 实现 DSH Cordis adapter，加入有界队列、本地 spool、批量提交和重试。
3. 在 DSH Host 暴露 ATE 查询代理和 SSE 更新流。
4. 在 Web 的 `sidebarRightTabs` 或 `conversation.chat.node` 扩展槽增加 ATE 侧栏。
5. 实现节点详情、上下游追踪、历史 Anchor 查看和 `ask_node`。
6. 用 DSH 真实长任务完成启用/禁用 ATE 的配对评测。
7. 只有在轮询和 Host SSE 无法满足延迟要求时，才增加 ATE 原生 Subscribe。

成功条件是 DSH 原始执行语义保持不变，ATE 能完整记录和查询多 Agent 长程轨迹，UI 能区分已收到事件和已 durable 事件，节点问答能引用并复核选中的历史节点。
