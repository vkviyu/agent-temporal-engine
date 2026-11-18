# ATE local protocol experimental 1

M1f 本地服务/SDK 的传输格式。它不改变 canonical object、RequestDigest、journal frame 或 CURRENT 字节格式。

## Frame

每个 frame 是 `uint32 BE length || UTF-8 JSON bytes`。长度必须为 1..16 MiB，读取和分配前检查；短读、EOF、零长度、超限均失败。单连接一次只有一个请求，响应 correlation_id 必须严格匹配。

## Envelope

Request:

```json
{ "version": 1, "correlation_id": "opaque", "method": "mutate", "body": {} }
```

Response:

```json
{
  "version": 1,
  "correlation_id": "opaque",
  "body": {},
  "error": { "code": "...", "message": "..." }
}
```

字段未知、对象键重复、UTF-8 无效、嵌套超过 32、尾随 JSON、未知错误 code 必须拒绝。`body` 是方法的 typed DTO；身份和业务 bytes 使用 JSON base64。uint64 按十进制整数解析，不经过浮点。

## 身份与方法

首个方法是 hello，body 只有 token。服务端将 token 映射稳定 Principal 和允许 Space，并把结果 capabilities 返回；其他 body 不允许自报 Principal。Core 方法为 commit、mutate、lookup_request、get_state、snapshot、open_view、close_view、get_view_state、get_view_entries；全局 snapshot 还要求 operator grant。

显式启用 Agent Profile 时增加 capability `agent/v1` 及 `apply_agent`、`get_agent_state`。同时实现 `AgentRecordReader` 和 `ReadDeliveryPreparer` 时增加 `agent/record-read-v1` 及 `get_agent_state_records`；该请求不传 Principal，服务端使用认证身份和 allowed Space。记录读取支持固定 Anchor 下的 256 条/8 MiB 页，Continuation 是一致性 cursor，不是签名授权。每页完整认证 record catalog 和 Snapshot 披露依赖；客户端可用 `ReadFullAgentState` 聚合并最终校验完整 RecordImage。启用相应后端时还增加 `graph/v1`（`apply_graph`、`get_graph_revision`、`get_graph_branch`）、`memory/v1`（`apply_memory`、`get_memory`）和 `knowledge/v1`（`apply_knowledge`、`get_knowledge`）。四类 apply 的 Command 都是对应 package `EncodeCommand` 产生的 canonical 二进制 bytes，再以 JSON base64 传输；Graph command 额外绑定 Space，服务端核对其与外层 Space 相同。传输 JSON 不作为领域 command 身份或替换 snapshot。此前三个 domain method 接受 StateBatch 的实验表示不再被接受，兼容边界见 [core registry](agent-core-experimental-1.md)。

`get_knowledge` 可带 Anchor 和 KnowledgeKey；服务端绑定 Principal，并且只读取该主体的分区。Key 由 Anchor 和请求重复给出时必须一致，按历史和当前控制面认证 Actor/Execution 所属，不回退 latest。省略 Key 也只返回该主体的分区。`execution_knowledge` View 固定同一个选择，只展开已采用的载荷、Observation 和 ContextPacket。领域 Cursor 从选定 Fence 的历史 BranchRefs 中解析 Revision；Knowledge Cursor 从同一 Key 的 Observation 链解析知识 Revision，二者命名空间独立。

提供 BasicNodeReader 的后端宣告 `core/node-query-v1`，对应 `list_nodes`、`get_node`、`get_node_context`；NodeExplorer 与支持临时读取许可的 ContentEmissionReader 另外宣告 `core/node-explore-v1` 与 `core/content-range-v1`，对应固定 View 的 literal Search、Trace 和 bounded ReadContent。请求沿用该 session 拥有的 ViewRef；跨连接借用 View、越过历史成员关系的 Object/Revision 引用不会得到内容。默认 entities 层提供 Agent 实体；content 层提供认证内容与 Revision，Knowledge View 只提供所选执行的观察内容。typed `memory`/`experience_path` 观察在 Knowledge View 中以逻辑 `memory` Entity 节点返回 JSON 投影，不是 NodeObject，不能通过 ReadContent 冒充 raw codec object。分页绑定 View 与过滤/排序条件，节点、边、深度、范围和字节均有界；Event occurrence 只在启用 event profile 后可导航。

所有对外读取响应由服务端消费单次临时许可。许可绑定认证连接与当前请求、method、View/Anchor 和来源闭包；它不出现在帧里。`read_content` 另外绑定 projection、确切 range 和内容摘要，新分块重新核验当前授权。OpenView 只建立连接拥有的 View 句柄，后续内容仍走该边界。

服务只传输 API DTO，不拥有 receipt、恢复或权限状态机。客户端 context 在副作用请求已写入后取消，连接被关闭，返回未知结果并保留原 RequestKey，不自动重试。

## 生命周期

ViewRef 绑定连接和服务 session，断线时回收；不能跨连接发送以继续分页或读 View。服务关闭先停止新连接，再关闭已登记 View，最后释放监听 socket。广播、订阅、push 和多路复用不在本格式范围。
