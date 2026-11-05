# M1f：reader views、pins、本地服务与 Go SDK

当前版本 `0.1.0-dev.m1f`，Go 1.27.0。M1e 的分段日志、逻辑地址、恢复和故障边界继续有效。本批在同一可信核心上增加了有界历史 View 生命周期、最小本地 Unix socket 服务和连接亲和的 Go SDK；完整 Agent profile、图合并和记忆仍未完成。

## 已完成

| 能力 | 当前保证 |
| --- | --- |
| View 捕获 | latest 只在一次核心锁内捕获 Fence/RootSet；显式 Anchor 只能读取已验证历史位置，不可换成 latest |
| View 引用 | 随机不透明 ViewRef、ViewDigest 与临时句柄分离；ViewRef 不是持久历史 Anchor |
| pin 生命周期 | MaxViews/TTL 有界；OpenView 使用 provisional 计数，锁外加载后重新检查；Close/过期先阻止新 acquire，再等待 active query 为零并释放一次 |
| 关闭 | Engine/Store CloseContext 先停止新 View，等待 provisional 与 active query 后再关闭 journal/owner；等待 ctx 取消不会释放资源 |
| 历史读 | GetViewState 只读 Anchor 对应 RootSet，支持 state/audit_snapshot；Cursor/KnowledgeKey 预留并明确拒绝，避免未实现的历史语义伪成功 |
| 输出授权 | Fenced 禁止新 View/读 grant；Draining 中已取得的 query 可完成到资源关闭边界；对象读取仍受当前 Journal 状态检查 |
| IPC | 单连接顺序请求/响应；4-byte big-endian bounded frame、版本化严格 JSON、重复键/未知字段/深度/UTF-8/尾随数据拒绝 |
| 身份 | hello token 映射稳定 Principal 与允许 Space；wire body 不能自报 Principal；服务不实现第二套 receipt/恢复状态机 |
| SDK | client.Dial/New、Commit/Mutate/Lookup/GetState/Snapshot/OpenView/CloseView/GetViewState；断线后的已写命令不自动重试，返回 OutcomeUnknownError 保留原 RequestKey |
| 单 owner | 服务后端直接持有 Store；库模式和服务模式不能同时打开同一目录；服务关闭连接时回收它登记的 ViewRef |

## View 生命周期

```text
OpenView:
  current lock → capture Anchor/RootSet → provisional pin → unlock/load → recheck → register
Close/TTL:
  closing=true → reject new acquire → wait activeOps=0 → release pin/delete entry
Store.CloseContext:
  stop admissions + new views → wait accepted commands/provisional/activeOps → close journal → release directory
```

View 的 Anchor 只定义历史内容选择，不授予权限。主体、Space、模式和当前 Journal 状态仍由核心/服务检查。查询本身不写领域状态，也不推进 KnowledgeKey。ViewDigest 不包含随机 Ref、pin 地址或 TTL。

M1f 只暴露 `GetViewState` 作为历史读的最小纵切面。Node list/detail/context、图关系、Projection、分页 Continuation 与 Agent-facing explore 仍待 M2/M4 的 schema registry 和权限投影；不能用当前 state key 读接口冒充完整图查询。

## 本地协议

服务使用 Unix domain socket，父目录 0700、socket 0600；服务只删除自己创建且关闭时仍为 socket 的路径。客户端先 hello，连接绑定稳定 Principal。请求无 correlation 复用，单连接禁止 pipelining；广播、订阅、push、多路复用、远程服务仍未实现。

协议 JSON 只是传输层。RequestDigest/ObjectID/CommitDigest 继续使用核心 canonical encoding，不对 JSON 重新编码后计算身份。`uint64` 由严格 decoder 直接解析，不能经过 float64；bytes 使用 JSON base64。客户端 context 取消在命令写入后关闭连接并返回 OutcomeUnknownError，不会用新 RequestID 自动重发。

服务返回稳定 ErrorClass 和尽可能完整的 typed body；`commit_unknown`、`wait_interrupted`、`store_fenced` 等结果不能被一条字符串错误覆盖。断线后用同一 RequestKey Lookup/重连对账；ViewRef 不跨连接续用。

## 资源上限

| 资源 | 默认 | 硬上限 |
| --- | --- | --- |
| MaxViews | 128 | 4096 |
| View TTL | 5m | 1h |
| Service connections | 16 | 128 |
| Protocol frame | 16 MiB | 16 MiB |
| JSON nesting | 32 | 32 |

View registry 的锁不跨对象 IO、模型、网络或大内容 hash；读对象使用现有 immutable catalog，发布写入用受保护的 location 更新。当前为读者保留所有段句柄的简单 no-GC 方案，独立 reader pins、checkpoint/compaction 和后台回收属于后续维护阶段。

## 限制与下一阶段

Unix socket 的 OS 身份目前由 token 配置映射，未实现跨平台 credentials/ACL；service 不提供远程 TCP。Cursor/Knowledge 历史选择、Node API、Revision/因果图、Fork/Diff/Join、agent/v1 authority、任务恢复、ContextPacket 和记忆仍未实现。

下一阶段进入 M2：持久 Revision/因果图、状态快照遍历、精确 Diff/Fork/Join，以及对应的参考模型和恢复验收。详见 [M1f 验收记录](validation/M1F_2026-10-31.md)。
