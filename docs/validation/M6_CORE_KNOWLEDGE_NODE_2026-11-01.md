# Core / Knowledge / Node 验收 — 2026-11-01

此验收是 V1 继续实施中的阶段证据，不是完整 V1 的完成证书。
基准目标与剩余缺口以 [交叉审计](V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-01.md) 为准。

## 修复与实现

1. Graph/Memory/Knowledge 的原始 canonical intent 在 admission 前冻结，receipt-first 判定后才做
   私有候选转换；三个 protected method 不再接收宿主 StateBatch。相同 RequestKey 不因最新状态变化而
   重新计算命令摘要。replay 使用同一 reducer，核对 StateOps、对象序、receipt 和 RootSet。
2. Graph 验证 Revision parent/head 存在、根曾属于该 Space 的真实领域历史、Entries 与完整树一致。
   不把一个合法摘要当成有认证的 root 或 Event。
3. Memory 的状态转换不能绕过 Validate/Promote，连续并发候选不会丢更新。来源同时绑定真实 Fence、
   Space 和成员关系；Knowledge 来源必须属于该执行已采用的观察，不能用全局可见代替已观察。
4. Knowledge 分区由 Space 和 Principal 共同决定，分区内按 Actor/Execution 保持连续序号。
   两个主体使用相同逻辑 ID 不泄漏、不共享序号。Packet 的成员必须在 Source.Fence 已采用，
   Frontier/ViewDigest/载荷/Key 均校验；当前存在不代表过去存在。
5. 历史 Knowledge 读取返回原 Fence/RootSet/Key，Key 在历史与当前控制面均认证。缺失或非法主体
   查询是请求错误，不能 fencing 健康 store。领域 Cursor 按历史 BranchRefs 选旧状态，不回退 latest。
6. Node 查询认证 View 成员关系，再读对象；支持 Agent 生命周期、领域 Revision 和已采用的 Knowledge
   内容的详情/Context。分页绑定 View/filter，查询资源有界。普通 Cursor View 只展开祖先，其他分支
   需要明确 audit_snapshot。SDK/RPC 复用核心且绑定 session View 所有权。
7. 普通 GetState 在锁内选择根，避免旧 root 与新 Fence 混用；genesis 历史为空快照，
   不读空 ObjectID。取消的读取在等待存储/核心锁前响应取消。

## 可执行证据

- `engine/graph_core_test.go`、`graph_publication_test.go`：raw mutation 绕过拒绝、CAS 后与重启后
  原 receipt、缺失或伪造图输入、CURRENT 前后故障及同组发布。
- `engine/knowledge_core_test.go`、`knowledge_owner_test.go`、`knowledge_publication_test.go`：
  历史切片、同 Actor 多 E、同逻辑 ID 多主体隔离、源发布/采用时序、Packet 选择器与 Frontier、
  错误请求不 fencing、原 key 重试、重启与 unknown publication。
- `engine/memory_core_test.go`、`memory/command_regression_test.go`：七种命令 codec、来源认证、
  禁止状态捷径、并发无丢写、晋升/撤回后重试及恢复。
- `engine/cursor*_test.go`、`node_queries_test.go`、`node_revision_test.go`：旧根、未来 Revision
  拒绝、实体及来源导航、Knowledge 内容隔离、分页、限额、普通/audit Cursor 差异与重启。
- `service/domain_test.go`、`service/nodes_test.go`：真实 native store + net.Pipe + SDK 端到端，
  schema、Space、主体伪造、View 归属、receipt 和恢复语义。

## 全量门槛

环境：Go 1.27.0，Darwin/arm64。以下均通过：

```sh
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test ./... -count=1 -timeout=240s
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test -race ./... -count=1 -timeout=300s
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go vet ./...
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go build ./...
gofmt -l . # 空
```

## 限制与兼容

本报告记录的是 2026-11-01 的阶段子集；其后已经补齐 Knowledge Revision/Import、formal Event、
Node Explorer/ContentReader、SourcePublicationRef、RecordedInput、核心 Join/Prune、Recovery control
和 AuditFull compaction。最新完成证据以交叉审计和 2026-11-02 验收为准。AuditFull 仍不会删除
权威历史，不能把物理副本退休称为 BoundedRetention 或隐私遗忘。

整分区 Knowledge/Memory snapshot 受 core value 1 MiB 上限约束，超限拒绝；
新 schema 并不解决大规模分区的增量存储。此前携带 StateBatch 的 protected method 字节不再接受；
实验格式未冻结，需要使用原版本检查/导出后显式迁移，禁止关闭语义校验继续打开。
当前 schema 及 root/control updates 见 [domain/core registry](../formats/domain-core-experimental-1.md)。
