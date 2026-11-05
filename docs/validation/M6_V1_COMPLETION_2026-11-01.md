# ATE M6 阶段验证（历史记录）— 2026-11-01

本文件记录此前阶段的测试结果，**不是 V1 完成证书**。后续交叉审计发现 raw StateBatch
保护根绕过、依赖最新状态的重试、Knowledge 主体碰撞及历史 Node 可见性缺口；这些问题的
修复证据和仍未完成的目标见 [当前交叉审计](V1_DOCUMENT_CODE_CROSS_AUDIT_2026-11-01.md)。
下述旧实现映射不作为当前 API/磁盘 registry；当前映射见 [domain/core](../formats/domain-core-experimental-1.md)。

This validation covers the first-version implementation after the lifecycle,
graph, memory, node-query, and retention additions.

## Implemented boundaries

- `agent.apply/v1` now includes invocation settlement (opcode 11) and the
  Query/Activity/Attempt/ActivityResult/Message/Consumption/Continuation
  lifecycle (opcode 12).
- Agent snapshots use version 2 only when lifecycle records are present;
  version 1 snapshots remain readable.
- Graph Revision/Branch commands use the core mutation/receipt path and
  branch compare-and-swap. `LoadGraph` reconstructs bounded ancestry.
- Memory commands operate on a private candidate snapshot and publish the
  resulting canonical snapshot through the core state mutation path.
- Node list/detail/context reads are authenticated against a captured View and
  bounded by traversal and page limits.
- `storage.Maintain` requires exclusive ownership, never removes published
  segments/manifests, and only removes quarantine evidence when explicitly
  requested.

## Verification

```text
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test ./... -count=1 -timeout=300s
GOTOOLCHAIN=local GOCACHE=/tmp/ate-go-build go test -race ./... -count=1 -timeout=300s
go vet ./...
go build ./...
gofmt -l .   # empty
```

All commands passed on Go 1.27.0.

## Explicit version boundaries

Knowledge Observation/ContextPacket remains a public deterministic package,
and Knowledge/Graph/Memory root facades now use the core domain-state and
receipt path. The local service/client now exposes authenticated Graph/Memory/Knowledge
methods; the standalone graphstore/memory/knowledge Durable adapters remain
available. Profile partitions are bounded whole snapshots (O(n) transition and
read cost); entity-level incremental projection and device power-loss
certification are later work.
