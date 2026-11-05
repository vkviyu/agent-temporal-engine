# M1c：持久状态、收据和确定性回放

> 这是 M1c 阶段的记录；当前 admission/group commit 能力见 [M1d 状态](M1D_STATUS.md)。


当前开发版本 `0.1.0-dev.m1c`，Go 1.27.0。独立 module、公开包结构、无第三方数据库/依赖，无 `internal` 目录；API、格式仍为 v0 experimental。

## 本批已实现

首条语义闭环已跑通：`Create → Mutate/Commit → DurableAck → Close/故障 → OpenExisting → LookupRequest/GetState`，重开后返回原 Ack 的 TxID、Fence、RequestDigest、ResultDigest、StateRoot 与 RootSet。

| 层 | 保证 |
| --- | --- |
| Object | 类型、Space、Version、规范强引用和 payload 一起寻址；固定 envelope，静态 reader 从字段重新提取 Refs |
| State | 自写不可变路径压缩 radix，按路径 copy-on-write；旧根不改；split/merge/空 key/二进制 key 有唯一表示；不把整树转 map |
| RootSet | DomainStates、ExecutionKnowledge、BranchRefs、LiveControl、CommandReceipts 分根；当前只有 domain 与 receipt 被核心命令更新 |
| Mutations | 版本化 `core.state.mutate/v1`；按字节排序的原子 batch；ExpectedFence、ExpectedRoot、逐 key absent/value CAS |
| Values | null、空字节、不存在三者独立；普通 Put/Delete 只能变更所选用户 space 的 domain tree |
| Receipt | 完整 (Space, stable Principal, RequestID) 键；持久 receipt tree；原结果在新 CAS/配额检查前返回；拒绝不占用幂等键 |
| Replay | 验证每笔 canonical command、历史 Before/After、StateOps、对象序与引用、receipt、派生 RootSet；应用记录的变更并核对根，不执行任意外部 reducer |
| Recovery | 语义回放必须先于任何 tail 隔离、截短和 CURRENT witness 同步；未知 schema 拒绝而不猜最新状态 |
| Readers | 对象索引仅保存 ID→已发布 payload 位置；树/值实际从 journal 读取，每次重新验证内容身份，禁止读取 staging/tail |
| Fail-stop | Unknown 后所有值、快照、receipt/absence 读取被拒绝；已存对象 decode limit 属于内容故障，不能当新请求配额继续 Ready；普通 EIO 保持 IO 分类但仍 fence |

EmptyRoot 为 `""` 的注册空树 sentinel，仅在可空 root/value-link 字段使用，不伪装成有效 ObjectID。genesis 没有域对象，首次成功命令才持久化非空 RootSet/receipt。

## 公共库入口

根包 `github.com/vkviyu/agent-temporal-engine` 提供：

- `ate.Create(existingPrivateDirectory, Options)`：创建并持有 owner/目录。
- `ate.OpenExisting(path, Options)`：完整物理和语义重开，只有 nil error 和可用 Store 才表示成功。
- `Store.Mutate`：可信宿主的 typed batch helper。
- `Store.Commit`：同一命令路径的规范字节入口，目前只接受注册的 core mutation method。
- `Store.LookupRequest`：原 receipt 或绑定完整已验证 Fence 的 NotFoundAtFence；Fenced 时只能 Indeterminate/error。
- `Store.GetState`：当前固定根下的值、存在性、StateRoot、RootSet 和 Fence。
- `Store.Snapshot`：当前五类 roots 与提交锚点；这不是历史 ReadView。
- `Store.Close`：失败可重试，engine/owner 完成退休后才关闭目录。

`engine.Create/OpenExisting(DurableIO, Options)` 留给自定义 I/O 的可信 embedding host，调用者负责其 IO 生命周期。构造错误的 `storage.CleanupError` 保留待关闭资源；根门面会把目录一同加入有序清理。

**这是一套可信宿主基础 API。** Principal 由宿主提供，当前没有网络身份认证/ACL 或 Agent-facing profile 门禁。Agent 不能直接获授这个通用写能力；后续受保护 agent/v1 命令会经独立状态机授权。当前 domain key 即使叫 authority/goal，也不会更新 LiveControl。

## 内部数据组织

DomainStates 是 `ate:system` 下的持久树，key 为用户 Space 的原始 UTF-8 字节，value 是该 space 的非空 domain root；空 domain 不占 catalog entry。域树 value 是同 Space 的 `ate.value` 对象。

CommandReceipts 也是系统树；key 为三个带长度前缀的 RequestKey 字段，value 是 receipt 对象。它与 domain 分根，删除 domain 最后一个 key 不影响历史 receipt。

每笔 transaction 按确定顺序保存：Command、必要新 Values、domain 新树节点、StateOps、catalog 新节点、Receipt、receipt-tree 新节点、RootSet。已有对象去重，节点按子到父输出，未采用的中间树节点不进日志。StateOps 可能因内容相同而引用旧对象；每笔仍有独立命令和 receipt。

Receipt 不含自己的 RootSet 或最终 CommitDigest；原 proof 从已验证 TxID envelope 索引绑定，避免 root↔receipt 摘要循环。内存对象位置/事务 proof 索引是完整日志可重建的加速结构，生产域状态仍由持久树和内容身份决定。

## 二进制

```sh
go run ./cmd/ate init-state /path/to/existing-empty-directory
go run ./cmd/ate recover-state /path/to/store
go run ./cmd/ate recover-state --quarantine-tail /path/to/store
```

成功时输出 `semantic_replay_verified=true`、RootSet 和五类 roots；关闭、回放或修复失败时不输出语义成功。旧 `init-journal/recover-journal` 只验证物理层，仍输出 semantic=false。CLI 目前没有任意 state writer；写入通过 Go 库的可信宿主 API。

## 资源和平台

沿用 M1b 的单段、batch=1、文件/目录 sync profile。新增加：

| 范围 | 当前上限 |
| --- | --- |
| 单值 bytes | 1 MiB |
| binary key | 1024 bytes |
| batch | 1..1024 个不重复 key，编码至多 4 MiB |
| Object | payload 8 MiB、encoded 16 MiB、强引用最多 4096 |
| 索引对象数 | 默认 1,000,000、硬上限 2,000,000，可降低 |
| radix 单次 Apply | 读取/新建最多各 65,536 个 node；构建预算 16 MiB，含中间节点 |

超过执行资源限额明确拒绝，不删历史或做有损恢复；receipt 命中不重新占用写配额。较低的重开内存容量可以拒绝打开较大 store，不能将未扫描部分伪称不存在。

Darwin arm64 实测；Linux/Windows 的编译结果见验收记录。平台/device 掉电认证范围仍按 M1b，不因新增语义回放扩大承诺。

## 尚未实现

- 官方 agent/v1 的 Query/Task/Execution/Step/Event/Activity/PendingAction 状态机、身份授权及业务动作唯一槽。
- Revision DAG、Branch head 管理、历史 ReadView/节点检索、Fork/Join/Diff。
- group commit、inflight 取消/共享、段轮转/跨段、reader pins、checkpoint/compaction、服务/SDK。
- ContextPacket、知识采用、时间旅行后的 authority 重新获取、记忆晋升/撤回。

本批完成的是核心状态与 receipt 的可验证恢复，不代表完整多 Agent 任务恢复已经交付。下一步按实施架构继续 M1d，再接后续存储生命周期与上层 profile。

实际命令和测试证据见 [M1c 验收记录](validation/M1C_2026-10-31.md)。
