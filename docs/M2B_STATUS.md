# M2b：Revision Journal 持久适配器

M2b 在 M2a 纯算法之上加入两个公开持久适配器：`graphstore` 保存 Revision 事务；`journalstore` 提供阶段语义记录的通用 Journal 发布/replay 边界。纯 `graph` 仍不依赖存储。

## 保证

- graphstore 每笔 Revision 为一个 `KindRevision` body record，BranchRef 的 Fork/Advance 为一个 `KindEvent` body record；两者都使用 Journal 的 LSN、前摘要、CURRENT P1 和恢复尾部策略。
- Open 只在完整物理前缀和 callback replay 成功后交换候选 graph；callback 失败不会暴露部分内存图。
- Append 在 P1 成功前不写入内存 graph；CommitUnknown 后不返回成功 Revision。
- Revision canonical bytes、父/JoinParents、StateRoot、Events 和 Request/Receipt 摘要在 replay 中重新验证。
- BranchRef 的 branch ID、head、版本 CAS 和所属 Space 在发布前验证；replay 按同一命令重放，重复 RequestKey 返回原始 publication proof。
- `CommitJoin` 将经过 `graph` 显式冲突决议的 JoinPlan 作为 Revision 持久发布，并在 reopen 时重验全部父关系和 canonical 内容。
- journalstore 适配器对任意阶段 typed record 提供 bounded payload、RequestKey、单事务 P1 和全前缀 replay；业务投影由调用者的 Apply 回调拥有。

## 边界

BranchRef 已在 graphstore 适配器内持久化，根门面也提供了通过 core domain state/receipt 的 Revision 与 Branch CAS 路径；它仍不是独立 BranchRefs root 的专用 wire publisher。Knowledge 保持 deterministic 包边界，适配器和根门面不创建第二个 Journal。

## 验证

`graph/` 的确定性算法/codec race 与 `graphstore` native create→append→close→open→replay 测试通过。`journalstore` native replay 测试验证 callback 只在完整前缀认证后接收记录。最终全量 race/vet/build 见第一版总验收记录。
