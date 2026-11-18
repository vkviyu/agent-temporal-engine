# ATE 阶段 A：存储边界与验证入口

本报告对应 [重构主计划](../ATE_REFACTOR_MASTER_PLAN.md) 的 R01、R02、R03、R05 和 R09 最小入口。阶段 A 已完成集成验证；文件日期沿用审计日期，最终验证日志时间为 2026-11-16。

## 改动与证据

| 项目         | 实现结果                                                                                                      | 关键回归                                                                                                                            |
| ------------ | ------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| R01          | adapter metadata v2 分开保存逻辑 LSN 和原始物理 Generation，保护两个计数器溢出，恢复保持原始 receipt          | `TestAppendAfterCompactionReopensWithOriginalReceipts`、`TestWritesAfterCompactionReopenWithOriginalReceipts`                       |
| R02          | Maintain 使用既有 cleanup 契约保留 Close 错误与重试句柄；获取 owner 同时返回 handle/error 的路径也被覆盖      | `TestMaintainOwnerCleanup`、`TestMaintainAcquisitionCleanup`                                                                        |
| R03          | 区分固定 radix scan 超限与 reader 独立预算；旧根枚举共用 count/key/scope 校验；非法持久化树按 corruption 处理 | `TestAgentRecordStoredRadixLimitClassification`、`TestAgentRecordStoredRadixLimitFences`、`TestAgentRecordReaderBudgetDoesNotFence` |
| R05          | CLI startup 和 hello 共用 `Server.Capabilities`，保留 optional backend 与 profile 门控，返回独立切片          | CLI 的真实服务启停配置测试、`TestCapabilitiesMatchHelloForOptionalBackends`                                                         |
| R09 最小入口 | 本地/CI 使用同一命令，记录源码指纹、逐项结果、skip 和环境，报告放在仓库外                                     | 验证工具自身的小仓库/子进程回归；[使用说明](VERIFY.md)                                                                              |

R01/R02/R03/R05 均先增加失败回归、再修复。审查检查了错误来源、资源所有权、replay/receipt 一致性和接口复用；未确认新的功能缺陷。长交易字面量、请求嵌套和函数签名已展开，避免把关键字段挤在单行中。

R01 采用明确的 [adapter metadata v2 断代](../formats/adapter-publication-metadata-v2.md)，没有迁移旧数据或保留运行时兼容分支。这也影响通过 journalstore 持久化的独立领域 store；不改变主 Engine 五根状态的编码。

## R03 扫描上限的依据

每个 canonical record key 为 SHA-256 加 uint64 ordinal，共 40 字节。设记录数为 N，最大 N=16,384。最终归一化 radix 不含无值单子节点，固定长度 key 不互为前缀，因此按绝对路径计数的节点数 V≤2N−1，边数 E=V−1。

| 项目             | 合法最终树保守上界        | 固定扫描预算 |
| ---------------- | ------------------------- | ------------ |
| entry 数         | 16,384                    | 16,384       |
| node 数          | 32,767                    | 65,536       |
| entries 返回字节 | N×(40+64) = 1,703,936     | 4 MiB        |
| radix 读取字节   | 360V+66N+68E = 15,105,552 | 32 MiB       |

字节估算包括最长 256 字节 space、节点前缀、计数字段、value、edge 及对象 envelope refs；record payload 独立读取，不计入 radix read bytes。

`TestAgentRecordMaximumLegalScan` 通过真实 writer 构建最大条数、最长 space 的状态，测得读取 18,429 个 radix 对象、8,196,660 字节，并覆盖完整 catalog、未缓存旧根复用及物理 journal reopen。该 fixture 验证最大条数，不声称覆盖总 payload 恰为 256 MiB。

`TestAgentRecordMultiBatchResourceFailureIsAtomic` 使用公开命令生成可独立 replay 的状态；一次取消产生 520 个 insert/delete mutations，跨三批。第三批失败和取消均不泄露 root/catalog、receipt 或 journal 写入，不污染 Work cache；重开和重试结果保持一致。

上界针对成功写出的最终树。候选构造仍可能触及对象数、事务字节或中间节点预算，这些仍返回 ResourceLimit。reader 自身的预算、取消和 IO 不因使用了相同错误 sentinel 而被当作存储损坏。

## 验证范围

单项实现阶段执行了相关 package test、定向 race、vet 和格式检查。集成阶段使用最终合并后的源码执行全量验证；R01/R02/R03/R05 已标为完成。

R09 本轮只交付最小验证入口。需求与测试的完整映射、skip 预算治理、benchmark/native lane 仍属于后续 R09/R26；操作系统测试通过不等于设备掉电认证。

## 最终执行记录

环境：Go 1.27.0，Darwin/arm64。HEAD 为 `1161b28eb34f3f40dd68352e87da0bc03bbeb497`，工作树 dirty；指纹同时覆盖已有和本批源码，不能将 HEAD 单独视为验证版本。

| 命令                                                                                  | 结果                                                          |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `go test -count=1 ./journalstore ./graphstore ./storage ./service ./cmd/ate ./engine` | 全部通过                                                      |
| `go test -count=1 ./tests/architecture ./scripts/verify`                              | 通过                                                          |
| `go run ./scripts/verify full`                                                        | 退出 0；format/diff/vet/build/test 全部通过，有明确 skip      |
| `go run ./scripts/verify race`                                                        | 退出 0；format/diff/vet/build/全库 race 全部通过，有明确 skip |

full 与 race 使用相同 `ate-source-v2` 指纹，且各自起止一致：

```text
e352c2fc3530a3d9ebd123b47979941792db4e77b8fe4b002c47f45131c2b88b
```

full 执行区间为 `2026-11-16T04:11:13Z` 至 `04:14:34Z`；race 为 `04:14:35Z` 至 `04:22:26Z`（UTC）。两份 manifest 的状态均为 `selected_passed_with_skips`，`source_stable=true`。

报告目录（本机临时证据，未上传）：

- full：`/private/var/folders/bw/p1qj0ln54pv806l_92fy0yh40000gn/T/ate-verify-1456289653`
- race：`/private/var/folders/bw/p1qj0ln54pv806l_92fy0yh40000gn/T/ate-verify-1069564503`

仓库保留 [机器可读摘要](REFACTOR_STAGE_A_2026-11-15.json)。上述目录内有完整文件指纹和原始命令日志；临时目录可能被系统清理，摘要不代替完整日志归档。

两次执行均跳过以下 3 个 opt-in 真实模型评估：`TestRealModelRecursiveSummaryAndEvidenceRecall`、`TestRealModelTypedSummaryReviewAndEvidenceRecallV2`、`TestRealModelCrossProjectMemoryApplicability`。原因是未启用 `ATE_RUN_MODEL_EVAL=1` 及其显式 endpoint/model/artifact 配置。另有 transfer 包的 `[no test files]` 包级 skip；它不表示一项失败或未执行的测试。

首次全量检查发现新 `scripts/verify` 未加入包依赖表。已将其登记为仅依赖标准库的独立工具，未放宽其他包约束；随后重新执行以上 full/race。先前发现问题后中止的运行不计为成功证据。

本次没有执行独立 native durability、benchmark 或设备掉电 lane，没有运行外部模型评估。完成验证后仅回填 README/docs，不修改指纹覆盖的源码。未提交 Git。
