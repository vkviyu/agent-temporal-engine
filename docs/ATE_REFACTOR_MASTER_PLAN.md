# ATE 全库重构主计划

审计日期：2026-11-18。计划状态：阶段 A 和阶段 B 的 R04、R06、R07、R08、R09、R10、R11，以及 R12、R13、R15、R26、R27 已完成；R14 已明确延期；R16、R17、R18、R19、R20、R21、R22、R23、R24、R25 已完成第一阶段基线或边界收口。真实模型和外部 durability 证据按环境条件延期。本文是后续 ATE 整体重构的任务入口、执行顺序和验收台账。

## 1. 目标与使用方式

ATE 为外部 Agent 保存可验证的时间轨迹：发生了什么、当时知道什么、结果如何被消费，以及故障后从哪里继续。Agent Loop、模型调用、工具执行和工作区管理仍由宿主负责。

本次重构面向这样的场景：一个任务运行数天，产生大量子 Execution、Activity、Observation、Memory 和分支；中间发生重启、知识撤回和物理压缩，宿主仍能增量读取轨迹、恢复执行，并验证历史事实。重构要同时保持时间语义、发布原子性和权限边界，解决增长后的准备、读取、恢复与资源成本。

本文汇总全库分析后需要处理的事项，而不是一次聊天摘要。后续实施遵循以下规则：

1. 每项工作绑定本文的 `Rxx` 编号；先更新状态，再实施、验证并回填结果。新增发现先补入本文及依赖关系，避免形成文档之外的零散重构。
2. 按第 8 节的阶段顺序推进。整体重构分批交付，各批都有完整且可验证的结果，不进行难以归因的全库同时改写。
3. 已确认缺陷先补最小回归再修复；结构改造保留语义对照；性能项先测量再选择实现。测量任务是计划的一部分，不能一直以“待优化”悬置。
4. 每项只能以“完成并有证据”或“明确延期并记录原因、触发条件”收口。没有实现的优化不能标为已完成。
5. 当前没有真实用户要求保留旧开发格式。允许必要的格式断代；不为假想旧数据保留运行时双格式 reader、双 replay 或迁移分支。不得自动删除用户本地 journal。
6. 本计划首先覆盖 ATE；GSEA 接入在 ATE 契约稳定后单独推进。Git 提交由用户处理。

“全库”表示所有当前 Go 包及主要跨包路径均纳入覆盖，不表示静态审计已经证明不存在其他缺陷。本文记录当前证据支持的完整工作清单；执行过程中以源码、复现和测试结果修订判断。

## 2. 审计基线与判断标准

### 2.1 基线

审计对象是 2026-11-15 的当前工作树，包含尚未提交的 Agent record-only、Work record catalog、TaskTrace 查询优化和 recovery checkpoint 实现，不是仅审计 `HEAD`。

- 起始提交：`1161b28`；工作树含修改及未跟踪文件，因此该提交不能独立重现本次审计状态。
- 本轮使用前序子系统审计、三组针对性复核及主流程源码核对；`go list ./...` 核实了 30 个 Go 包。
- 初始审计阶段是静态审计和文档编制，没有重新执行全量测试，也没有为新发现运行故障复现。下文“已确认缺陷”指源码及调用契约已能建立失效路径；各项实施结果以对应 Rxx 验证报告为准。
- 前序 record-only/checkpoint 改造曾通过 `go test ./...`、相关包 race、`go vet ./...` 和 diff 检查；这些是历史证据，不能替代后续各批验证。
- R09 已为当前工作树生成 `ate-source-v2` 内容指纹；本文件日期仍不是可重现版本标识，具体源码指纹和 manifest 路径见 R09 验证报告。

### 2.2 证据类型与优先级

证据类型与优先级分开记录：

| 类型   | 含义                                       | 实施要求                      |
| ------ | ------------------------------------------ | ----------------------------- |
| 缺陷   | 代码路径与现有契约冲突                     | 最小回归、修复、恢复/边界验证 |
| 待证实 | 可疑路径明确，但合法输入边界或影响尚需复现 | 先复现；支持或否定后更新定性  |
| 结构   | 已存在双重职责、重复语义或不清楚的边界     | 明确目标契约，保留行为对照    |
| 容量   | 已确定的增长成本或接口上限                 | 建基线，依据规模目标决定改造  |
| 验证   | 现有保障需要补齐或组织成可重复执行的集合   | 复用已有测试，补缺口而非重写  |

| 优先级 | 含义                                                                 |
| ------ | -------------------------------------------------------------------- |
| P0     | 已证实的严重数据丢失、权限突破或发布权威破坏；本轮没有确认此类新问题 |
| P1     | 应先于下一阶段结构/规模改造处理的正确性、格式收敛和关键验证工作      |
| P2     | 纳入整体计划的类型边界、API、容量、维护和工程工作                    |
| P3     | 不单独阻塞正确性修复的文档与工具治理                                 |

缺少仓库内 CI 配置不等于 P0；类型不明确不等于已发生越权；线性扫描不等于结果错误。

## 3. 全库覆盖与目标结构

### 3.1 包覆盖

下表以 `go list ./...` 的实际结果为准；根包记为 `ate`。

| 包/目录                                              | 审计内容                                                    | 对应工作或结论                                |
| ---------------------------------------------------- | ----------------------------------------------------------- | --------------------------------------------- |
| `ate`、`api`、`model`                                | 嵌入式门面、身份、时间、RootSet、请求/结果                  | R06、R12、R15；保留宿主边界                   |
| `engine`                                             | 准备/发布、Work、receipt、replay、读 cut、索引、权限、维护  | R03–R04、R06、R10–R24；详见下表               |
| `storage`                                            | CURRENT、journal、checkpoint、compaction、锁、恢复、cleanup | R01–R03、R07–R10、R22、R24–R25                |
| `journalstore`、`graphstore`                         | 独立持久 adapter、逻辑序列、外部 IO                         | R01、R23                                      |
| `state`、`codec`                                     | 不可变 radix、canonical object、扫描与解码上限              | R03、R14、R20；保留完整对象认证               |
| `agentprofile`、`agentstore`                         | reducer、record image、LiveControl、恢复与门面 authority    | R03、R14–R15；保留 record-only                |
| `knowledge`                                          | Observation、ContextPacket、索引、历史选择与 import         | R04、R06、R11、R21                            |
| `memory`                                             | Memory/ExperiencePath、撤回、policy、snapshot mutation      | R06、R11、R16、R21                            |
| `graph`                                              | Revision/Branch、历史 root、descendants、join               | R06、R11、R17–R18                             |
| `annotation`、`investigation`                        | owner scope、历史证据、判断与调查状态                       | R06、R10–R12、R21；未发现需独立重写的 reducer |
| `eventlog`、`transfer`                               | occurrence、跨空间导入、来源与授权                          | R06、R10–R12、R24；保留统一发布路径           |
| `protocol`、`client`、`service`                      | 帧、会话、身份、view、delivery grant、SDK                   | R05、R12、R15、R26–R27                        |
| `cmd/ate`                                            | 运维、服务启动、能力声明                                    | R05、R08、R12、R25、R27                       |
| `formats`                                            | schema 注册、方法清单、golden corpus                        | R04、R09、R27                                 |
| `reference`、`tests/reference`                       | 独立状态/receipt/control 模型                               | R11；已有实现，不是空白                       |
| `tests/acceptance`、`tests/architecture`             | 端到端边界、包依赖约束                                      | R09–R12、R26                                  |
| `tests/benchmarks`、`tests/evaluation`、`tests/host` | 规模、宿主场景、评价证据                                    | R09、R13–R26                                  |
| `docs`、`testdata`                                   | 规范、历史报告、fixture                                     | R09、R27                                      |

权限与 emission 没有独立 Go 包：权限跨 service 身份/view ownership、engine policy 和 delivery grant；emission 位于 engine/API/transport 中。它们已作为跨层路径纳入覆盖。

### 3.2 Engine 主要路径

| 路径                                                   | 当前判断                                                | 计划                           |
| ------------------------------------------------------ | ------------------------------------------------------- | ------------------------------ |
| admission → receipt-first → coordinator → staging → P1 | 单一发布权威成立；已有取消、unknown、fence 与批处理测试 | 补复合矩阵和观测，R10、R13     |
| Work 多领域子命令                                      | 私有 candidate、统一提交；已有 record catalog 复用      | R10、R14；不另建提交 authority |
| replay → checkpoint → suffix replay                    | checkpoint 是已认证投影；仍验证物理历史                 | R10–R11、R22、R24              |
| read-cut → 历史读取 → 当前 disclosure → final delivery | 正确性边界应保持                                        | R06、R12、R15、R19–R21         |
| Agent lifecycle / continuation / authority             | record-only 已落地；剩余全量准备和资源分类问题          | R03、R14–R15                   |
| Knowledge / import / context selection                 | 已收敛为 v2 indexed records，旧格式明确拒绝             | R04                            |
| Graph / Memory                                         | 语义与持久化可用；存在全量扫描/更新成本                 | R16–R18                        |
| TaskTrace / Node / Content / Summary                   | 已有索引与 cache，仍需控制 scope、物化和依赖闭包成本    | R06、R19–R21                   |

目标仍是“公共契约 → 单一 Engine 协调发布 → 不可变记录与认证存储”。增量读写、可重建索引和验证工具围绕该结构演进，不另起一个并行引擎。

## 4. 总任务台账

状态约定：`待实施`、`待复现`、`待测量`、`进行中`、`已完成`、`已延期`、`已否定`。以下均为本轮新增计划，不能因前序已有相关测试而直接勾选完成。

| ID  | 工作项                                         | 类型 / 优先级 | 状态                                               | 主要依赖             |
| --- | ---------------------------------------------- | ------------- | -------------------------------------------------- | -------------------- |
| R01 | 分离 adapter 逻辑序列与物理 generation         | 缺陷 / P1     | 已完成                                             | 无                   |
| R02 | 维护操作保留 owner cleanup 错误及重试句柄      | 缺陷 / P1     | 已完成                                             | 无                   |
| R03 | Agent 持久化扫描超限的来源与错误分类           | 缺陷 / P1     | 已完成                                             | 无                   |
| R04 | Knowledge 收敛为 indexed record 单格式         | 结构 / P1     | 已完成                                             | R09 最小验证入口     |
| R05 | CLI 与 hello 共用能力清单                      | 缺陷 / P2     | 已完成                                             | 无                   |
| R06 | 显式区分逻辑 policy 依赖与物理对象引用         | 结构 / P2     | 已完成                                             | R04                  |
| R07 | publication staging 元数据生命周期             | 结构 / P2     | 已完成                                             | R02                  |
| R08 | 明确 TailReject 与认证布局 cleanup 契约        | 结构 / P2     | 已完成                                             | R02                  |
| R09 | 统一验证入口与证据 manifest                    | 验证 / P1     | 已完成（本机）                                     | 无                   |
| R10 | 上层复合事务故障矩阵                           | 验证 / P1     | 已完成                                             | R09                  |
| R11 | 扩充领域与跨域独立 oracle                      | 验证 / P1     | 已完成                                             | R09；随 R04/R06 更新 |
| R12 | 公共 API 跨 transport conformance              | 验证 / P2     | 已完成                                             | R09                  |
| R13 | 统一资源和阶段观测接口                         | 结构 / P2     | 已完成                                             | R09                  |
| R14 | Agent 全状态准备的增量化边界                   | 容量 / P2     | 已延期（保留 canonical）                           | R03、R09；参考 R13   |
| R15 | 大 Agent 状态的有界公共读取                    | 容量 / P2     | 已完成（本机）                                     | R03、R12             |
| R16 | Memory mutation 与撤回索引的增长成本           | 容量 / P2     | 已测量（纯领域基线）                               | R09、R11；参考 R06   |
| R17 | Graph 历史 domain root 认证查询索引            | 容量 / P2     | 已测量（第一阶段 Graph 基线）                      | R09、R11             |
| R18 | Graph descendants 与 join 扫描成本             | 容量 / P2     | 已测量（第一阶段 Graph 基线）                      | R09、R11             |
| R19 | TaskTrace scope fan-out 的独立预算             | 容量 / P2     | 已测量（第一阶段 TaskTrace 基线）                  | R09                  |
| R20 | Node/View 查询按页读取与物化预算               | 容量 / P2     | 已测量（第一阶段 Node/View/Content 基线）          | R09、R12             |
| R21 | 历史 provenance/disclosure 闭包的复用          | 容量 / P2     | 已测量；第一阶段 projection 与真实 Engine 读基线   | R04、R06             |
| R22 | compaction/checkpoint 重复 IO 与认证证明复用   | 容量 / P2     | 已测量；第一阶段 compaction/checkpoint/replay 基线 | R01、R10             |
| R23 | adapter 锁内 IO 和外部 callback                | 容量 / P2     | 已测量；adapter 锁覆盖基线，保留当前边界           | R01                  |
| R24 | 常驻历史索引、cache 与 checkpoint 峰值内存预算 | 容量 / P2     | 已测量；第一阶段内存、索引与 cache 基线            | R09、R13             |
| R25 | 逻辑存储配额与实际磁盘预算                     | 结构 / P2     | 已收口；逻辑配额与物理文件长度边界已明确           | R07、R13             |
| R26 | 测试同步、平台 lane 与 skip 管理               | 验证 / P2     | 已完成                                             | R09                  |
| R27 | 方法/格式登记与文档证据防漂移                  | 验证 / P3     | 已完成                                             | R04、R05、R09        |

## 5. 正确性、格式与边界工作

### R01：adapter 逻辑事务序列与物理发布代次分离

**证据。** [journalstore/store.go](../journalstore/store.go) 的 Append 用 `p.Generation+1` 编码 metadata，恢复要求 metadata 逐条连续；[graphstore/store.go](../graphstore/store.go) 的 AddRevision/branch 写入也使用它，恢复还要求 `generation == tx.LSN`。[storage/compaction.go](../storage/compaction.go) 的 compaction 增加 Generation，但保留 Position/LSN。

**实施决定（2026-11-15）。** 两个 adapter 使用 metadata v2，同时保存逻辑 LSN 和首次发布的物理 Generation；恢复验证前者与事务一致、后者严格递增但允许跳跃，重复请求保留原始 receipt。仅把旧字段改成 LSN 会丢失原始 receipt 的 Generation，因此采用显式格式断代。详见 [格式说明](formats/adapter-publication-metadata-v2.md)；经 journalstore 持久化的独立领域 store 也受该断代影响。

**影响与复现。** adapter 关闭 → 用公开 storage API 打开同一 journal 并 AuditFull compact → adapter 重开 → 追加 → 再重开。压缩后第一次 reopen 可成功，新追加的 metadata 出现序号跳跃，下一次恢复失败。这是独立 adapter 的可达路径，不是主 Engine coordinator 的已证实故障。

**改造。** 以逻辑 LSN 或明确定义的独立逻辑序号生成 adapter metadata；物理 Generation 只描述布局发布。同步检查 overflow、恢复生成的 receipt/publication 投影和去重返回值，不只替换一处表达式。

**验收。** 两个 adapter 都覆盖正常追加、重复请求、一次/多次 compact、compact 后多次追加与 reopen；逻辑序列连续，历史 receipt 稳定，物理 generation 可独立增加。

### R02：Maintain 的 owner lock 关闭失败必须可恢复

**证据。** [storage/maintenance.go](../storage/maintenance.go) 使用 `defer owner.Close()` 丢弃错误；[storage/durableio.go](../storage/durableio.go) 明确 Close 失败可能仍持有资源，调用者须保留 handle 重试。[storage/cleanup.go](../storage/cleanup.go) 已提供 `CleanupError`、`closeInto` 等机制。

**改造。** 复用现有 cleanup ownership 契约，将业务错误与关闭错误一并返回，失败的 owner 保留在可重试对象中。覆盖所有提前返回路径；检查根门面和 CLI 是否继续保留该错误，而非只改 storage 层。

**验收。** 无删除操作、删除成功、业务失败、取消等情况下分别注入 Close 失败；不得假成功；调用者能通过错误取得重试对象；重试成功后重新获取 owner 成功；再次 Close 幂等。

### R03：区分合法资源预算与持久化状态违反上限

**证据。** [engine/agent_record_storage.go](../engine/agent_record_storage.go) 的 `loadAgentRecordCatalog.fail` 原样返回 `state.ErrLimit`、`storage.ErrLimit`；`agentRecordEntries` 有 entry/node/read-byte 上限。`isLimit` 和 `readFailureAt` 可把它归类为普通资源拒绝。现有 `TestAgentRecordStoredLimitRemainsCorruption` 覆盖单条记录的内容限制，并没有证明 radix 扫描限制的语义。`storeAgentRecords` 的旧根枚举也需一起检查。

**先解决的歧义。** 一个 scan budget 超限是否证明存储损坏，取决于合法 writer 能否产生超过该扫描预算的状态。不能把所有扫描超限直接改成 corruption，也不能把持久化 schema 违规当成新请求过大。

**复现与决定（2026-11-15）。** 已构造哈希正确、局部 canonical、展开成 32,768 个 key 的非法 radix，复现 load、旧根枚举和 registry 将固定扫描超限误当资源拒绝。合法 writer 生成最多 16,384 个固定 40 字节 key，归一化树节点上界为 32,767，radix 读取字节保守上界 15,105,552，均低于现有固定扫描预算。现通过 reader 包装保留来源，只有扫描器自身限制违规归为 stored-object corruption；reader 独立预算、取消、deadline、真实 IO 和候选构建限制保持原语义。

**改造。** 建立三类错误来源：新请求/候选超限；合法持久状态在可配置工作预算下未完成；不可能由合法 writer 产生的持久化结构超限。验证 writer/read budget 的闭包关系，决定哪些固定扫描限制属于格式不变量，并统一 prepare、public read、replay 的映射。

**验收。** 合法最大条数/多批次写入可读取和重开；伪造 record count、膨胀 radix、异常对象分别测试；schema 违规成为 stored-object corruption 并按契约 fence；合法资源拒绝无 root、journal、receipt 副作用；取消和真实 IO 保留原始语义。先完成复现再将本项定为具体缺陷。

### R04：删除 Knowledge 的运行时旧格式双轨

**原始证据。** 审计时 `knowledgeIndex.legacy`、observation/packet/sequence fallback 和 snapshot 合并读取 v1；`prepareKnowledge` 根据 `UsesLegacySnapshot` 选择整快照写入，import 也保留双路径。这些路径现已删除。

**改造。** Knowledge 当前读、历史读、写、import、replay 统一使用 indexed records。删除仅为旧格式存在的 command decoder/reducer、owner snapshot key、合并和重复写入 marker。先核查纯领域 Snapshot/codec 的调用者；内存 DTO、测试 oracle 和其他领域的 Snapshot 不因同名被删除。

**格式决定。** 当前无旧用户迁移需求，采用明确断代。旧 command、旧 state 和旧 checkpoint 不得被新运行时静默解释为空状态；通过必要的格式/schema 准入拒绝。仅为识别并拒绝旧格式留下的标识不是兼容 reader。同步 registry、fixture、格式说明及开发数据重建提示。

**验收。** 新写、重复请求、query-only、execution-scoped selection、import、历史 cut、reopen、checkpoint、compaction 全部只使用 canonical indexed 路径；无活动 v1 fallback；旧格式明确失败；现有 isolation/disclosure 语义不变。

**交付。** 2026-11-16 完成；删除旧 snapshot codec/reducer、fallback、marker 和旧 packet replay policy 分支，保留 `knowledge.Snapshot` DTO。v1 command、旧 owner key、混合 root 和含 v1 command 的 journal 均明确拒绝；checkpoint epoch 2 与 registry 同步。全量 full/race 通过，起止源码指纹一致；详细范围和未验证项见 [R04 验证报告](validation/REFACTOR_R04_2026-11-16.md)。

### R05：服务能力清单只有一个计算来源

**证据。** [cmd/ate/serve.go](../cmd/ate/serve.go) 的启动 JSON 手写基础加 Agent/Event 能力；[service/session.go](../service/session.go) 的 hello 按实际接口提供更多能力。依赖 stdout 做发现的客户端会少看到已支持功能。

**改造。** 提取只读能力计算，由启动事件、hello 和相关校验共用；保持 profile enable/disable 和 backend 可选接口的真实判断。

**验收。** 同一实例 startup capabilities 与 client identity capabilities 相同；完整、精简 backend 和关闭 profile 均覆盖；不得通过固定宣告所有能力修复。

### R06：逻辑依赖与物理对象引用分型

**证据。** [engine/content_policy.go](../engine/content_policy.go)、[engine/node_context_links.go](../engine/node_context_links.go)、[engine/task_trace_provenance.go](../engine/task_trace_provenance.go) 等将不同身份汇入 `[]model.ObjectID`。[memory/references.go](../memory/references.go)、[knowledge/references.go](../knowledge/references.go)、[graph/references.go](../graph/references.go) 已区分部分逻辑与物理引用。

**定性。** 当前找到的混合列表主要用于 policy 聚合；没有证实它们被无条件作为 raw object 地址读取，也没有证实权限绕过。应先明确 policy 是否有意按 digest 跨 namespace 撤回，不能以“加类型”为由偷偷改变该语义。

**语义决定（2026-11-16）。** 保留当前 Space 内按 digest 聚合的撤回语义：不同 namespace、不同已授权 owner 的同 digest 共用 policy identity，跨 Space 隔离。kind 用来约束依赖构建和物理读取边界，不改变撤回范围或赋予读取权限。持久化 derivation/Memory 格式保持现有契约；不添加双格式 reader 或迁移。

**改造。** 在依赖构建处使用带 kind 的 `PolicyDependency` 或现有 ReferenceSet 风格；raw content closure 只接受物理引用。仅在明确且有文档的 policy identity 边界做转换。优先改跨域接缝，避免给所有 model ID 大规模改名。

**验收。** 相同 digest 的不同 namespace、跨 owner/space、withdraw 后 warm cache、历史可读但不可复用等用例；raw reader 不接收逻辑 ID；公开 disclosure 行为有对照，若改变必须记录语义决定。

**交付。** 2026-11-16 完成。新增内部 `policyDependency` 和显式 `policyIdentities` 边界，覆盖 Knowledge、Summary、Node、TaskTrace、Annotation、Investigation 和 Graph 的混合依赖构建。物理闭包入口改为独立 struct，所有生产调用点从领域物理引用提取器构造。已解析的 policy digest 列表、持久化 derivation 和 Memory policy 继续保持原语义；不承诺把全库所有 `ObjectID` 分型。full/race 均通过，语义、回归和边界详见 [R06 验证报告](validation/REFACTOR_R06_2026-11-16.md)。

### R07：staging 元数据的归属与回收

**证据。** [storage/journal_io.go](../storage/journal_io.go) 的 publication 安装生成 `manifests/.STAGE.*`、`.CURRENT.*`；失败后可能保留它们及未被 CURRENT 选中的 MANIFEST。现有恢复坚持 CURRENT authority，这是正确保护，但没有统一的残留预算/回收规则。

**改造。** 给 staging 文件建立可证明归属、选中状态和保留策略；统计时区分 selected、retained proof、staged、unknown。清理必须在独占 ownership 下进行，不能按名称前缀批量删掉可能被认证链、pin 或恢复证据需要的文件。

**实施边界（2026-11-16）。** `Maintain` 默认只检查，`DropPublicationStaging` 显式开启已认证、未选中且不在证明闭包中的元数据回收。未知元数据阻止 publication 回收并列出 blockers；不删除 segment 或 checkpoint 流，不运行 tail repair。已过时候选的 source 也保留到下一轮，避免中断时留下悬空证明。目录同步先稳定当前观察到的 CURRENT，再删除旧 manifest；没有重写 authority。

**交付。** 已完成实现、独立安全复核和 153 个 publication/maintenance 故障集成场景；关闭失败保留 FILE → OWNER 重试链，空重试仍同步目录。最终 full/race 均通过，起止源码指纹一致。预算、保守回收限制和完整证据见 [R07 验证报告](validation/REFACTOR_R07_2026-11-16.md)。

**验收。** 对每个 publication 安装边界注入失败并重复 reopen/maintain；选中 authority 不变；未知文件保留；合法残留可重复回收；删除及目录同步失败可恢复。本项不包含选择性历史 GC。

### R08：TailReject 的行为契约明确化

**证据。** [storage/recovery.go](../storage/recovery.go) 在 TailPolicy 判断前处理 compaction intent 和已认证 retired segment。当前注释承诺不修改 tail，并未承诺整个目录只读。现有普通 tail 测试不代表所有布局状态都不变化。

**决定。** 保留 TailReject 拒绝 active tail repair 的语义，明确它仍可能清理已经认证的布局残留；不把 Open 的 TailReject 隐式改成完整只读检查 API。若将来需要只读诊断，用独立契约实现。

**实施。** `storage/recovery.go` 在 `collectOrphans` 成功后、任何 compaction intent 或 RetiredSegments 清理前，对存在 compaction residue 或退休布局的 publication 执行一次 `SyncDir(".")`。该 stabilization barrier 失败时返回 `ErrRecoveryIncomplete`，保留原布局，不执行 destructive cleanup，也不授予 Ready。普通 active tail 且没有 compaction residue 或退休描述符时不增加额外目录同步。

**验收。** `storage/tail_reject_contract_test.go`、`storage/tail_reject_cleanup_test.go` 和 `storage/tail_reject_publication_test.go` 覆盖 tail + orphan + retired segment + intent 组合、cleanup fault、malformed intent、CURRENT 未同步崩溃窗口、checkpoint/ordinary publish 继承退休布局、最终 Ready sync failure 和重复 reopen。CLI/恢复说明与测试一致；完整证据见 [R08 验证报告](validation/REFACTOR_R08_2026-11-16.md)。

## 6. 验证与工程支撑

### R09：统一验证入口和证据 manifest

**现状。** README 已给出 test/race/vet 命令，架构依赖测试也存在；本地树未见统一脚本/Makefile/workflow，不能据此判断外部 CI 不存在。

**交付。** 一个可在本地和 CI 复用的入口，区分快速检查、完整检查、race、benchmark、native durability。附机器可读证据清单：R 编号、场景/命令、Go/OS/arch、提交及 dirty fingerprint、结果、skip 原因、产物位置。工作树指纹覆盖本次涉及的修改与未跟踪源码，排除凭证及运行数据。

**验收。** 新环境可按同一入口重复执行；失败退出码可信；关键 lane 未执行不能显示全绿；报告可追溯到实际源码。最小入口先落地，完整清单随各阶段扩充，不阻塞 R01/R02 的直接修复。

**阶段 A 交付。** `go run ./scripts/verify quick|full|race|benchmark|native|fingerprint` 已落地，提供逐命令结果、skip 记录、HEAD/dirty 元数据及包含现有 embed 资源的 `ate-source-v2` 内容指纹；起止指纹不同则失败。benchmark 使用固定一次迭代验证 corpus 和证据链，且通过 JSON 事件流保留 benchmark skip；native 运行本机真实目录 IO 的 storage 测试。完整 full/race、benchmark/native 均已实际执行。使用方式与范围及本次证据见 [VERIFY.md](validation/VERIFY.md) 和 [R09 验证报告](validation/REFACTOR_R09_2026-11-17.md)。真实模型评估、Linux ext4 reboot/full-disk 和设备掉电仍需要外部环境或硬件，不能由本机 lane 冒充；skip 策略治理由 R26 继续处理。

### R10：复合事务 publication/recovery 故障矩阵

**现状。** [storage/journal_fault_test.go](../storage/journal_fault_test.go)、[engine/agent_publication_fault_test.go](../engine/agent_publication_fault_test.go)、[engine/graph_join_publication_test.go](../engine/graph_join_publication_test.go) 等已有保障。缺口是跨领域组合和故障位置没有统一覆盖表，不能表述为“没有故障测试”。R09 后已建立第一版选择性矩阵，并补充 Graph Join 在 AuditFull compaction 后发生 publication unknown 的四组合回归。

**交付。** 复用现有 fault IO，列出 Work/Agent+Knowledge+Continuation、child authority/result acceptance、message consume、Graph 多根更新、checkpoint/compaction/reopen 的场景矩阵。每类选择必要故障点，避免无意义的全排列。当前矩阵和新增回归见 [R10 验证报告](validation/REFACTOR_R10_2026-11-17.md)；Query result acceptance、Work partial segment write 以及 child authority + parent result acceptance 均已补充 old-or-new 回归。

**验收。** durable ack、明确拒绝、unknown、fenced 分别有预期；旧状态或完整新状态可恢复，无半组 RootSet；重试 receipt 不重复执行；当前/历史公开读取一致；关闭失败 ownership 可恢复。

### R11：独立 oracle 的覆盖扩充

**现状。** [engine/oracle_test.go](../engine/oracle_test.go)、[engine/independent_oracle_test.go](../engine/independent_oracle_test.go)、[engine/independent_knowledge_visibility_test.go](../engine/independent_knowledge_visibility_test.go) 和 [tests/reference/receipts.go](../tests/reference/receipts.go) 已有独立模型。

**交付。** 先记录哪些不变量已被独立验证，再补 Graph fork/join、Memory withdraw/dependency、Knowledge import/历史 cut、跨域 disclosure 与五根原子组合。模型使用简单映射和显式状态机，不调用被测 reducer/replay 生成期望。

**验收。** 固定随机种子与可缩减操作序列；正常、拒绝、reopen、checkpoint/full replay、compaction 结果可差分；故意注入语义偏差时 oracle 能报错。测试数量不是验收指标。现有 map oracle 保留种子 `1942703`；新增跨域用例采用确定性最小序列，新的领域随机生成器按 R11 报告中的原因和触发条件明确延期，不宣称具备自动缩减器。

**交付。** 2026-11-17 完成第一版选择性扩充。新增五根 RootSet 逻辑 slot、
Knowledge import/history cut、Memory dependency withdrawal 和 Transfer 跨域
disclosure 四组独立 oracle；均使用固定、可缩减序列，覆盖成功、拒绝、retry、
reopen、checkpoint/full replay 和 compaction 的适用路径。复核后补齐模型直接驱动
比较器、真实拒绝、checkpoint 命中计数、恢复后的 receipt/disclosure 复查和错误结果负控。
既有 Graph、Receipt、
AgentProfile 和 TaskTrace overview 独立模型不重复扩展。详细测试、边界和待延伸
条件见 [R11 验证报告](validation/REFACTOR_R11_2026-11-17.md)。

### R12：同一公共场景跨 transport 执行

**交付。** 统一场景 corpus，分别接入 embedded facade、真实 Unix service/client，以及适合协议边界的 net.Pipe harness。复用已有 service/client 测试，不强迫只在真实 Unix 上成立的身份/权限场景运行于 net.Pipe。

**验收。** 同一命令的 receipt、错误类别、historical anchor、分页、撤回和超大结果行为一致；额外覆盖 session principal 绑定、foreign view、一次性 grant、取消、断连、未知提交重试。使用能力矩阵标记不适用场景。

**交付。** 2026-11-17 完成代表场景扩充。State/View、TaskTrace、Graph query 和 domain corpus 已加入 embedded/pipe/unix 对照；新增真实 durable ACK 丢失后的重连 receipt/retry 与公共 content 输出预算拒绝。三包普通/race 通过，定向 verbose 确认 9 组共享场景和 3 项独立 Unix 场景均未跳过。能力矩阵区分共享对照、transport-only、Engine 分层证据和未覆盖组合；不承诺所有 API 全排列。详见 [R12 验证报告](validation/REFACTOR_R12_2026-11-17.md)。

### R13：统一观测接口

**交付。** 默认无额外依赖的可选 observer，覆盖 admission 等待、prepare/P1 时间、unknown/fence/corruption、checkpoint 命中/suffix replay、cache hit/eviction、扫描对象数/字节、pin 等待和常驻内存估计。

**边界。** 不改变提交结果，不在核心锁内执行任意用户 callback；不记录模型内容、密钥或高基数主体标识。明确丢弃事件、阻塞、重入和 observer 失败契约。

**实施选择。** 使用可选的有界观测队列，宿主主动 Drain；生产者不等待消费，满或竞争时丢弃并计数，不创建用户 callback 或后台 worker。默认关闭。指标只复制低基数标量，常驻估计明确限定来源，不将 cache 估算冒充进程总内存。

**验收。** 无 observer 开销有基线；慢/失败 observer 不阻塞发布和读取；指标能关联测试场景而不成为第二份状态 authority。

**实现与定向证据。** `observation.Collector` 已接入创建/恢复、admission、prepare/P1、提交结果、维护、公共 read cut、两个 TaskTrace cache 和 pin 等待；state scan 可显式接入。定向普通/race 与无消费者、丢弃、checkpoint/full replay、故障类别测试通过，mailbox disabled/enabled/full 均零分配。资源记账限于 cache，不宣称全引擎内存；更全面的容量计量继续由 R24 承担。详见 [R13 报告](validation/REFACTOR_R13_2026-11-17.md)，统一全量验证见联合批次。

**完成证据。** 全库 full 和显式 20 分钟预算的 race 均退出 0，起止源码指纹
`bd06a8f16842725c1c9b69d928dc4e6158e05b4153dbbc8530e6cc6ad315dc57` 一致。
首次默认 10 分钟整包超时保留，不计为通过；只有三个真实模型 opt-in 测试未执行，
无 unexpected/Unix skip。详见[联合验证批次](validation/REFACTOR_R11_R13_R26_R27_2026-11-17.md)。

### R26：测试同步和环境证据

**交付。** 将确实用于等待就绪的固定 sleep 改为 channel/barrier/deadline 条件；不机械删除模拟时间流逝的 sleep。集中记录 Unix/native/full-disk/reboot/race 的运行条件与允许 skip 清单。

**验收。** 重复运行关键并发测试稳定；未满足环境时明确标记“未验证”；CI 或本地报告不把 skip 当 pass；原生 IO 与设备掉电证据不互相替代。

**实现与定向证据。** 删除 Unix listener 已建立后的启动 sleep，普通/race 各重复 20 次且无 skip。runner 依据 mode/package/test/reason/环境条件匹配窄 allowlist；未知 skip 使入口非零，允许环境 skip 仍未验证，no-test-files 和未选择 lane 分开记录。实际 CLI 子进程负控与 runner 单测/race/vet 通过。详见 [R26 报告](validation/REFACTOR_R26_2026-11-17.md)。

### R27：registry、规范与验证报告防漂移

**证据。** [formats/registry_test.go](../formats/registry_test.go) 的协议分派扫描依赖固定 service 文件列表；后续移动 handler 可能降低检查覆盖。历史 benchmark 报告和旧重构计划也可能被误读为当前状态。

阶段 A 又确认 `formats/registry.json` 及 registry 说明仍列出已删除的 Agent snapshot 格式。2026-11-17 已将该条目替换为实际 `agent.record-state` v1，增加当前常量对照与旧 family 拒绝检查；Snapshot API DTO 保留。

**交付。** 方法清单检查覆盖所有有效 dispatch 来源，或使用显式且可校验的统一声明；registry 仍作为验证资产，不代替 runtime decoder。报告标明源版本、命令、历史/当前属性；旧格式删除时同步 fixture、API 和格式说明。历史报告保留原始测量含义。

**验收。** 新增/移动路由但不更新声明会使检查失败；文档引用的当前符号可定位；历史符号明确标成历史；主入口指向本文，其他报告不再各自维护互相矛盾的待办。

**实施。** 路由检查改为发现 service 包全部非测试 Go 文件，包含 `.Method` switch 与 equality 分派；跨文件移动、改变量名、未登记新增方法、删除声明方法和动态 case 均有 fixture 验证。已核对 README/旧 baseline/M6 状态的当前与历史入口。范围及验证见 [R27 报告](validation/REFACTOR_R27_2026-11-17.md)。

## 7. 容量、读取与性能工作

本节的“待测量”要求产出基线和决定。规模用小、中、接近现有上限三级，至少记录操作延迟、对象读取次数/字节、分配、峰值内存和 journal 增量。尚无机器无关的延迟 SLO，不能事后选取阈值来宣称优化成功；测量后、实现前在条目中写下目标。

**测量资产盘点（2026-11-17，仅静态核对）。** 优先复用 `BenchmarkWorkAgentImagePreparation`、
`BenchmarkTaskTraceProfile`、`BenchmarkTaskTraceScopeLookup`、`BenchmarkNativeStore` 和
`BenchmarkNativeScale`。它们分别只覆盖私有 prepare、profile 构造、已构造 scope 查找或
原生 IO 总量；不据此认定 R14、R16–R25 完成。新增测量按 Agent、Memory、Graph、
公共读取/闭包、维护/恢复、adapter 六组复用 fixture；实际命令须明确 package，
当前统一 benchmark lane 不自动包含 engine、memory 和 adapter 的基准。
`B/op` 是累计分配，GC 前后差值只是采样点的存活堆，均不等于峰值内存。
峰值若采用周期采样，报告采样间隔和遗漏短暂峰值的限制；若采用进程 high-water 指标，
明确它包含 runtime、fixture 和非被测阶段。压力测量与全库 race 串行执行，避免竞争资源
使前后耗时不可比。历史 legacy 对照只保留其当时含义，不重新引入已删除格式来造基线。

### R14：Agent 准备成本不再被总状态无条件放大

**证据。** [agentprofile/command_apply.go](../agentprofile/command_apply.go)、[agentprofile/record_image.go](../agentprofile/record_image.go)、[engine/agent_record_storage.go](../engine/agent_record_storage.go) 仍有全 image 构造/校验；[engine/work_agent_image.go](../engine/work_agent_image.go) 已复用 Work 内 catalog，持久层也已只写变化 key。不能把“磁盘增量”当作“CPU/内存增量”。

**任务。** 分别测单命令 cold、连续命令、Work warm、多 record 变化及多批次失败。若总状态成本主导，设计 mutation delta + 必要的关系索引，逐步减少全量编码、恢复和 catalog 拷贝；不要再引入旧 snapshot cache。

**基准口径修正与测量准备。** `workImageFixture` 返回的 `profileBytes` 是
`len(builder.staged[profile])`，即 record-root envelope 大小；已有 benchmark 的
`profile-B` 已更名为 `root-envelope-B`。新增 `BenchmarkR14AgentPrepareGrowth`
分别记录 `RecordImage.Size()`、record 数量、root envelope、源对象读取和已编码事务字节，
覆盖 16/128/4000 groups 下的单命令、Work 内 cache 开关和后段失败。
4000 groups 对应初始 16000 records，接近记录数量上限，不是接近 payload 字节上限。
fixture 使用内存编码对象源，不测真实文件 IO、发布或峰值内存；尚待执行并结合其余维度作决定。

**基线结果（2026-11-17）。** `BenchmarkR14AgentPrepareGrowth` 使用
`-benchtime=100ms -count=3 -cpu=1` 在 Darwin/arm64 Apple M1 通过。16/128/4000
groups 对应 64/512/16000 records；单命令约为 1.8–2.8ms、14.1–19.2ms、0.40–0.42s，
累计分配约 1.9MiB、14.7MiB、450MiB。4000 groups 下 Work uncached 约 2.94–6.80s、
约 3.15GiB 累计分配，warm 约 0.87–1.04s、约 0.99GiB；后段失败仍约 0.81–0.85s、
约 1.02GiB。对象读取和事务字节也已写入 benchmark 输出。该结果证明全量 record
准备是增长成本，但尚未证明 mutation-delta 能在完整关系校验、失败隔离、RootSet、
receipt、transaction bytes、reopen 和 full replay 上保持等价。当前决定不把 delta
接入 canonical production path，不新增第二份 Ledger 或旧 snapshot cache；保留现有
全量 `ApplyCommandRecords` 作为唯一语义 authority。只有 shadow delta 通过同源
equivalence、recovery 和失败矩阵，且关系索引成本低于节省的 clone/encode/validation
成本时，才重新评估 command whitelist 方式的生产接入。真实文件 IO、峰值 RSS 和发布
成本仍未测量。

**验收。** 与 canonical 全量路径结果一致，跨记录 lifecycle/authority 校验不被跳过；失败不污染其他 Work；相同改动量下的总状态增长曲线得到改善，且额外常驻索引成本被记录。

### R15：合法大状态可以通过有界接口读取

**证据。** Agent record 上限为单条 4 MiB、16,384 条、合计 256 MiB；本地协议完整 JSON frame 上限为 16 MiB，GetAgentState 无分页。现有 oversized response 安全拒绝是正确行为，但不能满足大状态正常读取。

**交付。** 提供受固定 anchor 约束的分段/分页读取，优先评估 record catalog 分页及 task/execution summary；明确输出是完整可重组 state 还是投影，不能把片段冒充有效完整 Snapshot。单条记录的 JSON/envelope 放大也纳入预算。

**本机实现（2026-11-17）。** 新增 `AgentRecordReader`、8 MiB/256 条上限、
`agent/record-read-v1` capability、Unix/pipe service 路径和 `ReadFullAgentState`
客户端聚合。每页认证完整 catalog 与 disclosure，continuation 只作一致性 cursor；
旧完整读取保持 16 MiB `ResourceLimit` 语义。定向普通/race、registry/路由/指纹回归
和统一 full/race 通过；真实约 18 MiB Store 状态可经 Unix SDK 重组，当前实现和证据详见
[R15 验证报告](validation/REFACTOR_R15_2026-11-17.md)。

**实施前分析（2026-11-17）。** 优先独立的 canonical record 分页方法，保留原完整 `GetAgentState` 契约；复用 `RecordImage.RecordKeys/RecordAt`、历史 root 认证和全 Snapshot disclosure 检查，SDK 聚合后用 `DecodeRecordImage` 重组校验。只返回 Task/Execution 摘要不足以覆盖 Query、消息、Continuation 等完整状态。单条 4 MiB raw record 的 base64 约 5.6 MB，页 body 可选 8 MiB 硬上限并由 service 最后检查完整 envelope；token 绑定 owner/space/anchor/record root/limit/byte budget，每页重查当前撤回。首版允许完整认证 catalog，不宣称解决 R20 的分页前物化成本。

**契约细化。** 首版固定页 body 上限 8 MiB，最多 256 条记录，令牌绑定版本与有效 limit；不开放可变 byte budget。新增独立 capability，仅实际支持读取和最终 delivery 检查的 backend 宣告。分页固定 durable anchor，不使用 `ViewRef`，也不绑定 session；同身份重连可续读。每次 RPC 的 one-shot delivery grant 仍按现有 session 契约消费。SDK 检查跨页 metadata 一致、record key 严格递增、总数/字节一致，再进行完整 record image 解码；这些检查不构成针对不可信服务端的 Merkle proof。

**验收。** 大于单帧的合法状态经 Unix SDK 可完整重组或按契约查询；每页含 frame budget；token 绑定 owner/space/anchor/filter；并发写不混页；撤回、取消和 token 篡改正常拒绝，同身份断线重连可续页；完整快照接口保留明确 ResourceLimit，而非简单放大帧。原先笼统的“session/view 失效拒绝”不适用于 durable anchor 分页，失效检查限于每次 RPC 的内部 delivery grant。

### R16：Memory 全量 mutation 与撤回成本

**证据。** [engine/memory.go](../engine/memory.go)、[memory/command.go](../memory/command.go)、[memory/snapshot.go](../memory/snapshot.go) 存在整 partition restore/clone/encode；withdraw 建立反向依赖需遍历 memories/paths。read-cut 已缓存部分 policy，不能概括为每次访问都全量扫描。

**基线结果（2026-11-17）。** 新增纯领域 benchmark，使用 64 KiB、1 MiB 和约
89.3% `MaxSnapshotBytes` 的合法 snapshot，覆盖 ApplyCommand、Get/Search、
ReusableMemoryFor、AccessPolicy、Encode/Restore 和 RetractSource。near 档约 24,552
memory records、3 paths、29,970,435 raw bytes：ApplyCommand 约 2.54–2.65s/op、
2.907GB/op；Encode/Restore 约 1.45–1.65s/op、1.729GB/op；RetractSource 约
1.23–1.30s/op、1.289GB/op；AccessPolicy 约 45.9–204.9ms/op、39.1MB/op。
mutation 基准包含每次从固定 baseline `RestoreSnapshot` 的成本，读基准不修改 fixture。
这些结果证明纯领域全分区 clone/encode/restore 和撤回闭包是增长成本，但尚未决定
引入 immutable records 或反向索引；Engine 恢复、policy admission、真实 journal
增量和峰值内存仍待第二阶段分层测量。

**任务。** 测普通追加、更新、单点/级联撤回和 policy projection；证明 journal 写放大及 CPU 成本后，决定是否迁移到 immutable records 与可验证反向索引。迁移须覆盖纯领域 durable adapter 和 Engine 路径。

**验收。** source ban、successor、ExperiencePath 和历史 access/reuse 语义一致；增量索引可重建验证；重复/循环依赖、失败原子性有独立模型；未达到触发条件时保留 snapshot 并记录容量上限。

### R17：Graph 历史 root membership

**证据。** [engine/graph.go](../engine/graph.go) 的 `graphDomainRootKnown` 合并 history/baseHistory、排序 LSN、读取历史 RootSet，最坏成本随历史增长。

**任务。** 测命中当前、命中早期、不命中及多 space 场景；必要时从已认证 publication 建立 `(space, domain root)` membership 索引。索引须受当前验证前缀约束，staging 不得看到未来提交的 root。

**验收。** arbitrary radix、其他 slot/space root 仍拒绝；Work 私有历史、reopen、checkpoint、full audit 一致；缩短查找不能牺牲 root 的发布证明。

**第一阶段基线（2026-11-17）。** 新增真实 Engine API fixture，覆盖 current、early、miss、forged entries、跨 space 和历史 anchor 语义。history 目标为 16、256、1024，因真实 publication fixture 构造成本过高，正式测量为 16、64、128，并在报告中保留 reduced tier 标识。history=128 的 current 查询约 24.5–26.8ms/op、约 1.71MB/op；miss 约 3.20–4.41ms/op、约 1.20MB/op。结果确认当前实现按已认证历史前缀扫描，尚不足以证明 membership index 的收益或安全重建方式。

**决定。** 暂不把 membership index 接入 production path。若重新评估，必须先证明索引能够从 authenticated publication prefix、checkpoint/replay 和 staging/reopen 路径重建，并保持 historical root、space 隔离及 full audit 一致；当前证据只完成基线，不代表产品上限。

### R18：Graph descendants 和 join 全量扫描

**证据。** [engine/graph_queries.go](../engine/graph_queries.go) 的无 cursor descendants 通过 `graphAllRevisions` 构造 reverse adjacency；[engine/graph_join.go](../engine/graph_join.go) 验证 revision/current domain 时扫描状态。已有 walk/scan limits，问题是小 page 仍可能有大输入成本。

**任务。** descendants 与 join 分开测量。前者评估随 revision 发布原子维护的 child index 或 branch-scoped 遍历；后者区分必须检查的完整输入和可借助认证 diff/index 避免的重复读取。

**验收。** historical anchor 不返回未来 children；StateParent/JoinParents、共享祖先、冲突和顺序保持；page=1 的无关图读取有明确预算；join 不因跳过检查接受伪造 Entries。若保留全图操作，必须明确复杂度与管理用途。

**第一阶段基线（2026-11-17）。** 新增真实 Engine API fixture，覆盖 deep、wide、shared descendants 的 full/page=1、带 128 个无关 revision 的 page=1，以及 8/128 entries 的 join conflict/no-conflict。descendants full 约 2.58–12.44ms/op，page=1 约 1.55–2.29ms/op；128-entry join 约 2.97–4.28ms/op。page=1 仍执行完整输入认证和图扫描；当前 descendants 路径没有向 `state.Entries` 传 observation collector，因此 scan 指标为 0/0，不能解释为没有扫描。原计划的 1024 unrelated revisions 和 1024 join entries 因真实 fixture 构造成本过高缩小为 128，属于测量边界，不是产品上限。

**决定。** 暂不接入 child index、branch-scoped traversal 或 join shortcut。后续若实施，必须分别证明 historical anchor 的 child 可见性、publication/reopen/checkpoint 可重建性和 forged Entries 拒绝仍成立；join 不得通过跳过完整 state/entry authentication 换取速度。当前保留全图查询语义，并把其 page=1 的全输入成本作为管理查询预算约束。

### R19：TaskTrace scope fan-out

**证据。** [engine/task_trace_profile.go](../engine/task_trace_profile.go) 展开 task 的 execution subtree；图遍历已有边/深度限制，但 scope key 构造需要独立评估。

**任务。** 用深链、宽树和共享引用测量 scope 生成、事件筛选和分页成本，增加独立且可解释的 scope budget，或采用分段索引查询。保持 query-only 不制造 Task/Execution。

**验收。** 超限可预测、可取消，不生成误导性的完整结果；分页稳定；scope cache 不跨 principal/历史 cut 复用权限；对已有小任务无明显回退。

**第一阶段基线（2026-11-17）。** 新增内存层 benchmark，分开测量 immutable
`taskTraceProfile.scope` 的 deep/wide/shared topology、事件 metadata filter
与 32 条小页、shared ancestry overview，以及 causal evidence 的多 agent
fan-out。scope 覆盖 32/256/1024 深度或宽度；overview 输入深度覆盖 32/256/1024，
fixture 实际遍历包含 source，因此对应 33/257/1025 个 ancestry 节点；selected
page 覆盖 1/32/128；evidence 固定约 2050 path events，拆分
1×2048、16×128、64×32 三种拓扑。warm scope 随 topology 增长，deep 1024
约 0.35–0.39ms/op；overview ancestry 1024 下 page 1/32/128 仍约
0.48–0.66ms/op；causal evidence 约 2,050 path events 时约 3.6–5.5ms/op、约 2.5MiB/op
累计分配。小 page 没有消除 ancestry/path 的发现成本。

**决定。** R19 第一阶段只完成容量基线，暂不接入独立 scope budget、分段索引或
第二份 topology authority。当前没有机器无关延迟 SLO，且正式 benchmark 是
profile/query 内存层 fixture，不包含 journal、对象解码、authorization、
publication 或峰值 RSS。后续先结合 R20 物化和 R24 峰值内存，再决定是否实施；
任何 cache/index 必须绑定 principal、historical anchor 和 authenticated
profile root，query-only 不能制造 Task/Execution。该阶段直接调用
projection/index/evidence 内部方法，没有覆盖 `GetTaskTrace` 的完整历史
anchor/fence、delivery gate 或真实 profile cache/replay。详细范围和限制见
[R19 验证报告](validation/REFACTOR_R19_2026-11-17.md)。

### R20：Node/View/Content 的物化边界

**任务。** 按具体方法记录分页前读取了哪些 catalog、对象和字段；优先让列表只取 metadata、详情按需读取，必要时引入持 anchor 的 iterator。

**证据入口。** [engine/node_queries.go](../engine/node_queries.go)、[engine/node_explore.go](../engine/node_explore.go)、[engine/views.go](../engine/views.go) 及相关 Content 路径。不得仅凭文件规模决定重构。

**验收。** stable order、continuation binding、complete_layer、历史 View 和 disclosure 完整；小页的内存/对象读取有界。Content identity 依赖完整 digest；不能将未认证裸 range read 当成等价替代。

**第一阶段基线（2026-11-17）。** 新增真实 Directory Engine benchmark，使用
32/256/1024 条 content state records，覆盖 current/historical View、List/Search/
Trace page=1、GetNode、ReadContent、GetViewState 和 GetViewEntries。1024 records
时，List page=1 约 34.0–35.3ms/op、约 21.9MB/op 累计分配；Search 约
33.7–36.5ms/op；Trace 约 15.0–15.8ms/op；GetViewEntries 约 11.0–13.3ms/op。
三类 page=1 查询的代表性读取约 2,172 objects/420,405 bytes；GetViewState
保持约 8 objects/2,747 bytes。ReadContent 的 range 输出仍支付完整 projection
和 digest 路径，GetNode 也会重建 catalog。

**决定。** 当前证据确认“小页只限制输出”是实际成本边界，但尚未证明可以安全
引入通用 iterator。暂不修改 production query。后续应按 metadata-only list、
详情按需读取、完整关系认证和 content digest 四类分别设计，先绑定 historical
anchor、ViewDigest、disclosure 和 continuation，再评估持 anchor iterator 或
分段 catalog；不能通过跳过 complete layer、authorization 或 digest 验证换取
速度。完整范围和限制见 [R20 验证报告](validation/REFACTOR_R20_2026-11-17.md)。

### R21：复用历史结构闭包，不复用授权结论

**证据入口。** [engine/derived_disclosure.go](../engine/derived_disclosure.go)、[engine/content_closure.go](../engine/content_closure.go)、[engine/task_trace_provenance.go](../engine/task_trace_provenance.go) 及 annotation/investigation/summary 读取。

**任务。** 先测重复解析、遍历和解码占比，再决定是否抽取共享只读 traversal 原语或按认证 root 缓存结构事实。复用之前完成 R06 的引用语义划分。第一阶段已测 Summary provenance projection、三类 Node disclosure JSON decode，以及真实 Directory Engine 的 `GetNode`/`GetNodeContext`；暂不引入 production cache。

**验收。** cycle、depth、edge、byte、cancellation 预算保持；结构 cache 不保存 authorization 结果、reader 或 pin；warm cache 后撤回立即生效；不同领域的证据语义不被统一抽象抹平。当前第一阶段已确认 projection 解码和完整 Node 物化存在重复成本，但真实 nested Summary/Annotation/Investigation state IO、reopen、withdraw、AuditFull compaction 对照仍待第二阶段，不将“已测量”写成“已优化”。

### R22：compaction/checkpoint IO 与证明复用

**任务。** 拆分计量源物理验证、semantic replay、copy、目标验证、publication recheck 和 checkpoint 编码。只有重复工作占主要成本时，才复用带 source identity、prefix digest、transaction boundary 的认证结果。

**第一阶段交付。** 已新增 `engine/r22_maintenance_benchmark_test.go` 和
[R22 验证报告](validation/REFACTOR_R22_2026-11-18.md)。真实 Directory fixture
覆盖 core-only、AgentProfile/EventProfile、完整 replay、checkpoint suffix、
checkpoint 创建、AuditFull compaction、compaction 后 warm open 和禁用
checkpoint 的 full replay。正式样本使用 16/64/128 records；256/1024 在
fixture 构造阶段触发当前 journal frame/resource limit，未计入正式结果。

**当前判断。** checkpoint warm open 仍执行完整物理扫描和认证，但减少已覆盖
历史的 semantic replay；compaction 后 warm open 的 `replayed=0`，而禁用
checkpoint 后会重新 replay 全部事务。当前 observation 只有总维护耗时和粗粒度
计数，不能拆分 source verification、prefix hash、projection validation、
checkpoint sync、target verification、publication install 和 cleanup。没有证据
证明 proof reuse 已经值得进入 production path，因此第一阶段不引入 proof reuse
或生产 tracing。

**验收。** CURRENT/IDENTITY/manifest 变化使 proof 失效；短写、目录同步失败、
suffix 损坏和 pin 生命周期测试通过；不能删除目标验证或最终 authority 检查来换
速度。checkpoint 仍是投影，不成为独立事实来源。详细 benchmark 范围、失败校准、
正式结果、observation 限制和源码指纹见 [R22 验证报告](validation/REFACTOR_R22_2026-11-18.md)。

### R23：adapter 锁内 IO

**证据。** journalstore 的 append/read 和 graphstore 的 StateReader 调用受到 adapter 总锁串行化；部分 callback 已有禁止 reentry 的契约。主 Engine `e.mu` 已避免跨 P1 持有，writerMu 跨 IO 是有意的单 writer 保护。

**第一阶段交付。** 新增 `journalstore/r23_adapter_lock_test.go` 和
`graphstore/r23_adapter_lock_test.go`，确认 Append publication IO 和
StateReader IO 期间 adapter 读接口保持阻塞。详细证据见
[R23 验证报告](validation/REFACTOR_R23_2026-11-18.md)。

**任务。** 测慢 IO 下读、写、Close 的阻塞；有需要时拆为锁内捕获 immutable metadata/pin、锁外读取、结果 fence 校验。不要随意释放 journal writer ownership。

**验收。** 并发 Close/reopen、cancel、compaction 和 callback 契约测试；无 stale result、悬空 reader 或死锁；没有收益则记录保留现状的决定。

**当前决定。** 当前证据证明 adapter 总锁覆盖慢 IO，但没有提供足够的等待占比
和安全拆锁方案；直接改 production path 会改变 callback reentry、private
projection 和 publication unknown 契约。因此 R23 第一阶段保留现状，后续只有在
增加 phase timing 和 candidate staging 后才重新评估拆锁。

### R24：历史索引与恢复峰值内存

**证据入口。** [engine/engine.go](../engine/engine.go)、[engine/object_index.go](../engine/object_index.go)、[engine/events.go](../engine/events.go)、[engine/task_trace_index.go](../engine/task_trace_index.go)、[engine/checkpoint_codec.go](../engine/checkpoint_codec.go)。对象数/事务数上限不等于内存预算。

**任务。** 区分权威状态、可重建索引和可驱逐 cache；测 steady state、冷恢复、checkpoint 编码和 Work 峰值。将历史规模推至配置允许的边界；超过默认限额的压力实验须显式记录配置，不称作默认能力。

**第一阶段交付。** 复用 TaskTrace profile/index 和 Agent prepare growth
benchmark，覆盖 128/1024 profile、1,000/100,000 children/events 以及
16/128/4000 groups。结果显示 bounded cache 的 warm 路径明显低于 cold
projection，但大规模 Work candidate 的累计分配继续增长。详细证据见
[R24 验证报告](validation/REFACTOR_R24_2026-11-18.md)。

**验收。** 有明确的每项预算、拒绝/驱逐策略和总量观测；receipt、historical anchor、root membership 所需证据不能被随意淘汰；考虑分段索引或流式 checkpoint 时证明重建一致。

**当前决定。** 本阶段只完成累计分配、结构 bytes 和 bounded cache 基线，不把
B/op、fixture bytes 或 RetainedEstimate 当作峰值 RSS；不新增全局 cache、增量
reducer 或强制 eviction。真正的峰值预算需要明确并发、GC、cold/warm recovery
和 runtime heap profile 后再做。

### R25：逻辑配额与实际磁盘空间分开

**证据。** MaxPublishedBytes 限制逻辑已发布 journal 数据，不自动包含所有 segment/manifest/checkpoint/quarantine、staging 和 compaction 临时副本。

**第一阶段交付。** 已通过配额、rotation、checkpoint、compaction、maintenance
和 quarantine targeted tests，明确 `PublishedEnd`、segment header/footer、
checkpoint 文件、staging、retired layout 和 quarantine 的不同生命周期。详细
边界见 [R25 验证报告](validation/REFACTOR_R25_2026-11-18.md)。

**当前决定。** 不把逻辑文件长度称作 filesystem block 占用，不新增未经平台和
产品预算约束的目录级 quota，也不把 checkpoint、quarantine、staging 和 retired
副本强行纳入 MaxPublishedBytes。未来若需要物理预算，先定义平台统计口径、
retention/cleanup 责任和 ENOSPC 行为，再增加只读 inventory 与独立拒绝策略。
rotation、checkpoint、compaction、pin 延迟回收和现有 fault tests 继续作为
正确性证据。

## 8. 分阶段执行与依赖

### 阶段 A：先封住明确故障，建立可重复基线

执行 R09 的最小入口，随后 R01、R02、R03、R05。R03 必须先完成合法 writer/read limit 分析和最小复现；未证实前不做 blanket corruption 包装。

退出条件：明确故障有回归并修复；争议错误分类有决定；当前源码和测试结果可追溯。R01/R02 不等待大型测试基础设施全部完成。

### 阶段 B：收敛格式与引用语义

执行 R04 → R06，配套推进 R10/R11；完成 R07/R08 的维护契约和 R27 对受影响格式/路由的同步。

退出条件：Knowledge 不再依赖双格式读写；policy/raw 引用边界明确；格式断代、历史查询、import、recovery 和 cleanup 行为均有验证。

### 阶段 C：公共读取与验证基础

完成 R12、R13、R15、R26，并补齐 R09–R11 的覆盖台账。R15 的 API 必须从 Engine 到 service/client 同步交付。

退出条件：合法大 Agent 状态有有界读取路径；关键公共场景跨 transport 一致；测试和观测能定位后续增长成本。

### 阶段 D：按测量推进增量化

对 R14、R16–R24 建立基线，再按主要瓶颈实施；R25 同步落实存储预算。优先减少已被证明的重复工作，避免同时重写 Agent、Memory、Graph 和恢复格式。

建议链路：

```text
R09 验证与指纹 ──→ R10/R11 正确性证据 ──→ 领域增量改造
R04 Knowledge 单格式 ──→ R06 引用语义 ──→ R21 闭包复用
R01/R02 存储修复 ──→ R07/R08 维护边界 ──→ R22/R25
R12 公共一致性 ──→ R15 大状态读取、R20 按页物化
R13 观测 ──→ R14/R16–R24 基线与资源预算
```

退出条件：每个容量项有测量及“实施完成”或“明确延期”的决定；已实施项有前后同源对照；无以扩大上限掩盖复杂度的问题。

### 阶段 E：整体验收

完成 R27，重跑第 10 节全量检查，检查所有条目状态、公开文档和格式 corpus。

只有所有必做项完成、待证实项已解决、容量项有明确决策，才可称“本轮整体重构收口”。若有延期，结论必须逐项列明，不能称为所有重构均已实现。

## 9. 必须保持的边界与停止条件

- 保留单一 coordinator/P1 发布 authority、receipt-first 幂等和多根原子性。
- 保留 read-cut 固定 roots/rootSet/publication、锁外 IO 和最终 delivery fence。`engine/read_api.go` 是该路径的一部分，本轮没有证据支持为拆文件而重写它。
- 保留 View 的 physical pin 生命周期；关闭前不能回收其需要的布局。
- 历史事实与当前 disclosure/reuse policy 分开。cache 不得冻结权限，不得扩大 owner/space 可见性。
- 保留 Work 私有 catalog ownership；Agentstore 不再成为第二份 Ledger/recovery authority。
- 保留 Agent record-only。`agentprofile.Snapshot` 内存/API DTO 与旧持久化格式不是一回事。
- checkpoint 保持认证投影；AuditFull compaction 不是选择性历史 GC；不删除完整物理历史的认证步骤来伪造恢复加速。
- Query-only 不生成假的 Task/Execution/Step；父子结果消费、Knowledge import、Graph join 保持各自明确语义。
- 不因文件行数、大包名称、泛化“解耦”目标而拆接口。必要抽取必须能说明减少了哪项重复语义或验证成本。
- 不扩展到模型效果、工具调度、Git 工作区合并或外部 exactly-once 承诺；这些属于宿主及外部证据。

优化测量无收益、抽象隐藏领域差异、索引增加的常驻成本高于收益，或必须牺牲认证/授权才能达标时，停止该实现并记录决定。已延期项仍保留触发条件，不从台账消失。

## 10. 验收与状态更新

### 10.1 每批验证

先执行受影响包和本项回归；存储、并发、授权变化补相应 race/fault/recovery 场景。无关的小文档修改不要求重跑所有测试。

每阶段与最终收口执行：

```sh
go test ./...
go test -race ./...
go vet ./...
go build ./...
git diff --check
```

同时检查改动 Go 文件的 gofmt。未来统一入口应包装这些命令，保留原始输出及失败退出码，不改变其含义。

涉及格式/发布的改造额外验证：CURRENT/IDENTITY binding、checkpoint 与 full replay 对照、unknown outcome、关闭/cleanup 重试、旧格式明确拒绝、reader pin 和 compaction 后重开。原生 durability lane 单独记录平台和环境；它不能替代硬件掉电认证。

### 10.2 单项完成记录模板

每完成一项，在本节添加记录，同时更新第 4 节状态和该条目的最终决定：

```text
ID：
状态：已完成 / 已否定 / 已延期
最终决定与原因：
变更范围与格式/API 影响：
验证命令及关键结果：
源码版本/工作树指纹与报告位置：
未覆盖边界：
延期触发条件（如适用）：
```

不得只写“测试通过”；性能项必须含相同 workload 的前后结果，正确性项必须指向可执行回归。若后续证据推翻本次判断，保留修订原因，不将旧推测继续列为已确认缺陷。

### 10.3 当前执行记录

2026-11-15：完成全库审计汇总与争议复核，建立 R01–R27 主台账。

阶段 A（最终验证日志时间为 2026-11-16）：

| ID  | 状态与结果                                                                 | 验证与边界                                                                                                                                                                                               |
| --- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| R01 | 已完成；metadata v2 分离 LSN/Generation，历史 receipt 稳定，旧格式明确拒绝 | 两个 adapter 多轮 compact/append/reopen、各类写入、重复请求、counter overflow；独立 store 格式断代，无数据删除                                                                                           |
| R02 | 已完成；业务错误、owner Close 错误和可重试 handle 全部保留                 | 获取失败/无删除/部分删除/取消/同步失败及关闭重试；仅改 Maintain，复用现有 cleanup 机制                                                                                                                   |
| R03 | 已完成；真实非法 radix 复现，扫描自身预算与 reader 预算分型                | 最大合法条数、最长 space、旧根读取、三批失败原子性、replay 和公开 fence；未构造总 payload 恰为 256 MiB 的 fixture                                                                                        |
| R05 | 已完成；startup/hello 统一能力计算                                         | 真实 Unix client、精简 backend、profile 开关和返回切片隔离                                                                                                                                               |
| R09 | 本机入口和最终源码批次验证已完成；外部模型与 durability 证据按环境延期     | quick、full、race、benchmark、native、fingerprint、vet、build、格式和 diff 检查通过；3 项真实模型评估、Linux 外部 reboot/full-disk、设备掉电仍需外部条件                                                 |
| R10 | 已完成；复合事务 publication/recovery 选择性故障矩阵和跨层回归已闭合       | Work、Query、Graph Join、child authority/result acceptance、partial segment write、checkpoint/compaction/reopen 均有 old-or-new oracle                                                                   |
| R11 | 已完成；四组领域与跨域独立 oracle 已闭合                                   | RootSet slot composition、Knowledge import/history cut、Memory dependency withdrawal、Transfer disclosure；固定最小序列覆盖成功/拒绝/recovery 语义                                                       |
| R12 | 已完成；代表场景跨 embedded/pipe/unix 对照                                 | State/View、TaskTrace、Graph、domain/reopen、真实 ACK 丢失重试、content 预算拒绝；三包普通/race 和明确无 skip 的 Unix 定向证据                                                                           |
| R13 | 已完成；有界、主动消费且无用户 callback 的可选观测                         | 定向普通/race/benchmark 与联合 full/race 通过；只报告已定义计数，cache 记账不代表全引擎内存                                                                                                              |
| R14 | 已延期；保留 canonical Agent record 准备路径                               | 已完成 64/512/16000 records 基线；mutation-delta 等待 shadow equivalence 和关系索引成本证据                                                                                                              |
| R15 | 已完成（本机）；canonical record 分页读取                                  | 定向普通/race 与联合 full/race 通过；三个真实模型和外部 durability 仍延期                                                                                                                                |
| R16 | 已测量；纯领域 Memory mutation、撤回和 policy 基线                         | near 档约 24,552 records、29,970,435 raw bytes；Engine 恢复、真实 journal 增量和峰值内存仍待第二阶段                                                                                                     |
| R17 | 已测量；第一阶段 Graph historical root membership 基线                     | 真实 API 覆盖 current/early/miss 和隔离边界；history 正式测到 16/64/128，未实施 membership index                                                                                                         |
| R18 | 已测量；第一阶段 Graph descendants/join 基线                               | 真实 API 覆盖 deep/wide/shared、page=1、128 unrelated、8/128-entry join；未实施 child index 或 join shortcut                                                                                             |
| R19 | 已测量；第一阶段 TaskTrace scope fan-out 基线                              | profile scope、事件过滤/小页、shared ancestry overview、causal evidence 已测；未实施 scope budget 或分段索引                                                                                             |
| R20 | 已测量；第一阶段 Node/View/Content 物化基线                                | 真实 Directory Engine 覆盖 32/256/1024 content records、历史 View、page=1、entries/state/content；未实施 iterator 或 catalog 分段                                                                        |
| R21 | 已测量；第一阶段 provenance/disclosure projection 与 Engine 读基线         | Summary 8/32/128 depth、Node 三类 JSON payload、真实 `GetNode`/`GetNodeContext` 32/256/1024 records；未实施结构 cache，授权结论仍每次重算                                                                |
| R22 | 已测量；第一阶段 compaction/checkpoint/replay 基线                         | 真实 Directory Engine 覆盖 full replay、checkpoint suffix、checkpoint creation、AuditFull compaction、compaction 后 warm open 和 full replay；有效规模 16/64/128；未实施 proof reuse 或 phase IO tracing |
| R23 | 已测量；adapter 锁覆盖基线                                                 | journalstore Append publication IO 与 graphstore StateReader IO 期间读接口保持阻塞；未修改 production 锁边界，后续需 phase timing/candidate staging 才重新评估                                           |
| R24 | 已测量；第一阶段内存、索引与 cache 基线                                    | TaskTrace profile/index 128/1024、1,000/100,000 children/events、Agent prepare 16/128/4000 groups；报告 B/op 和结构 bytes，不宣称峰值 RSS                                                                |
| R25 | 已收口；逻辑配额与物理文件长度边界                                         | 覆盖 published quota、rotation、checkpoint、compaction、maintenance、retired/pinned 和 quarantine；不实施目录级 quota 或跨平台 block accounting                                                          |
| R26 | 已完成；就绪等待和环境 skip 可执行治理                                     | 移除多余启动 sleep，普通/race 各 20 次；未知 skip 令 CLI 非零，环境/no-test-files/未选 lane 分开记录                                                                                                     |
| R27 | 已完成；格式、路由与指纹目录检查补齐                                       | 全 service 文件盘点及移动/新增负控；record-state 替代已删除 snapshot family；当前 package 与指纹范围对照                                                                                                 |

详细代码与测试证据、格式影响、源码指纹和报告位置见 [阶段 A 验证报告](validation/REFACTOR_STAGE_A_2026-11-15.md)、[R09](validation/REFACTOR_R09_2026-11-17.md)、[R10](validation/REFACTOR_R10_2026-11-17.md)、[R11](validation/REFACTOR_R11_2026-11-17.md)、[R12](validation/REFACTOR_R12_2026-11-17.md)、[R13](validation/REFACTOR_R13_2026-11-17.md)、[R14](validation/REFACTOR_R14_2026-11-17.md)、[R15](validation/REFACTOR_R15_2026-11-17.md)、[R16](validation/REFACTOR_R16_2026-11-17.md)、[R17/R18](validation/REFACTOR_R17_R18_2026-11-17.md)、[R19](validation/REFACTOR_R19_2026-11-17.md)、[R20](validation/REFACTOR_R20_2026-11-17.md)、[R21](validation/REFACTOR_R21_2026-11-18.md)、[R22](validation/REFACTOR_R22_2026-11-18.md)、[R23](validation/REFACTOR_R23_2026-11-18.md)、[R24](validation/REFACTOR_R24_2026-11-18.md) 和 [R25](validation/REFACTOR_R25_2026-11-18.md)。各报告分别标明历史全量批次与后续定向补测，不能混用源码指纹。阶段 B 的 R04、R06、R07、R08 已完成，证据分别见 [R04](validation/REFACTOR_R04_2026-11-16.md)、[R06](validation/REFACTOR_R06_2026-11-16.md)、[R07](validation/REFACTOR_R07_2026-11-16.md)、[R08](validation/REFACTOR_R08_2026-11-16.md) 验证报告。后续按台账推进最终验证和 GSEA 适配；未提交 Git。

## 11. 与既有文档的关系

- [2026-11-02 重构基线](REFACTOR_BASELINE_2026-11-02.md) 保留为当时局部拆分的历史记录；后续任务、格式策略和执行状态以本文为入口。
- [实现状态](ATE_IMPLEMENTATION_STATUS.md) 说明产品当前能做什么；本文说明下一步如何改造，两者不可互相代替。
- [V1 跨审计](validation/V1_CROSS_AUDIT_2026-11-06.md) 和 [V1 需求矩阵](validation/V1_REQUIREMENTS_MATRIX_2026-11-02.md) 保留验收证据边界；R09/R27 负责建立其与新验证结果的关联。
- [Agent record 存储验证](validation/AGENT_RECORD_STORAGE_2026-11-14.md)、[增量准备验证](validation/AGENT_RECORD_STORAGE_PREPARATION_2026-11-15.md)、[TaskTrace 索引成本](validation/TASK_TRACE_INDEX_COST_2026-11-14.md) 是相关基线，不代表本文后续性能任务已经完成。
