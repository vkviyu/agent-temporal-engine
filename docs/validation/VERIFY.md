# 本地/CI 验证入口（R09 / R26）

在仓库根目录执行，依赖仓库要求的 Go 工具链和 Git，无第三方依赖。
本入口只提供本地/CI 验证与证据，不修改源码、不提交 Git。

```sh
go run ./scripts/verify quick
go run ./scripts/verify full
go run ./scripts/verify race
go run ./scripts/verify benchmark
go run ./scripts/verify native
go run ./scripts/verify fingerprint
```

| 模式          | 实际工作                                                                                       |
| ------------- | ---------------------------------------------------------------------------------------------- |
| `quick`       | 改动 Go 文件格式检查、diff check、`go vet ./...`、`go build ./...`；不运行测试                 |
| `full`        | quick 全部检查，加 `go test -count=1 -json ./...`                                              |
| `race`        | quick 全部检查，加 `go test -race -count=1 -json ./...`，全量包                                |
| `benchmark`   | quick 全部检查，加固定 `-benchtime=1x` 的 `tests/benchmarks` native benchmark                  |
| `native`      | quick 全部检查，加 `storage` 的 Darwin/Linux native publication、crash/reopen 和 recovery 测试 |
| `fingerprint` | 仅生成开始/结束指纹，明确标为 `fingerprint_only`                                               |

`-json` 用于保留 Go 测试事件和识别 skip，不改变 full/race 的测试范围；benchmark
也使用 JSON 事件流，因此 benchmark 内部的 `b.Skip` 会进入 manifest，而不会被误记为
无条件通过。full 不包含 race，race 不另跑一遍普通 full。benchmark 使用固定一次迭代，
用于验证 benchmark corpus 和输出链路，不作为稳定性能基线；需要比较性能时使用独立的
`-benchtime`、`-count` 和环境记录。

`native` 运行本机 Darwin/Linux 的真实目录 IO、发布、崩溃子进程和 recovery 测试。
Linux ext4 reboot、专用小容量 ext4 full-disk 以及设备掉电仍要求外部环境或硬件，
未满足条件时由测试显式记录 skip，不能把这些 skip 解释为 durability 认证。
race 的平台/CGO 条件不满足时按命令失败处理，不自动降级。

Go 测试默认对每个包设置 10 分钟总时限。较大的 race 集合若达到该时限，
先根据日志区分整包预算耗尽与单个用例卡住；保留失败记录，再按证据调整预算。例如：

```sh
GOFLAGS=-timeout=20m go run ./scripts/verify race
```

验证器继承子进程环境；上例只改变时间预算，不缩减测试范围。
manifest 不导出 `GOFLAGS` 等环境值，应在对应验证报告中另行记录所用预算。
增大时限本身不构成通过证据，仍须检查最终状态、起止指纹和 skip 分类。

所有选中步骤都会执行，即使前序步骤失败也会继续收集结果。任一步骤失败、
日志解析失败、unexpected skip、指纹读取失败或开始/结束指纹不同，入口均非零退出。
命令失败时保留子命令退出码；命令成功但 skip 策略拒绝时，步骤 `exit_code=1`、
`status=failed`，总状态同为 `failed`。入口成功为 0，验证失败为 1，参数/报告初始化失败为 2。
`go run` 自身可能将程序非零码转换为 1；自动化应判断是否非零，
精确的子命令退出码以 manifest 为准。

报告默认创建于操作系统临时目录下独一无二的 `ate-verify-*` 目录；
终端只打印报告位置和入口退出码，不转发测试输出。可指定一个**尚不存在**的目录，
父目录必须存在，参数须位于模式之前：

```sh
go run ./scripts/verify -report "/tmp/ate validation run-001" full
```

报告必须在仓库外，拒绝已有目录、指向仓库内部的符号链接别名，
以及指向仓库内部的 `TMPDIR`。不会清空、复用或静默覆盖报告。
新目录权限为 0700，文件为 0600。CI 负责收集和保留该目录，工具不自动上传。

每次报告包含：

- `started.json`：开始时间、初始指纹、`running` 状态；中断后不能视作通过。
- `manifest.json`：schema 版本、R09、模式、Go/OS/arch、起止时间、命令参数、
  结果、退出码、日志位置、测试 skip 和未执行 lane、起止指纹与 `source_stable`。
- 每个选中步骤的独立日志；测试使用 Go JSON 原始事件日志，失败仍保留。

`selected_passed` 只表示当前选中的检查通过。真实测试 skip 带
`classification` 和 `policy_id`，由 `scripts/verify/skip_policy.go` 的静态窄 allowlist
按 mode、完整 package、完整 test/subtest 名、稳定原因与环境条件共同判定：

| classification        | 含义与结果                                                                                        |
| --------------------- | ------------------------------------------------------------------------------------------------- |
| `allowed_environment` | 明确环境条件未满足；该测试仍未验证，步骤 `passed_with_skips`，总状态 `selected_passed_with_skips` |
| `unexpected`          | 未列入策略、条件不符、原因变化或无理由；即使 `go test` 返回 0，步骤与 runner 仍失败               |
| `no_test_files`       | 仅包级 skip 且输出为该包的 `[no test files]`；保留记录，不计为真实测试 skip，不改变通过状态       |

环境例外仅覆盖列出的用例：Linux ext4 full-disk 的 `ATE_NATIVE_FULL_DISK_ROOT`
为空、reboot 的 `ATE_NATIVE_REBOOT_PHASE` 为空；真实模型的 `ATE_RUN_MODEL_EVAL`
不为 `1`；Darwin/Linux 指定 Unix socket 用例报告权限拒绝；指定 storage native、
NativeScale benchmark 与 formats 用例在非 Darwin/Linux 上报告对应平台限制。
已配置 full-disk/reboot/model 后再次报告缺少 opt-in 属 unexpected；配置错误与
测试失败不会被豁免。socket 的两处动态原因仅允许固定前缀、socket 文件名、
`listen unix ...: bind:` 结构和 EPERM/EACCES 文本，不接受任意错误后缀。
未列出的测试（包括其他包的平台 skip）一律失败，不根据 `native` 等词自动放行。

当前四处无理由 native `t.Skip()`（graphstore、journalstore、knowledge、
agentprofile）没有例外；在触发这些分支的平台上，统一验证会失败。
父子 subtest 独立判定，不继承子测试原因；重复执行后清除原因状态。
原因只在内存中匹配，manifest 仅保存固定说明、分类、策略 ID、
package/test、相关输出 SHA-256 和私有日志位置，不保存原因原文或环境值。

未选择 lane 在 `skipped_lanes` 中标为 `not_selected` / `lane-not-selected`；
设备掉电标为 `unverified_environment` / `device-power-loss-external-harness`。
它们不参与真实 test skip 判定。Linux build tag 排除的测试不会产生 skip 事件，
不能据此声称已验证 Linux。CI 若要求零真实测试 skip，应额外拒绝
`selected_passed_with_skips`，不能将入口退出 0 当成所有 lane 或环境已验证。

指纹策略为 `ate-source-v2`，定义在 `scripts/verify/fingerprint.go`；
v2 补齐当前运行时 embed 输入 `formats/registry.json`，此前 v1 报告未覆盖该资源。

- 根目录下的 `.go/.s/.S/.c/.h/.cc/.cpp/.syso/.sh/.py` 文件，
  以及 `go.mod/go.sum/go.work/go.work.sum/Makefile`。
- 以下明确允许的顶层目录中，同样的源码扩展名文件：
  `agentprofile agentstore annotation api client cmd codec engine eventlog formats graph
graphstore investigation journalstore knowledge memory model observation protocol reference service
state storage transfer tests scripts testdata`。
- 显式包含 `formats/registry.json`，它是当前非 testdata 的运行时 `go:embed`
  构建输入；不放开其他任意 JSON 文件。
- 上述范围内任意 `testdata` 子树的普通文件，不限扩展名，包含
  `testdata/journal` 中的测试 fixture。tracked、untracked，甚至被 Git ignore
  的允许范围内源码与 fixture 都参与哈希。
- 所有层级排除隐藏路径、`credentials`、`secrets`、`runtime`、`results`、
  `generated`、`node_modules`、`vendor`，以及 `.pem/.key`、
  `id_rsa*`、`id_ed25519*` 名称；因此 `.git/.env/.ate`、运行结果、
  生成数据与通常的凭证目录不参与。允许范围外的目录不遍历。

新增生产 package 时必须同步白名单；`TestFingerprintCoversRepositoryPackages`
将当前 `go list ./...` 得到的包目录与指纹范围对照，避免测试执行了新包却没有记录
其源码。`observation` 的新增与内容变更另有指纹回归。

- 允许范围内的符号链接和非普通文件直接失败；不会沿链接读取仓库外文件。

每个文件仅记录路径、权限位和内容 SHA-256；聚合指纹对策略版本、HEAD 和排序后的
文件列表做 JSON 编码后 SHA-256。新增、删除、重命名、内容和权限变更均影响指纹。
`dirty` 是 Git 全工作树的布尔元数据，不输出 `git status` 内容；
聚合指纹不包含该布尔值，因此仅修改排除范围内文件不会改变源码内容指纹。
这里不导出环境变量、凭证值、源码快照或 fixture 原文。

允许范围是有意限定的：README/docs、运行 journal、其他目录均不属于源码指纹。
新增源码根目录、非 testdata 的 embed 资源或其他构建输入时，必须先扩展策略并升级版本。
不要把实际凭证或实时 journal 放入 fixture 目录；路径规则不承担内容语义分类。
外部 module/cache、Go 环境设置、编译器之外的外部程序及硬件状态不由源码指纹认证。
manifest 的 Go/OS/arch 是运行入口的工具链/平台，不是完整环境快照。

格式检查使用 `go/format` 比较字节，只检查 Git 暂存区/工作区变更及非 ignored
未跟踪文件中的允许范围 Go 文件，跳过已删除文件，不修改任何文件。
已有但未修改的格式问题不会阻塞。diff check 分别检查工作区和暂存区，
并对允许范围内非 ignored 未跟踪文件做 `git diff --no-index --check`。
Git 的 diff 诊断可能包含源码行，因此日志仅保存诊断哈希；可本地运行
`git diff --check` 或 `git diff --cached --check` 定位，不将原文放进 manifest。
其他命令的原始诊断保存在私有日志；被测代码自己打印的内容不会由工具自动脱敏。

开始/结束指纹不同会使整个结果失败，即使命令全部退出 0。
双端采样不是文件系统快照或锁，不能证明“期间修改后又完全恢复”从未发生，
也不覆盖 `go run` 编译入口之前的修改。需要强一致证据时应在独占、冻结的 checkout
中执行；不要在运行期间编辑源码。强制中断留下的 `running` 报告或不完整 JSON
不构成成功证据。

小规模自身验证：

```sh
go test -count=1 ./scripts/verify
go test -race -count=1 ./scripts/verify
go vet ./scripts/verify
```

自测仅在临时小仓库/小 Go module 中运行：故意失败的子进程与退出码/日志保留、
真实 Go 测试失败、允许/未知/无理由/父子 subtest/benchmark skip 与 runner 状态和退出码、
no-test-files 和未选择 lane 区分、策略环境条件与敏感原因不进入 manifest、
命令成功但修改源码、未跟踪/ignored 源码和二进制 fixture、
`formats/registry.json` 纳入清单且仅修改其内容即改变指纹、
删除、排除凭证/运行数据、含空格路径、报告防覆盖/防自污染、符号链接、
仅检查改动文件格式，以及暂存区/未跟踪文件的 whitespace 错误。
测试里的 Git 初始化与提交只用于临时 fixture，不触碰工作仓库的 Git 状态。
这些自测不替代产品全量 full/race，产品全量由主流程最后执行。
