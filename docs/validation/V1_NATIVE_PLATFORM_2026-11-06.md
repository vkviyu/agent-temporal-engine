# V1 Linux 原生平台验证 · 2026-11-06

Linux/arm64 测试程序已在本机 Colima aarch64 guest 实际执行。全量普通测试、独立小型
ext4 的真实 ENOSPC、跨 guest 重启的 publication witness 校验通过。此次关闭此前仅有
Linux 交叉编译证据的缺口，支持组合与证据等级见 [平台支持表](../PLATFORM_SUPPORT.md)。

原始日志和环境记录在 [native-platform-2026-11-06](results/native-platform-2026-11-06/)。
本轮在基线提交 `0417ac11614eafcb18279a53e2a5e03ad7d41568` 之上的未提交源树运行。
源树仍有其他严格 V1 工作，最终全量复验以最终交叉验收报告记录的源码为准。

## 环境与实际运行范围

| 项目 | 实际环境 |
| --- | --- |
| 宿主 | macOS 26.3.1，Darwin 25.3.0，arm64，Colima 0.10.3，VZ guest |
| guest | Ubuntu 24.04.4 LTS，Linux `6.8.0-117-generic`，aarch64 |
| 编译工具链 | Go 1.27.0 `darwin/arm64`，`GOOS=linux GOARCH=arm64 CGO_ENABLED=0` 生成测试程序 |
| 全量/重启 runner | `ate-v1-native-check`，镜像 `golang:1.25.1`，镜像 ID `sha256:d7098379b7da665ab25b99795465ec320b1ca9d4addb9f77409c4827dc904211`。执行的是上行 Go 1.27.0 生成的独立测试程序 |
| 原生数据与临时目录 | Docker named volume `ate-v1-native-check-data` 挂载 `/ate-data`，guest `/var/lib/docker/volumes/ate-v1-native-check-data/_data` 位于 `/dev/vdb1`，ext4 `rw,relatime` |
| ENOSPC runner | `ate-v1-full-check`，`golang:1.27.0`，Debian 13.6 容器用户空间，共享上述 Linux kernel |
| ENOSPC 文件系统 | 独立 32 MiB ext4 loop image，UUID `542845cb-1582-4670-afd3-174eb132f6f9`，`/dev/loop0`，`rw,relatime`，无保留 block，挂载于 guest `/tmp/ate-v1-full.dDtHOA/mount`，容器 `/ate-full` |

全量测试使用 [Linux runner](results/native-platform-2026-11-06/linux-test-runner.sh)，把每个
测试程序复制进 guest 容器后实际执行。测试运行工作目录为容器中的对应源码包目录，
`TMPDIR=/ate-data/tmp` 使本地目录、socket 与持久化测试使用上述 ext4 volume。

```sh
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test -count=1 \
  -exec /tmp/ate-linux-exec.sh ./...
```

此命令的 runner 会返回实际测试程序的状态。此前使用 `/usr/bin/true` 的 Linux/amd64
命令仍只构成编译证据。此次未运行 Linux/amd64 原生程序。

## 已运行检查

| 检查 | 结果与已证明范围 |
| --- | --- |
| 全量普通测试 | 所有有测试的包通过，包括 storage、engine、service、client、acceptance、reference 和 embedded/Unix host 场景，见 `linux-native.log` |
| 新 witness 文件加入后的 storage | 同一 Linux guest 实际运行完整 storage 测试程序，退出 0，见 `storage-native.log`。opt-in 测试在未给定环境变量时跳过 |
| 真实 ENOSPC | `TestNativeExt4FullDiskPreservesAcknowledgedPrefix` 通过。写满独立文件系统至真实 `ENOSPC`，下一次 Publish 返回 `ErrCommitUnknown` 且无 Publication，writer fenced。释放空间并隔离尾部后恢复原已 ACK 前缀，续写 LSN 2，第二次重开仍保留两笔事务 |
| 同 boot 负例 | create 后立即运行 verify，按预期退出 1，并报告 `verification requires a different Linux boot ID` |
| guest 重启 | `colima restart` 正常停止 Docker、guest/VZ，再启动 guest。重启后 verify 退出 0，精确 Publication 和八笔事务保留，owner lock 拒绝第二个 writer |

全量 native 日志对应添加新 opt-in 测试之前的源树。新测试使用同一物理 publisher 和
DirectoryIO 实现单独编译并执行，测试程序 SHA-256 与两个 Linux opt-in 测试源码 SHA-256
记录在 `build-witness.txt`。

## 可复核的 guest 重启见证

测试文件为 `storage/native_reboot_linux_test.go`。create 在调用 Publish 返回成功后，
把最后一笔完整 Publication、事务数与 Linux boot ID 写入 store 外的 JSON 文件。
见证文件和父目录均执行同步。fixture 的八笔事务跨八个 segment，以覆盖 rotation 后的恢复。
create 正常关闭 journal，再由测试外部操作 guest 重启。

持久 root 为 `/ate-data/reboot-root`，见证文件为 `/ate-data/reboot-witness.json`。
二者均在上述 ext4 named volume，guest 重启后保留。重复 create 会拒绝覆写已有 store/witness。

```sh
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test -c \
  -o /tmp/ate-native-reboot-linux-arm64.test ./storage
docker cp /tmp/ate-native-reboot-linux-arm64.test \
  ate-v1-native-check:/ate-data/bin/native-reboot.test
docker exec ate-v1-native-check mkdir -m 700 /ate-data/reboot-root
docker exec -e ATE_NATIVE_REBOOT_PHASE=create \
  -e ATE_NATIVE_REBOOT_ROOT=/ate-data/reboot-root \
  -e ATE_NATIVE_REBOOT_WITNESS=/ate-data/reboot-witness.json \
  ate-v1-native-check /ate-data/bin/native-reboot.test \
  -test.run '^TestNativeExt4RebootPreservesAcknowledgedPrefix$' -test.v
colima restart
docker start ate-v1-native-check
docker exec -e ATE_NATIVE_REBOOT_PHASE=verify \
  -e ATE_NATIVE_REBOOT_ROOT=/ate-data/reboot-root \
  -e ATE_NATIVE_REBOOT_WITNESS=/ate-data/reboot-witness.json \
  ate-v1-native-check /ate-data/bin/native-reboot.test \
  -test.run '^TestNativeExt4RebootPreservesAcknowledgedPrefix$' -test.v
```

create boot ID 为 `8e779063-6474-46cf-ad82-c79cdf454280`，verify boot ID 为
`49bef12f-2125-4a69-a638-bbf5edf11cb2`。verify 要求 boot ID 不同，且 recovery report 的
`Verified`、`PublicationSynced`、完整 Publication 和扫描事务数均匹配。

重启期间 guest 正常清理了 `/tmp`，原 ENOSPC image/mount 因此消失，loop0 也已释放。
此次没有主动删除测试 volume。named volume 中的重启 root/witness 保留，ENOSPC 的原始
日志、容量、UUID、挂载信息和源码 hash 已存档。再次运行 ENOSPC 需重新创建专用小型 ext4。

## 与原 M6 门槛的对应及证据边界

D §2、§21.3、末尾 M1/M6 平台表要求分别记录确定性掉电模型、进程退出和真实
OS/文件系统/设备证据。A §8、§16.3、§17.3/M6、§18.2 要求同步契约与支持矩阵。
P §8 与 W 的阶段工件将平台支持和字节格式交给相应 registry/实施验收。
本次将实际 Linux/arm64 ext4、真实满盘和正常 guest 重启写入支持表。已有格式 registry
为 `v1.0.0` frozen，旧平台表中“registry 尚未冻结”的门槛已更正。

有序 guest 重启经过 OS flush/unmount。其实际证据覆盖重启后 publication 和锁恢复。
设备断电、宿主电源中断、磁盘缓存丢失、实际块设备撕裂写、macOS `F_FULLFSYNC` 的设备
保证仍需专门证据。确定性故障模型与 native 子进程退出继续由各自测试拥有。
本轮也未提供 Linux/amd64、Darwin/amd64 或 ext4/APFS 以外文件系统的原生运行证明。

D §21.5 的规模是候选实验输入，实际规模与开销按既有压力报告和最终验收记录解释。
真实 ENOSPC 证明明确的磁盘满恢复路径，不能推导全部规模下的吞吐或空间上界。
