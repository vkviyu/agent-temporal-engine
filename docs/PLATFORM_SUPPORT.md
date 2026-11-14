# 平台支持与耐久证据

本表冻结当前原生后端的支持范围与证据等级。格式 inventory 已在 `formats/registry.json`
登记为 `release: v1.0.0`、`status: frozen`，迁移规则见
[registry 与迁移政策](formats/registry-and-migration-policy.md)。严格 V1 的其他验收仍由四文档
交叉报告判定。平台证据按下面的实际 OS/文件系统组合解释。

| 平台 | 原生实现 | 本轮证据 | 状态 |
| --- | --- | --- | --- |
| Darwin/arm64 · APFS | `storage/directory_unix.go`、`service/socket_unix.go` | Go 1.27.0，macOS 26.3.1，Darwin 25.3.0；本地目录、owner lock、Unix socket、reopen、故障注入与 host 场景；工作目录位于 `/dev/disk3s5` 的 APFS Data volume，具体全量结果见验收记录 | 支持 `file-sync-experimental-1` 的原生 OS/进程契约，设备掉电未经认证 |
| Linux/arm64 · ext4 | 同一 build tag 下的 DirectoryIO/flock/socket 实现 | Go 1.27.0 测试程序在 Colima aarch64 Linux guest 实际执行，全量测试通过；临时目录在 `/dev/vdb1` 的 ext4 Docker volume。独立 32 MiB loop ext4 的真实 ENOSPC 与保存 publication witness 的 guest 重启验证见 [Linux 原生报告](validation/V1_NATIVE_PLATFORM_2026-11-06.md) | 支持下述已测组合及证据等级，设备掉电未经认证 |
| Linux/amd64 | 同一 build tag 下的 DirectoryIO/flock/socket 实现 | `GOOS=linux GOARCH=amd64 go test -run '^$' -exec /usr/bin/true ./...` 本轮 exit 0；此命令只交叉编译各测试程序，所有“ok”均不代表测试实际执行 | 编译检查通过，运行与文件系统耐久矩阵仍待验证 |
| Darwin/amd64 | 同一实现目标 | 本轮没有独立运行或交叉编译证据 | 未验收 |
| Windows、其他 GOOS | `directory_unsupported.go`、`socket_unsupported.go` 明确拒绝原生持久目录/本地服务 | 可使用纯算法包，但不据此声称具备原生持久存储或 Unix 服务 | 未支持原生持久后端 |

## 证据等级

1. **纯算法/编解码**：独立向量、截断/翻转、规范重编码与边界测试。
2. **确定性 IO 故障模型**：短写、写/同步/rename/目录同步中断；只能覆盖该模拟模型。
3. **原生 OS/进程**：真实目录、文件锁、Unix socket、子进程退出、真实副作用与重新打开。Linux ext4 另有真实 ENOSPC、writer fencing、已 ACK 前缀恢复和释放空间后续写证据。
4. **guest 重启**：Linux ext4 上先保存已返回 ACK 的 publication witness，执行有序 Colima guest 重启，再以不同 boot ID 恢复同一前缀与 writer lock。记录见上述原生报告。
5. **设备掉电**：另需记录文件系统、挂载参数、介质、设备缓存与电源中断方式，当前没有此级证据。

`DirectoryIO` 依赖独占 owner、已存在且可信的根目录、同一文件系统内的受控路径，以及文件/目录
同步和原子替换语义。生产平台的支持声明须以实际运行的 OS/文件系统组合补齐，不能由 Go 能编译、
`fsync` 调用存在或 kill 进程测试代替。数据加密、权限映射和物理介质故障亦不由这些测试自动证明。

## 当前支持边界

- Linux 实测组合是 Ubuntu 24.04.4 LTS guest、Linux `6.8.0-117-generic` aarch64，Docker volume ext4 `rw,relatime` 和 32 MiB loop ext4 `rw,relatime`。容器镜像和测试程序版本分别记录在原生报告中。
- guest 重启经过正常 OS 停机，覆盖重启后 ACK 前缀和锁恢复。物理设备断电、宿主掉电、块设备缓存丢失和撕裂写仍须专门实验。
- Darwin 的设备同步强度、Linux/amd64 及其他文件系统组合继续按各自证据验收。D §21.5 的候选规模矩阵按实际运行的压力报告解释。
- V1 registry、兼容性 corpus 和 fail-closed 迁移规则已经存在。通用语义 store migrator 的支持范围按迁移政策声明。
