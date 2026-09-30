# 应用扩展加载系统（本地子进程）

Go 实现的子进程式扩展框架。宿主应用只执行**本地预构建、可信的扩展二进制**，
不下载、不加载任何远程代码。扩展通过 stdin/stdout 上的换行分隔 JSON
消息通道与宿主通信。

## 架构

- internal/protocol — 线协议：消息信封、协议版本协商。
- internal/extsdk — 扩展侧 SDK：握手、能力注册、任务分发、优雅退出。
- internal/host — 宿主侧管理器：
  - 启动前检查：依赖拓扑排序、能力兼容、重复提供、依赖环；失败即回滚。
  - 每个子进程协商独立随机会话令牌；重启换新令牌，旧会话消息直接丢弃，
    旧进程无法冒充原会话。
  - 每请求超时；进程崩溃只使该扩展的在途请求失败（状态 crashed），
    其他扩展与请求不受影响。
  - 关闭时先停止接单（Shutdown 后新调用被拒绝），再按启动逆序通知
    各扩展收尾退出，超过宽限期才 SIGKILL。
  - JSONL 任务日志（默认 state/tasks.jsonl）记录每个任务交给了哪个
    扩展的哪个版本、哪个会话、结果状态。
  - 新旧版本平滑交接（UpgradeSet）：所有新版本先启动并完成身份、
    能力与依赖（含依赖扩展同时升级）的整体校验，然后一次性提交
    任务归属；新任务只发给新一代，旧进程继续完成已接任务，发完
    bye 退出，或在宽限边界被结束。交接期间管理器持锁，任务不会
    同时发给两代进程。
  - 任一新版启动或校验失败则丢弃所有新进程、保留旧版继续服务，
    失败与成功切换都写入同一份 JSONL 日志（action=upgrade_failed/
    upgraded，含新旧版本、新旧会话与旧进程结束原因）。
  - 所有等待都有明确结束原因：drained（收到 bye）、exited（未发
    bye 即退出）、grace_killed（超宽限期 SIGKILL）、already_exited
    （此前已退出）、absent（旧进程不存在）；宿主关闭原因可通过
    Manager.ShutdownReasons() 查询。
- cmd/host — 主程序演示；cmd/textkit、cmd/hashkit 为两个自带扩展。

textkit 额外提供 sleep（慢任务，用于超时验证）和 panic（异常退出，
用于崩溃隔离验证），它们是内置自检能力，不执行任何外部输入代码。

## 运行

~~~sh
go build -o bin/host ./cmd/host
go build -o bin/textkit ./cmd/textkit
go build -o bin/hashkit ./cmd/hashkit
./bin/host          # 二进制目录默认 ./bin
cat state/tasks.jsonl
~~~

## 测试

~~~sh
go test ./...
~~~

覆盖：缺失依赖 / 依赖环在启动前拒绝、依赖启动顺序、未声明能力不兼容、
超时隔离、崩溃隔离、重启会话令牌轮换、关闭后拒单、新旧版本交接
（旧进程排完已接任务、会话身份轮换）、依赖联动升级整体校验、
新版启动失败保留旧版、宿主关闭显式结束原因（含宽限结束）。
