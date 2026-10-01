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
  - 零中断依赖升级（Upgrade）：候选扩展全部以**新进程**拉起并完成
    握手、身份、能力与依赖校验之前，宿主不切换任何流量，旧组合继续
    接单。候选全部确认后一次性切换；已在途的请求始终持有旧实例
    指针，不会被投递两次。
  - 升级失败回滚：准备阶段任一候选启动失败、退出或拒绝握手，只销毁
    候选（审计事件 abort/rollback），旧进程（即使其中有独立崩溃的）
    仍是任务属主，版本与会话都不变。
  - 旧会话收尾区分真实结束原因：bye 协商退出（drained/migrated）、
    自行退出（drain_exited）、超出约定收尾宽限被杀（drain_killed）、
    宿主关闭被杀（host_close）。只有**全部**旧会话都在约定范围内
    干净退出才记录 upgrade_complete；否则流量虽已切到新版本，也只
    记录 upgrade_incomplete 并返回错误，不会提前宣布迁移完成。
  - 每次拉起进程都生成新的随机会话令牌：连续升级、失败后重试都不会
    复用任何旧会话身份；任务日志可逐条核对请求由哪个版本/会话处理。
- cmd/host — 主程序演示；cmd/textkit、cmd/hashkit 为两个自带扩展；
  cmd/upgrade-demo 用真实扩展进程走通依赖升级、失败保留旧组合、连续
  升级和旧会话超宽限被杀三个场景。

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

### 升级演示

~~~sh
go run ./cmd/upgrade-demo
# 产物：state/upgrade-demo/switch.jsonl
#   task 行：每个请求命中的扩展版本 + 会话令牌 + 结果
#   switch 行：validate / prepared / cutover / abort / rollback /
#              drained / drain_exited / drain_killed / host_close /
#              migrated / upgrade_complete / upgrade_incomplete
~~~

扩展版本与收尾延迟可用环境变量模拟不同构建（仅本地预构建二进制）：
`TEXTKIT_VERSION`、`TEXTKIT_SHUTDOWN_DELAY_MS`，hashkit 同名变量。

## 测试

~~~sh
go test ./...
~~~

覆盖：缺失依赖 / 依赖环在启动前拒绝、依赖启动顺序、未声明能力不兼容、
超时隔离、崩溃隔离、重启会话令牌轮换、关闭后拒单。
