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
  - 依赖有序的滚动升级（Upgrade）：候选进程先完成握手与能力校验，
    全部就绪后才统一切流；任一环失败只拆除候选，旧组合继续服务。
  - 关闭时先停止接单（Shutdown 后新调用被拒绝），再按启动逆序通知
    各扩展收尾退出，超过宽限期才 SIGKILL。
  - JSONL 任务日志（默认 state/tasks.jsonl）记录每个任务交给了哪个
    扩展的哪个版本、哪个会话、结果状态。

## 升级语义

Upgrade 用一份候选清单替换同名扩展，期间严格区分两类进程：

- **候选（candidate）**：新拉起、尚未接单的进程。准备阶段任一候选
  退出或拒绝握手（身份不符、能力缺失、协议不兼容），升级即中止：
  已就绪的候选按逆序拆除（审计事件 rollback），未拉起的记为 abort。
  候选的任何失败都**不影响**在运行的旧进程。
- **旧进程（old）**：切流前一直是在途与新请求的唯一承接者。全部候选
  确认可用后才统一切流（cutover），此后旧进程不再接新请求，按依赖
  逆序收尾（drain）。收尾结束必须给出真实原因并记入审计：
  自愿退出（bye/exited）、超出约定收尾边界被 SIGKILL（drain_killed）、
  或宿主关闭打断（host_close）。只有全部旧进程收尾完成后才记录
  activated —— 不会提前宣布迁移完成。

每次升级、失败重试、连续升级都重新协商随机会话令牌，会话身份绝不
复用；任务日志中的 ext_version + session 可核对每个请求究竟由哪个
版本、哪一代进程处理。切换审计（validate / prepared / cutover /
drained / activated / abort / rollback）通过 Journal.SwitchRecords
导出，演示程序会完整打印。

## 有状态会话的迁移

扩展可以通过 extsdk 保存带版本号的会话状态（SetState / DeclareStateKeys），
宿主把升级拆成**准备**与**确认**两个阶段：

- PrepareUpgrade 拉起并校验候选后，导出旧会话当前保存的状态，并让候选
  报告哪些状态项能够接受、哪些无法解释（审计事件 state_check）。存在
  不兼容项时 ConfirmUpgrade 要求用户对每个会话显式给出处置方式
  （migrate 迁移可接受项 / discard 整体丢弃 / cancel 取消整个升级），
  否则拒绝生效。
- 准备期间旧会话仍在服务，状态继续变化。ConfirmUpgrade 生效前会重新
  导出旧状态，确认期间产生的变化不会遗失；切流后旧进程迟到的状态推送
  只有在版本号更新时才会前转，绝不会覆盖新版本已经接受的状态
  （审计事件 confirmed 记录用户决定）。
- 每个会话令牌的最终负责版本可通过 Manager.SessionOwner 查询：连续
  升级、候选进程退出、准备中途停止（Shutdown 中止未确认的升级）之后，
  每一代会话由哪个版本负责、是否仍活跃都有记录。
- Rollback 回退到旧构建前同样做状态检查：旧构建能接收的状态项被交还，
  不能接收的在 RollbackReport.Rejected 中列明，StateFullyAccepted 为假
  时说明进程已回退但会话状态只恢复了一部分——不会仅恢复旧进程就声称
  状态已经恢复。
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
超时隔离、崩溃隔离、重启会话令牌轮换、关闭后拒单、升级切流与审计顺序、
候选握手前退出时旧组合保留、失败重试与连续升级的会话身份隔离。
