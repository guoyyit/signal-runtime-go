# signal-runtime-go

本地进程组内的信号传递与屏蔽语义运行时。多个受管进程共享同一个 `Runtime`，
外部调用方通过统一入口向单个进程或进程组投递信号，运行时按 POSIX 风格完成：

- 待决排队（标准信号折叠 / 实时信号排队）
- 阻塞屏蔽与解除阻塞后的交付
- 处置动作（默认 / 忽略 / 自定义处理器）
- 终止、停止、继续等默认动作
- 可中断、可自动恢复的阻塞等待
- 并发投递下的确定性
- 处置配置、屏蔽字、待决队列、停止状态的崩溃恢复

本包模拟的是进程内部的信号语义模型，不直接操作操作系统信号。

## 快速开始

```go
rt := signalruntime.NewRuntime()
_ = rt.NewProcess(100, 100) // pid=100, pgid=100

rt.SetAction(100, signalruntime.SIGUSR1, signalruntime.SigAction{
    Action:  signalruntime.ActCatch,
    Name:    "on-usr1",                 // 稳定名称，用于崩溃恢复后重新绑定
    Mask:    signalruntime.NewSigSet(signalruntime.SIGUSR2),
    Handler: func(sig signalruntime.Signal, value int) { /* ... */ },
})

if err := rt.Send(100, signalruntime.SIGUSR1); err != nil { /* ... */ }
_ = rt.SendGroup(100, signalruntime.SIGTERM)
```

## 信号编号

| 类别 | 区间 | 说明 |
| --- | --- | --- |
| 标准信号 | `SIGHUP(1)` … 31 | 采用 Linux 通用编号约定（含 `SIGUSR1/2`、`SIGCHLD` 等） |
| 实时信号 | `SIGRTMIN(32)` … `SIGRTMAX(64)` | 共 33 个，可携带随队值 |

非法编号（`<1`、`>64`）投递或配置时返回 `ErrInvalidSig`。

## 待决与排队

### 标准信号：单实例折叠

同一进程上，某标准信号只要已有一个待决实例，后续到达的同编号标准信号被折叠，
不增加待决数量（POSIX standard signals 语义）。被屏蔽期间无论到达多少次，
解除阻塞后至多交付一次。

### 实时信号：FIFO 排队

实时信号的每个实例都进入该进程的实时队列，携带投递时给出的随队值
（`SendValue` / `SendGroupValue`），不会被折叠。

### 队列上限策略（明确约定）

- 每个进程的实时队列容量为固定上限 `rtQueueLimit = 100` 个实例（所有实时
  信号编号共享该容量）。
- 当队列已满时，**新的实时信号不被接收**：`Send` / `SendValue` 返回
  `ErrQueueFull`，本次实例既不入队也不覆盖或挤掉任何已排队实例
  （不丢弃旧信号、不回绕）。调用方自行决定重试、退避或上报。
- 组投递 `SendGroup` 逐成员（按 pid 升序）投递；某一成员队列满只导致该成员
  返回错误并记录为该次组调用的返回值，**不影响组内其他成员**的投递。
- 队列上限只作用于实时信号：标准信号、`SIGKILL`、`SIGSTOP`、`SIGCONT`
  不受实时队列容量影响。
- 交付（或进程被终止）腾出槽位后，可再次入队。

## 屏蔽与处置

- `SetMask(pid, set)` 设置进程当前屏蔽字。被屏蔽的信号保持待决，解除阻塞后
  按下文“交付顺序”交付。`SIGKILL` 与 `SIGSTOP` 不可阻塞，设置屏蔽字时这两
  个位被强制剔除。
- 处置通过 `SetAction(pid, sig, act)` 配置：
  - `ActDefault`：执行默认动作；
  - `ActIgnore`：忽略，信号被丢弃，不打断等待；
  - `ActCatch`：交由自定义处理器 `HandlerFunc func(sig Signal, value int)`。
    `value` 为实时信号随队值，标准信号为 `0`。
- `SIGKILL` 与 `SIGSTOP` 既不能被阻塞，也不能被忽略或捕获：对它们调用
  `SetAction` 返回 `ErrUncatchable`，即使存在残留配置也始终执行默认动作。

### 处理器执行期间的屏蔽字

处理器执行期间的有效屏蔽字为：

```
当前进程屏蔽字 ∪ 该信号 SigAction.Mask（处理器屏蔽字） ∪ {当前信号}
```

因此：
- 处理器屏蔽字与当前屏蔽字叠加，处理器返回后恢复进入前的屏蔽字；
- **同信号处理器不可重入**：处理器执行期间，同编号信号被阻塞而保持待决，
  处理器返回后才会交付，绝不会并发重入同一处理器；
- 处理器在进程锁之外同步执行，处理器之间（不同信号）不相互阻塞状态查询。

## 默认动作

| 默认动作 | 信号（示例） | 效果 |
| --- | --- | --- |
| 终止 | `SIGTERM` `SIGKILL` 及全部实时信号 | 进程进入 `terminated`，未交付的待决队列作废，等待以 `ErrTerminated` 返回 |
| 核心转储终止 | `SIGQUIT` `SIGILL` `SIGSEGV` 等 | 本模型中效果同为终止 |
| 忽略 | `SIGCHLD` `SIGURG` `SIGWINCH` | 丢弃 |
| 停止 | `SIGSTOP` `SIGTSTP` `SIGTTIN` `SIGTTOU` | 进程进入 `stopped` |
| 继续 | `SIGCONT` | 进程从 `stopped` 恢复 `running` |

## 停止与继续

- 进程处于 `stopped` 时，除 `SIGKILL`（终止）与 `SIGCONT`（继续）外，
  所有信号——包括被捕获的信号——都保持待决，不交付、不丢失。
- 已排队的实时信号在停止期间原样保留；继续后按原规则交付。
- `SIGCONT` 的继续动作先于其自身处置生效：进程恢复运行，并按 POSIX 语义
  **丢弃所有尚在待决的作业控制停止信号**（`SIGSTOP`/`SIGTSTP`/`SIGTTIN`/
  `SIGTTOU`），其余待决信号（含实时信号）全部保留。
- 对运行中的进程投递 `SIGCONT` 仅丢弃待决停止信号，不改变其他状态。
- 进程组停止/继续按成员逐个处理，状态变化过程中不丢失任何信号。

## 进程组投递

- `NewProcess(pid, pgid)` 登记进程及其所属组；`SendGroup(pgid, sig)` 对组内
  **每个**成员投递。
- 为保证结果可重现，组内成员固定按 **pid 升序** 处理；每个成员的到达序号由
  运行时全局单调分配。
- 单成员失败（如该成员实时队列满、已终止）不阻断其余成员；组调用返回遇到的
  第一个错误。空组返回 `ErrNoProcess`。

## 交付顺序规则（公开约定）

同一进程上的交付遵循两条规则：

1. **跨信号轮转，避免饿死**：待决信号按编号在环形游标上轮转扫描，每一轮中
   每个信号编号**至多交付一个实例**；交付一个实例后游标前进到下一个编号。
   因而即使低编号信号持续大量到达，高编号信号也会在有限时间内被交付，
   低编号信号无法饿死高编号信号。
2. **同编号内 FIFO**：同一信号编号的多个实时实例严格按到达顺序（运行时全局
   单调序号）交付；随队值与实例一一对应，不重排、不重复。

标准信号由于每个编号至多一个待决实例，天然满足上述轮转；实时信号在同编号内
按 FIFO、跨编号按轮转。被屏蔽、被忽略（直接丢弃）、进程停止（除 KILL/CONT）
都会使对应实例跳过本轮而不影响游标对其他编号的推进。

## 可中断等待

提供两种等待原语：

- `Wait(pid, done <-chan struct{}, autoRestart bool) error`
- `WaitFn(pid, ready func() bool, autoRestart bool) error`

语义：
- 等待期间**完成一次被捕获信号的交付**（处理器已返回）时，等待被打断：
  - `autoRestart == false`：返回 `ErrInterrupted`；
  - `autoRestart == true`：处理器返回后重新进入等待。
- 被屏蔽而保持待决的信号不打断等待；被忽略的信号与默认动作也不打断；
  `SIGKILL` 等导致终止时等待返回 `ErrTerminated`。
- **竞争的确定性**：每一轮判定都“先判定完成条件，再消费信号中断”。
  使用 `Wait` 并以 `close(done)` 表达不可逆的完成事件时，只要 `done` 已关闭，
  即使同一时刻有捕获信号完成交付，等待也必然成功返回 `nil`，绝不返回
  `ErrInterrupted`。需要严格确定性时优先使用 `Wait` + 关闭通道；`WaitFn`
  适用于稳定的幂等谓词。

## 并发与确定性

- 每个进程由独立互斥锁保护全部信号状态；不同进程可真正并行。
- 每次 `Send`/`SendGroup` 分配一个全局单调到达序号，作为同编号 FIFO 与
  恢复重排的确定依据。
- 标准信号折叠、实时信号入队、出队交付、处理器返回后的再交付都在进程锁内
  串行判定（处理器本身在锁外执行），因此并发投递不会丢失或重复交付。

## 崩溃恢复

通过 `Snapshot()` / `Restore(state, resolve)` 或 `Save(path)` /
`Load(path, resolve)` 持久化与恢复以下关键状态：

- 每个进程的生命周期状态（running / stopped / terminated）；
- 当前屏蔽字；
- 各信号处置配置（动作、处理器屏蔽字、处理器稳定名称 `Name`）；
- 标准待决集合与实时 FIFO 队列（含随队值与到达序号）；
- 已离开待决队列、处理器尚未返回的在途（inflight）实例。

要点：
- Go 函数无法序列化，因此处理器以稳定 `Name` 记录，恢复时通过
  `resolve func(name string) HandlerFunc` 重新绑定；名称缺失或解析不到时，
  该捕获处置退化为默认动作。
- 快照在每个进程的静止点（持锁）取样，待决队列与在途集合并集完备且互斥，
  恢复后实时实例按原到达序号重排，不重复入队、不丢失。
- 对于崩溃前“可能已经执行过处理器”的在途实例，保证 **至少交付一次**
  （at-least-once）：崩溃点无法区分处理器是否已完成副作用，恢复时重新交付。
    待决（尚未开始交付）实例则为恰好一次。
- `Save` 使用“临时文件 + fsync + 原子 rename”，避免写出半完整状态文件。
- 恢复后：运行中的进程立即按当前屏蔽字尝试交付；停止中的进程继续保持待决，
    直到收到 `SIGCONT`。

## 错误一览

| 错误 | 触发场景 |
| --- | --- |
| `ErrNotFound` | pid 不存在 |
| `ErrNoProcess` | 向空进程组投递 |
| `ErrInvalidSig` | 非法信号编号 |
| `ErrUncatchable` | 对 `SIGKILL`/`SIGSTOP` 配置捕获或忽略 |
| `ErrQueueFull` | 进程实时队列达到上限 |
| `ErrTerminated` | 向已终止进程投递，或等待中进程被终止 |
| `ErrInterrupted` | 等待被已交付的捕获信号打断且未启用自动恢复 |
