package signalruntime

import "sync"

// rtQueueLimit 为每个进程实时信号排队实例数上限（队列满时的策略见文档）。
const rtQueueLimit = 100

// procState 为进程生命周期状态。
type procState int

const (
	procRunning procState = iota
	procStopped
	procTerminated
)

// queuedSignal 是实时信号队列中的一个排队实例。
type queuedSignal struct {
	sig   Signal
	value int
	seq   uint64
}

// Process 表示一个受管进程的全部信号运行时状态。
type Process struct {
	// mu 保护本进程的全部字段（runtime 中的进程注册表除外）。
	mu sync.Mutex

	pid  int
	pgid int

	// state 为进程生命周期状态。
	state procState

	// mask 为当前屏蔽字；SIGKILL/SIGSTOP 永远不会出现在其中。
	mask SigSet

	// actions[sig] 为各信号的处置配置；零值即默认动作。
	actions [signalCount]SigAction

	// stdPending 记录标准信号是否已有一个待决实例。
	stdPending SigSet

	// rtQueue 为实时信号的 FIFO 排队队列（全局到达序号已在此序中）。
	rtQueue []queuedSignal

	// runningHandlers 为当前正在执行的处理器集合。
	runningHandlers SigSet

	// cursor 为待决信号轮转交付的扫描起点，保证高编号信号不被饿死。
	cursor Signal

	// sigCh 为等待中断事件通道：每完成一次捕获信号交付，投递一个非阻塞令牌。
	// 带缓冲使得“中断发生在消费之前”也不会丢失；等待方每次循环先取空再等待。
	sigCh chan struct{}

	// termCh 在进程终止（默认终止动作或 SIGKILL）时关闭，唤醒所有等待。
	termCh chan struct{}

	// inflight 为已离开待决队列、处理器尚未返回的实例（用于崩溃恢复）。
	inflight []queuedSignal
}

// newProcess 创建一个进程骨架。
func newProcess(pid, pgid int) *Process {
	return &Process{
		pid:    pid,
		pgid:   pgid,
		state:  procRunning,
		cursor: 1,
		sigCh:  make(chan struct{}, 1),
	}
}

// setMask 更新屏蔽字，SIGKILL/SIGSTOP 不可阻塞而被强制剔除，返回旧屏蔽字。
// 调用方须持有 mu。
func (p *Process) setMask(set SigSet) SigSet {
	old := p.mask
	set.bits &^= uncatchable.bits
	p.mask = set
	return old
}

// effectiveMask 返回当前屏蔽字与正在执行处理器屏蔽字的叠加结果。
// 调用方须持有 mu。
func (p *Process) effectiveMask() SigSet {
	m := p.mask
	m.bits |= p.runningHandlers.bits
	return m
}

// masked 判断信号在当前叠加屏蔽字下是否被阻塞。
// 调用方须持有 mu。
func (p *Process) masked(sig Signal) bool {
	return p.effectiveMask().Has(sig)
}

// hasPending 判断信号是否有待决实例（标准待决位或实时队列）。
// 调用方须持有 mu。
func (p *Process) hasPending(sig Signal) bool {
	if sig.IsStandard() {
		return p.stdPending.Has(sig)
	}
	for _, q := range p.rtQueue {
		if q.sig == sig {
			return true
		}
	}
	return false
}

// removePending 移除信号的一个待决实例：标准信号清待决位，
// 实时信号移除队列中该信号最靠前的一个实例。返回是否存在待决实例。
// 调用方须持有 mu。
func (p *Process) removePending(sig Signal) (queuedSignal, bool) {
	if sig.IsStandard() {
		if !p.stdPending.Has(sig) {
			return queuedSignal{}, false
		}
		p.stdPending = p.stdPending.Remove(sig)
		return queuedSignal{sig: sig}, true
	}
	for i, q := range p.rtQueue {
		if q.sig == sig {
			p.rtQueue = append(p.rtQueue[:i], p.rtQueue[i+1:]...)
			return q, true
		}
	}
	return queuedSignal{}, false
}

// enqueue 将一个信号实例记入待决集合：标准信号折叠为单个待决位，
// 实时信号追加到 FIFO 队列尾部。队列满时返回 false。
// 调用方须持有 mu。
func (p *Process) enqueue(sig Signal, value int, seq uint64) bool {
	if sig.IsStandard() {
		if p.stdPending.Has(sig) {
			return true
		}
		p.stdPending = p.stdPending.Add(sig)
		return true
	}
	if len(p.rtQueue) >= rtQueueLimit {
		return false
	}
	p.rtQueue = append(p.rtQueue, queuedSignal{sig: sig, value: value, seq: seq})
	return true
}

// setAction 设置处置并返回旧处置。SIGKILL/SIGSTOP 不可配置，
// 即使提供了 ActCatch/ActIgnore 也强制保留为默认动作。
// 调用方须持有 mu。
func (p *Process) setAction(sig Signal, act SigAction) SigAction {
	old := p.actions[sig]
	if isUncatchable(sig) {
		p.actions[sig] = SigAction{Action: ActDefault}
		return old
	}
	if act.Action != ActCatch {
		act.Handler = nil
	}
	p.actions[sig] = act
	return old
}

// actionFor 返回信号当前生效的处置；ActCatch 缺少处理器时退化为默认动作。
// 调用方须持有 mu。
func (p *Process) actionFor(sig Signal) SigAction {
	if isUncatchable(sig) {
		return SigAction{Action: ActDefault}
	}
	a := p.actions[sig]
	if a.Action == ActCatch && a.Handler == nil {
		return SigAction{Action: ActDefault}
	}
	return a
}

// signalInterrupt 用“捕获信号交付”事件通知所有阻塞等待。
// 调用方须持有 mu。
func (p *Process) signalInterrupt() {
	select {
	case p.sigCh <- struct{}{}:
	default:
	}
}
