package signalruntime

import "errors"

import "sync"

// Runtime 是多个受管进程共享的信号运行时。
type Runtime struct {
	// regMu 保护进程注册表本身。
	regMu sync.RWMutex

	procs map[int]*Process

	// seq 为全局单调到达序号，用于进程组投递与并发投递的确定性排序。
	seqMu sync.Mutex
	seq   uint64
}

// 常见错误（骨架，后续补充）。
var (
	ErrNotFound    = errors.New("signalruntime: process not found")
	ErrInvalidSig  = errors.New("signalruntime: invalid signal")
	ErrTerminated  = errors.New("signalruntime: process already terminated")
	ErrInterrupted = errors.New("signalruntime: interrupted by signal")
	ErrQueueFull   = errors.New("signalruntime: realtime signal queue full")
	ErrUncatchable = errors.New("signalruntime: signal cannot be caught, blocked or ignored")
	ErrNoProcess   = errors.New("signalruntime: process group has no member")
)

// NewRuntime 创建一个空运行时（骨架）。
func NewRuntime() *Runtime {
	return &Runtime{procs: make(map[int]*Process)}
}

// NewProcess 在运行时中登记一个进程（骨架）。
func (r *Runtime) NewProcess(pid, pgid int) error {
	r.regMu.Lock()
	defer r.regMu.Unlock()
	if _, ok := r.procs[pid]; ok {
		return errors.New("signalruntime: process already exists")
	}
	r.procs[pid] = newProcess(pid, pgid)
	return nil
}

// RemoveProcess 移除一个进程（骨架）。
func (r *Runtime) RemoveProcess(pid int) {
	r.regMu.Lock()
	delete(r.procs, pid)
	r.regMu.Unlock()
}

// mustProcess 按 pid 查找进程。
func (r *Runtime) mustProcess(pid int) (*Process, error) {
	r.regMu.RLock()
	p, ok := r.procs[pid]
	r.regMu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return p, nil
}

// nextSeq 分配全局单调到达序号。
func (r *Runtime) nextSeq() uint64 {
	r.seqMu.Lock()
	r.seq++
	s := r.seq
	r.seqMu.Unlock()
	return s
}

// Send 向单个进程投递信号。标准信号对同一进程折叠为一个待决实例；
// 实时信号排队，队列满时返回 ErrQueueFull 且不影响其他信号。
func (r *Runtime) Send(pid int, sig Signal) error {
	return r.sendValue(pid, sig, 0)
}

// SendValue 与 Send 相同，但可为实时信号携带一个随队值。
func (r *Runtime) SendValue(pid int, sig Signal, value int) error {
	return r.sendValue(pid, sig, value)
}

func (r *Runtime) sendValue(pid int, sig Signal, value int) error {
	if !sig.Valid() {
		return ErrInvalidSig
	}
	p, err := r.mustProcess(pid)
	if err != nil {
		return err
	}
	return p.receive(r, sig, value, r.nextSeq())
}

// groupMembers 返回进程组内的成员，按 pid 升序排列，保证组投递顺序确定。
func (r *Runtime) groupMembers(pgid int) []*Process {
	r.regMu.RLock()
	out := make([]*Process, 0)
	for _, p := range r.procs {
		if p.pgid == pgid {
			out = append(out, p)
		}
	}
	r.regMu.RUnlock()
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].pid > out[j].pid; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// SendGroup 向进程组内每个进程投递信号，按 pid 升序逐个交付。
// 任一成员的实时队列满只影响该成员，其他成员继续投递。
func (r *Runtime) SendGroup(pgid int, sig Signal) error {
	return r.sendGroupValue(pgid, sig, 0)
}

// SendGroupValue 与 SendGroup 相同，但携带随队值。
func (r *Runtime) SendGroupValue(pgid int, sig Signal, value int) error {
	return r.sendGroupValue(pgid, sig, value)
}

func (r *Runtime) sendGroupValue(pgid int, sig Signal, value int) error {
	if !sig.Valid() {
		return ErrInvalidSig
	}
	members := r.groupMembers(pgid)
	if len(members) == 0 {
		return ErrNoProcess
	}
	var firstErr error
	for _, p := range members {
		if err := p.receive(r, sig, value, r.nextSeq()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// SetAction 设置信号处置并返回旧处置（骨架）。
func (r *Runtime) SetAction(pid int, sig Signal, act SigAction) (SigAction, error) {
	if !sig.Valid() {
		return SigAction{}, ErrInvalidSig
	}
	p, err := r.mustProcess(pid)
	if err != nil {
		return SigAction{}, err
	}
	if isUncatchable(sig) {
		return SigAction{}, ErrUncatchable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	old := p.setAction(sig, act)
	p.deliver(r)
	return old, nil
}

// GetAction 查询信号处置（骨架）。
func (r *Runtime) GetAction(pid int, sig Signal) (SigAction, error) {
	if !sig.Valid() {
		return SigAction{}, ErrInvalidSig
	}
	p, err := r.mustProcess(pid)
	if err != nil {
		return SigAction{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.actionFor(sig), nil
}

// SetMask 设置进程屏蔽字并返回旧屏蔽字（骨架）。
func (r *Runtime) SetMask(pid int, set SigSet) (SigSet, error) {
	p, err := r.mustProcess(pid)
	if err != nil {
		return SigSet{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	old := p.setMask(set)
	p.deliver(r)
	return old, nil
}

// Pending 返回标准待决集合与排队中的实时信号（骨架）。
func (r *Runtime) Pending(pid int) (SigSet, []Signal, error) {
	p, err := r.mustProcess(pid)
	if err != nil {
		return SigSet{}, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	rt := make([]Signal, len(p.rtQueue))
	for i, q := range p.rtQueue {
		rt[i] = q.sig
	}
	return p.stdPending, rt, nil
}

// State 返回进程状态字符串（骨架）。
func (r *Runtime) State(pid int) (string, error) {
	p, err := r.mustProcess(pid)
	if err != nil {
		return "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch p.state {
	case procRunning:
		return "running", nil
	case procStopped:
		return "stopped", nil
	default:
		return "terminated", nil
	}
}
