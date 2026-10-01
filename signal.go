package signalruntime

// Signal 表示一个标准信号或实时信号的编号。
type Signal int

// 标准信号编号（采用 Linux 通用编号约定）。
const (
	SIGHUP  Signal = 1
	SIGINT  Signal = 2
	SIGQUIT Signal = 3
	SIGILL  Signal = 4
	SIGTRAP Signal = 5
	SIGABRT Signal = 6
	SIGBUS  Signal = 7
	SIGFPE  Signal = 8
	SIGKILL Signal = 9
	SIGUSR1 Signal = 10
	SIGSEGV Signal = 11
	SIGUSR2 Signal = 12
	SIGPIPE Signal = 13
	SIGALRM Signal = 14
	SIGTERM Signal = 15
	SIGCHLD Signal = 17
	SIGCONT Signal = 18
	SIGSTOP Signal = 19
	SIGTSTP Signal = 20
	SIGTTIN Signal = 21
	SIGTTOU Signal = 22
)

// 实时信号区间（含端点）。
const (
	SIGRTMIN Signal = 32
	SIGRTMAX Signal = 64
)

// signalCount 为内部屏蔽字与处置表的容量。
const signalCount = 65

// SigSet 是信号集合，用于屏蔽字等场景。
type SigSet struct {
	bits uint64
}

// NewSigSet 创建一个包含给定信号的集合（骨架）。
func NewSigSet(sigs ...Signal) SigSet {
	var s SigSet
	for _, sig := range sigs {
		s = s.Add(sig)
	}
	return s
}

// Add 返回加入信号后的新集合（骨架）。
func (s SigSet) Add(sig Signal) SigSet {
	if sig < 1 || int(sig) >= signalCount {
		return s
	}
	s.bits |= 1 << uint(sig-1)
	return s
}

// Remove 返回移除信号后的新集合（骨架）。
func (s SigSet) Remove(sig Signal) SigSet {
	if sig < 1 || int(sig) >= signalCount {
		return s
	}
	s.bits &^= 1 << uint(sig-1)
	return s
}

// Has 判断集合是否包含信号（骨架）。
func (s SigSet) Has(sig Signal) bool {
	if sig < 1 || int(sig) >= signalCount {
		return false
	}
	return s.bits&(1<<uint(sig-1)) != 0
}

// Empty 判断集合是否为空（骨架）。
func (s SigSet) Empty() bool {
	return s.bits == 0
}

// Union 返回两个集合的并集（骨架）。
func (s SigSet) Union(other SigSet) SigSet {
	s.bits |= other.bits
	return s
}

// Signals 按编号升序返回集合中的全部信号。
func (s SigSet) Signals() []Signal {
	out := make([]Signal, 0, 8)
	for sig := Signal(1); int(sig) < signalCount; sig++ {
		if s.Has(sig) {
			out = append(out, sig)
		}
	}
	return out
}

// IsStandard 判断是否为标准信号（骨架）。
func (s Signal) IsStandard() bool {
	return s >= 1 && s < SIGRTMIN
}

// IsRealtime 判断是否为实时信号（骨架）。
func (s Signal) IsRealtime() bool {
	return s >= SIGRTMIN && s <= SIGRTMAX
}

// Valid 判断信号编号是否合法（骨架）。
func (s Signal) Valid() bool {
	return s.IsStandard() || s.IsRealtime()
}

// 默认动作类别。
type defaultAction int

const (
	defTerminate defaultAction = iota
	defIgnore
	defStop
	defContinue
	defCore
)

// uncatchable 为既不能被阻塞、忽略，也不能被捕获的信号集合：SIGKILL 与 SIGSTOP。
var uncatchable = NewSigSet(SIGKILL, SIGSTOP)

// isUncatchable 判断信号是否永远只能执行默认动作。
func isUncatchable(sig Signal) bool {
	return uncatchable.Has(sig)
}

// stopSignals 为 POSIX 作业控制停止信号集合。
var stopSignals = NewSigSet(SIGSTOP, SIGTSTP, SIGTTIN, SIGTTOU)

// isStopSignal 判断信号的默认动作是否为停止进程。
func isStopSignal(sig Signal) bool {
	return stopSignals.Has(sig)
}

// defaultActions 按编号给出各信号的默认动作（采用 Linux 通用约定）。
var defaultActions = [...]defaultAction{
	0:       defTerminate, // 占位，信号编号从 1 开始
	SIGHUP:  defTerminate,
	SIGINT:  defTerminate,
	SIGQUIT: defCore,
	SIGILL:  defCore,
	SIGTRAP: defCore,
	SIGABRT: defCore,
	SIGBUS:  defCore,
	SIGFPE:  defCore,
	SIGKILL: defTerminate,
	SIGUSR1: defTerminate,
	SIGSEGV: defCore,
	SIGUSR2: defTerminate,
	SIGPIPE: defTerminate,
	SIGALRM: defTerminate,
	SIGTERM: defTerminate,
	16:      defTerminate, // SIGSTKFLT
	SIGCHLD: defIgnore,
	SIGCONT: defContinue,
	SIGSTOP: defStop,
	SIGTSTP: defStop,
	SIGTTIN: defStop,
	SIGTTOU: defStop,
	23:      defIgnore,    // SIGURG
	24:      defCore,      // SIGXCPU
	25:      defCore,      // SIGXFSZ
	26:      defTerminate, // SIGVTALRM
	27:      defTerminate, // SIGPROF
	28:      defIgnore,    // SIGWINCH
	29:      defTerminate, // SIGIO
	30:      defTerminate, // SIGPWR
	31:      defCore,      // SIGSYS
}

// defaultActionFor 返回信号的默认动作；实时信号默认为终止。
func defaultActionFor(sig Signal) defaultAction {
	if sig.IsRealtime() {
		return defTerminate
	}
	if sig < 1 || int(sig) >= len(defaultActions) {
		return defTerminate
	}
	return defaultActions[sig]
}
