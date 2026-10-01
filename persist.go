package signalruntime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

// sigActionState 为单个信号处置的可持久化表示。
type sigActionState struct {
	Action int    `json:"action"`
	Mask   int64  `json:"mask"`
	Name   string `json:"name,omitempty"`
}

// queuedSignalState 为一个待决或在途信号实例的可持久化表示。
type queuedSignalState struct {
	Sig   int    `json:"sig"`
	Value int    `json:"value"`
	Seq   uint64 `json:"seq"`
}

// processState 是单个进程关键状态的快照。
type processState struct {
	PID        int                    `json:"pid"`
	PGID       int                    `json:"pgid"`
	State      int                    `json:"state"`
	Mask       int64                  `json:"mask"`
	Cursor     int                    `json:"cursor"`
	Actions    map[int]sigActionState `json:"actions"`
	StdPending []int                  `json:"std_pending"`
	RTQueue    []queuedSignalState    `json:"rt_queue"`
	Inflight   []queuedSignalState    `json:"inflight"`
}

// State 是可持久化的运行时状态快照。
type State struct {
	Seq       uint64         `json:"seq"`
	Processes []processState `json:"processes"`
}

// State 是可持久化的运行时状态快照（骨架）。
// Snapshot 导出当前关键状态（骨架）。
func (r *Runtime) Snapshot() *State {
	r.regMu.RLock()
	procs := make([]*Process, 0, len(r.procs))
	for _, p := range r.procs {
		procs = append(procs, p)
	}
	r.regMu.RUnlock()
	sort.Slice(procs, func(i, j int) bool { return procs[i].pid < procs[j].pid })

	st := &State{}
	for _, p := range procs {
		// 逐个进程在静止点取样：持锁期间不会有入队、出队或状态变更，
		// 因而快照中的待决队列与在途集合互斥且并集完备，恢复时不会重复或丢失。
		p.mu.Lock()
		ps := processState{
			PID:     p.pid,
			PGID:    p.pgid,
			State:   int(p.state),
			Mask:    int64(p.mask.bits),
			Cursor:  int(p.cursor),
			Actions: make(map[int]sigActionState),
		}
		for sig := Signal(1); int(sig) < signalCount; sig++ {
			a := p.actions[sig]
			if a.Action == ActDefault && a.Name == "" && a.Mask.bits == 0 {
				continue
			}
			ps.Actions[int(sig)] = sigActionState{
				Action: int(a.Action),
				Mask:   int64(a.Mask.bits),
				Name:   a.Name,
			}
		}
		for _, sig := range p.stdPending.Signals() {
			ps.StdPending = append(ps.StdPending, int(sig))
		}
		for _, q := range p.rtQueue {
			ps.RTQueue = append(ps.RTQueue, queuedSignalState{Sig: int(q.sig), Value: q.value, Seq: q.seq})
		}
		for _, q := range p.inflight {
			ps.Inflight = append(ps.Inflight, queuedSignalState{Sig: int(q.sig), Value: q.value, Seq: q.seq})
		}
		p.mu.Unlock()
		st.Processes = append(st.Processes, ps)
	}

	r.seqMu.Lock()
	st.Seq = r.seq
	r.seqMu.Unlock()
	return st
}

// Restore 从快照恢复关键状态（骨架）。
// resolve 在重启后按稳定名称重新绑定自定义处理器；可为 nil，
// 此时所有捕获处置退化为默认动作。
func (r *Runtime) Restore(state *State, resolve func(name string) HandlerFunc) error {
	if state == nil {
		return nil
	}

	r.regMu.Lock()
	r.procs = make(map[int]*Process, len(state.Processes))
	r.regMu.Unlock()

	r.seqMu.Lock()
	r.seq = state.Seq
	r.seqMu.Unlock()

	for _, ps := range state.Processes {
		p := newProcess(ps.PID, ps.PGID)
		p.state = procState(ps.State)
		p.mask = SigSet{bits: uint64(ps.Mask)}
		if ps.Cursor >= 1 && ps.Cursor < signalCount {
			p.cursor = Signal(ps.Cursor)
		}
		for s, a := range ps.Actions {
			sig := Signal(s)
			if !sig.Valid() {
				continue
			}
			act := SigAction{
				Action: Action(a.Action),
				Name:   a.Name,
				Mask:   SigSet{bits: uint64(a.Mask)},
			}
			if act.Action == ActCatch && resolve != nil && act.Name != "" {
				act.Handler = resolve(act.Name)
			}
			p.actions[sig] = act
		}
		for _, s := range ps.StdPending {
			p.stdPending = p.stdPending.Add(Signal(s))
		}
		// 先放入在途实例（崩溃前已离开队列、交付结果未知的实例），
		// 再放入队列实例，并按到达序号整体重排：崩溃恢复对这些实例
		// 保证“至少交付一次”，恢复后严格按原到达顺序交付，且不重复入队。
		pending := make([]queuedSignal, 0, len(ps.Inflight)+len(ps.RTQueue))
		for _, q := range ps.Inflight {
			if Signal(q.Sig) < SIGRTMIN {
				continue
			}
			pending = append(pending, queuedSignal{sig: Signal(q.Sig), value: q.Value, seq: q.Seq})
		}
		for _, q := range ps.RTQueue {
			if Signal(q.Sig) < SIGRTMIN {
				continue
			}
			pending = append(pending, queuedSignal{sig: Signal(q.Sig), value: q.Value, seq: q.Seq})
		}
		// 实时实例严格按原到达序号重排；崩溃前已入队与在途的实例一并恢复。
		sort.SliceStable(pending, func(i, j int) bool { return pending[i].seq < pending[j].seq })
		for _, q := range pending {
			p.enqueue(q.sig, q.value, q.seq)
		}
		// 标准信号（含在途实例）按标准语义折叠：同一标准信号最多一个待决位。
		stdSeen := SigSet{}
		for _, q := range ps.Inflight {
			sig := Signal(q.Sig)
			if sig.IsStandard() && !stdSeen.Has(sig) {
				stdSeen = stdSeen.Add(sig)
				p.enqueue(sig, q.Value, q.Seq)
			}
		}
		r.regMu.Lock()
		r.procs[p.pid] = p
		r.regMu.Unlock()

		// 运行中的进程恢复后立即交付解除阻塞的待决信号；
		// 停止中的进程继续保持待决，直到收到 SIGCONT。
		if p.state == procRunning {
			p.mu.Lock()
			p.deliver(r)
			p.mu.Unlock()
		}
	}
	return nil
}

// Save 将快照写入文件（骨架）。
func (r *Runtime) Save(path string) error {
	state := r.Snapshot()
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".signal-state-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

// Load 从文件恢复状态（骨架）。
func (r *Runtime) Load(path string, resolve func(name string) HandlerFunc) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	return r.Restore(&state, resolve)
}
