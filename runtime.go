package signalrt

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
)

// ErrUnknownProcess is returned when a pid does not name a live process.
var ErrUnknownProcess = errors.New("signalrt: unknown process")

// ErrProcessExited is returned when sending to a terminated process.
var ErrProcessExited = errors.New("signalrt: process has exited")

// ErrInvalidSignal is returned for out-of-range or uncatchable operations.
var ErrInvalidSignal = errors.New("signalrt: invalid signal")

// ErrQueueOverflow is returned when a realtime pending queue is full;
// the signal is dropped (drop-newest policy).
var ErrQueueOverflow = errors.New("signalrt: realtime pending queue full")

// ErrUnknownHandler is returned when setting a handler that is not
// present in the runtime registry.
var ErrUnknownHandler = errors.New("signalrt: unknown handler name")

// Runtime manages a group of local processes and their signal state.
// All methods are safe for concurrent use; delivery is deterministic.
type Runtime struct {
	mu       sync.Mutex
	procs    map[int]*process
	nextPID  int
	registry *HandlerRegistry
	seq      atomic.Uint64 // arrival sequence for realtime ordering
}

// New returns an empty Runtime using reg to resolve handler names.
// A nil registry is replaced with an empty one.
func New(reg *HandlerRegistry) *Runtime {
	if reg == nil {
		reg = NewHandlerRegistry()
	}
	return &Runtime{procs: make(map[int]*process), registry: reg, nextPID: 1}
}

// Spawn creates a process in group pgid and returns its pid.
func (r *Runtime) Spawn(pgid int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	pid := r.nextPID
	r.nextPID++
	r.procs[pid] = &process{
		id:      pid,
		group:   pgid,
		actions: make(map[Signal]Action),
	}
	return pid
}

// lookup returns the process for pid, or nil.
func (r *Runtime) lookup(pid int) *process {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.procs[pid]
}

// Send delivers sig to process pid, following the delivery rules.
func (r *Runtime) Send(pid int, sig Signal) error {
	if !sig.Valid() {
		return ErrInvalidSignal
	}
	p := r.lookup(pid)
	if p == nil {
		return ErrUnknownProcess
	}
	p.mu.Lock()
	if p.dead {
		p.mu.Unlock()
		return ErrProcessExited
	}
	err := p.enqueueLocked(sig, r.seq.Add(1))
	p.mu.Unlock()
	if err != nil {
		return err
	}
	r.deliver(p)
	return nil
}

// SendGroup delivers sig to every live process in group pgid.
// Every member receives the signal even when some sends fail; the first
// error encountered (in ascending pid order) is returned.
func (r *Runtime) SendGroup(pgid int, sig Signal) error {
	if !sig.Valid() {
		return ErrInvalidSignal
	}
	r.mu.Lock()
	var pids []int
	for pid, p := range r.procs {
		if p.group == pgid {
			pids = append(pids, pid)
		}
	}
	r.mu.Unlock()
	sort.Ints(pids)
	var first error
	for _, pid := range pids {
		if err := r.Send(pid, sig); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// SetMask replaces the blocked-signal mask of pid and triggers delivery
// of any signals that became unblocked. Uncatchable signals are silently
// removed from the mask.
func (r *Runtime) SetMask(pid int, mask Set) error {
	p := r.lookup(pid)
	if p == nil {
		return ErrUnknownProcess
	}
	mask = mask.Del(SIGKILL).Del(SIGSTOP)
	p.mu.Lock()
	if p.dead {
		p.mu.Unlock()
		return ErrProcessExited
	}
	p.mask = mask
	p.mu.Unlock()
	r.deliver(p)
	return nil
}

// Mask returns the current mask of pid.
func (r *Runtime) Mask(pid int) (Set, error) {
	p := r.lookup(pid)
	if p == nil {
		return EmptySet, ErrUnknownProcess
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mask, nil
}

// SetAction installs the disposition of sig for pid. Uncatchable signals
// reject non-default dispositions with ErrInvalidSignal.
func (r *Runtime) SetAction(pid int, sig Signal, a Action) error {
	if !sig.Valid() {
		return ErrInvalidSignal
	}
	if !Catchable(sig) && a.Kind != ActionDefault {
		return ErrInvalidSignal
	}
	if a.Kind == ActionHandler {
		if _, ok := r.registry.Lookup(a.Handler); !ok {
			return ErrUnknownHandler
		}
	}
	p := r.lookup(pid)
	if p == nil {
		return ErrUnknownProcess
	}
	p.mu.Lock()
	if p.dead {
		p.mu.Unlock()
		return ErrProcessExited
	}
	if a.Kind == ActionDefault {
		delete(p.actions, sig)
	} else {
		p.actions[sig] = a
	}
	p.mu.Unlock()
	return nil
}

// ActionOf returns the current disposition of sig for pid.
func (r *Runtime) ActionOf(pid int, sig Signal) (Action, error) {
	if !sig.Valid() {
		return Action{}, ErrInvalidSignal
	}
	p := r.lookup(pid)
	if p == nil {
		return Action{}, ErrUnknownProcess
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.actionForLocked(sig), nil
}

// Pending reports the pending standard set and realtime queue of pid.
func (r *Runtime) Pending(pid int) (Set, []Signal, error) {
	p := r.lookup(pid)
	if p == nil {
		return EmptySet, nil, ErrUnknownProcess
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	rt := make([]Signal, len(p.rtQueue))
	for i, e := range p.rtQueue {
		rt[i] = e.Sig
	}
	return p.pending, rt, nil
}

// Overflow returns the count of realtime signals dropped for pid.
func (r *Runtime) Overflow(pid int) (uint64, error) {
	p := r.lookup(pid)
	if p == nil {
		return 0, ErrUnknownProcess
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.overflow, nil
}

// Stopped reports whether pid is currently stopped.
func (r *Runtime) Stopped(pid int) (bool, error) {
	p := r.lookup(pid)
	if p == nil {
		return false, ErrUnknownProcess
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopped, nil
}

// Exited reports whether pid has terminated, and if so the signal that
// terminated it (0 for none yet).
func (r *Runtime) Exited(pid int) (bool, Signal, error) {
	p := r.lookup(pid)
	if p == nil {
		return false, 0, ErrUnknownProcess
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dead, p.exitSig, nil
}

// deliveryBudget bounds how many signals a single delivery loop may
// deliver before yielding. A loop that keeps finding new work is
// normally driven by handlers that send further signals; the budget
// guarantees that such (mutually) self-triggering chains cannot make
// Send/SetMask/Restore spin forever. Signals left over when the budget
// is exhausted stay pending — they are not dropped — and are delivered
// by the next delivery trigger (Send, SetMask, Restore, ...).
const deliveryBudget = 4096

// deliver runs the delivery loop for p. At most one loop per process runs
// at a time; concurrent callers return immediately and the active loop
// picks up their signals, so handlers never execute re-entrantly and no
// signal is delivered twice.
//
// The loop works in rounds. Each round snapshots the currently pending
// signals (standard bitmap plus the highest queued realtime sequence)
// and delivers only signals from that snapshot, lowest number first.
// Signals arriving while a round runs are deferred to the next round,
// so no signal can be postponed indefinitely by the continuous arrival
// of lower-numbered ones: every deliverable signal is reached within
// the round that sees it pending.
//
// While a handler runs, the delivered signal is recorded as in-flight
// and is included in snapshots; a crash mid-handler therefore does not
// lose it (Restore re-queues it exactly once).
func (r *Runtime) deliver(p *process) {
	p.mu.Lock()
	if p.delivering {
		p.mu.Unlock()
		return
	}
	p.delivering = true
	delivered := 0
	for {
		// Round snapshot: standard signals pending right now, and the
		// highest realtime sequence queued right now. Only these are
		// eligible until the round ends.
		roundStd := p.pending
		var rtMaxSeq uint64
		for _, e := range p.rtQueue {
			if e.Seq > rtMaxSeq {
				rtMaxSeq = e.Seq
			}
		}
		for {
			sig, ok := p.nextDeliverableLocked(roundStd, rtMaxSeq)
			if !ok || p.dead {
				break
			}
			// A standard signal is delivered at most once per round:
			// a new instance arriving mid-round waits for the next one.
			roundStd = roundStd.Del(sig)
			seq := p.popLocked(sig)
			if sig == SIGCONT {
				// SIGCONT resumes the process even when caught, and
				// discards any pending SIGSTOP.
				p.stopped = false
				p.pending = p.pending.Del(SIGSTOP)
			}
			act := p.actionForLocked(sig)
			switch act.Kind {
			case ActionIgnore:
				// Discard.
			case ActionHandler:
				fn, ok := r.registry.Lookup(act.Handler)
				if !ok {
					// Handler unknown (e.g. missing after restore):
					// fall back to the default action.
					r.applyDefaultLocked(p, sig)
					break
				}
				saved := p.mask
				mask := saved.Union(act.HandlerMask)
				if !act.NoDefer {
					mask = mask.Add(sig)
				}
				p.mask = mask
				p.commitInterruptLocked(act.Restart)
				// Mark the signal in-flight before releasing the
				// lock: a snapshot taken while the handler runs
				// preserves it (and the pre-handler mask) so a
				// crash cannot make it disappear.
				p.inflightSig = sig
				p.inflightSeq = seq
				p.savedMask = saved
				p.mu.Unlock()
				fn(p.id, sig)
				p.mu.Lock()
				p.inflightSig = 0
				p.mask = saved
				p.notifyWaitersLocked()
			default: // ActionDefault
				r.applyDefaultLocked(p, sig)
			}
			delivered++
			if delivered >= deliveryBudget {
				// The chain of handler-triggered sends is not
				// converging: yield to the caller. Leftover signals
				// remain pending for the next delivery trigger.
				p.delivering = false
				p.mu.Unlock()
				return
			}
		}
		if p.dead || !p.anyDeliverableLocked() {
			break
		}
	}
	p.delivering = false
	p.mu.Unlock()
}

// applyDefaultLocked applies the default action of sig. The caller holds p.mu.
func (r *Runtime) applyDefaultLocked(p *process, sig Signal) {
	switch DefaultAction(sig) {
	case DefIgn:
	case DefTerm:
		p.dead = true
		p.exitSig = sig
		p.pending = EmptySet
		p.rtQueue = nil
		p.notifyWaitersLocked()
	case DefStop:
		p.stopped = true
		// POSIX: delivering a stop signal discards pending SIGCONT.
		p.pending = p.pending.Del(SIGCONT)
	case DefCont:
		p.stopped = false
		// POSIX: delivering SIGCONT discards pending SIGSTOP.
		p.pending = p.pending.Del(SIGSTOP)
	}
}
