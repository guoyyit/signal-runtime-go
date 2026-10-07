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

// deliver runs the delivery loop for p. At most one loop per process runs
// at a time; concurrent callers return immediately and the active loop
// picks up their signals, so handlers never execute re-entrantly and no
// signal is delivered twice.
//
// The loop delivers in passes (see nextDeliverableLocked): signals that
// arrive during a pass are deferred to the next pass, which guarantees
// every unblocked pending signal is delivered within a bounded number of
// deliveries. A single activation runs at most maxDeliveryPasses passes;
// if handler-triggered sends keep regenerating work, the loop stops and
// the remaining signals stay pending until the next delivery trigger, so
// no API call can loop forever.
//
// While a handler runs, the signal being delivered is recorded in
// p.inflight (and the pre-handler mask in p.savedMask) so that a snapshot
// taken mid-delivery preserves it for recovery.
func (r *Runtime) deliver(p *process) {
	p.mu.Lock()
	if p.delivering {
		p.mu.Unlock()
		return
	}
	p.delivering = true
	p.passes = 0
	for {
		sig, seq, ok := p.nextDeliverableLocked()
		if !ok || p.dead {
			break
		}
		p.popLocked(sig, seq)
		p.inflight = &rtEntry{Sig: sig, Seq: seq}
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
			p.inflight = nil
		case ActionHandler:
			fn, ok := r.registry.Lookup(act.Handler)
			if !ok {
				// Handler unknown (e.g. missing after restore):
				// fall back to the default action.
				p.inflight = nil
				r.applyDefaultLocked(p, sig)
				continue
			}
			saved := p.mask
			mask := saved.Union(act.HandlerMask)
			if !act.NoDefer {
				mask = mask.Add(sig)
			}
			p.mask = mask
			p.savedMask = saved
			p.commitInterruptLocked(act.Restart)
			p.mu.Unlock()
			fn(p.id, sig)
			p.mu.Lock()
			p.inflight = nil
			p.mask = saved
			p.notifyWaitersLocked()
		default: // ActionDefault
			r.applyDefaultLocked(p, sig)
			p.inflight = nil
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
