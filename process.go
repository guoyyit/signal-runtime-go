package signalrt

import "sync"

// rtQueueCap is the per-process realtime pending queue capacity.
// Overflow policy: the incoming signal is dropped and Send returns
// ErrQueueOverflow; the process overflow counter is incremented.
const rtQueueCap = 32

// rtEntry is one queued realtime signal occurrence.
type rtEntry struct {
	Sig Signal `json:"sig"`
	Seq uint64 `json:"seq"`
}

// process is the runtime state of one managed process.
type process struct {
	mu         sync.Mutex
	id         int
	group      int
	mask       Set
	actions    map[Signal]Action
	pending    Set       // coalesced pending standard signals
	rtQueue    []rtEntry // FIFO pending realtime signals
	overflow   uint64    // dropped realtime signals
	stopped    bool
	dead       bool
	exitSig    Signal // signal that terminated the process, if any
	delivering bool   // a delivery loop currently owns this process

	// In-flight delivery: a signal whose handler has started (or is
	// about to start) but has not returned yet. Persisted so that a
	// crash while a handler runs does not lose the signal: Restore
	// re-queues it and delivers it exactly once more. At most one
	// signal is in flight per process because delivery is serialized.
	inflightSig Signal // 0 when nothing is in flight
	inflightSeq uint64 // arrival sequence of an in-flight realtime signal
	savedMask   Set    // process mask to persist while in flight

	// Wait/interrupt state. See wait.go.
	waitCh      chan struct{} // closed to broadcast wake/interrupt
	waitGen     uint64        // bumped on every notify
	intrSeq     uint64        // bumped on every committed interrupt
	intrRestart bool          // AND of Restart flags of committed interrupts
}

// actionForLocked returns the disposition for sig, defaulting when unset.
// Uncatchable signals always report the default disposition.
func (p *process) actionForLocked(sig Signal) Action {
	if !Catchable(sig) {
		return DefaultActionValue()
	}
	if a, ok := p.actions[sig]; ok {
		return a
	}
	return DefaultActionValue()
}

// notifyWaitersLocked wakes every current waiter. The caller holds p.mu.
func (p *process) notifyWaitersLocked() {
	p.waitGen++
	if p.waitCh != nil {
		close(p.waitCh)
		p.waitCh = nil
	}
}

// commitInterruptLocked records that a caught signal whose disposition has
// the given restart flag is being delivered. The caller holds p.mu.
func (p *process) commitInterruptLocked(restart bool) {
	p.intrSeq++
	p.intrRestart = p.intrRestart && restart
}

// enqueueLocked adds sig to the pending state. The caller holds p.mu.
// Standard signals coalesce; realtime signals append to the FIFO queue.
// Returns ErrQueueOverflow when the realtime queue is full.
func (p *process) enqueueLocked(sig Signal, seq uint64) error {
	if !sig.Realtime() {
		p.pending = p.pending.Add(sig)
		return nil
	}
	if len(p.rtQueue) >= rtQueueCap {
		p.overflow++
		return ErrQueueOverflow
	}
	p.rtQueue = append(p.rtQueue, rtEntry{Sig: sig, Seq: seq})
	return nil
}

// nextDeliverableLocked picks the next signal to deliver under the
// documented ordering rules, restricted to one delivery round:
//   - stdEligible limits which standard signals may be picked this round
//     (a round snapshot; signals arriving mid-round are not added to it);
//   - rtMaxSeq limits which realtime entries may be picked this round
//     (only entries with Seq <= rtMaxSeq, i.e. queued before the round
//     started).
//
// The caller holds p.mu.
//
// Ordering rules within a round:
//  1. A stopped process only considers SIGKILL and SIGCONT.
//  2. Otherwise the lowest-numbered unblocked pending signal wins;
//     standard signals (1-31) therefore precede realtime ones (32-64).
//  3. Multiple occurrences of one realtime signal are delivered FIFO.
//
// Restricting each round to a snapshot taken at round start is what
// guarantees freedom from starvation: a signal that keeps arriving
// while a round runs is only picked up by the next round, so every
// already-pending deliverable signal is reached within one round.
func (p *process) nextDeliverableLocked(stdEligible Set, rtMaxSeq uint64) (Signal, bool) {
	if p.stopped {
		cand := stdEligible.Intersect(p.pending)
		switch {
		case cand.Has(SIGKILL):
			return SIGKILL, true
		case cand.Has(SIGCONT):
			return SIGCONT, true
		}
		return 0, false
	}
	if sig, ok := stdEligible.Intersect(p.pending).Intersect(^p.mask).Lowest(); ok {
		return sig, true
	}
	best := -1
	for i, e := range p.rtQueue {
		if e.Seq > rtMaxSeq || p.mask.Has(e.Sig) {
			continue
		}
		if best < 0 || e.Sig < p.rtQueue[best].Sig ||
			(e.Sig == p.rtQueue[best].Sig && e.Seq < p.rtQueue[best].Seq) {
			best = i
		}
	}
	if best < 0 {
		return 0, false
	}
	return p.rtQueue[best].Sig, true
}

// anyDeliverableLocked reports whether any signal is deliverable right
// now, ignoring round restrictions. The caller holds p.mu.
func (p *process) anyDeliverableLocked() bool {
	_, ok := p.nextDeliverableLocked(p.pending, ^uint64(0))
	return ok
}

// popLocked removes sig from the pending state and returns the arrival
// sequence of the removed realtime entry (0 for standard signals).
// The caller holds p.mu. For realtime signals the oldest queued
// occurrence is removed; entries queued after the round snapshot always
// sit behind older ones, so the removed entry is the round-eligible one.
func (p *process) popLocked(sig Signal) uint64 {
	if !sig.Realtime() {
		p.pending = p.pending.Del(sig)
		return 0
	}
	for i, e := range p.rtQueue {
		if e.Sig == sig {
			p.rtQueue = append(p.rtQueue[:i], p.rtQueue[i+1:]...)
			return e.Seq
		}
	}
	return 0
}
