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
// documented ordering rules. The caller holds p.mu.
//
// Ordering rules:
//  1. A stopped process only considers SIGKILL and SIGCONT.
//  2. Otherwise the lowest-numbered unblocked pending signal wins;
//     standard signals (1-31) therefore precede realtime ones (32-64).
//  3. Multiple occurrences of one realtime signal are delivered FIFO.
func (p *process) nextDeliverableLocked() (Signal, bool) {
	if p.stopped {
		switch {
		case p.pending.Has(SIGKILL):
			return SIGKILL, true
		case p.pending.Has(SIGCONT):
			return SIGCONT, true
		}
		return 0, false
	}
	if sig, ok := p.pending.Intersect(^p.mask).Lowest(); ok {
		return sig, true
	}
	best := -1
	for i, e := range p.rtQueue {
		if p.mask.Has(e.Sig) {
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

// popLocked removes sig from the pending state. The caller holds p.mu.
// For realtime signals the oldest queued occurrence is removed.
func (p *process) popLocked(sig Signal) {
	if !sig.Realtime() {
		p.pending = p.pending.Del(sig)
		return
	}
	for i, e := range p.rtQueue {
		if e.Sig == sig {
			p.rtQueue = append(p.rtQueue[:i], p.rtQueue[i+1:]...)
			return
		}
	}
}
