package signalrt

import "sync"

// rtQueueCap is the per-process realtime pending queue capacity.
// Overflow policy: the incoming signal is dropped and Send returns
// ErrQueueOverflow; the process overflow counter is incremented.
const rtQueueCap = 32

// maxDeliveryPasses bounds the number of delivery passes a single
// delivery-loop activation may run. Each pass delivers every signal that
// was pending when the pass began, so one activation makes at most
// maxDeliveryPasses*(31+rtQueueCap) deliveries before returning. When a
// handler keeps sending new signals (e.g. two handlers ping-ponging), the
// budget eventually runs out: the remaining deliverable signals stay
// pending and are picked up by the next delivery trigger (Send, SetMask,
// Restore). This keeps every single API call bounded in time.
const maxDeliveryPasses = 1024

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

	// Delivery-pass scheduling state (transient, not persisted). See
	// nextDeliverableLocked for the ordering and fairness rules.
	passActive bool   // a pass is currently in progress
	passStd    Set    // standard signals eligible in the current pass
	passRTSeq  uint64 // realtime entries with Seq <= passRTSeq are eligible
	passes     int    // passes run by the current loop activation

	// In-flight delivery state. inflight records the signal that has been
	// popped from the pending state but whose delivery has not completed
	// (its handler is still running). It is persisted by Snapshot so a
	// crash mid-delivery does not lose the signal: Restore re-queues it.
	// savedMask holds the process mask as it was before the in-flight
	// handler started; snapshots persist it instead of the transient
	// handler mask so the restored process can re-deliver the signal.
	inflight  *rtEntry
	savedMask Set

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
// documented ordering rules. The caller holds p.mu. It returns the signal
// and, for realtime signals, the sequence number of the queued occurrence
// to deliver (0 for standard signals).
//
// Ordering and fairness rules:
//  1. A stopped process only considers SIGKILL and SIGCONT.
//  2. Otherwise delivery proceeds in passes. A pass snapshots the pending
//     standard set and the realtime queue when it begins; only those
//     signals are eligible during the pass. Signals arriving later become
//     eligible in the next pass.
//  3. Within a pass the lowest-numbered unblocked eligible signal wins;
//     standard signals (1-31) therefore precede realtime ones (32-64), and
//     multiple occurrences of one realtime signal are delivered FIFO.
//  4. Because arrivals during a pass wait for the next pass, every
//     unblocked pending signal is delivered by the end of the current or
//     next pass: no signal can be deferred indefinitely by a sustained
//     stream of lower-numbered signals.
//  5. A single loop activation runs at most maxDeliveryPasses passes; when
//     the budget is exhausted the remaining deliverable signals stay
//     pending for the next delivery trigger, so handler-triggered sends
//     cannot keep one API call running forever.
func (p *process) nextDeliverableLocked() (Signal, uint64, bool) {
	if p.stopped {
		// The stopped state discards any in-progress pass; a fresh pass
		// begins once the process is continued.
		p.passActive = false
		p.passStd = 0
		switch {
		case p.pending.Has(SIGKILL):
			return SIGKILL, 0, true
		case p.pending.Has(SIGCONT):
			return SIGCONT, 0, true
		}
		return 0, 0, false
	}
	for {
		if !p.passActive {
			if !p.anyDeliverableLocked() {
				return 0, 0, false
			}
			if p.passes >= maxDeliveryPasses {
				// Activation budget exhausted: leave the remaining
				// deliverable signals pending for the next trigger.
				return 0, 0, false
			}
			p.passes++
			p.passActive = true
			p.passStd = p.pending
			p.passRTSeq = p.rtMaxSeqLocked()
		}
		if sig, ok := p.passStd.Intersect(^p.mask).Lowest(); ok {
			return sig, 0, true
		}
		best := -1
		for i, e := range p.rtQueue {
			if e.Seq > p.passRTSeq || p.mask.Has(e.Sig) {
				continue
			}
			if best < 0 || e.Sig < p.rtQueue[best].Sig ||
				(e.Sig == p.rtQueue[best].Sig && e.Seq < p.rtQueue[best].Seq) {
				best = i
			}
		}
		if best >= 0 {
			e := p.rtQueue[best]
			return e.Sig, e.Seq, true
		}
		// The pass is exhausted: close it and consider a fresh pass.
		p.passActive = false
		p.passStd = 0
	}
}

// anyDeliverableLocked reports whether any signal is both pending and
// unblocked. The caller holds p.mu.
func (p *process) anyDeliverableLocked() bool {
	if p.pending.Intersect(^p.mask) != 0 {
		return true
	}
	for _, e := range p.rtQueue {
		if !p.mask.Has(e.Sig) {
			return true
		}
	}
	return false
}

// rtMaxSeqLocked returns the highest sequence number currently in the
// realtime queue, or 0 when the queue is empty. Entries enqueued later
// carry higher sequence numbers, so a pass started now excludes them.
func (p *process) rtMaxSeqLocked() uint64 {
	var max uint64
	for _, e := range p.rtQueue {
		if e.Seq > max {
			max = e.Seq
		}
	}
	return max
}

// popLocked removes sig from the pending state. The caller holds p.mu.
// For realtime signals the queued occurrence with the given sequence
// number is removed; the signal is also dropped from the current pass so
// it is delivered at most once per pass no matter how often it re-arrives.
func (p *process) popLocked(sig Signal, seq uint64) {
	if !sig.Realtime() {
		p.pending = p.pending.Del(sig)
		p.passStd = p.passStd.Del(sig)
		return
	}
	for i, e := range p.rtQueue {
		if e.Sig == sig && e.Seq == seq {
			p.rtQueue = append(p.rtQueue[:i], p.rtQueue[i+1:]...)
			return
		}
	}
	// Defensive fallback: remove the oldest queued occurrence.
	for i, e := range p.rtQueue {
		if e.Sig == sig {
			p.rtQueue = append(p.rtQueue[:i], p.rtQueue[i+1:]...)
			return
		}
	}
}
