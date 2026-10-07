package signalrt

import (
	"encoding/json"
	"sort"
)

// snapshot is the serialized form of the whole runtime.
type snapshot struct {
	NextPID int            `json:"next_pid"`
	Seq     uint64         `json:"seq"`
	Procs   []procSnapshot `json:"procs"`
}

// procSnapshot is the serialized form of one process. Handler functions
// travel as registry names; the Action struct is JSON-friendly as-is.
type procSnapshot struct {
	ID          int               `json:"id"`
	Group       int               `json:"group"`
	Mask        Set               `json:"mask"`
	Actions     map[Signal]Action `json:"actions,omitempty"`
	Pending     Set               `json:"pending"`
	RTQueue     []rtEntry         `json:"rt_queue,omitempty"`
	Overflow    uint64            `json:"overflow,omitempty"`
	Stopped     bool              `json:"stopped"`
	Dead        bool              `json:"dead"`
	ExitSig     Signal            `json:"exit_sig,omitempty"`
	InflightSig Signal            `json:"inflight_sig,omitempty"`
	InflightSeq uint64            `json:"inflight_seq,omitempty"`
}

// Snapshot serializes the recoverable state of every process: dispositions,
// masks, pending queues, stop and exit state. Handler functions are stored
// by registry name; restoring into a Runtime whose registry lacks a name
// keeps the disposition but the handler is dropped to ActionDefault.
//
// A signal whose handler is running when the snapshot is taken is recorded
// as in-flight (together with the mask that was in effect before the
// handler started), so a crash in the middle of delivery does not make it
// disappear: Restore re-queues it and it is delivered exactly once more.
func (r *Runtime) Snapshot() ([]byte, error) {
	r.mu.Lock()
	pids := make([]int, 0, len(r.procs))
	for pid := range r.procs {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	snap := snapshot{NextPID: r.nextPID, Seq: r.seq.Load()}
	for _, pid := range pids {
		p := r.procs[pid]
		p.mu.Lock()
		// While a handler runs, p.mask is the temporary handler mask;
		// persist the mask that was in effect before delivery instead.
		mask := p.mask
		if p.inflightSig != 0 {
			mask = p.savedMask
		}
		ps := procSnapshot{
			ID:          p.id,
			Group:       p.group,
			Mask:        mask,
			Pending:     p.pending,
			Overflow:    p.overflow,
			Stopped:     p.stopped,
			Dead:        p.dead,
			ExitSig:     p.exitSig,
			InflightSig: p.inflightSig,
			InflightSeq: p.inflightSeq,
		}
		if len(p.actions) > 0 {
			ps.Actions = make(map[Signal]Action, len(p.actions))
			for sig, a := range p.actions {
				ps.Actions[sig] = a
			}
		}
		if len(p.rtQueue) > 0 {
			ps.RTQueue = append([]rtEntry(nil), p.rtQueue...)
		}
		p.mu.Unlock()
		snap.Procs = append(snap.Procs, ps)
	}
	r.mu.Unlock()
	return json.Marshal(snap)
}

// Restore replaces the runtime state with the contents of a snapshot
// produced by Snapshot. Pending signals are preserved exactly once: they
// are not delivered during Restore itself, but a delivery pass runs
// afterwards so unblocked pending signals are delivered without loss or
// duplication.
//
// A signal recorded as in-flight (its handler was running when the
// snapshot was taken) is put back onto the pending state — merged into
// the standard bitmap, or prepended to the realtime queue keeping its
// original arrival sequence so per-signal FIFO order is preserved — and
// is therefore delivered exactly once by the delivery pass. Signals that
// were fully delivered before the snapshot are not re-delivered.
func Restore(data []byte, reg *HandlerRegistry) (*Runtime, error) {
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	r := New(reg)
	r.nextPID = snap.NextPID
	if r.nextPID < 1 {
		r.nextPID = 1
	}
	r.seq.Store(snap.Seq)
	for _, ps := range snap.Procs {
		// Drop anything that is not a valid, deliverable signal.
		var queue []rtEntry
		for _, e := range ps.RTQueue {
			if e.Sig.Realtime() {
				queue = append(queue, e)
			}
		}
		var pending Set
		for sig := MinStandard; sig <= MaxStandard; sig++ {
			if ps.Pending.Has(sig) {
				pending = pending.Add(sig)
			}
		}
		// Re-queue a signal that was in-flight at snapshot time so its
		// delivery completes after the crash instead of vanishing.
		if ps.InflightSig.Valid() {
			if ps.InflightSig.Realtime() {
				queue = append([]rtEntry{{Sig: ps.InflightSig, Seq: ps.InflightSeq}}, queue...)
			} else {
				pending = pending.Add(ps.InflightSig)
			}
		}
		p := &process{
			id:       ps.ID,
			group:    ps.Group,
			mask:     ps.Mask,
			actions:  make(map[Signal]Action),
			pending:  pending,
			rtQueue:  queue,
			overflow: ps.Overflow,
			stopped:  ps.Stopped,
			dead:     ps.Dead,
			exitSig:  ps.ExitSig,
		}
		for sig, a := range ps.Actions {
			p.actions[sig] = a
		}
		r.procs[p.id] = p
	}
	// Delivery pass: pending signals that are unblocked (and not held back
	// by the stopped state) are delivered exactly once, in pid order.
	pids := make([]int, 0, len(r.procs))
	for pid := range r.procs {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	for _, pid := range pids {
		r.deliver(r.procs[pid])
	}
	return r, nil
}
