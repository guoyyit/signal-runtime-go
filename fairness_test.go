package signalrt

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestFairnessUnderSustainedLowPressure sends one high-numbered realtime
// signal while a low-numbered standard signal keeps re-sending itself
// from its own handler (sustained arrival pressure). The realtime signal
// must be delivered within a bounded number of deliveries: round-based
// delivery defers mid-round arrivals to the next round, so a pending
// deliverable signal can never be starved by lower-numbered newcomers.
func TestFairnessUnderSustainedLowPressure(t *testing.T) {
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)

	const pressure = 1000 // length of the SIGUSR1 self-resend chain
	var mu sync.Mutex
	var order []Signal
	remaining := pressure

	reg.Register("low", func(pid int, sig Signal) {
		mu.Lock()
		order = append(order, sig)
		mu.Unlock()
		if remaining > 0 {
			remaining--
			if err := rt.Send(pid, SIGUSR1); err != nil {
				t.Error(err)
			}
		}
	})
	reg.Register("high", func(pid int, sig Signal) {
		mu.Lock()
		order = append(order, sig)
		mu.Unlock()
	})
	low := Action{Kind: ActionHandler, Handler: "low"}
	high := Action{Kind: ActionHandler, Handler: "high"}
	if err := rt.SetAction(pid, SIGUSR1, low); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetAction(pid, MinRT+5, high); err != nil {
		t.Fatal(err)
	}

	// Queue both while blocked: the realtime signal is pending from the
	// very start of the delivery loop.
	if err := rt.SetMask(pid, Of(SIGUSR1, MinRT+5)); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, MinRT+5); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	pos := -1
	for i, sig := range order {
		if sig == MinRT+5 {
			pos = i
			break
		}
	}
	if pos < 0 {
		t.Fatalf("realtime signal was never delivered, order = %v...", order[:8])
	}
	// Round 1 delivers the SIGUSR1 instance pending at its start, then
	// immediately the realtime signal; mid-round SIGUSR1 arrivals are
	// deferred to later rounds. Without fairness the realtime signal
	// would sit behind the whole 1000-long chain.
	if pos > 1 {
		t.Fatalf("realtime signal starved: delivered at position %d, want <= 1", pos)
	}
	// The whole pressure chain must still be delivered (no loss).
	counts := map[Signal]int{}
	for _, sig := range order {
		counts[sig]++
	}
	if counts[SIGUSR1] != pressure+1 {
		t.Fatalf("SIGUSR1 delivered %d times, want %d", counts[SIGUSR1], pressure+1)
	}
	if counts[MinRT+5] != 1 {
		t.Fatalf("realtime signal delivered %d times, want 1", counts[MinRT+5])
	}
}

// TestCrashDuringHandlerRedeliversAfterRestore simulates a crash while a
// handler is executing: the snapshot is taken from another goroutine in
// the middle of delivery. The in-flight signal must not vanish — Restore
// re-delivers it exactly once — and signals fully delivered before the
// crash must not be delivered again.
func TestCrashDuringHandlerRedeliversAfterRestore(t *testing.T) {
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)

	var rec1 recorder
	reg.Register("rec", rec1.handler())
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	reg.Register("block", func(_ int, _ Signal) {
		startOnce.Do(func() { close(started) })
		<-release // simulate a handler still running when the crash hits
	})
	if err := rt.SetAction(pid, SIGUSR2, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: "block"}); err != nil {
		t.Fatal(err)
	}

	// Fully delivered before the crash: must not be re-delivered.
	if err := rt.Send(pid, SIGUSR2); err != nil {
		t.Fatal(err)
	}
	if got := rec1.got(); len(got) != 1 || got[0] != SIGUSR2 {
		t.Fatalf("pre-crash deliveries = %v, want [SIGUSR2]", got)
	}

	// Start a delivery whose handler blocks mid-execution.
	sendDone := make(chan error, 1)
	go func() { sendDone <- rt.Send(pid, SIGUSR1) }()
	<-started

	// Crash here: the SIGUSR1 handler has started but not returned.
	data, err := rt.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	var rec2 recorder
	reg2 := NewHandlerRegistry()
	reg2.Register("rec", rec2.handler())
	reg2.Register("block", rec2.handler())
	rt2, err := Restore(data, reg2)
	if err != nil {
		t.Fatal(err)
	}

	// The in-flight signal is re-delivered exactly once by the restore
	// delivery pass; the pre-crash SIGUSR2 is not repeated.
	got := rec2.got()
	if len(got) != 1 || got[0] != SIGUSR1 {
		t.Fatalf("deliveries after restore = %v, want exactly [SIGUSR1]", got)
	}
	if pending, rtq, _ := rt2.Pending(pid); pending != EmptySet || len(rtq) != 0 {
		t.Fatalf("nothing may remain pending after restore delivery, got %v %v", pending, rtq)
	}

	// Let the crashed runtime's handler finish; it must not interfere.
	close(release)
	if err := <-sendDone; err != nil {
		t.Fatal(err)
	}
	if got := rec2.got(); len(got) != 1 {
		t.Fatalf("late handler return changed restored deliveries: %v", got)
	}
}

// TestCrashDuringRTHandlerRestoresFIFO covers an in-flight realtime
// signal: it is re-queued ahead of still-pending entries of the same
// signal, keeping per-signal FIFO order, and the mask in effect before
// the handler started is restored (not the temporary handler mask).
func TestCrashDuringRTHandlerRestoresFIFO(t *testing.T) {
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)

	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	reg.Register("block", func(_ int, _ Signal) {
		startOnce.Do(func() { close(started) })
		<-release
	})
	act := Action{Kind: ActionHandler, Handler: "block", HandlerMask: Of(SIGUSR2)}
	if err := rt.SetAction(pid, MinRT+1, act); err != nil {
		t.Fatal(err)
	}
	// Block the signal so three occurrences queue up, then let exactly
	// one delivery start (the handler blocks inside, so the unblocking
	// SetMask runs on its own goroutine).
	if err := rt.SetMask(pid, Of(MinRT+1)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := rt.Send(pid, MinRT+1); err != nil {
			t.Fatal(err)
		}
	}
	unblockDone := make(chan error, 1)
	go func() { unblockDone <- rt.SetMask(pid, EmptySet) }()
	<-started

	data, err := rt.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	var rec2 recorder
	reg2 := NewHandlerRegistry()
	reg2.Register("block", rec2.handler())
	rt2, err := Restore(data, reg2)
	if err != nil {
		t.Fatal(err)
	}
	// All three occurrences are delivered exactly once: the in-flight
	// one plus the two still queued — no loss, no duplication.
	want := []Signal{MinRT + 1, MinRT + 1, MinRT + 1}
	if got := rec2.got(); len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("deliveries after restore = %v, want %v", got, want)
	}
	// The persisted mask is the pre-handler mask (empty), not the
	// temporary union with HandlerMask.
	if mask, _ := rt2.Mask(pid); mask != EmptySet {
		t.Fatalf("restored mask = %v, want empty (handler mask must not leak)", mask)
	}
	close(release)
	if err := <-unblockDone; err != nil {
		t.Fatal(err)
	}
}

// TestHandlerTriggeredSendProgresses: a handler that sends another
// signal must neither starve already-pending signals nor prevent the
// triggering Send from returning. The handler of SIGUSR1 sends MinRT
// once; SIGUSR2 was pending from the start. All three must be delivered
// and the outer Send must return.
func TestHandlerTriggeredSendProgresses(t *testing.T) {
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)

	var mu sync.Mutex
	var order []Signal
	triggered := false
	record := func(sig Signal) {
		mu.Lock()
		order = append(order, sig)
		mu.Unlock()
	}
	reg.Register("usr1", func(pid int, sig Signal) {
		record(sig)
		if !triggered {
			triggered = true
			if err := rt.Send(pid, MinRT); err != nil {
				t.Error(err)
			}
		}
	})
	reg.Register("usr2", func(_ int, sig Signal) { record(sig) })
	reg.Register("rt", func(_ int, sig Signal) { record(sig) })
	act := map[Signal]Action{
		SIGUSR1: {Kind: ActionHandler, Handler: "usr1"},
		SIGUSR2: {Kind: ActionHandler, Handler: "usr2"},
		MinRT:   {Kind: ActionHandler, Handler: "rt"},
	}
	for sig, a := range act {
		if err := rt.SetAction(pid, sig, a); err != nil {
			t.Fatal(err)
		}
	}
	// SIGUSR2 pends first; SIGUSR1's handler will trigger MinRT.
	if err := rt.SetMask(pid, Of(SIGUSR1, SIGUSR2, MinRT)); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGUSR2); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- rt.SetMask(pid, EmptySet) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delivery with handler-triggered sends hung")
	}

	mu.Lock()
	defer mu.Unlock()
	// Round 1: SIGUSR1, SIGUSR2 (both pending at round start). The
	// handler-triggered MinRT is deferred to round 2.
	want := []Signal{SIGUSR1, SIGUSR2, MinRT}
	if len(order) != 3 || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] {
		t.Fatalf("delivery order = %v, want %v", order, want)
	}
}

// TestSelfTriggeringHandlerTerminates: a handler that re-sends its own
// signal on every invocation would loop forever without a bound. The
// delivery budget makes the triggering Send return; the leftover signal
// stays pending (not lost) and a later trigger resumes delivery.
func TestSelfTriggeringHandlerTerminates(t *testing.T) {
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)

	var delivered atomic.Int64
	reg.Register("loop", func(pid int, sig Signal) {
		delivered.Add(1)
		if err := rt.Send(pid, sig); err != nil {
			t.Error(err)
		}
	})
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: "loop"}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- rt.Send(pid, SIGUSR1) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("self-triggering handler made Send hang")
	}
	if got := delivered.Load(); got != deliveryBudget {
		t.Fatalf("delivered = %d, want exactly the budget %d", got, deliveryBudget)
	}
	// The last re-sent instance remains pending: deferred, not dropped.
	pending, _, err := rt.Pending(pid)
	if err != nil {
		t.Fatal(err)
	}
	if !pending.Has(SIGUSR1) {
		t.Fatal("leftover self-triggered signal must stay pending, not vanish")
	}

	// Stop the self-resend and confirm a later trigger resumes delivery
	// of the leftover signal. (SIGUSR2 gets a no-op handler so the
	// trigger itself does not terminate the process.)
	reg.Register("loop", func(pid int, sig Signal) { delivered.Add(1) })
	reg.Register("noop", func(int, Signal) {})
	if err := rt.SetAction(pid, SIGUSR2, Action{Kind: ActionHandler, Handler: "noop"}); err != nil {
		t.Fatal(err)
	}
	before := delivered.Load()
	if err := rt.Send(pid, SIGUSR2); err != nil {
		t.Fatal(err)
	}
	if got := delivered.Load(); got != before+1 {
		t.Fatalf("leftover delivery did not resume: delivered = %d, want %d", got, before+1)
	}
	if pending, _, _ := rt.Pending(pid); pending.Has(SIGUSR1) {
		t.Fatal("leftover SIGUSR1 should have been delivered by the later trigger")
	}
	if dead, _, _ := rt.Exited(pid); dead {
		t.Fatal("process must survive the resumption trigger")
	}
}

// TestConcurrentPressureNoLoss combines the concurrent-sender scenario
// with sustained low-numbered pressure: several goroutines flood
// SIGUSR1 while others send a high-numbered realtime signal and a third
// batch uses SendGroup. Every accepted realtime signal must be delivered
// exactly once (delivered + dropped == sent), the standard signal must
// be delivered at least once despite coalescing, and the whole run must
// finish without hanging.
func TestConcurrentPressureNoLoss(t *testing.T) {
	reg := NewHandlerRegistry()
	var rec recorder
	reg.Register("rec", rec.handler())
	rt := New(reg)
	pid := rt.Spawn(1)
	peer := rt.Spawn(1) // same group: receives the SendGroup traffic
	for _, p := range []int{pid, peer} {
		for _, sig := range []Signal{SIGUSR1, MinRT + 7} {
			if err := rt.SetAction(p, sig, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
				t.Fatal(err)
			}
		}
	}

	const (
		flooders   = 4 // goroutines flooding SIGUSR1
		floodEach  = 200
		rtSenders  = 4 // goroutines sending the realtime signal
		rtEach     = 30
		groupSends = 20 // SendGroup(SIGUSR1) rounds
	)
	var rtDropped atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < flooders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < floodEach; j++ {
				if err := rt.Send(pid, SIGUSR1); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	for i := 0; i < rtSenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < rtEach; j++ {
				err := rt.Send(pid, MinRT+7)
				if errors.Is(err, ErrQueueOverflow) {
					rtDropped.Add(1)
				} else if err != nil {
					t.Error(err)
				}
			}
		}()
	}
	for i := 0; i < groupSends; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rt.SendGroup(1, SIGUSR1); err != nil {
				t.Error(err)
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent senders hung")
	}

	// The realtime signal must be fully accounted for despite the
	// concurrent SIGUSR1 flood: no silent loss, no duplication.
	counts := map[Signal]int{}
	for _, sig := range rec.got() {
		counts[sig]++
	}
	rtSent := int64(rtSenders * rtEach)
	if got := int64(counts[MinRT+7]); got+rtDropped.Load() != rtSent {
		t.Fatalf("realtime delivered(%d) + dropped(%d) != sent(%d)",
			got, rtDropped.Load(), rtSent)
	}
	if n, _ := rt.Overflow(pid); n != uint64(rtDropped.Load()) {
		t.Fatalf("overflow counter = %d, want %d", n, rtDropped.Load())
	}
	// The standard signal was delivered at least once per process in
	// spite of coalescing under pressure.
	if counts[SIGUSR1] < 2 {
		t.Fatalf("SIGUSR1 delivered %d times for 2 processes, want >= 2", counts[SIGUSR1])
	}
}

// TestConcurrentSendThenRestoreKeepsAccounting stops the senders, then
// snapshots and restores: signals accepted before the crash but not yet
// delivered must be delivered by the restored runtime, so the total
// across both runtimes still matches sent-minus-dropped exactly.
func TestConcurrentSendThenRestoreKeepsAccounting(t *testing.T) {
	reg := NewHandlerRegistry()
	var rec1 recorder
	reg.Register("rec", rec1.handler())
	rt := New(reg)
	pid := rt.Spawn(1)
	// Keep the realtime signal blocked so sends race to enqueue; the
	// mask is lifted only after the restore.
	if err := rt.SetAction(pid, MinRT+3, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetMask(pid, Of(MinRT+3)); err != nil {
		t.Fatal(err)
	}

	const senders = 6
	const each = 20
	var dropped atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				err := rt.Send(pid, MinRT+3)
				if errors.Is(err, ErrQueueOverflow) {
					dropped.Add(1)
				} else if err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()

	data, err := rt.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var rec2 recorder
	reg2 := NewHandlerRegistry()
	reg2.Register("rec", rec2.handler())
	rt2, err := Restore(data, reg2)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt2.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}

	sent := int64(senders * each)
	total := int64(len(rec1.got()) + len(rec2.got()))
	if total+dropped.Load() != sent {
		t.Fatalf("across crash: delivered(%d) + dropped(%d) != sent(%d)",
			total, dropped.Load(), sent)
	}
	if pending, rtq, _ := rt2.Pending(pid); pending != EmptySet || len(rtq) != 0 {
		t.Fatalf("restored runtime must drain everything, left %v %v", pending, rtq)
	}
}
