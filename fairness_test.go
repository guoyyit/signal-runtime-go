package signalrt

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestHighSignalDeliveredUnderLowSignalPressure reproduces the starvation
// scenario: a low-numbered signal keeps re-arriving (its handler re-sends
// it, a deterministic stand-in for a high arrival rate) while a
// high-numbered realtime signal waits. The realtime signal must be
// delivered within a bounded number of deliveries, not after the whole
// low-signal stream.
func TestHighSignalDeliveredUnderLowSignalPressure(t *testing.T) {
	var rec recorder
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)

	const chainLen = 500
	var usr1Calls int
	reg.Register("chain", func(pid int, sig Signal) {
		rec.handler()(pid, sig)
		usr1Calls++
		if usr1Calls == 1 {
			// The high-numbered signal arrives while the
			// low-numbered stream is in full flight.
			if err := rt.Send(pid, MinRT+1); err != nil {
				t.Error(err)
			}
		}
		if usr1Calls <= chainLen {
			if err := rt.Send(pid, SIGUSR1); err != nil {
				t.Error(err)
			}
		}
	})
	reg.Register("rt", rec.handler())
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: "chain"}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetAction(pid, MinRT+1, Action{Kind: ActionHandler, Handler: "rt"}); err != nil {
		t.Fatal(err)
	}

	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}

	got := rec.got()
	if len(got) != chainLen+2 {
		t.Fatalf("deliveries = %d, want %d (chain of %d + 1 realtime)", len(got), chainLen+2, chainLen+1)
	}
	// The realtime signal must not wait for the whole chain: it becomes
	// eligible in the pass after its arrival and is delivered there.
	if got[2] != MinRT+1 {
		t.Fatalf("realtime signal starved: delivered at position %d of %d, want position 2; order = %v...",
			indexOf(got, MinRT+1), len(got), got[:8])
	}
	var usr1Count, rtCount int
	for _, sig := range got {
		switch sig {
		case SIGUSR1:
			usr1Count++
		case MinRT + 1:
			rtCount++
		}
	}
	if usr1Count != chainLen+1 || rtCount != 1 {
		t.Fatalf("counts: SIGUSR1=%d want %d, MinRT+1=%d want 1", usr1Count, chainLen+1, rtCount)
	}
}

func indexOf(s []Signal, sig Signal) int {
	for i, v := range s {
		if v == sig {
			return i
		}
	}
	return -1
}

// TestNoStarvationWithConcurrentFlood floods the process with a
// low-numbered standard signal from a concurrent goroutine and requires
// that a high-numbered realtime signal sent in the middle of the flood is
// delivered while the flood is still ongoing.
func TestNoStarvationWithConcurrentFlood(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetAction(pid, MinRT+1, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if err := rt.Send(pid, SIGUSR1); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	// Let the flood reach full speed.
	time.Sleep(20 * time.Millisecond)

	if err := rt.Send(pid, MinRT+1); err != nil {
		t.Fatal(err)
	}
	// The realtime signal must be delivered while the flood continues.
	deadline := time.Now().Add(5 * time.Second)
	delivered := false
	for time.Now().Before(deadline) {
		for _, sig := range rec.got() {
			if sig == MinRT+1 {
				delivered = true
			}
		}
		if delivered {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !delivered {
		t.Fatal("realtime signal starved under sustained low-signal flood")
	}
	stop.Store(true)
	wg.Wait()

	// Drain whatever the flood left pending and check the final state:
	// exactly one realtime delivery, no goroutine left running.
	if err := rt.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	var rtCount int
	for _, sig := range rec.got() {
		if sig == MinRT+1 {
			rtCount++
		}
	}
	if rtCount != 1 {
		t.Fatalf("realtime signal delivered %d times, want exactly 1", rtCount)
	}
}

// TestHandlerTriggeredSendProgresses checks that signals sent from inside
// a handler are delivered promptly: a finite cascade must complete within
// the triggering Send call, in pass order.
func TestHandlerTriggeredSendProgresses(t *testing.T) {
	var rec recorder
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)
	reg.Register("a", func(pid int, sig Signal) {
		rec.handler()(pid, sig)
		if err := rt.Send(pid, SIGUSR2); err != nil {
			t.Error(err)
		}
	})
	reg.Register("b", func(pid int, sig Signal) {
		rec.handler()(pid, sig)
		if err := rt.Send(pid, MinRT); err != nil {
			t.Error(err)
		}
	})
	reg.Register("c", rec.handler())
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetAction(pid, SIGUSR2, Action{Kind: ActionHandler, Handler: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: "c"}); err != nil {
		t.Fatal(err)
	}

	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	// The cascade SIGUSR1 -> SIGUSR2 -> MinRT completes before Send
	// returns; each triggered signal is delivered in its own pass.
	want := []Signal{SIGUSR1, SIGUSR2, MinRT}
	got := rec.got()
	if len(got) != len(want) {
		t.Fatalf("deliveries = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("deliveries = %v, want %v", got, want)
		}
	}
}

// TestHandlerPingPongTerminates checks that two handlers endlessly
// triggering each other cannot make a single Send call loop forever: the
// delivery loop stops after its pass budget, Send returns, and the
// remaining signal stays pending for the next trigger.
func TestHandlerPingPongTerminates(t *testing.T) {
	var rec recorder
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)
	var stop atomic.Bool
	reg.Register("ping", func(pid int, sig Signal) {
		rec.handler()(pid, sig)
		if !stop.Load() {
			if err := rt.Send(pid, SIGUSR2); err != nil {
				t.Error(err)
			}
		}
	})
	reg.Register("pong", func(pid int, sig Signal) {
		rec.handler()(pid, sig)
		if !stop.Load() {
			if err := rt.Send(pid, SIGUSR1); err != nil {
				t.Error(err)
			}
		}
	})
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: "ping"}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetAction(pid, SIGUSR2, Action{Kind: ActionHandler, Handler: "pong"}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- rt.Send(pid, SIGUSR1) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Send did not return: handler ping-pong loops forever")
	}

	// The activation ran exactly its pass budget: one delivery per pass,
	// alternating between the two signals.
	counts := map[Signal]int{}
	for _, sig := range rec.got() {
		counts[sig]++
	}
	if counts[SIGUSR1] != maxDeliveryPasses/2 || counts[SIGUSR2] != maxDeliveryPasses/2 {
		t.Fatalf("deliveries per signal = %v, want %d each", counts, maxDeliveryPasses/2)
	}
	// The last handler's send stays pending; it is not lost.
	pending, _, err := rt.Pending(pid)
	if err != nil {
		t.Fatal(err)
	}
	if !pending.Has(SIGUSR1) {
		t.Fatal("the signal left over by the bounded loop must stay pending")
	}

	// The next trigger resumes delivery; with the ping-pong stopped the
	// residue drains immediately.
	stop.Store(true)
	if err := rt.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	counts = map[Signal]int{}
	for _, sig := range rec.got() {
		counts[sig]++
	}
	if counts[SIGUSR1] != maxDeliveryPasses/2+1 {
		t.Fatalf("after drain SIGUSR1 deliveries = %d, want %d", counts[SIGUSR1], maxDeliveryPasses/2+1)
	}
	pending, _, _ = rt.Pending(pid)
	if pending != EmptySet {
		t.Fatalf("pending = %v after drain, want empty", pending)
	}
}
