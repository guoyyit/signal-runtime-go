package signalrt

import (
	"reflect"
	"testing"
)

// TestCrashDuringHandlerDeliveryRecoversInflight simulates a crash while a
// handler is executing: the snapshot is taken from inside the handler, so
// the signal being delivered is neither pending nor delivered. Restore
// must complete its delivery exactly once, must not repeat signals that
// were fully delivered before the crash, and must not lose signals that
// were still pending.
func TestCrashDuringHandlerDeliveryRecoversInflight(t *testing.T) {
	var rec1 recorder
	reg1 := NewHandlerRegistry()
	reg1.Register("rec", rec1.handler())
	rt1 := New(reg1)
	pid := rt1.Spawn(1)

	var snapshot []byte
	var snapshotErr error
	reg1.Register("crashy", func(pid int, sig Signal) {
		rec1.handler()(pid, sig)
		// The process "crashes" here, mid-delivery: the last persisted
		// state is this snapshot.
		snapshot, snapshotErr = rt1.Snapshot()
	})

	if err := rt1.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
		t.Fatal(err)
	}
	if err := rt1.SetAction(pid, SIGUSR2, Action{Kind: ActionHandler, Handler: "crashy"}); err != nil {
		t.Fatal(err)
	}
	if err := rt1.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
		t.Fatal(err)
	}
	// Queue everything while blocked, then release: SIGUSR1 is delivered
	// before the crash, SIGUSR2 is in flight during the crash, and the
	// two realtime occurrences are still pending at the crash.
	if err := rt1.SetMask(pid, Of(SIGUSR1, SIGUSR2, MinRT)); err != nil {
		t.Fatal(err)
	}
	for _, sig := range []Signal{SIGUSR1, SIGUSR2, MinRT, MinRT} {
		if err := rt1.Send(pid, sig); err != nil {
			t.Fatal(err)
		}
	}
	if err := rt1.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	if snapshot == nil {
		t.Fatal("handler did not take a snapshot")
	}
	// Sanity: the pre-crash runtime delivered SIGUSR1, ran the crashing
	// handler, and then drained the queue (that tail is lost in a real
	// crash; only the snapshot survives).
	if got := rec1.got(); len(got) != 4 {
		t.Fatalf("pre-crash deliveries = %v", got)
	}

	// Restart from the snapshot with a fresh registry/runtime.
	var rec2 recorder
	reg2 := NewHandlerRegistry()
	reg2.Register("rec", rec2.handler())
	reg2.Register("crashy", rec2.handler())
	rt2, err := Restore(snapshot, reg2)
	if err != nil {
		t.Fatal(err)
	}

	// Exactly once across the crash boundary:
	//   - SIGUSR1 (fully delivered before the crash) must not repeat;
	//   - SIGUSR2 (in flight at the crash) must be delivered once;
	//   - both pending realtime occurrences must be delivered once.
	want := []Signal{SIGUSR2, MinRT, MinRT}
	if got := rec2.got(); !reflect.DeepEqual(got, want) {
		t.Fatalf("post-crash deliveries = %v, want %v", got, want)
	}
	// The restored mask is the durable pre-handler mask, not the
	// transient handler mask (which contained SIGUSR2 at crash time).
	if mask, _ := rt2.Mask(pid); mask != EmptySet {
		t.Fatalf("restored mask = %v, want empty (pre-handler mask)", mask)
	}
	if pending, rtq, _ := rt2.Pending(pid); pending != EmptySet || len(rtq) != 0 {
		t.Fatalf("nothing may remain pending after recovery, got %v %v", pending, rtq)
	}
}

// TestCrashMidDeliveryPreservesRealtimeOrder crashes with a realtime
// signal in flight and more realtime occurrences queued behind it. The
// restored runtime must deliver every occurrence exactly once, in the
// deterministic (signal, arrival) order.
func TestCrashMidDeliveryPreservesRealtimeOrder(t *testing.T) {
	var rec1 recorder
	reg1 := NewHandlerRegistry()
	rt1 := New(reg1)
	pid := rt1.Spawn(1)

	var snapshot []byte
	crashed := false
	reg1.Register("crashy", func(_ int, _ Signal) {
		rec1.handler()(pid, MinRT)
		if !crashed {
			crashed = true
			snapshot, _ = rt1.Snapshot()
		}
	})
	reg1.Register("rec", rec1.handler())
	if err := rt1.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: "crashy"}); err != nil {
		t.Fatal(err)
	}
	if err := rt1.SetAction(pid, MinRT+1, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
		t.Fatal(err)
	}
	if err := rt1.SetMask(pid, Of(MinRT, MinRT+1)); err != nil {
		t.Fatal(err)
	}
	// Interleaved arrival order: RT+1, RT, RT+1, RT.
	for _, sig := range []Signal{MinRT + 1, MinRT, MinRT + 1, MinRT} {
		if err := rt1.Send(pid, sig); err != nil {
			t.Fatal(err)
		}
	}
	if err := rt1.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	if snapshot == nil {
		t.Fatal("no snapshot taken")
	}

	var rec2 recorder
	reg2 := NewHandlerRegistry()
	reg2.Register("crashy", rec2.handler())
	reg2.Register("rec", rec2.handler())
	if _, err := Restore(snapshot, reg2); err != nil {
		t.Fatal(err)
	}
	// At crash time the first MinRT was in flight and three occurrences
	// were queued. Recovery delivers all four exactly once, ordered by
	// (signal number, arrival): MinRT, MinRT, MinRT+1, MinRT+1.
	want := []Signal{MinRT, MinRT, MinRT + 1, MinRT + 1}
	if got := rec2.got(); !reflect.DeepEqual(got, want) {
		t.Fatalf("post-crash deliveries = %v, want %v", got, want)
	}
}

// TestSnapshotWhileHandlerBlocked takes a snapshot from another goroutine
// while a handler is blocked mid-execution and concurrent sends are
// queueing behind it. The in-flight signal and everything queued behind
// it must survive the restart exactly once.
func TestSnapshotWhileHandlerBlocked(t *testing.T) {
	var rec1 recorder
	reg1 := NewHandlerRegistry()
	reg1.Register("rec", rec1.handler())
	rt1 := New(reg1)
	pid := rt1.Spawn(1)

	entered := make(chan struct{})
	release := make(chan struct{})
	reg1.Register("block", func(_ int, _ Signal) {
		close(entered)
		<-release
	})
	if err := rt1.SetAction(pid, SIGUSR2, Action{Kind: ActionHandler, Handler: "block"}); err != nil {
		t.Fatal(err)
	}
	if err := rt1.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
		t.Fatal(err)
	}

	sendDone := make(chan error, 1)
	go func() { sendDone <- rt1.Send(pid, SIGUSR2) }()
	<-entered // the handler is now executing, delivery mid-flight

	// Concurrent sends queue up behind the in-flight delivery.
	const rtSends = 3
	for i := 0; i < rtSends; i++ {
		if err := rt1.Send(pid, MinRT); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := rt1.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-sendDone; err != nil {
		t.Fatal(err)
	}

	var rec2 recorder
	reg2 := NewHandlerRegistry()
	reg2.Register("rec", rec2.handler())
	reg2.Register("block", rec2.handler())
	if _, err := Restore(snapshot, reg2); err != nil {
		t.Fatal(err)
	}
	// The restored runtime completes the in-flight SIGUSR2 and delivers
	// every queued realtime occurrence exactly once.
	want := []Signal{SIGUSR2, MinRT, MinRT, MinRT}
	if got := rec2.got(); !reflect.DeepEqual(got, want) {
		t.Fatalf("post-crash deliveries = %v, want %v", got, want)
	}
}

// TestCrashBetweenDeliveriesLosesNothing covers the complementary crash
// window: the snapshot is taken while no delivery is in flight, so the
// restored runtime must see exactly the pending state.
func TestCrashBetweenDeliveriesLosesNothing(t *testing.T) {
	var rec1 recorder
	rt1, name := newRuntimeWithHandler(t, &rec1)
	pid := rt1.Spawn(1)
	if err := rt1.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	if err := rt1.SetMask(pid, Of(SIGUSR1)); err != nil {
		t.Fatal(err)
	}
	if err := rt1.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	snapshot, err := rt1.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	var rec2 recorder
	reg2 := NewHandlerRegistry()
	reg2.Register("rec", rec2.handler())
	rt2, err := Restore(snapshot, reg2)
	if err != nil {
		t.Fatal(err)
	}
	// Blocked at crash time: still pending, not delivered.
	if got := rec2.got(); len(got) != 0 {
		t.Fatalf("blocked signal delivered during restore: %v", got)
	}
	if err := rt2.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	if got := rec2.got(); !reflect.DeepEqual(got, []Signal{SIGUSR1}) {
		t.Fatalf("deliveries after unblock = %v, want exactly [SIGUSR1]", got)
	}
}
