package signalrt

import (
	"reflect"
	"testing"
)

func TestSnapshotRestorePreservesState(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(11)
	act := Action{Kind: ActionHandler, Handler: name, Restart: true}
	for _, sig := range []Signal{SIGUSR1, MinRT} {
		if err := rt.SetAction(pid, sig, act); err != nil {
			t.Fatal(err)
		}
	}
	if err := rt.SetMask(pid, Of(SIGUSR1, MinRT)); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := rt.Send(pid, MinRT); err != nil {
			t.Fatal(err)
		}
	}
	stopped := rt.Spawn(11)
	if err := rt.Send(stopped, SIGSTOP); err != nil {
		t.Fatal(err)
	}
	dead := rt.Spawn(22)
	if err := rt.Send(dead, SIGKILL); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("nothing may be delivered before restore, got %v", got)
	}

	data, err := rt.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a crash: build a fresh runtime from the snapshot only.
	var rec2 recorder
	reg2 := NewHandlerRegistry()
	reg2.Register("rec", rec2.handler())
	rt2, err := Restore(data, reg2)
	if err != nil {
		t.Fatal(err)
	}

	// Dispositions, mask and stop/exit state survive.
	a, err := rt2.ActionOf(pid, SIGUSR1)
	if err != nil || a.Kind != ActionHandler || a.Handler != name || !a.Restart {
		t.Fatalf("restored action = %+v, %v", a, err)
	}
	if mask, _ := rt2.Mask(pid); mask != Of(SIGUSR1, MinRT) {
		t.Fatalf("restored mask = %v", mask)
	}
	if stopped2, _ := rt2.Stopped(stopped); !stopped2 {
		t.Fatal("stopped state lost")
	}
	if dead2, sig, _ := rt2.Exited(dead); !dead2 || sig != SIGKILL {
		t.Fatalf("exit state lost: %v %v", dead2, sig)
	}
	// Pending queues survive; the restore delivery pass respects the mask.
	pending, rtq, _ := rt2.Pending(pid)
	if !pending.Has(SIGUSR1) || len(rtq) != 3 {
		t.Fatalf("restored pending = %v %v", pending, rtq)
	}
	if got := rec2.got(); len(got) != 0 {
		t.Fatalf("blocked signals delivered during restore: %v", got)
	}
	// Unblock: everything pending is delivered exactly once, in order.
	if err := rt2.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	want := []Signal{SIGUSR1, MinRT, MinRT, MinRT}
	if got := rec2.got(); !reflect.DeepEqual(got, want) {
		t.Fatalf("delivery after restore = %v, want %v", got, want)
	}
	// Group membership survives.
	if err := rt2.SendGroup(11, SIGTERM); err != nil {
		t.Fatal(err)
	}
	if dead2, _, _ := rt2.Exited(pid); !dead2 {
		t.Fatal("group send after restore must reach restored members")
	}
}

func TestRestoreStoppedProcessDeliversAfterContinue(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGSTOP); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := rt.Send(pid, MinRT); err != nil {
			t.Fatal(err)
		}
	}
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
	// Restore must not deliver to a stopped process.
	if got := rec2.got(); len(got) != 0 {
		t.Fatalf("restore delivered to stopped process: %v", got)
	}
	if err := rt2.Send(pid, SIGCONT); err != nil {
		t.Fatal(err)
	}
	want := []Signal{MinRT, MinRT}
	if got := rec2.got(); !reflect.DeepEqual(got, want) {
		t.Fatalf("delivery after continue = %v, want exactly %v (no loss, no dup)", got, want)
	}
}

func TestRestoreOverflowCounterPreserved(t *testing.T) {
	rt := New(nil)
	pid := rt.Spawn(1)
	if err := rt.SetMask(pid, Of(MinRT)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rtQueueCap+4; i++ {
		_ = rt.Send(pid, MinRT)
	}
	data, err := rt.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	rt2, err := Restore(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := rt2.Overflow(pid); n != 4 {
		t.Fatalf("restored overflow = %d, want 4", n)
	}
	if _, rtq, _ := rt2.Pending(pid); len(rtq) != rtQueueCap {
		t.Fatalf("restored queue length = %d, want %d", len(rtq), rtQueueCap)
	}
}

func TestRestoreMissingHandlerFallsBackToDefault(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetMask(pid, Of(SIGUSR1)); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	data, err := rt.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// Restore into a runtime whose registry lacks "rec": the disposition
	// is kept, but delivery falls back to the default action (terminate).
	rt2, err := Restore(data, NewHandlerRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if err := rt2.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	if dead, sig, _ := rt2.Exited(pid); !dead || sig != SIGUSR1 {
		t.Fatalf("missing handler must fall back to default action, Exited = %v, %v", dead, sig)
	}
}

func TestRestoreRejectsGarbage(t *testing.T) {
	if _, err := Restore([]byte("{not json"), nil); err == nil {
		t.Fatal("garbage snapshot must be rejected")
	}
}
