package signalrt

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

// recorder collects delivered signals from handlers.
type recorder struct {
	mu   sync.Mutex
	sigs []Signal
}

func (rec *recorder) handler() HandlerFunc {
	return func(_ int, sig Signal) {
		rec.mu.Lock()
		rec.sigs = append(rec.sigs, sig)
		rec.mu.Unlock()
	}
}

func (rec *recorder) got() []Signal {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]Signal(nil), rec.sigs...)
}

func newRuntimeWithHandler(t *testing.T, rec *recorder) (*Runtime, string) {
	t.Helper()
	reg := NewHandlerRegistry()
	reg.Register("rec", rec.handler())
	return New(reg), "rec"
}

func TestStandardSignalCoalesces(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetMask(pid, Of(SIGUSR1)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := rt.Send(pid, SIGUSR1); err != nil {
			t.Fatal(err)
		}
	}
	pending, _, err := rt.Pending(pid)
	if err != nil {
		t.Fatal(err)
	}
	if !pending.Has(SIGUSR1) {
		t.Fatal("SIGUSR1 should be pending while blocked")
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("handler ran while blocked: %v", got)
	}
	if err := rt.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); len(got) != 1 || got[0] != SIGUSR1 {
		t.Fatalf("standard signal must coalesce to one delivery, got %v", got)
	}
}

func TestRealtimeSignalsQueueFIFO(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetMask(pid, Of(MinRT)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := rt.Send(pid, MinRT); err != nil {
			t.Fatal(err)
		}
	}
	if err := rt.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	want := []Signal{MinRT, MinRT, MinRT}
	if got := rec.got(); !reflect.DeepEqual(got, want) {
		t.Fatalf("realtime signals must each be delivered, got %v want %v", got, want)
	}
}

func TestDeliveryOrderLowestNumberFirst(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	act := Action{Kind: ActionHandler, Handler: name}
	for _, sig := range []Signal{SIGUSR2, SIGUSR1, MinRT + 1, MinRT} {
		if err := rt.SetAction(pid, sig, act); err != nil {
			t.Fatal(err)
		}
	}
	mask := Of(SIGUSR1, SIGUSR2, MinRT, MinRT+1)
	if err := rt.SetMask(pid, mask); err != nil {
		t.Fatal(err)
	}
	// Send in "wrong" order; delivery must follow ascending signal number,
	// standard before realtime, same realtime signal FIFO.
	send := []Signal{MinRT + 1, MinRT, SIGUSR2, MinRT, SIGUSR1}
	for _, sig := range send {
		if err := rt.Send(pid, sig); err != nil {
			t.Fatal(err)
		}
	}
	if err := rt.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	want := []Signal{SIGUSR1, SIGUSR2, MinRT, MinRT, MinRT + 1}
	if got := rec.got(); !reflect.DeepEqual(got, want) {
		t.Fatalf("delivery order = %v, want %v", got, want)
	}
}

func TestRealtimeQueueOverflowDropsNewest(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetMask(pid, Of(MinRT)); err != nil {
		t.Fatal(err)
	}
	total := rtQueueCap + 5
	var overflows int
	for i := 0; i < total; i++ {
		err := rt.Send(pid, MinRT)
		if errors.Is(err, ErrQueueOverflow) {
			overflows++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if overflows != 5 {
		t.Fatalf("expected 5 overflow errors, got %d", overflows)
	}
	if n, err := rt.Overflow(pid); err != nil || n != 5 {
		t.Fatalf("overflow counter = %d, %v; want 5, nil", n, err)
	}
	if err := rt.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); len(got) != rtQueueCap {
		t.Fatalf("expected %d deliveries after overflow, got %d", rtQueueCap, len(got))
	}
}

func TestIgnoreDisposition(t *testing.T) {
	rt := New(nil)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, SIGTERM, Action{Kind: ActionIgnore}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGTERM); err != nil {
		t.Fatal(err)
	}
	dead, _, err := rt.Exited(pid)
	if err != nil {
		t.Fatal(err)
	}
	if dead {
		t.Fatal("ignored SIGTERM must not terminate the process")
	}
}

func TestDefaultTerminateAndExitSignal(t *testing.T) {
	rt := New(nil)
	pid := rt.Spawn(1)
	if err := rt.Send(pid, SIGTERM); err != nil {
		t.Fatal(err)
	}
	dead, sig, err := rt.Exited(pid)
	if err != nil {
		t.Fatal(err)
	}
	if !dead || sig != SIGTERM {
		t.Fatalf("Exited = %v, %v; want true, SIGTERM", dead, sig)
	}
	if err := rt.Send(pid, SIGUSR1); !errors.Is(err, ErrProcessExited) {
		t.Fatalf("send to dead process = %v, want ErrProcessExited", err)
	}
}

func TestUncatchableSignals(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	for _, sig := range []Signal{SIGKILL, SIGSTOP} {
		if err := rt.SetAction(pid, sig, Action{Kind: ActionHandler, Handler: name}); !errors.Is(err, ErrInvalidSignal) {
			t.Fatalf("SetAction(%d) = %v, want ErrInvalidSignal", sig, err)
		}
		if err := rt.SetAction(pid, sig, Action{Kind: ActionIgnore}); !errors.Is(err, ErrInvalidSignal) {
			t.Fatalf("SetAction(%d, ignore) = %v, want ErrInvalidSignal", sig, err)
		}
	}
	// Mask silently strips uncatchable signals.
	if err := rt.SetMask(pid, Of(SIGKILL, SIGSTOP, SIGUSR1)); err != nil {
		t.Fatal(err)
	}
	mask, err := rt.Mask(pid)
	if err != nil {
		t.Fatal(err)
	}
	if mask.Has(SIGKILL) || mask.Has(SIGSTOP) || !mask.Has(SIGUSR1) {
		t.Fatalf("mask = %v, want only SIGUSR1", mask)
	}
	// SIGKILL terminates even when "blocked" and "caught".
	if err := rt.Send(pid, SIGKILL); err != nil {
		t.Fatal(err)
	}
	dead, sig, _ := rt.Exited(pid)
	if !dead || sig != SIGKILL {
		t.Fatalf("SIGKILL must terminate, Exited = %v, %v", dead, sig)
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("no handler may run for SIGKILL, got %v", got)
	}
}

func TestHandlerMaskUnionDuringHandler(t *testing.T) {
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)
	if err := rt.SetMask(pid, Of(SIGHUP)); err != nil {
		t.Fatal(err)
	}
	var seen Set
	reg.Register("probe", func(pid int, sig Signal) {
		seen, _ = rt.Mask(pid)
	})
	act := Action{
		Kind:        ActionHandler,
		Handler:     "probe",
		HandlerMask: Of(SIGUSR2),
	}
	if err := rt.SetAction(pid, SIGUSR1, act); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	// Inside the handler: original mask ∪ handler mask ∪ the signal itself.
	for _, sig := range []Signal{SIGHUP, SIGUSR1, SIGUSR2} {
		if !seen.Has(sig) {
			t.Fatalf("mask during handler should contain %d, got %v", sig, seen)
		}
	}
	// After the handler the original mask is restored.
	mask, _ := rt.Mask(pid)
	if mask != Of(SIGHUP) {
		t.Fatalf("mask after handler = %v, want SIGHUP only", mask)
	}
}

func TestNoDeferLeavesSignalOutOfHandlerMask(t *testing.T) {
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)
	var seen Set
	reg.Register("probe", func(pid int, sig Signal) {
		seen, _ = rt.Mask(pid)
	})
	act := Action{Kind: ActionHandler, Handler: "probe", NoDefer: true}
	if err := rt.SetAction(pid, SIGUSR1, act); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	if seen.Has(SIGUSR1) {
		t.Fatalf("NoDefer handler mask must not contain the signal, got %v", seen)
	}
}

func TestSameSignalHandlerNotInterrupted(t *testing.T) {
	reg := NewHandlerRegistry()
	rt := New(reg)
	pid := rt.Spawn(1)
	var mu sync.Mutex
	var events []string
	reg.Register("reent", func(pid int, sig Signal) {
		mu.Lock()
		events = append(events, "enter")
		first := len(events) == 1
		mu.Unlock()
		// Re-send the same signal from inside its own handler, once.
		if first {
			if err := rt.Send(pid, SIGUSR1); err != nil {
				t.Error(err)
			}
		}
		mu.Lock()
		events = append(events, "exit")
		mu.Unlock()
	})
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: "reent"}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	want := []string{"enter", "exit", "enter", "exit"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v (no re-entrant interruption)", events, want)
	}
}

func TestUnknownHandlerRejected(t *testing.T) {
	rt := New(nil)
	pid := rt.Spawn(1)
	err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: "nope"})
	if !errors.Is(err, ErrUnknownHandler) {
		t.Fatalf("SetAction with unknown handler = %v, want ErrUnknownHandler", err)
	}
}

func TestUnknownProcessErrors(t *testing.T) {
	rt := New(nil)
	if err := rt.Send(42, SIGUSR1); !errors.Is(err, ErrUnknownProcess) {
		t.Fatalf("Send = %v", err)
	}
	if _, err := rt.Mask(42); !errors.Is(err, ErrUnknownProcess) {
		t.Fatalf("Mask = %v", err)
	}
	if err := rt.Send(42, Signal(0)); !errors.Is(err, ErrInvalidSignal) {
		t.Fatalf("invalid signal = %v", err)
	}
}
