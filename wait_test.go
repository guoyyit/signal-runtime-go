package signalrt

import (
	"errors"
	"testing"
	"time"
)

func TestWaitInterruptedByCaughtSignal(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		if err := rt.Send(pid, SIGUSR1); err != nil {
			t.Error(err)
		}
	}()
	start := time.Now()
	err := rt.Wait(pid, 10*time.Second)
	if !errors.Is(err, ErrInterrupted) {
		t.Fatalf("Wait = %v, want ErrInterrupted", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("interrupt must return promptly, not at timeout")
	}
	if got := rec.got(); len(got) != 1 || got[0] != SIGUSR1 {
		t.Fatalf("handler must run before Wait returns, got %v", got)
	}
}

func TestWaitRestartResumesWaiting(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	act := Action{Kind: ActionHandler, Handler: name, Restart: true}
	if err := rt.SetAction(pid, SIGUSR1, act); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		if err := rt.Send(pid, SIGUSR1); err != nil {
			t.Error(err)
		}
		time.Sleep(40 * time.Millisecond)
		rt.Wake(pid)
	}()
	start := time.Now()
	err := rt.Wait(pid, 10*time.Second)
	if err != nil {
		t.Fatalf("Restart disposition must resume the wait, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("wait returned after %v; it must have survived the signal at ~20ms", elapsed)
	}
	if got := rec.got(); len(got) != 1 {
		t.Fatalf("handler should have run once, got %v", got)
	}
}

func TestWaitTimeoutAndWake(t *testing.T) {
	rt := New(nil)
	pid := rt.Spawn(1)
	start := time.Now()
	if err := rt.Wait(pid, 30*time.Millisecond); err != nil {
		t.Fatalf("timeout wait = %v, want nil", err)
	}
	if time.Since(start) < 25*time.Millisecond {
		t.Fatal("wait returned before the timeout")
	}
	// Wake releases the waiter immediately.
	done := make(chan error, 1)
	go func() { done <- rt.Wait(pid, 10*time.Second) }()
	time.Sleep(20 * time.Millisecond)
	rt.Wake(pid)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("woken wait = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wake did not release the waiter")
	}
}

func TestWaitOnTerminatedProcess(t *testing.T) {
	rt := New(nil)
	pid := rt.Spawn(1)
	done := make(chan error, 1)
	go func() { done <- rt.Wait(pid, 10*time.Second) }()
	time.Sleep(20 * time.Millisecond)
	if err := rt.Send(pid, SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrProcessExited) {
			t.Fatalf("wait on killed process = %v, want ErrProcessExited", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("termination did not release the waiter")
	}
	if err := rt.Wait(pid, time.Millisecond); !errors.Is(err, ErrProcessExited) {
		t.Fatalf("wait on dead process = %v, want ErrProcessExited", err)
	}
	if err := rt.Wait(999, time.Millisecond); !errors.Is(err, ErrUnknownProcess) {
		t.Fatalf("wait on unknown process = %v, want ErrUnknownProcess", err)
	}
}

func TestSignalBeforeWaitDoesNotInterrupt(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	// Deliver the signal before the wait begins; it must not poison it.
	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	if err := rt.Wait(pid, 20*time.Millisecond); err != nil {
		t.Fatalf("wait after handled signal = %v, want nil", err)
	}
}

func TestBlockedSignalDoesNotInterruptWait(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	if err := rt.SetMask(pid, Of(SIGUSR1)); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		if err := rt.Send(pid, SIGUSR1); err != nil {
			t.Error(err)
		}
	}()
	// The signal stays pending and blocked: no handler, no interrupt.
	if err := rt.Wait(pid, 60*time.Millisecond); err != nil {
		t.Fatalf("blocked signal must not interrupt, got %v", err)
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("blocked handler ran: %v", got)
	}
}
