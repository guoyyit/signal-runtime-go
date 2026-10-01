package signalruntime

import (
	"errors"
	"sync/atomic"
	"testing"
)

func newTestRuntime(t *testing.T, procs ...int) *Runtime {
	t.Helper()
	r := NewRuntime()
	for _, pid := range procs {
		if err := r.NewProcess(pid, pid); err != nil {
			t.Fatalf("NewProcess(%d): %v", pid, err)
		}
	}
	return r
}

func TestStandardSignalCoalesces(t *testing.T) {
	r := newTestRuntime(t, 1)
	var got int32
	if _, err := r.SetAction(1, SIGUSR1, SigAction{Action: ActCatch, Name: "h",
		Handler: func(Signal, int) { atomic.AddInt32(&got, 1) }}); err != nil {
		t.Fatal(err)
	}
	r.SetMask(1, NewSigSet(SIGUSR1))
	if err := r.Send(1, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	if err := r.Send(1, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	std, rt, err := r.Pending(1)
	if err != nil || !std.Has(SIGUSR1) || len(rt) != 0 {
		t.Fatalf("pending = %v %v %v", std, rt, err)
	}
	r.SetMask(1, SigSet{})
	if atomic.LoadInt32(&got) != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestIgnoreAndDefault(t *testing.T) {
	r := newTestRuntime(t, 1)
	var got int32
	r.SetAction(1, SIGUSR1, SigAction{Action: ActIgnore})
	r.Send(1, SIGUSR1)
	if got != 0 {
		t.Fatalf("ignored signal delivered: %d", got)
	}
	// 未配置处置的 SIGTERM 走默认终止动作。
	if err := r.Send(1, SIGTERM); err != nil {
		t.Fatal(err)
	}
	if st, _ := r.State(1); st != "terminated" {
		t.Fatalf("state = %s, want terminated", st)
	}
	// 已终止进程再投递返回错误。
	if err := r.Send(1, SIGUSR1); !errors.Is(err, ErrTerminated) {
		t.Fatalf("err = %v, want ErrTerminated", err)
	}
}

func TestInvalidSignal(t *testing.T) {
	r := newTestRuntime(t, 1)
	for _, sig := range []Signal{0, 65, -1} {
		if err := r.Send(1, sig); !errors.Is(err, ErrInvalidSig) {
			t.Fatalf("Send(%d) err = %v", sig, err)
		}
	}
	if err := r.Send(99, SIGUSR1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
