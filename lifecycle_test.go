package signalruntime

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestUncatchableSignals(t *testing.T) {
	r := newTestRuntime(t, 1)
	if _, err := r.SetAction(1, SIGKILL, SigAction{Action: ActCatch,
		Handler: func(Signal, int) { t.Fatal("SIGKILL handler must not run") }}); !errors.Is(err, ErrUncatchable) {
		t.Fatalf("SetAction(SIGKILL) err = %v", err)
	}
	if _, err := r.SetAction(1, SIGSTOP, SigAction{Action: ActIgnore}); !errors.Is(err, ErrUncatchable) {
		t.Fatalf("SetAction(SIGSTOP) err = %v", err)
	}
	// SIGKILL 不能被屏蔽，必须执行默认终止。
	r.SetMask(1, NewSigSet(SIGKILL))
	if err := r.Send(1, SIGKILL); err != nil {
		t.Fatal(err)
	}
	if st, _ := r.State(1); st != "terminated" {
		t.Fatalf("state = %s, want terminated", st)
	}
	std, _, _ := r.Pending(1)
	if !std.Empty() {
		t.Fatalf("pending after kill = %v", std)
	}

	// SIGSTOP 不能被屏蔽，必须停止；SIGKILL 可穿透停止。
	r2 := newTestRuntime(t, 2)
	r2.SetMask(2, NewSigSet(SIGSTOP))
	if err := r2.Send(2, SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if st, _ := r.State(1); st != "terminated" {
		t.Fatalf("unexpected")
	}
	if st, _ := r2.State(2); st != "stopped" {
		t.Fatalf("state = %s, want stopped", st)
	}
	r2.SetAction(2, SIGTSTP, SigAction{Action: ActCatch, Name: "t",
		Handler: func(Signal, int) { t.Fatal("must not deliver while stopped") }})
	if err := r2.Send(2, SIGTSTP); err != nil {
		t.Fatal(err)
	}
	if err := r2.Send(2, SIGKILL); err != nil {
		t.Fatal(err)
	}
	if st, _ := r2.State(2); st != "terminated" {
		t.Fatalf("state = %s, want terminated", st)
	}
}

func TestStopContinuePreservesSignals(t *testing.T) {
	r := newTestRuntime(t, 1)
	var mu sync.Mutex
	var order []int
	handler := func(s Signal, _ int) {
		mu.Lock()
		order = append(order, int(s))
		mu.Unlock()
	}
	r.SetAction(1, SIGRTMIN, SigAction{Action: ActCatch, Name: "rt", Handler: handler})
	r.SetAction(1, SIGUSR1, SigAction{Action: ActCatch, Name: "u1", Handler: handler})

	// 先排队一个被屏蔽的实时信号，再被 SIGSTOP 停止。
	r.SetMask(1, NewSigSet(SIGRTMIN, SIGUSR1))
	if err := r.Send(1, SIGRTMIN); err != nil {
		t.Fatal(err)
	}
	if err := r.Send(1, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	if err := r.Send(1, SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if st, _ := r.State(1); st != "stopped" {
		t.Fatalf("state = %s", st)
	}
	// 停止期间到达的实时与标准信号保持待决，不交付。
	if err := r.Send(1, SIGRTMIN); err != nil {
		t.Fatal(err)
	}
	if err := r.Send(1, SIGRTMIN); err != nil {
		t.Fatal(err)
	}
	if err := r.Send(1, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	if len(order) != 0 {
		t.Fatalf("delivered while stopped: %v", order)
	}

	if err := r.Send(1, SIGCONT); err != nil {
		t.Fatal(err)
	}
	if st, _ := r.State(1); st != "running" {
		t.Fatalf("state after cont = %s", st)
	}
	r.SetMask(1, SigSet{})
	// 3 个实时实例 + 1 个折叠后的标准信号实例，全部保留并交付。
	if len(order) != 4 {
		t.Fatalf("delivered = %d, want 4: %v", len(order), order)
	}
	rtCount := 0
	for _, s := range order {
		if s == int(SIGRTMIN) {
			rtCount++
		}
	}
	if rtCount != 3 {
		t.Fatalf("rt count = %d, want 3, order=%v", rtCount, order)
	}
}

func TestContinueDiscardsPendingStopSignals(t *testing.T) {
	r := newTestRuntime(t, 1)
	r.Send(1, SIGSTOP)
	// 停止期间排队 TSTP/TTIN/TTOU（均被捕获，保持待决直到继续判定）。
	for _, sig := range []Signal{SIGTSTP, SIGTTIN, SIGTTOU} {
		r.SetAction(1, sig, SigAction{Action: ActCatch, Name: "s",
			Handler: func(Signal, int) {}})
	}
	// 这些停止信号在 stopped 状态下保持待决（除 KILL/CONT 不交付）。
	if err := r.Send(1, SIGTSTP); err != nil {
		t.Fatal(err)
	}
	if err := r.Send(1, SIGTTIN); err != nil {
		t.Fatal(err)
	}
	r.Send(1, SIGCONT)
	// POSIX：SIGCONT 继续时丢弃待决的作业控制停止信号。
	std, _, _ := r.Pending(1)
	if std.Has(SIGTSTP) || std.Has(SIGTTIN) || std.Has(SIGTTOU) {
		t.Fatalf("stop signals not discarded: %v", std.Signals())
	}
	if st, _ := r.State(1); st != "running" {
		t.Fatalf("state = %s", st)
	}
}

func TestGroupDelivery(t *testing.T) {
	r := NewRuntime()
	for _, pid := range []int{10, 11, 12} {
		if err := r.NewProcess(pid, 100); err != nil {
			t.Fatal(err)
		}
	}
	var hits int32
	for _, pid := range []int{10, 11, 12} {
		r.SetAction(pid, SIGUSR1, SigAction{Action: ActCatch, Name: "h",
			Handler: func(Signal, int) { atomic.AddInt32(&hits, 1) }})
	}
	if err := r.SendGroup(100, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&hits) != 3 {
		t.Fatalf("group hits = %d, want 3", hits)
	}

	// 全组停止。
	if err := r.SendGroup(100, SIGTSTP); err != nil {
		t.Fatal(err)
	}
	for _, pid := range []int{10, 11, 12} {
		if st, _ := r.State(pid); st != "stopped" {
			t.Fatalf("pid %d state = %s", pid, st)
		}
	}
	// 停止期间组投递捕获信号全部保持待决。
	var hits2 int32
	for _, pid := range []int{10, 11, 12} {
		r.SetAction(pid, SIGUSR2, SigAction{Action: ActCatch, Name: "h2",
			Handler: func(Signal, int) { atomic.AddInt32(&hits2, 1) }})
	}
	if err := r.SendGroup(100, SIGUSR2); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&hits2) != 0 {
		t.Fatalf("delivered to stopped group: %d", hits2)
	}
	if err := r.SendGroup(100, SIGCONT); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&hits2) != 3 {
		t.Fatalf("hits after cont = %d, want 3", hits2)
	}

	if err := r.SendGroup(999, SIGUSR1); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("err = %v, want ErrNoProcess", err)
	}
}

func TestGroupQueueFullIsolated(t *testing.T) {
	r := NewRuntime()
	r.NewProcess(1, 1)
	r.NewProcess(2, 1)
	r.SetMask(1, NewSigSet(SIGRTMIN))
	r.SetMask(2, NewSigSet(SIGRTMIN))
	for i := 0; i < rtQueueLimit; i++ {
		r.Send(1, SIGRTMIN)
	}
	err := r.SendGroup(1, SIGRTMIN)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("group err = %v, want ErrQueueFull", err)
	}
	_, rt, _ := r.Pending(2)
	if len(rt) != 1 {
		t.Fatalf("pid2 pending = %d, want 1", len(rt))
	}
}
