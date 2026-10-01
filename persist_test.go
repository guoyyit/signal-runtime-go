package signalruntime

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestSnapshotRestoreMaskAndPending(t *testing.T) {
	r := newTestRuntime(t, 1, 2)
	r.NewProcess(3, 10)

	var usr1 int32
	r.SetAction(1, SIGUSR1, SigAction{Action: ActCatch, Name: "usr1",
		Mask:    NewSigSet(SIGUSR2),
		Handler: func(Signal, int) { atomic.AddInt32(&usr1, 1) }})
	r.SetAction(1, SIGUSR2, SigAction{Action: ActIgnore})
	r.SetAction(1, SIGRTMIN, SigAction{Action: ActCatch, Name: "rtlo",
		Handler: func(Signal, int) {}})
	r.SetAction(1, SIGRTMIN+1, SigAction{Action: ActCatch, Name: "rthi",
		Handler: func(Signal, int) {}})
	r.SetMask(1, NewSigSet(SIGUSR1))

	// 进程 1：一个待决标准信号 + 两个排队实时信号。
	r.Send(1, SIGUSR1)
	r.SetMask(1, NewSigSet(SIGUSR1, SIGRTMIN, SIGRTMIN+1))
	r.SendValue(1, SIGRTMIN, 7)
	r.SendValue(1, SIGRTMIN+1, 8)

	// 进程 2：被停止，且有待决实时信号。
	r.SetAction(2, SIGRTMIN, SigAction{Action: ActCatch, Name: "rt2",
		Handler: func(Signal, int) {}})
	r.Send(2, SIGSTOP)
	r.Send(2, SIGRTMIN)

	st := r.Snapshot()

	// 全新运行时，按名称重新绑定处理器。
	r2 := NewRuntime()
	resolver := func(name string) HandlerFunc {
		switch name {
		case "usr1":
			return func(Signal, int) { atomic.AddInt32(&usr1, 1) }
		case "rtlo", "rthi", "rt2":
			return func(Signal, int) {}
		}
		return nil
	}
	if err := r2.Restore(st, resolver); err != nil {
		t.Fatal(err)
	}

	// 进程 2 恢复后仍停止，实时信号保持待决。
	if s, _ := r2.State(2); s != "stopped" {
		t.Fatalf("pid2 state = %s, want stopped", s)
	}
	_, rt2, _ := r2.Pending(2)
	if len(rt2) != 1 {
		t.Fatalf("pid2 rt pending = %d, want 1", len(rt2))
	}

	// 进程 1 的处置与屏蔽字恢复：SIGUSR1 仍被屏蔽，未提前交付。
	before := atomic.LoadInt32(&usr1)
	if before != 0 {
		t.Fatalf("blocked signal delivered during restore: %d", before)
	}
	act, err := r2.GetAction(1, SIGUSR2)
	if err != nil || act.Action != ActIgnore {
		t.Fatalf("action restore = %+v, %v", act, err)
	}

	// 解除屏蔽后，标准信号交付一次，实时信号按 FIFO 交付且随队值保留。
	var vals []int
	var mu = newTestMu()
	r2.SetAction(1, SIGRTMIN, SigAction{Action: ActCatch, Name: "rtlo",
		Handler: func(_ Signal, v int) { mu.Lock(); vals = append(vals, v); mu.Unlock() }})
	r2.SetAction(1, SIGRTMIN+1, SigAction{Action: ActCatch, Name: "rthi",
		Handler: func(_ Signal, v int) { mu.Lock(); vals = append(vals, v); mu.Unlock() }})
	r2.SetMask(1, SigSet{})

	if atomic.LoadInt32(&usr1) != 1 {
		t.Fatalf("usr1 deliveries = %d, want exactly 1 (no dup, no loss)", usr1)
	}
	if len(vals) != 2 || vals[0] != 7 || vals[1] != 8 {
		t.Fatalf("rt values = %v, want [7 8]", vals)
	}

	// 继续进程 2，待决实时信号随后交付。
	var rt2Delivered int32
	r2.SetAction(2, SIGRTMIN, SigAction{Action: ActCatch, Name: "rt2",
		Handler: func(Signal, int) { atomic.AddInt32(&rt2Delivered, 1) }})
	if err := r2.Send(2, SIGCONT); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&rt2Delivered) != 1 {
		t.Fatalf("pid2 rt after cont = %d, want 1", rt2Delivered)
	}

	// 进程 3 属于组 10，恢复后仍存在。
	if s, err := r2.State(3); err != nil || s != "running" {
		t.Fatalf("pid3 = %s %v", s, err)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	r := newTestRuntime(t, 5)
	r.SetAction(5, SIGRTMIN, SigAction{Action: ActCatch, Name: "rt",
		Handler: func(Signal, int) {}})
	r.SetMask(5, NewSigSet(SIGRTMIN))
	for i := 0; i < 5; i++ {
		if err := r.SendValue(5, SIGRTMIN, i); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Save(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file missing: %v", err)
	}

	var got []int
	var mu = newTestMu()
	r2 := NewRuntime()
	if err := r2.Load(path, func(name string) HandlerFunc {
		if name == "rt" {
			return func(_ Signal, v int) { mu.Lock(); got = append(got, v); mu.Unlock() }
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r2.SetMask(5, SigSet{})
	if len(got) != 5 {
		t.Fatalf("delivered after load = %v, want 5", got)
	}
	for i := range got {
		if got[i] != i {
			t.Fatalf("order/value = %v, want 0..4", got)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	r := NewRuntime()
	if err := r.Load(filepath.Join(t.TempDir(), "nope.json"), nil); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v", err)
	}
}

func TestInflightRedeliveryAfterCrash(t *testing.T) {
	// 模拟“处理器执行中崩溃”：人为构造一个含 inflight 实例的快照，
	// 恢复后该实例必须被重新交付（至少一次语义），且不与队列实例重复。
	r := NewRuntime()
	st := &State{
		Processes: []processState{{
			PID:   1,
			PGID:  1,
			State: int(procRunning),
			Actions: map[int]sigActionState{
				int(SIGRTMIN): {Action: int(ActCatch), Name: "rt"},
			},
			RTQueue:  []queuedSignalState{{Sig: int(SIGRTMIN), Value: 2, Seq: 20}},
			Inflight: []queuedSignalState{{Sig: int(SIGRTMIN), Value: 1, Seq: 10}},
		}},
		Seq: 20,
	}
	var vals []int
	var mu = newTestMu()
	if err := r.Restore(st, func(string) HandlerFunc {
		return func(_ Signal, v int) { mu.Lock(); vals = append(vals, v); mu.Unlock() }
	}); err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 || vals[0] != 1 || vals[1] != 2 {
		t.Fatalf("inflight/queue redelivery = %v, want [1 2]", vals)
	}
}
