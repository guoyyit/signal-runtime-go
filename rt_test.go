package signalruntime

import (
	"errors"
	"sync"
	"testing"
)

func TestRealtimeFIFO(t *testing.T) {
	r := newTestRuntime(t, 1)
	var mu sync.Mutex
	var order []int
	mk := func() HandlerFunc {
		return func(s Signal, _ int) {
			mu.Lock()
			order = append(order, int(s))
			mu.Unlock()
		}
	}
	if _, err := r.SetAction(1, SIGRTMIN, SigAction{Action: ActCatch, Name: "lo", Handler: mk()}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.SetAction(1, SIGRTMIN+1, SigAction{Action: ActCatch, Name: "hi", Handler: mk()}); err != nil {
		t.Fatal(err)
	}
	r.SetMask(1, NewSigSet(SIGRTMIN, SIGRTMIN+1))

	for i := 0; i < 3; i++ {
		if err := r.SendValue(1, SIGRTMIN, i+1); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := r.SendValue(1, SIGRTMIN+1, i+1); err != nil {
			t.Fatal(err)
		}
	}
	_, rt, _ := r.Pending(1)
	if len(rt) != 5 {
		t.Fatalf("rt pending = %d, want 5", len(rt))
	}

	r.SetMask(1, SigSet{})
	// 公开的交付顺序：跨信号轮转（每轮每个信号至多一个实例，
	// 高编号不被低编号饿死）；同一信号内严格按到达顺序 FIFO。
	want := []int{32, 33, 32, 33, 32}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestRealtimeValuePreserved(t *testing.T) {
	r := newTestRuntime(t, 1)
	var mu sync.Mutex
	var values []int
	r.SetMask(1, NewSigSet(SIGRTMIN+5))
	r.SetAction(1, SIGRTMIN+5, SigAction{Action: ActCatch, Name: "h",
		Handler: func(_ Signal, v int) {
			mu.Lock()
			values = append(values, v)
			mu.Unlock()
		}})
	for i := 0; i < 4; i++ {
		if err := r.SendValue(1, SIGRTMIN+5, 100+i); err != nil {
			t.Fatal(err)
		}
	}
	_, rt, _ := r.Pending(1)
	if len(rt) != 4 {
		t.Fatalf("pending = %d, want 4", len(rt))
	}
	r.SetMask(1, SigSet{})
	want := []int{100, 101, 102, 103}
	if len(values) != len(want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("values = %v, want %v", values, want)
		}
	}
}

func TestRealtimeQueueLimit(t *testing.T) {
	r := newTestRuntime(t, 1)
	r.SetAction(1, SIGRTMIN, SigAction{Action: ActCatch, Name: "h",
		Handler: func(Signal, int) {}})
	r.SetMask(1, NewSigSet(SIGRTMIN))
	for i := 0; i < rtQueueLimit; i++ {
		if err := r.Send(1, SIGRTMIN); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if err := r.Send(1, SIGRTMIN); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}
	if err := r.Send(1, SIGRTMIN+1); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}
	// 标准信号不受实时队列上限影响。
	r.SetAction(1, SIGUSR1, SigAction{Action: ActIgnore})
	if err := r.Send(1, SIGUSR1); err != nil {
		t.Fatalf("standard send: %v", err)
	}
	// 消费一个后可以再入队一个。
	r.SetMask(1, SigSet{})
	r.SetMask(1, NewSigSet(SIGRTMIN))
	if err := r.Send(1, SIGRTMIN); err != nil {
		t.Fatalf("send after drain: %v", err)
	}
}
