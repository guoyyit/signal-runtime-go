package signalruntime

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentStandardNoLossForDistinctSignals(t *testing.T) {
	r := newTestRuntime(t, 1)
	// 使用多个不同的标准捕获信号并全部屏蔽，并发投递，解除阻塞后逐个都应交付。
	sigs := []Signal{SIGHUP, SIGINT, SIGQUIT, SIGUSR1, SIGUSR2,
		SIGPIPE, SIGALRM, SIGTERM, SIGCHLD, SIGTSTP}
	var mu sync.Mutex
	delivered := make(map[Signal]int)
	for _, sig := range sigs {
		s := sig
		r.SetAction(1, s, SigAction{Action: ActCatch, Name: "h",
			Handler: func(Signal, int) {
				mu.Lock()
				delivered[s]++
				mu.Unlock()
			}})
	}

	var set SigSet
	for _, sig := range sigs {
		set = set.Add(sig)
	}
	r.SetMask(1, set)

	var wg sync.WaitGroup
	for _, sig := range sigs {
		wg.Add(1)
		go func(s Signal) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = r.Send(1, s)
			}
		}(sig)
	}
	wg.Wait()

	std, _, _ := r.Pending(1)
	for _, sig := range sigs {
		if !std.Has(sig) {
			t.Fatalf("standard signal %d missing from pending", sig)
		}
	}

	r.SetMask(1, SigSet{})
	for _, sig := range sigs {
		if delivered[sig] != 1 {
			t.Fatalf("signal %d delivered %d times, want exactly 1", sig, delivered[sig])
		}
	}
}

func TestConcurrentRealtimeNoLossNoDup(t *testing.T) {
	r := newTestRuntime(t, 1)
	const senders = 10
	const per = 10
	var total int32
	var accepted int32
	r.SetAction(1, SIGRTMIN+3, SigAction{Action: ActCatch, Name: "rt",
		Handler: func(Signal, int) { atomic.AddInt32(&total, 1) }})
	r.SetMask(1, NewSigSet(SIGRTMIN+3))

	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				if err := r.Send(1, SIGRTMIN+3); err == nil {
					atomic.AddInt32(&accepted, 1)
				}
			}
		}()
	}
	wg.Wait()

	_, rt, _ := r.Pending(1)
	want := atomic.LoadInt32(&accepted)
	if int32(len(rt)) != want {
		t.Fatalf("queued = %d, want %d (lost or duplicated)", len(rt), want)
	}
	r.SetMask(1, SigSet{})
	if atomic.LoadInt32(&total) != want {
		t.Fatalf("delivered = %d, want %d", total, want)
	}
	if want != senders*per {
		t.Fatalf("expected all sends accepted within limit, got %d", want)
	}
}

func TestHighSignalNotStarved(t *testing.T) {
	// 低编号信号持续大量到达时，高编号信号仍必须被交付（轮转公平）。
	r := newTestRuntime(t, 1)
	var low, high int32
	releaseLow := make(chan struct{})
	lowStarted := make(chan struct{})

	r.SetAction(1, SIGRTMIN, SigAction{Action: ActCatch, Name: "low",
		Handler: func(Signal, int) {
			if atomic.AddInt32(&low, 1) == 1 {
				close(lowStarted)
				<-releaseLow
			}
		}})
	r.SetAction(1, SIGRTMAX, SigAction{Action: ActCatch, Name: "high",
		Handler: func(Signal, int) { atomic.AddInt32(&high, 1) }})

	// 第一个低编号处理器进入并阻塞。
	go r.Send(1, SIGRTMIN)
	<-lowStarted

	// 此时再投递大量低编号实例与一个高编号实例。
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = r.Send(1, SIGRTMIN) }()
	}
	wg.Wait()
	if err := r.Send(1, SIGRTMAX); err != nil {
		t.Fatal(err)
	}

	// 释放第一个处理器；高编号信号必须在有限时间内交付，而非被低编号无限压制。
	close(releaseLow)
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&high) == 0 {
		select {
		case <-deadline:
			t.Fatal("high-numbered signal starved by low-numbered traffic")
		default:
			runtimeYield()
		}
	}
	if atomic.LoadInt32(&high) != 1 {
		t.Fatalf("high deliveries = %d, want 1", high)
	}
}
