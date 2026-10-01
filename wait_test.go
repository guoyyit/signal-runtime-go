package signalruntime

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitInterruptNoRestart(t *testing.T) {
	r := newTestRuntime(t, 1)
	var calls int32
	r.SetAction(1, SIGUSR1, SigAction{Action: ActCatch, Name: "h",
		Handler: func(Signal, int) { atomic.AddInt32(&calls, 1) }})

	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() { errCh <- r.Wait(1, done, false) }()

	time.Sleep(30 * time.Millisecond)
	if err := r.Send(1, SIGUSR1); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrInterrupted) {
			t.Fatalf("err = %v, want ErrInterrupted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait was not interrupted by caught signal")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
}

func TestWaitAutoRestart(t *testing.T) {
	r := newTestRuntime(t, 1)
	var calls int32
	r.SetAction(1, SIGUSR1, SigAction{Action: ActCatch, Name: "h",
		Handler: func(Signal, int) { atomic.AddInt32(&calls, 1) }})

	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() { errCh <- r.Wait(1, done, true) }()

	time.Sleep(30 * time.Millisecond)
	r.Send(1, SIGUSR1)
	time.Sleep(30 * time.Millisecond)
	r.Send(1, SIGUSR1)
	time.Sleep(30 * time.Millisecond)

	select {
	case err := <-errCh:
		t.Fatalf("Wait returned early: %v", err)
	default:
	}

	close(done)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Wait err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after done")
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}

func TestWaitDoneWinsRace(t *testing.T) {
	// 确定性规则：完成条件与捕获信号在同一时刻到达，完成优先。
	// 用通道关闭表达完成，保证不可逆与可原子判定。
	for i := 0; i < 200; i++ {
		r := newTestRuntime(t, 1)
		r.SetAction(1, SIGUSR1, SigAction{Action: ActCatch, Name: "h",
			Handler: func(Signal, int) {}})
		done := make(chan struct{})
		errCh := make(chan error, 1)
		go func() { errCh <- r.Wait(1, done, false) }()
		runtimeYield()
		close(done)
		_ = r.Send(1, SIGUSR1)
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("iteration %d: done must win, got %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: Wait did not return", i)
		}
	}
}

func TestWaitInterruptedByKill(t *testing.T) {
	r := newTestRuntime(t, 1)
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() { errCh <- r.Wait(1, done, true) }()
	time.Sleep(30 * time.Millisecond)
	if err := r.Send(1, SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrTerminated) {
			t.Fatalf("err = %v, want ErrTerminated", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait was not woken by termination")
	}
}

func TestWaitIgnoredSignalDoesNotInterrupt(t *testing.T) {
	r := newTestRuntime(t, 1)
	r.SetAction(1, SIGUSR1, SigAction{Action: ActIgnore})
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() { errCh <- r.Wait(1, done, false) }()
	time.Sleep(30 * time.Millisecond)
	r.Send(1, SIGUSR1)
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("ignored signal interrupted wait: %v", err)
	default:
	}
	close(done)
	<-errCh
}

func TestWaitBlockedSignalDoesNotInterrupt(t *testing.T) {
	r := newTestRuntime(t, 1)
	r.SetAction(1, SIGUSR1, SigAction{Action: ActCatch, Name: "h",
		Handler: func(Signal, int) {}})
	r.SetMask(1, NewSigSet(SIGUSR1))
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() { errCh <- r.Wait(1, done, false) }()
	time.Sleep(30 * time.Millisecond)
	r.Send(1, SIGUSR1) // 被屏蔽，不交付，因此不应打断等待
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("blocked signal interrupted wait: %v", err)
	default:
	}
	close(done)
	<-errCh
}

func TestWaitSurvivesStopContinue(t *testing.T) {
	r := newTestRuntime(t, 1)
	r.SetAction(1, SIGUSR1, SigAction{Action: ActCatch, Name: "h",
		Handler: func(Signal, int) {}})
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() { errCh <- r.Wait(1, done, false) }()
	time.Sleep(30 * time.Millisecond)

	// 停止：等待保持挂起；停止期间到达的捕获信号也不打断。
	if err := r.Send(1, SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if err := r.Send(1, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("wait woke during stop: %v", err)
	default:
	}

	// SIGCONT 恢复运行，停止期间排队的 USR1 随之交付并打断等待。
	if err := r.Send(1, SIGCONT); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrInterrupted) {
			t.Fatalf("err = %v, want ErrInterrupted after queued signal", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait not interrupted by signal delivered after continue")
	}
}

func TestWaitFnInterrupt(t *testing.T) {
	r := newTestRuntime(t, 1)
	r.SetAction(1, SIGUSR1, SigAction{Action: ActCatch, Name: "h",
		Handler: func(Signal, int) {}})
	var ready int32
	errCh := make(chan error, 1)
	go func() {
		errCh <- r.WaitFn(1, func() bool { return atomic.LoadInt32(&ready) == 1 }, false)
	}()
	time.Sleep(30 * time.Millisecond)
	r.Send(1, SIGUSR1)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrInterrupted) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitFn not interrupted")
	}
}
