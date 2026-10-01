package signalruntime

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestHandlerNonReentrantAndMask(t *testing.T) {
	r := newTestRuntime(t, 1)
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	var conc int32
	var termDelivered int32

	release := make(chan struct{})

	// 处理器屏蔽字含 SIGTERM；同信号 SIGUSR1 在处理器执行期间自动屏蔽。
	r.SetAction(1, SIGUSR1, SigAction{
		Action: ActCatch,
		Name:   "h1",
		Mask:   NewSigSet(SIGTERM),
		Handler: func(Signal, int) {
			if !atomic.CompareAndSwapInt32(&conc, 0, 1) {
				t.Errorf("SIGUSR1 handler reentered concurrently")
			}
			// 第一次进入时阻塞，观察期间到达的信号。
			select {
			case <-firstEntered:
				// 第二次投递触发的处理器进入。
				<-release
			default:
				close(firstEntered)
				<-release
			}
			atomic.StoreInt32(&conc, 0)
		},
	})
	r.SetAction(1, SIGTERM, SigAction{Action: ActCatch, Name: "term",
		Handler: func(Signal, int) { atomic.AddInt32(&termDelivered, 1) }})

	go func() { _ = r.Send(1, SIGUSR1) }()
	<-firstEntered

	// 第一次处理器仍在执行：同信号再来一次必须等待，SIGTERM 被处理器屏蔽字挡住。
	go func() { _ = r.Send(1, SIGUSR1); close(secondEntered) }()
	_ = r.Send(1, SIGTERM)

	std, rt, _ := r.Pending(1)
	// 第二个 SIGUSR1 已出队但在第一处理器之后排队（不可重入）；
	// SIGTERM 被处理器屏蔽字阻塞而保持待决。
	if !std.Has(SIGTERM) {
		t.Fatalf("SIGTERM should be pending under handler mask, std=%v", std)
	}
	_ = rt

	// 等待 50ms 确认没有重入、SIGTERM 也未提前交付。
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&termDelivered) != 0 {
		t.Fatalf("SIGTERM delivered under handler mask")
	}

	close(release)
	select {
	case <-secondEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("second SIGUSR1 delivery never completed")
	}

	// 第二次 SIGUSR1 处理器返回后，SIGTERM 不再被屏蔽，应已交付。
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&termDelivered) == 0 {
		select {
		case <-deadline:
			t.Fatal("SIGTERM not delivered after handlers finished")
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
}

func TestHandlerRunsWhileProcessMasked(t *testing.T) {
	r := newTestRuntime(t, 1)
	done := make(chan struct{})
	r.SetMask(1, NewSigSet(SIGUSR1))
	r.SetAction(1, SIGUSR1, SigAction{Action: ActCatch, Name: "h",
		Handler: func(Signal, int) { close(done) }})
	r.Send(1, SIGUSR1) // 被进程屏蔽，保持待决
	select {
	case <-done:
		t.Fatal("handler ran while blocked")
	default:
	}
	r.SetMask(1, SigSet{})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not run after unblock")
	}
}
