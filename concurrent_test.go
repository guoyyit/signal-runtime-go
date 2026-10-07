package signalrt

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pidRecorder counts deliveries per (pid, signal).
type pidRecorder struct {
	mu     sync.Mutex
	counts map[int]map[Signal]int
}

func (pr *pidRecorder) handler() HandlerFunc {
	return func(pid int, sig Signal) {
		pr.mu.Lock()
		if pr.counts == nil {
			pr.counts = make(map[int]map[Signal]int)
		}
		if pr.counts[pid] == nil {
			pr.counts[pid] = make(map[Signal]int)
		}
		pr.counts[pid][sig]++
		pr.mu.Unlock()
	}
}

func (pr *pidRecorder) count(pid int, sig Signal) int {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	return pr.counts[pid][sig]
}

// TestConcurrentDirectAndGroupSendsNoLossNoDup hammers one process with
// direct sends while other goroutines send to its whole process group.
// Every accepted send must be delivered exactly once; overflows must be
// accounted by the drop counter; a process outside the group must see
// nothing.
func TestConcurrentDirectAndGroupSendsNoLossNoDup(t *testing.T) {
	var pr pidRecorder
	reg := NewHandlerRegistry()
	reg.Register("rec", pr.handler())
	rt := New(reg)

	const group = 5
	var members []int
	for i := 0; i < 3; i++ {
		pid := rt.Spawn(group)
		for _, sig := range []Signal{SIGUSR1, MinRT, MinRT + 1} {
			if err := rt.SetAction(pid, sig, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
				t.Fatal(err)
			}
		}
		members = append(members, pid)
	}
	control := rt.Spawn(6)
	if err := rt.SetAction(control, MinRT+1, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
		t.Fatal(err)
	}

	const (
		directSenders = 4
		directEach    = 50
		groupSenders  = 3
		groupEach     = 40
		stdSenders    = 2
		stdEach       = 30
	)
	target := members[0]
	var wg sync.WaitGroup
	// Direct realtime sends to one process.
	for s := 0; s < directSenders; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < directEach; i++ {
				if err := rt.Send(target, MinRT); err != nil && !errors.Is(err, ErrQueueOverflow) {
					t.Error(err)
				}
			}
		}()
	}
	// Group realtime sends reaching every member.
	for s := 0; s < groupSenders; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < groupEach; i++ {
				if err := rt.SendGroup(group, MinRT+1); err != nil && !errors.Is(err, ErrQueueOverflow) {
					t.Error(err)
				}
			}
		}()
	}
	// Concurrent standard sends (coalesced; counted separately).
	for s := 0; s < stdSenders; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < stdEach; i++ {
				if err := rt.Send(members[1], SIGUSR1); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()

	for i, pid := range members {
		sent := groupSenders * groupEach
		if i == 0 {
			sent += directSenders * directEach
		}
		delivered := pr.count(pid, MinRT) + pr.count(pid, MinRT+1)
		overflow, err := rt.Overflow(pid)
		if err != nil {
			t.Fatal(err)
		}
		if delivered+int(overflow) != sent {
			t.Fatalf("pid %d: delivered(%d) + dropped(%d) != sent(%d): loss or duplication",
				pid, delivered, overflow, sent)
		}
	}
	// The coalesced standard signal was delivered at least once.
	if got := pr.count(members[1], SIGUSR1); got < 1 {
		t.Fatal("standard signal lost under concurrent load")
	}
	// The process outside the group received nothing.
	if got := pr.count(control, MinRT+1); got != 0 {
		t.Fatalf("control process got %d deliveries, want 0", got)
	}
	if n, _ := rt.Overflow(control); n != 0 {
		t.Fatalf("control process overflow = %d, want 0", n)
	}
}

// TestConcurrentSnapshotRestoreConsistency takes a snapshot right after a
// concurrent send storm and checks that across the old and restored
// runtimes every accepted send is delivered exactly once.
func TestConcurrentSnapshotRestoreConsistency(t *testing.T) {
	var pr1 pidRecorder
	reg1 := NewHandlerRegistry()
	reg1.Register("rec", pr1.handler())
	rt1 := New(reg1)
	pid := rt1.Spawn(1)
	if err := rt1.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
		t.Fatal(err)
	}

	const senders = 6
	const perSender = 40
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perSender; i++ {
				if err := rt1.Send(pid, MinRT); err != nil && !errors.Is(err, ErrQueueOverflow) {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()

	snapshot, err := rt1.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var pr2 pidRecorder
	reg2 := NewHandlerRegistry()
	reg2.Register("rec", pr2.handler())
	rt2, err := Restore(snapshot, reg2)
	if err != nil {
		t.Fatal(err)
	}

	sent := senders * perSender
	delivered := pr1.count(pid, MinRT) + pr2.count(pid, MinRT)
	overflow1, _ := rt1.Overflow(pid)
	overflow2, _ := rt2.Overflow(pid)
	if overflow1 != overflow2 {
		t.Fatalf("overflow counter not preserved: %d vs %d", overflow1, overflow2)
	}
	if delivered+int(overflow2) != sent {
		t.Fatalf("across crash boundary: delivered(%d) + dropped(%d) != sent(%d)",
			delivered, overflow2, sent)
	}
	if pending, rtq, _ := rt2.Pending(pid); pending != EmptySet || len(rtq) != 0 {
		t.Fatalf("restored runtime still has pending signals: %v %v", pending, rtq)
	}
}

// TestConcurrentFloodThenQuietDrainsEverything checks that after a
// concurrent flood stops, every queued signal is eventually delivered
// without any further explicit trigger beyond the sends themselves.
func TestConcurrentFloodThenQuietDrainsEverything(t *testing.T) {
	var pr pidRecorder
	reg := NewHandlerRegistry()
	reg.Register("rec", pr.handler())
	rt := New(reg)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: "rec"}); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for s := 0; s < 4; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				err := rt.Send(pid, MinRT)
				if err == nil {
					accepted.Add(1)
				} else if !errors.Is(err, ErrQueueOverflow) {
					t.Error(err)
					return
				}
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	// After the senders go quiet, one more delivery trigger (a no-op
	// mask update) must drain anything a budget-bounded loop left over:
	// nothing may remain pending and nothing may be lost.
	if err := rt.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	_, rtq, err := rt.Pending(pid)
	if err != nil {
		t.Fatal(err)
	}
	if len(rtq) != 0 {
		t.Fatalf("%d realtime signals stranded after senders went quiet", len(rtq))
	}
	if got := pr.count(pid, MinRT); int64(got) != accepted.Load() {
		t.Fatalf("delivered %d, want exactly the %d accepted sends", got, accepted.Load())
	}
}
