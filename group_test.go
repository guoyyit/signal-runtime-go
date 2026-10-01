package signalrt

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

func TestStopKeepsRealtimePendingAndContinueDelivers(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	// Stop the process, then queue realtime signals while stopped.
	if err := rt.Send(pid, SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if stopped, _ := rt.Stopped(pid); !stopped {
		t.Fatal("process should be stopped after SIGSTOP")
	}
	for i := 0; i < 3; i++ {
		if err := rt.Send(pid, MinRT); err != nil {
			t.Fatal(err)
		}
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("no delivery allowed while stopped, got %v", got)
	}
	_, rtPending, _ := rt.Pending(pid)
	if len(rtPending) != 3 {
		t.Fatalf("realtime queue must survive stop, pending = %v", rtPending)
	}
	// Continue: queued signals are delivered in arrival order.
	if err := rt.Send(pid, SIGCONT); err != nil {
		t.Fatal(err)
	}
	if stopped, _ := rt.Stopped(pid); stopped {
		t.Fatal("process should be running after SIGCONT")
	}
	want := []Signal{MinRT, MinRT, MinRT}
	if got := rec.got(); !reflect.DeepEqual(got, want) {
		t.Fatalf("deliveries after continue = %v, want %v", got, want)
	}
}

func TestStoppedProcessOnlyTakesKillAndCont(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("SIGUSR1 must stay pending while stopped, got %v", got)
	}
	if err := rt.Send(pid, SIGCONT); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); !reflect.DeepEqual(got, []Signal{SIGUSR1}) {
		t.Fatalf("pending SIGUSR1 must be delivered after continue, got %v", got)
	}
	// Stop again, then SIGKILL terminates even from the stopped state.
	if err := rt.Send(pid, SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGKILL); err != nil {
		t.Fatal(err)
	}
	dead, sig, _ := rt.Exited(pid)
	if !dead || sig != SIGKILL {
		t.Fatalf("SIGKILL on stopped process: Exited = %v, %v", dead, sig)
	}
}

func TestCaughtSIGCONTAlsoResumes(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, SIGCONT, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if err := rt.Send(pid, SIGCONT); err != nil {
		t.Fatal(err)
	}
	if stopped, _ := rt.Stopped(pid); stopped {
		t.Fatal("caught SIGCONT must still resume the process")
	}
	if got := rec.got(); !reflect.DeepEqual(got, []Signal{SIGCONT}) {
		t.Fatalf("SIGCONT handler should have run, got %v", got)
	}
}

func TestGroupSendReachesEveryMember(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	const members = 3
	pids := make([]int, 0, members)
	for i := 0; i < members; i++ {
		pid := rt.Spawn(100)
		if err := rt.SetAction(pid, SIGUSR1, Action{Kind: ActionHandler, Handler: name}); err != nil {
			t.Fatal(err)
		}
		pids = append(pids, pid)
	}
	other := rt.Spawn(200) // different group, must be unaffected
	if err := rt.SendGroup(100, SIGUSR1); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); len(got) != members {
		t.Fatalf("group delivery = %v, want %d handler calls", got, members)
	}
	// Group terminate kills every member and only them.
	if err := rt.SendGroup(100, SIGTERM); err != nil {
		t.Fatal(err)
	}
	for _, pid := range pids {
		if dead, _, _ := rt.Exited(pid); !dead {
			t.Fatalf("pid %d should be dead after group SIGTERM", pid)
		}
	}
	if dead, _, _ := rt.Exited(other); dead {
		t.Fatal("process in another group must survive")
	}
}

func TestGroupStopContinueLosesNoSignals(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	var pids []int
	for i := 0; i < 3; i++ {
		pid := rt.Spawn(7)
		if err := rt.SetAction(pid, MinRT+2, Action{Kind: ActionHandler, Handler: name}); err != nil {
			t.Fatal(err)
		}
		pids = append(pids, pid)
	}
	if err := rt.SendGroup(7, SIGSTOP); err != nil {
		t.Fatal(err)
	}
	for _, pid := range pids {
		if err := rt.Send(pid, MinRT+2); err != nil {
			t.Fatal(err)
		}
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("delivery while group stopped: %v", got)
	}
	if err := rt.SendGroup(7, SIGCONT); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); len(got) != len(pids) {
		t.Fatalf("each member must get its queued signal, got %v", got)
	}
	for _, pid := range pids {
		if stopped, _ := rt.Stopped(pid); stopped {
			t.Fatalf("pid %d still stopped after group SIGCONT", pid)
		}
	}
}

func TestConcurrentSendersLoseNothing(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	if err := rt.SetAction(pid, MinRT, Action{Kind: ActionHandler, Handler: name}); err != nil {
		t.Fatal(err)
	}
	const senders = 8
	const perSender = 25
	var overflowCount atomic.Int64
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perSender; i++ {
				err := rt.Send(pid, MinRT)
				if errors.Is(err, ErrQueueOverflow) {
					// Documented drop-newest policy under overflow:
					// the send fails loudly, the signal is not lost silently.
					overflowCount.Add(1)
				} else if err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	sent := int64(senders * perSender)
	dropped := overflowCount.Load()
	delivered := int64(len(rec.got()))
	if delivered+dropped != sent {
		t.Fatalf("delivered(%d) + dropped(%d) != sent(%d): silent loss or duplication",
			delivered, dropped, sent)
	}
	if n, _ := rt.Overflow(pid); n != uint64(dropped) {
		t.Fatalf("overflow counter = %d, want %d", n, dropped)
	}
}

func TestConcurrentMixedSignalsNoStarvation(t *testing.T) {
	var rec recorder
	rt, name := newRuntimeWithHandler(t, &rec)
	pid := rt.Spawn(1)
	act := Action{Kind: ActionHandler, Handler: name}
	sigs := []Signal{SIGUSR1, SIGUSR2, MinRT, MinRT + 1}
	for _, sig := range sigs {
		if err := rt.SetAction(pid, sig, act); err != nil {
			t.Fatal(err)
		}
	}
	if err := rt.SetMask(pid, Of(sigs...)); err != nil {
		t.Fatal(err)
	}
	// rounds*2 realtime signals must fit the queue capacity of 32.
	const rounds = 10
	var wg sync.WaitGroup
	for _, sig := range sigs {
		sig := sig
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				if err := rt.Send(pid, sig); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if err := rt.SetMask(pid, EmptySet); err != nil {
		t.Fatal(err)
	}
	// Standard signals coalesce (1 each); realtime signals queue (rounds each).
	counts := map[Signal]int{}
	for _, sig := range rec.got() {
		counts[sig]++
	}
	if counts[SIGUSR1] != 1 || counts[SIGUSR2] != 1 {
		t.Fatalf("standard signals must coalesce to one each, got %v", counts)
	}
	if counts[MinRT] != rounds || counts[MinRT+1] != rounds {
		t.Fatalf("every realtime signal must be delivered (no starvation), got %v", counts)
	}
	// The recorded order must be non-decreasing across signal numbers;
	// equal realtime signals keep FIFO arrival order.
	got := rec.got()
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("delivery order violated at %d: %v", i, got)
		}
	}
}
