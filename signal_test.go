package signalrt

import "testing"

func TestSignalValidity(t *testing.T) {
	for sig := MinStandard; sig <= MaxRT; sig++ {
		if !sig.Valid() {
			t.Fatalf("signal %d should be valid", sig)
		}
	}
	for _, sig := range []Signal{0, -1, 65, 100} {
		if sig.Valid() {
			t.Fatalf("signal %d should be invalid", sig)
		}
	}
	if SIGUSR1.Realtime() {
		t.Fatal("SIGUSR1 must not be realtime")
	}
	if !MinRT.Realtime() || !MaxRT.Realtime() {
		t.Fatal("32..64 must be realtime")
	}
}

func TestSetOperations(t *testing.T) {
	set := Of(SIGUSR1, SIGTERM, MinRT)
	for _, sig := range []Signal{SIGUSR1, SIGTERM, MinRT} {
		if !set.Has(sig) {
			t.Fatalf("set should contain %d", sig)
		}
	}
	if set.Has(SIGUSR2) {
		t.Fatal("set should not contain SIGUSR2")
	}
	set = set.Del(SIGTERM)
	if set.Has(SIGTERM) {
		t.Fatal("Del failed")
	}
	if u := Of(SIGUSR1).Union(Of(SIGUSR2)); !u.Has(SIGUSR1) || !u.Has(SIGUSR2) {
		t.Fatal("Union failed")
	}
	if i := Of(SIGUSR1, SIGUSR2).Intersect(Of(SIGUSR2)); i.Has(SIGUSR1) || !i.Has(SIGUSR2) {
		t.Fatal("Intersect failed")
	}
	low, ok := Of(SIGTERM, SIGUSR1).Lowest()
	if !ok || low != SIGUSR1 {
		t.Fatalf("Lowest = %v, %v; want %v, true", low, ok, Signal(SIGUSR1))
	}
	if _, ok := EmptySet.Lowest(); ok {
		t.Fatal("empty set has no lowest")
	}
	// Out-of-range signals are ignored by set operations.
	if s := EmptySet.Add(0).Add(99); s != EmptySet {
		t.Fatal("invalid signals must not enter the set")
	}
}

func TestDefaultActions(t *testing.T) {
	cases := map[Signal]DefaultActionKind{
		SIGKILL: DefTerm,
		SIGTERM: DefTerm,
		SIGCHLD: DefIgn,
		SIGSTOP: DefStop,
		SIGCONT: DefCont,
		MinRT:   DefTerm,
	}
	for sig, want := range cases {
		if got := DefaultAction(sig); got != want {
			t.Fatalf("DefaultAction(%d) = %v, want %v", sig, got, want)
		}
	}
}

func TestCatchable(t *testing.T) {
	if Catchable(SIGKILL) || Catchable(SIGSTOP) {
		t.Fatal("SIGKILL/SIGSTOP must be uncatchable")
	}
	if !Catchable(SIGTERM) || !Catchable(MinRT) {
		t.Fatal("SIGTERM and realtime signals must be catchable")
	}
	if Catchable(0) {
		t.Fatal("invalid signal must not be catchable")
	}
}
