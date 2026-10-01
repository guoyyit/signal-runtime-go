// Package signalrt implements POSIX-style signal delivery semantics for a
// group of managed local processes sharing one runtime.
package signalrt

import "math/bits"

// Signal identifies a signal. Standard signals occupy [MinStandard, MaxStandard],
// realtime signals occupy [MinRT, MaxRT].
type Signal int32

const (
	MinStandard Signal = 1
	MaxStandard Signal = 31
	MinRT       Signal = 32
	MaxRT       Signal = 64

	// Named standard signals.
	SIGHUP  Signal = 1
	SIGINT  Signal = 2
	SIGKILL Signal = 9
	SIGUSR1 Signal = 10
	SIGUSR2 Signal = 12
	SIGTERM Signal = 15
	SIGCONT Signal = 18
	SIGSTOP Signal = 19
	SIGCHLD Signal = 20
)

// Valid reports whether s is a known signal number.
func (s Signal) Valid() bool { return s >= MinStandard && s <= MaxRT }

// Realtime reports whether s is a realtime signal.
func (s Signal) Realtime() bool { return s >= MinRT && s <= MaxRT }

// Set is a bitmask of signals, used for masks and pending standard signals.
type Set uint64

// EmptySet is the empty signal set.
const EmptySet Set = 0

// Of builds a Set containing exactly the given signals.
func Of(sigs ...Signal) Set {
	var set Set
	for _, sig := range sigs {
		set = set.Add(sig)
	}
	return set
}

// Add returns the set with sig included.
func (s Set) Add(sig Signal) Set {
	if !sig.Valid() {
		return s
	}
	return s | (1 << uint(sig-1))
}

// Del returns the set with sig removed.
func (s Set) Del(sig Signal) Set {
	if !sig.Valid() {
		return s
	}
	return s &^ (1 << uint(sig-1))
}

// Has reports whether sig is in the set.
func (s Set) Has(sig Signal) bool {
	if !sig.Valid() {
		return false
	}
	return s&(1<<uint(sig-1)) != 0
}

// Union returns the union of two sets.
func (s Set) Union(o Set) Set { return s | o }

// Intersect returns the intersection of two sets.
func (s Set) Intersect(o Set) Set { return s & o }

// Lowest returns the lowest-numbered signal in the set and true,
// or 0 and false when the set is empty.
func (s Set) Lowest() (Signal, bool) {
	if s == 0 {
		return 0, false
	}
	return Signal(bits.TrailingZeros64(uint64(s)) + 1), true
}

// DefaultActionKind is the action the runtime takes for a signal whose
// disposition is the default.
type DefaultActionKind int

const (
	// DefTerm terminates the process.
	DefTerm DefaultActionKind = iota
	// DefIgn discards the signal.
	DefIgn
	// DefStop stops the process.
	DefStop
	// DefCont resumes a stopped process.
	DefCont
)

// DefaultAction returns the default action kind for sig.
func DefaultAction(sig Signal) DefaultActionKind {
	switch sig {
	case SIGCHLD:
		return DefIgn
	case SIGSTOP:
		return DefStop
	case SIGCONT:
		return DefCont
	default:
		return DefTerm
	}
}

// Catchable reports whether sig may be caught, ignored or blocked.
// SIGKILL and SIGSTOP always take their default action.
func Catchable(sig Signal) bool {
	return sig.Valid() && sig != SIGKILL && sig != SIGSTOP
}
