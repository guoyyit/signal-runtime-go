package signalrt

import "sync"

// ActionKind selects how a signal is handled for a process.
type ActionKind int

const (
	// ActionDefault applies the signal's default action.
	ActionDefault ActionKind = iota
	// ActionIgnore discards the signal.
	ActionIgnore
	// ActionHandler invokes a registered handler.
	ActionHandler
)

// HandlerFunc is a user signal handler. It runs with the handler mask
// unioned onto the process mask (plus the signal itself unless NoDefer).
type HandlerFunc func(pid int, sig Signal)

// Action describes the disposition of one signal for a process.
type Action struct {
	Kind ActionKind
	// Handler is the registry name of the handler; only for ActionHandler.
	Handler string
	// HandlerMask is added to the process mask while the handler runs.
	HandlerMask Set
	// NoDefer allows the same signal to interrupt its own handler.
	NoDefer bool
	// Restart makes interruptible waits resume after the handler returns.
	Restart bool
}

// DefaultActionValue is the disposition applied to every signal of a
// freshly spawned process.
func DefaultActionValue() Action { return Action{Kind: ActionDefault} }

// HandlerRegistry maps handler names to functions so dispositions survive
// persistence snapshots. It is safe for concurrent use.
type HandlerRegistry struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

// NewHandlerRegistry returns an empty registry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: make(map[string]HandlerFunc)}
}

// Register adds or replaces a handler under name.
func (r *HandlerRegistry) Register(name string, fn HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[name] = fn
}

// Lookup returns the handler for name and whether it exists.
func (r *HandlerRegistry) Lookup(name string) (HandlerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.handlers[name]
	return fn, ok
}
