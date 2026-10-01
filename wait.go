package signalrt

import (
	"errors"
	"time"
)

// ErrInterrupted is returned by Wait when a caught signal without the
// Restart flag interrupts the wait.
var ErrInterrupted = errors.New("signalrt: wait interrupted by signal")

// Wait blocks until the runtime wakes pid (via Wake) or timeout elapses.
// Delivery of a caught signal during the wait runs its handler and then:
//   - dispositions with Restart resume waiting for the remaining time;
//   - other dispositions make Wait return ErrInterrupted.
//
// The race between signal arrival and wait completion is deterministic:
// whichever side commits first under the process lock wins. If the process
// terminates while waiting, Wait returns ErrProcessExited. A non-positive
// timeout makes Wait return immediately.
func (r *Runtime) Wait(pid int, timeout time.Duration) error {
	p := r.lookup(pid)
	if p == nil {
		return ErrUnknownProcess
	}
	deadline := time.Now().Add(timeout)
	p.mu.Lock()
	p.intrRestart = true
	p.mu.Unlock()
	for {
		p.mu.Lock()
		if p.dead {
			p.mu.Unlock()
			return ErrProcessExited
		}
		if p.waitCh == nil {
			p.waitCh = make(chan struct{})
		}
		ch := p.waitCh
		intr := p.intrSeq
		p.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
			timer.Stop()
			p.mu.Lock()
			switch {
			case p.dead:
				p.mu.Unlock()
				return ErrProcessExited
			case p.intrSeq != intr:
				restart := p.intrRestart
				p.mu.Unlock()
				if !restart {
					return ErrInterrupted
				}
				// Restart semantics: resume waiting for the
				// remaining time after the handler returned.
			default:
				p.mu.Unlock()
				return nil
			}
		case <-timer.C:
			p.mu.Lock()
			interrupted := p.intrSeq != intr
			restart := p.intrRestart
			p.mu.Unlock()
			if interrupted && !restart {
				return ErrInterrupted
			}
			if interrupted {
				continue
			}
			return nil
		}
	}
}

// Wake releases one waiter on pid. It is a no-op when nobody waits.
func (r *Runtime) Wake(pid int) {
	p := r.lookup(pid)
	if p == nil {
		return
	}
	p.mu.Lock()
	p.notifyWaitersLocked()
	p.mu.Unlock()
}
