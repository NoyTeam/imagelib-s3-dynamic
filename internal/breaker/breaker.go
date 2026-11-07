// Package breaker is a small consecutive-failure circuit breaker.
//
//	closed ──N consecutive failures──▶ open ──cooldown──▶ half-open (one probe at a time)
//	   ▲                                 ▲                   │
//	   └──────────── probe succeeds ─────┼───────────────────┘
//	                                     └──── probe fails ──┘
package breaker

import (
	"log/slog"
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

type Breaker struct {
	name      string
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	onChange func(State)

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	probing  bool
	probeAt  time.Time
}

// New returns a breaker that opens after threshold consecutive failures and probes
// again after cooldown. threshold <= 0 disables the breaker (always allows).
func New(name string, threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{name: name, threshold: threshold, cooldown: cooldown, now: time.Now}
}

// OnStateChange registers fn, called (with the breaker lock held, so it must be cheap and
// not call back into the breaker) on every state transition. fn is also called once now with
// the current state. Set it before the breaker is shared.
func (b *Breaker) OnStateChange(fn func(State)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onChange = fn
	if fn != nil {
		fn(b.state)
	}
}

// setState transitions to s. Caller holds mu.
func (b *Breaker) setState(s State) {
	if b.state == s {
		return
	}
	b.state = s
	if b.onChange != nil {
		b.onChange(s)
	}
}

// Allow reports whether a call may proceed. In half-open state only one probe is let
// through; if its outcome is never reported (e.g. the client went away), another probe
// is allowed after cooldown so the breaker can't get stuck.
func (b *Breaker) Allow() bool {
	if b.threshold <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	switch b.state {
	case Closed:
		return true
	case Open:
		if now.Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.setState(HalfOpen)
		slog.Info("circuit breaker half-open, probing", "breaker", b.name)
	case HalfOpen:
		if b.probing && now.Sub(b.probeAt) < b.cooldown {
			return false
		}
	}
	b.probing = true
	b.probeAt = now
	return true
}

// Success records a successful call.
func (b *Breaker) Success() {
	if b.threshold <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != Closed {
		slog.Info("circuit breaker closed", "breaker", b.name)
	}
	b.setState(Closed)
	b.failures = 0
	b.probing = false
}

// Failure records a failed call.
func (b *Breaker) Failure() {
	if b.threshold <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == HalfOpen || (b.state == Closed && b.failures >= b.threshold) {
		if b.state == Closed {
			slog.Warn("circuit breaker opened", "breaker", b.name, "failures", b.failures, "cooldown", b.cooldown)
		}
		b.setState(Open)
		b.openedAt = b.now()
		b.probing = false
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
