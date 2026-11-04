package breaker

import (
	"testing"
	"time"
)

func TestBreakerLifecycle(t *testing.T) {
	now := time.Unix(0, 0)
	b := New("t", 3, 10*time.Second)
	b.now = func() time.Time { return now }

	for i := 0; i < 2; i++ {
		b.Failure()
	}
	b.Success() // resets the consecutive count
	b.Failure()
	b.Failure()
	if b.State() != Closed {
		t.Fatal("must stay closed below threshold")
	}
	b.Failure()
	if b.State() != Open || b.Allow() {
		t.Fatal("must open after 3 consecutive failures")
	}

	now = now.Add(10 * time.Second)
	if !b.Allow() || b.State() != HalfOpen {
		t.Fatal("first call after cooldown is a probe")
	}
	if b.Allow() {
		t.Fatal("only one probe at a time")
	}
	b.Failure()
	if b.State() != Open || b.Allow() {
		t.Fatal("failed probe re-opens")
	}

	now = now.Add(10 * time.Second)
	if !b.Allow() {
		t.Fatal("probe allowed again")
	}
	// Probe outcome never reported: another probe is allowed after cooldown.
	now = now.Add(10 * time.Second)
	if !b.Allow() {
		t.Fatal("lost probe must not wedge the breaker")
	}
	b.Success()
	if b.State() != Closed || !b.Allow() {
		t.Fatal("successful probe closes")
	}
}

func TestBreakerDisabled(t *testing.T) {
	b := New("t", 0, time.Second)
	for i := 0; i < 100; i++ {
		b.Failure()
	}
	if !b.Allow() || b.State() != Closed {
		t.Fatal("threshold 0 disables the breaker")
	}
}
