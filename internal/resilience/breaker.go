// Package resilience provides dependency-free building blocks for calling unreliable
// remote systems: a circuit breaker, jittered exponential backoff and Retry-After
// parsing. The HTTP-specific retry loop lives in the upstream package; these types stay
// transport-agnostic so they are trivial to test with a fake clock.
package resilience

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned by Breaker.Allow while the circuit is open (or while the
// half-open probe budget is exhausted).
var ErrCircuitOpen = errors.New("circuit breaker is open")

// State is the circuit breaker state.
type State int

// Breaker states. The numeric values are exported as the circuit_state gauge.
const (
	StateClosed   State = 0
	StateHalfOpen State = 1
	StateOpen     State = 2
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half_open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

// Outcome is the result a caller reports back to the breaker.
type Outcome int

const (
	// Success resets the failure count (and closes a half-open circuit).
	Success Outcome = iota
	// Failure counts towards opening the circuit.
	Failure
	// Ignored releases the call slot without affecting breaker state, e.g. when the
	// caller cancelled the request or the error was the caller's own fault (HTTP 400).
	Ignored
)

// BreakerConfig configures a Breaker.
type BreakerConfig struct {
	// FailureThreshold is the number of consecutive failures that opens the circuit.
	FailureThreshold int
	// Cooldown is how long the circuit stays open before admitting a probe.
	Cooldown time.Duration
	// HalfOpenProbes is the number of concurrent trial calls admitted when half-open.
	HalfOpenProbes int
	// OnStateChange is invoked (outside the lock) on every transition.
	OnStateChange func(from, to State)
	// Now is the clock; defaults to time.Now.
	Now func() time.Time
}

// Breaker is a consecutive-failure circuit breaker.
//
// Closed: calls flow; FailureThreshold consecutive failures open the circuit.
// Open: calls fail fast with ErrCircuitOpen until Cooldown elapses.
// Half-open: up to HalfOpenProbes trial calls are admitted; a success closes the
// circuit, a failure re-opens it for another Cooldown.
//
// Each transition starts a new generation; outcomes reported for calls admitted in an
// earlier generation are discarded so a slow request from before the circuit opened
// cannot corrupt the current state.
type Breaker struct {
	cfg BreakerConfig

	mu         sync.Mutex
	state      State
	generation uint64
	failures   int
	openUntil  time.Time
	probes     int
}

// NewBreaker creates a closed breaker. Zero-valued config fields get sane defaults.
func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.FailureThreshold < 1 {
		cfg.FailureThreshold = 5
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 30 * time.Second
	}
	if cfg.HalfOpenProbes < 1 {
		cfg.HalfOpenProbes = 1
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Breaker{cfg: cfg}
}

// Allow asks permission to make a call. On success the caller must invoke done exactly
// once with the call's outcome.
func (b *Breaker) Allow() (done func(Outcome), err error) {
	b.mu.Lock()
	var transition func()
	if b.state == StateOpen && !b.cfg.Now().Before(b.openUntil) {
		transition = b.setStateLocked(StateHalfOpen)
	}
	switch b.state {
	case StateClosed:
	case StateOpen:
		b.mu.Unlock()
		return nil, ErrCircuitOpen
	case StateHalfOpen:
		if b.probes >= b.cfg.HalfOpenProbes {
			b.mu.Unlock()
			notify(transition)
			return nil, ErrCircuitOpen
		}
		b.probes++
	}
	gen := b.generation
	b.mu.Unlock()
	notify(transition)

	var once sync.Once
	return func(o Outcome) { once.Do(func() { b.report(gen, o) }) }, nil
}

func (b *Breaker) report(gen uint64, o Outcome) {
	b.mu.Lock()
	if gen != b.generation {
		b.mu.Unlock()
		return
	}
	var transition func()
	switch b.state {
	case StateClosed:
		switch o {
		case Success:
			b.failures = 0
		case Failure:
			b.failures++
			if b.failures >= b.cfg.FailureThreshold {
				transition = b.setStateLocked(StateOpen)
			}
		case Ignored:
		}
	case StateHalfOpen:
		b.probes--
		switch o {
		case Success:
			transition = b.setStateLocked(StateClosed)
		case Failure:
			transition = b.setStateLocked(StateOpen)
		case Ignored:
		}
	case StateOpen:
		// Calls are never admitted while open, so nothing can report in this generation.
	}
	b.mu.Unlock()
	notify(transition)
}

// setStateLocked transitions to s and returns the notification to run after unlocking.
func (b *Breaker) setStateLocked(s State) func() {
	from := b.state
	b.state = s
	b.generation++
	b.failures = 0
	b.probes = 0
	if s == StateOpen {
		b.openUntil = b.cfg.Now().Add(b.cfg.Cooldown)
	}
	if b.cfg.OnStateChange == nil || from == s {
		return nil
	}
	return func() { b.cfg.OnStateChange(from, s) }
}

func notify(f func()) {
	if f != nil {
		f()
	}
}

// State returns the current state, accounting for an elapsed cooldown.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateOpen && !b.cfg.Now().Before(b.openUntil) {
		return StateHalfOpen
	}
	return b.state
}

// RetryAfter returns how long until the open circuit admits a probe (0 if not open).
// It is surfaced to API clients via the Retry-After header.
func (b *Breaker) RetryAfter() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateOpen {
		return 0
	}
	return max(b.openUntil.Sub(b.cfg.Now()), 0)
}
