package upstream

import (
	"fmt"
	"time"
)

// Kind classifies an upstream failure. It drives retry decisions, circuit breaker
// accounting, metric labels and the HTTP status returned to API clients.
type Kind string

// Failure kinds.
const (
	KindTimeout     Kind = "timeout"      // attempt or request deadline exceeded
	KindNetwork     Kind = "network"      // DNS, connect, reset, truncated body...
	KindRateLimited Kind = "rate_limited" // vendor answered 429
	KindServer      Kind = "server_error" // vendor answered 5xx
	KindClient      Kind = "client_error" // vendor answered 400/422/...: our request was invalid
	KindRejected    Kind = "rejected"     // vendor answered 401/403/404/410: it refuses us, not this one request
	KindBadResponse Kind = "bad_response" // unexpected status, oversized or undecodable body
	KindCircuitOpen Kind = "circuit_open" // breaker is failing fast
	KindThrottled   Kind = "throttled"    // local quota guard or bulkhead could not admit in time
	KindCanceled    Kind = "canceled"     // the caller went away
)

// Error is returned for every failed upstream call. Its message never contains
// credentials.
type Error struct {
	Upstream   string
	Kind       Kind
	StatusCode int           // vendor HTTP status, 0 if no response
	RetryAfter time.Duration // vendor or breaker hint, 0 if unknown
	Reason     string        // vendor-supplied reason for 4xx responses
	Err        error         // underlying cause, already redacted
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("upstream %s: %s", e.Upstream, e.Kind)
	if e.StatusCode != 0 {
		msg += fmt.Sprintf(" (HTTP %d)", e.StatusCode)
	}
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *Error) Unwrap() error { return e.Err }

// Unavailable reports whether the failure means "the vendor cannot serve us right
// now" (as opposed to a bad request or a departed caller). Only these failures may
// fall back to stale cached data.
func (e *Error) Unavailable() bool {
	switch e.Kind {
	case KindTimeout, KindNetwork, KindRateLimited, KindServer, KindBadResponse, KindRejected, KindCircuitOpen, KindThrottled:
		return true
	case KindClient, KindCanceled:
		return false
	default:
		return false
	}
}

// retryable reports whether another attempt could plausibly succeed.
func (e *Error) retryable() bool {
	switch e.Kind {
	case KindTimeout, KindNetwork, KindRateLimited, KindServer:
		return true
	case KindBadResponse, KindClient, KindRejected, KindCircuitOpen, KindThrottled, KindCanceled:
		return false
	default:
		return false
	}
}
