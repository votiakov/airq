package telemetry

import (
	"crypto/rand"
	"encoding/hex"
)

// TraceContext is a W3C Trace Context (https://www.w3.org/TR/trace-context/) span
// identity. The service does not run a tracing SDK (see docs/adr/0006); it propagates
// and logs W3C-compatible IDs so its logs and metric exemplars join up with traces
// recorded by callers, and so a real tracer can be added later without changing the
// wire format.
type TraceContext struct {
	TraceID [16]byte
	SpanID  [8]byte
	Flags   byte
}

// NewTraceContext starts a new trace with random IDs.
func NewTraceContext() TraceContext {
	var tc TraceContext
	_, _ = rand.Read(tc.TraceID[:]) // crypto/rand.Read never fails since Go 1.24
	_, _ = rand.Read(tc.SpanID[:])
	return tc
}

// Child returns a new span in the same trace.
func (tc TraceContext) Child() TraceContext {
	child := TraceContext{TraceID: tc.TraceID, Flags: tc.Flags}
	_, _ = rand.Read(child.SpanID[:])
	return child
}

// TraceIDString returns the trace ID as 32 lowercase hex characters.
func (tc TraceContext) TraceIDString() string { return hex.EncodeToString(tc.TraceID[:]) }

// SpanIDString returns the span ID as 16 lowercase hex characters.
func (tc TraceContext) SpanIDString() string { return hex.EncodeToString(tc.SpanID[:]) }

// Traceparent renders the traceparent header value.
func (tc TraceContext) Traceparent() string {
	return "00-" + tc.TraceIDString() + "-" + tc.SpanIDString() + "-" + hex.EncodeToString([]byte{tc.Flags})
}

// ParseTraceparent parses a traceparent header. Invalid headers are rejected as the
// spec requires, in which case the caller should start a new trace.
func ParseTraceparent(s string) (TraceContext, bool) {
	var tc TraceContext
	// version(2) - trace-id(32) - parent-id(16) - flags(2) = 55 chars. Future versions
	// may append fields after another '-'.
	if len(s) < 55 || s[2] != '-' || s[35] != '-' || s[52] != '-' {
		return tc, false
	}
	if len(s) > 55 && (s[:2] == "00" || s[55] != '-') {
		return tc, false
	}
	var ver [1]byte
	if !decodeLowerHex(ver[:], s[0:2]) || ver[0] == 0xff {
		return tc, false
	}
	var flags [1]byte
	if !decodeLowerHex(tc.TraceID[:], s[3:35]) ||
		!decodeLowerHex(tc.SpanID[:], s[36:52]) ||
		!decodeLowerHex(flags[:], s[53:55]) {
		return tc, false
	}
	if tc.TraceID == [16]byte{} || tc.SpanID == [8]byte{} {
		return tc, false
	}
	tc.Flags = flags[0]
	return tc, true
}

func decodeLowerHex(dst []byte, src string) bool {
	for i := range len(src) {
		c := src[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	n, err := hex.Decode(dst, []byte(src))
	return err == nil && n == len(dst)
}
