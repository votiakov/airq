// Package telemetry holds the cross-cutting observability plumbing: structured
// logging with request correlation, W3C trace context, and the Prometheus registry.
package telemetry

import (
	"context"
	"io"
	"log/slog"

	"github.com/votiakov/airq/internal/version"
)

// RequestInfo identifies the request a log line or metric observation belongs to.
type RequestInfo struct {
	RequestID string
	Trace     TraceContext
}

type requestInfoKey struct{}

// WithRequestInfo returns a context carrying info.
func WithRequestInfo(ctx context.Context, info RequestInfo) context.Context {
	return context.WithValue(ctx, requestInfoKey{}, info)
}

// RequestInfoFrom extracts the request info stored by WithRequestInfo.
func RequestInfoFrom(ctx context.Context) (RequestInfo, bool) {
	info, ok := ctx.Value(requestInfoKey{}).(RequestInfo)
	return info, ok
}

// RequestID returns the request ID from ctx, or "" outside a request.
func RequestID(ctx context.Context) string {
	info, _ := RequestInfoFrom(ctx)
	return info.RequestID
}

// NewLogger builds the process logger. Every record logged with a request context
// (logger.InfoContext(ctx, ...)) automatically carries request_id, trace_id and span_id,
// so handlers never have to thread correlation fields by hand.
func NewLogger(w io.Writer, level slog.Leveler, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if format == "text" {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	v := version.Get()
	return slog.New(contextHandler{h}).With(
		slog.String("service", "airq"),
		slog.String("version", v.Version),
	)
}

type contextHandler struct{ slog.Handler }

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if info, ok := RequestInfoFrom(ctx); ok {
		r.AddAttrs(
			slog.String("request_id", info.RequestID),
			slog.String("trace_id", info.Trace.TraceIDString()),
			slog.String("span_id", info.Trace.SpanIDString()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}
