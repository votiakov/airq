// Package telemetry holds the cross-cutting observability plumbing: structured
// logging with request and trace correlation, OpenTelemetry tracing setup, and the
// Prometheus registry.
package telemetry

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"

	"github.com/votiakov/airq/internal/version"
)

type requestIDKey struct{}

// WithRequestID returns a context carrying the request ID.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID returns the request ID from ctx, or "" outside a request.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// NewLogger builds the process logger. Every record logged with a request context
// (logger.InfoContext(ctx, ...)) automatically carries request_id, and trace_id and
// span_id from the active OpenTelemetry span, so handlers never thread correlation
// fields by hand. (The otelslog bridge was not used: it ships records to an OTLP log
// pipeline, whereas these logs go to stdout for the platform's collector.)
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
	if id := RequestID(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}
