package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// spanContext returns a context carrying a remote span with fixed IDs.
func spanContext(t *testing.T, sampled bool) (context.Context, trace.SpanContext) {
	t.Helper()
	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	cfg := trace.SpanContextConfig{TraceID: tid, SpanID: sid, Remote: true}
	if sampled {
		cfg.TraceFlags = trace.FlagsSampled
	}
	sc := trace.NewSpanContext(cfg)
	return trace.ContextWithSpanContext(context.Background(), sc), sc
}

func TestSetupTracingWithoutExporter(t *testing.T) {
	tp, err := SetupTracing(context.Background(), func(string) string { return "" }, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := otel.Tracer("test").Start(context.Background(), "op")
	defer span.End()
	if sc := span.SpanContext(); !sc.IsValid() || !sc.IsSampled() {
		t.Fatalf("spans must carry valid, sampled IDs even without an exporter: %+v", sc)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if !strings.HasPrefix(carrier.Get("traceparent"), "00-"+span.SpanContext().TraceID().String()) {
		t.Errorf("W3C propagator not installed: %v", carrier)
	}
}

func TestSetupTracingWithExporter(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	tp, err := SetupTracing(context.Background(), os.Getenv, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tp.Shutdown(ctx) // nothing buffered; must return promptly
}

func TestLoggerAddsRequestContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, slog.LevelInfo, "json")
	ctx, sc := spanContext(t, false)
	ctx = WithRequestID(ctx, "req-1")

	logger.With("component", "test").InfoContext(ctx, "hello", "k", "v")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("invalid json log: %v: %s", err, buf.String())
	}
	for k, want := range map[string]string{
		"msg": "hello", "request_id": "req-1", "trace_id": sc.TraceID().String(),
		"span_id": sc.SpanID().String(), "service": "airq", "component": "test", "k": "v",
	} {
		if rec[k] != want {
			t.Errorf("%s = %v, want %v", k, rec[k], want)
		}
	}

	buf.Reset()
	logger.Info("no request")
	if strings.Contains(buf.String(), "request_id") || strings.Contains(buf.String(), "trace_id") {
		t.Errorf("correlation fields must not appear without a request context: %s", buf.String())
	}
}

func TestMetricsHandlerExposesBuildInfoAndRuntime(t *testing.T) {
	reg := NewRegistry()
	srv := httptest.NewServer(MetricsHandler(reg, slog.New(slog.DiscardHandler)))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body bytes.Buffer
	_, _ = body.ReadFrom(resp.Body)
	for _, want := range []string{"airq_build_info{", "go_goroutines", "process_start_time_seconds", "promhttp_metric_handler_requests_total"} {
		if !strings.Contains(body.String(), want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

func TestObserveWithExemplar(t *testing.T) {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_seconds", Help: "h"})
	ctx, sc := spanContext(t, true)
	unsampled, _ := spanContext(t, false)
	ObserveWithExemplar(ctx, h, 0.2)
	ObserveWithExemplar(unsampled, h, 3)              // not recorded anywhere: no exemplar
	ObserveWithExemplar(context.Background(), h, 0.3) // no span: no exemplar, must not panic

	reg := prometheus.NewRegistry()
	reg.MustRegister(h)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, b := range mfs[0].GetMetric()[0].GetHistogram().GetBucket() {
		if ex := b.GetExemplar(); ex != nil {
			found++
			if ex.GetLabel()[0].GetValue() != sc.TraceID().String() {
				t.Errorf("exemplar trace_id = %s", ex.GetLabel()[0].GetValue())
			}
		}
	}
	if found != 1 {
		t.Errorf("found %d exemplars, want exactly 1 (sampled span only)", found)
	}
}
