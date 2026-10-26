package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestParseTraceparent(t *testing.T) {
	valid := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	tc, ok := ParseTraceparent(valid)
	if !ok {
		t.Fatalf("valid header rejected")
	}
	if tc.TraceIDString() != "4bf92f3577b34da6a3ce929d0e0e4736" || tc.SpanIDString() != "00f067aa0ba902b7" || tc.Flags != 1 {
		t.Errorf("parsed %+v", tc)
	}
	if tc.Traceparent() != valid {
		t.Errorf("round trip = %q", tc.Traceparent())
	}

	// A future version may carry extra fields after the flags.
	if _, ok := ParseTraceparent("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra"); !ok {
		t.Error("future version with extra fields should be accepted")
	}

	for _, bad := range []string{
		"",
		"garbage",
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",   // upper-case hex
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",   // zero trace id
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",   // zero span id
		"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",   // forbidden version
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-x", // v00 must be exact
		"00-4bf92f3577b34da6a3ce929d0e0e473z-00f067aa0ba902b7-01",   // non-hex
	} {
		if _, ok := ParseTraceparent(bad); ok {
			t.Errorf("ParseTraceparent(%q) accepted invalid header", bad)
		}
	}
}

func TestNewTraceContextAndChild(t *testing.T) {
	a := NewTraceContext()
	if _, ok := ParseTraceparent(a.Traceparent()); !ok {
		t.Fatalf("generated traceparent %q does not parse", a.Traceparent())
	}
	c := a.Child()
	if c.TraceID != a.TraceID || c.SpanID == a.SpanID {
		t.Errorf("child must share trace id and have a new span id")
	}
}

func TestLoggerAddsRequestContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, slog.LevelInfo, "json")
	tc := NewTraceContext()
	ctx := WithRequestInfo(context.Background(), RequestInfo{RequestID: "req-1", Trace: tc})

	logger.With("component", "test").InfoContext(ctx, "hello", "k", "v")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("invalid json log: %v: %s", err, buf.String())
	}
	for k, want := range map[string]string{
		"msg": "hello", "request_id": "req-1", "trace_id": tc.TraceIDString(),
		"span_id": tc.SpanIDString(), "service": "airq", "component": "test", "k": "v",
	} {
		if rec[k] != want {
			t.Errorf("%s = %v, want %v", k, rec[k], want)
		}
	}

	buf.Reset()
	logger.Info("no request")
	if strings.Contains(buf.String(), "request_id") {
		t.Errorf("request_id must not appear without a request context: %s", buf.String())
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
	tc := NewTraceContext()
	ctx := WithRequestInfo(context.Background(), RequestInfo{RequestID: "r", Trace: tc})
	ObserveWithExemplar(ctx, h, 0.2)
	ObserveWithExemplar(context.Background(), h, 0.3) // no exemplar, must not panic

	reg := prometheus.NewRegistry()
	reg.MustRegister(h)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, b := range mfs[0].GetMetric()[0].GetHistogram().GetBucket() {
		if ex := b.GetExemplar(); ex != nil && ex.GetLabel()[0].GetValue() == tc.TraceIDString() {
			found = true
		}
	}
	if !found {
		t.Error("exemplar with trace id not recorded")
	}
}
