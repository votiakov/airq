package upstream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sony/gobreaker/v2"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

type memRecorder struct {
	mu  sync.Mutex
	exs []Exchange
}

func (r *memRecorder) Record(e Exchange) {
	r.mu.Lock()
	r.exs = append(r.exs, e)
	r.mu.Unlock()
}

func (r *memRecorder) all() []Exchange {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Exchange(nil), r.exs...)
}

type fixture struct {
	client   *Client
	metrics  *Metrics
	recorder *memRecorder
	hits     *atomic.Int32
	spans    *tracetest.SpanRecorder
}

func newFixture(t *testing.T, handler http.HandlerFunc, mutate func(*Config)) *fixture {
	t.Helper()
	hits := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)

	m := NewMetrics(prometheus.NewRegistry())
	rec := &memRecorder{}
	spans := tracetest.NewSpanRecorder()
	cfg := Config{
		TracerProvider:   sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
		Name:             "test",
		BaseURL:          srv.URL,
		APIKey:           "secret-key",
		AttemptTimeout:   500 * time.Millisecond,
		MaxAttempts:      3,
		RetryBaseDelay:   time.Millisecond,
		RetryMaxDelay:    5 * time.Millisecond,
		BreakerThreshold: 100,
		BreakerCooldown:  time.Minute,
		RateLimit:        rate.Inf,
		RateBurst:        1,
		MaxConcurrent:    4,
		MaxResponseBytes: 1 << 10,
		Metrics:          m,
		Recorder:         rec,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{client: c, metrics: m, recorder: rec, hits: hits, spans: spans}
}

func (f *fixture) count(vec *prometheus.CounterVec, labels ...string) float64 {
	return testutil.ToFloat64(vec.WithLabelValues(labels...))
}

func kindOf(t *testing.T, err error) Kind {
	t.Helper()
	var uerr *Error
	if !errors.As(err, &uerr) {
		t.Fatalf("expected *upstream.Error, got %T: %v", err, err)
	}
	return uerr.Kind
}

func TestGetJSONSuccess(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/thing" || r.URL.Query().Get("lat") != "1.5" || r.URL.Query().Get("apikey") != "secret-key" {
			t.Errorf("unexpected request %s", r.URL)
		}
		if !strings.HasPrefix(r.Header.Get("User-Agent"), "airq/") {
			t.Errorf("missing user agent")
		}
		_, _ = w.Write([]byte(`{"value":42}`))
	}, nil)

	var out struct{ Value int }
	if err := f.client.GetJSON(context.Background(), "/v1/thing", url.Values{"lat": {"1.5"}}, &out); err != nil {
		t.Fatal(err)
	}
	if out.Value != 42 {
		t.Fatalf("decoded %+v", out)
	}
	if got := f.count(f.metrics.calls, "test", "success"); got != 1 {
		t.Errorf("calls{success} = %v", got)
	}
	exs := f.recorder.all()
	if len(exs) != 1 {
		t.Fatalf("recorded %d exchanges", len(exs))
	}
	ex := exs[0]
	if ex.StatusCode != http.StatusOK || ex.Outcome != "success" || ex.Endpoint != "/v1/thing" || string(ex.Body) != `{"value":42}` {
		t.Errorf("exchange = %+v", ex)
	}
	if _, leaked := ex.Query["apikey"]; leaked || ex.Query["lat"] != "1.5" {
		t.Errorf("recorded query must be redacted but keep params: %v", ex.Query)
	}
}

func TestRetriesTransientFailuresThenSucceeds(t *testing.T) {
	var n atomic.Int32
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}, nil)

	var out struct{}
	if err := f.client.GetJSON(context.Background(), "/", nil, &out); err != nil {
		t.Fatalf("expected success after retries: %v", err)
	}
	if f.hits.Load() != 3 {
		t.Errorf("hits = %d, want 3", f.hits.Load())
	}
	if got := f.count(f.metrics.retries, "test"); got != 2 {
		t.Errorf("retries = %v, want 2", got)
	}
	if got := f.count(f.metrics.attempts, "test", "server_error"); got != 2 {
		t.Errorf("attempts{server_error} = %v", got)
	}
	if got := f.count(f.metrics.calls, "test", "success"); got != 1 {
		t.Errorf("calls{success} = %v", got)
	}
	if len(f.recorder.all()) != 3 {
		t.Errorf("every attempt must be recorded")
	}
}

func TestSpansNeverLeakTheAPIKeyOrPropagateToTheVendor(t *testing.T) {
	var n atomic.Int32
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("traceparent") != "" {
			t.Error("trace context must not be sent to a third party")
		}
		if n.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}, nil)
	if err := f.client.GetJSON(context.Background(), "/v1/thing", url.Values{"lat": {"1"}}, &struct{}{}); err != nil {
		t.Fatal(err)
	}

	spans := f.spans.Ended()
	if len(spans) != 3 {
		t.Fatalf("got %d spans, want 2 attempts + 1 logical call", len(spans))
	}
	call := spans[2]
	if call.Name() != "test /v1/thing" {
		t.Errorf("call span = %q", call.Name())
	}
	for i, s := range spans[:2] {
		if s.SpanKind() != trace.SpanKindClient || s.Parent().SpanID() != call.SpanContext().SpanID() {
			t.Errorf("attempt %d: kind %v, parent %v", i+1, s.SpanKind(), s.Parent().SpanID())
		}
		for _, kv := range s.Attributes() {
			if strings.Contains(kv.Value.String(), "secret-key") {
				t.Errorf("attempt span leaks the API key in %s", kv.Key)
			}
			if kv.Key == "url.full" && !strings.Contains(kv.Value.String(), "lat=1") {
				t.Errorf("url.full should keep non-secret params: %s", kv.Value.String())
			}
		}
	}
	if spans[0].Status().Code != codes.Error || spans[1].Status().Code == codes.Error {
		t.Errorf("statuses = %v, %v", spans[0].Status(), spans[1].Status())
	}
	if ex := f.recorder.all()[0]; ex.Trace.SpanID() != call.SpanContext().SpanID() {
		t.Error("recorded exchanges must reference the logical call span")
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}, nil)
	err := f.client.GetJSON(context.Background(), "/", nil, &struct{}{})
	if kindOf(t, err) != KindServer {
		t.Fatalf("err = %v", err)
	}
	if f.hits.Load() != 3 {
		t.Errorf("hits = %d, want MaxAttempts=3", f.hits.Load())
	}
	var uerr *Error
	errors.As(err, &uerr)
	if !uerr.Unavailable() {
		t.Error("server errors must allow stale fallback")
	}
}

func TestClientErrorIsNotRetriedAndCarriesReason(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":true,"reason":"Latitude must be in range of -90 to 90°."}`))
	}, func(c *Config) { c.BreakerThreshold = 1 })

	err := f.client.GetJSON(context.Background(), "/", nil, &struct{}{})
	var uerr *Error
	if !errors.As(err, &uerr) || uerr.Kind != KindClient || uerr.StatusCode != http.StatusBadRequest {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(uerr.Reason, "Latitude must be in range") {
		t.Errorf("reason = %q", uerr.Reason)
	}
	if f.hits.Load() != 1 {
		t.Errorf("4xx must not be retried, hits = %d", f.hits.Load())
	}
	if f.client.breaker.State() != gobreaker.StateClosed {
		t.Error("4xx must not trip the breaker even with threshold 1")
	}
	if uerr.Unavailable() {
		t.Error("client errors must not fall back to stale data")
	}
}

func TestRateLimitedHonoursRetryAfter(t *testing.T) {
	var n atomic.Int32
	var first, second time.Time
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			first = time.Now()
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		second = time.Now()
		_, _ = w.Write([]byte(`{}`))
	}, nil)

	if err := f.client.GetJSON(context.Background(), "/", nil, &struct{}{}); err != nil {
		t.Fatal(err)
	}
	if gap := second.Sub(first); gap < 900*time.Millisecond {
		t.Errorf("retry came after %s, want >= Retry-After of 1s", gap)
	}
}

func TestRetryStopsWhenDeadlineBudgetIsExhausted(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	err := f.client.GetJSON(ctx, "/", nil, &struct{}{})
	if kindOf(t, err) != KindRateLimited {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > time.Second {
		t.Errorf("must fail fast instead of sleeping past the deadline")
	}
	if f.hits.Load() != 1 {
		t.Errorf("hits = %d", f.hits.Load())
	}
}

func TestAttemptTimeout(t *testing.T) {
	f := newFixture(t, func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}, func(c *Config) {
		c.AttemptTimeout = 50 * time.Millisecond
		c.MaxAttempts = 2
	})
	err := f.client.GetJSON(context.Background(), "/", nil, &struct{}{})
	if kindOf(t, err) != KindTimeout {
		t.Fatalf("err = %v", err)
	}
	if f.hits.Load() != 2 {
		t.Errorf("timeouts should be retried: hits = %d", f.hits.Load())
	}
}

func TestCircuitBreakerFailsFast(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}, func(c *Config) {
		c.MaxAttempts = 1
		c.BreakerThreshold = 2
	})
	for range 2 {
		_ = f.client.GetJSON(context.Background(), "/", nil, &struct{}{})
	}
	if f.client.breaker.State() != gobreaker.StateOpen {
		t.Fatalf("breaker = %s, want open", f.client.breaker.State())
	}

	err := f.client.GetJSON(context.Background(), "/", nil, &struct{}{})
	var uerr *Error
	if !errors.As(err, &uerr) || uerr.Kind != KindCircuitOpen || uerr.RetryAfter <= 0 {
		t.Fatalf("err = %v", err)
	}
	if f.hits.Load() != 2 {
		t.Errorf("open circuit must not reach the server, hits = %d", f.hits.Load())
	}
	if got := testutil.ToFloat64(f.metrics.circuitState.WithLabelValues("test")); got != float64(gobreaker.StateOpen) {
		t.Errorf("circuit_state gauge = %v", got)
	}
	if got := f.count(f.metrics.transitions, "test", "open"); got != 1 {
		t.Errorf("transitions{open} = %v", got)
	}
}

func TestCircuitBreakerRecoversThroughHalfOpen(t *testing.T) {
	var healthy atomic.Bool
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}, func(c *Config) {
		c.MaxAttempts = 1
		c.BreakerThreshold = 1
		c.BreakerCooldown = 50 * time.Millisecond
	})
	_ = f.client.GetJSON(context.Background(), "/", nil, &struct{}{})
	if !f.client.CircuitOpen() {
		t.Fatal("breaker should be open")
	}
	healthy.Store(true)
	time.Sleep(60 * time.Millisecond)
	if err := f.client.GetJSON(context.Background(), "/", nil, &struct{}{}); err != nil {
		t.Fatalf("trial request after cooldown: %v", err)
	}
	if f.client.breaker.State() != gobreaker.StateClosed {
		t.Errorf("breaker = %s, want closed after a successful trial", f.client.breaker.State())
	}
	if got := f.count(f.metrics.transitions, "test", "half-open"); got != 1 {
		t.Errorf("transitions{half-open} = %v", got)
	}
}

func TestBadResponses(t *testing.T) {
	tests := map[string]http.HandlerFunc{
		"invalid json": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{nope`)) },
		"oversized":    func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(strings.Repeat("x", 2<<10))) },
		"redirect": func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://evil.example.com/steal", http.StatusFound)
		},
	}
	for name, h := range tests {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, h, nil)
			err := f.client.GetJSON(context.Background(), "/", nil, &struct{}{})
			if kindOf(t, err) != KindBadResponse {
				t.Fatalf("err = %v", err)
			}
			if f.hits.Load() != 1 {
				t.Errorf("bad responses are not retried, hits = %d", f.hits.Load())
			}
		})
	}
}

func TestCanceledContextIsNotABreakerFailure(t *testing.T) {
	f := newFixture(t, func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}, func(c *Config) { c.BreakerThreshold = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	err := f.client.GetJSON(ctx, "/", nil, &struct{}{})
	if kindOf(t, err) != KindCanceled {
		t.Fatalf("err = %v", err)
	}
	if f.client.breaker.State() != gobreaker.StateClosed {
		t.Error("caller cancellation must not trip the breaker")
	}
}

func TestNetworkErrorsNeverLeakAPIKey(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	addr := srv.URL
	srv.Close() // connection refused from now on

	c, err := New(Config{
		Name: "test", BaseURL: addr, APIKey: "secret-key", AttemptTimeout: time.Second, MaxAttempts: 1,
		RateLimit: rate.Inf, MaxResponseBytes: 1024, Metrics: NewMetrics(prometheus.NewRegistry()),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = c.GetJSON(context.Background(), "/", url.Values{"lat": {"1"}}, &struct{}{})
	if kindOf(t, err) != KindNetwork {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("error leaks API key: %v", err)
	}
	if !strings.Contains(err.Error(), "lat=1") {
		t.Errorf("redacted error should keep non-secret params for debugging: %v", err)
	}
}

func TestLocalRateLimitThrottles(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) },
		func(c *Config) {
			c.RateLimit = rate.Every(time.Hour)
			c.RateBurst = 1
		})
	if err := f.client.GetJSON(context.Background(), "/", nil, &struct{}{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := f.client.GetJSON(ctx, "/", nil, &struct{}{})
	if kindOf(t, err) != KindThrottled {
		t.Fatalf("err = %v", err)
	}
	if f.hits.Load() != 1 {
		t.Errorf("throttled call must not reach the server")
	}
}

func TestBulkheadBoundsConcurrency(t *testing.T) {
	var cur, peak atomic.Int32
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
		_, _ = w.Write([]byte(`{}`))
	}, func(c *Config) { c.MaxConcurrent = 2 })

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := f.client.GetJSON(context.Background(), "/", nil, &struct{}{}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if peak.Load() > 2 {
		t.Errorf("peak concurrency %d exceeded bulkhead of 2", peak.Load())
	}
}

func TestNewValidatesConfig(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	if _, err := New(Config{Name: "x", BaseURL: "ftp://x", Metrics: m}); err == nil {
		t.Error("expected invalid base URL error")
	}
	if _, err := New(Config{Name: "x", BaseURL: "http://x"}); err == nil {
		t.Error("expected missing metrics error")
	}
}
