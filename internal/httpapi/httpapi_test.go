package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/votiakov/airq/internal/airquality"
	"github.com/votiakov/airq/internal/health"
	"github.com/votiakov/airq/internal/telemetry"
	"github.com/votiakov/airq/internal/upstream"
)

type fakeService struct {
	current  func(ctx context.Context, c airquality.Coordinates) (airquality.Result[airquality.Current], error)
	forecast func(ctx context.Context, c airquality.Coordinates, hours int) (airquality.Result[airquality.Forecast], error)
	resolve  func(ctx context.Context, name, country string) (airquality.Result[airquality.Location], error)
}

func (f *fakeService) Current(ctx context.Context, c airquality.Coordinates) (airquality.Result[airquality.Current], error) {
	return f.current(ctx, c)
}

func (f *fakeService) Forecast(ctx context.Context, c airquality.Coordinates, h int) (airquality.Result[airquality.Forecast], error) {
	return f.forecast(ctx, c, h)
}

func (f *fakeService) ResolveLocation(ctx context.Context, n, c string) (airquality.Result[airquality.Location], error) {
	return f.resolve(ctx, n, c)
}

func okService() *fakeService {
	eaqi := 35.0
	return &fakeService{
		current: func(_ context.Context, c airquality.Coordinates) (airquality.Result[airquality.Current], error) {
			return airquality.Result[airquality.Current]{
				Value: airquality.Current{
					Coordinates: c,
					Current:     airquality.Reading{Time: time.Unix(1793030400, 0).UTC(), EuropeanAQI: airquality.NewEuropeanAQI(&eaqi)},
				},
				Status: airquality.CacheMiss, TTL: 10 * time.Minute,
			}, nil
		},
		forecast: func(_ context.Context, c airquality.Coordinates, hours int) (airquality.Result[airquality.Forecast], error) {
			return airquality.Result[airquality.Forecast]{
				Value:  airquality.Forecast{Coordinates: c, Hourly: make([]airquality.Reading, hours)},
				Status: airquality.CacheHit, Age: 30 * time.Second, TTL: 5 * time.Minute,
			}, nil
		},
		resolve: func(_ context.Context, name, country string) (airquality.Result[airquality.Location], error) {
			if name == "Atlantis" {
				return airquality.Result[airquality.Location]{}, airquality.ErrLocationNotFound
			}
			return airquality.Result[airquality.Location]{Value: airquality.Location{
				Name: name, CountryCode: country, Coordinates: airquality.Coordinates{Latitude: 52.52, Longitude: 13.41},
			}}, nil
		},
	}
}

type harness struct {
	handler http.Handler
	reg     *prometheus.Registry
	logs    *bytes.Buffer
	spans   *tracetest.SpanRecorder
}

func newHarness(t *testing.T, svc Service, mutate func(*Options)) *harness {
	t.Helper()
	reg := prometheus.NewRegistry()
	logs := &bytes.Buffer{}
	logger := telemetry.NewLogger(&syncWriter{w: logs}, slog.LevelDebug, "json")
	spans := tracetest.NewSpanRecorder()
	opts := Options{
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
		Service:        svc,
		Health:         health.NewChecker(reg, 0),
		Registry:       reg,
		Logger:         logger,
		Attribution:    "test attribution",
		RequestTimeout: time.Second,
		MaxInFlight:    100,
		RateLimitRPS:   1000,
		RateLimitBurst: 1000,
	}
	if mutate != nil {
		mutate(&opts)
	}
	return &harness{handler: NewHandler(opts), reg: reg, logs: logs, spans: spans}
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func (h *harness) do(t *testing.T, method, target string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) Problem {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != problemContentType {
		t.Fatalf("Content-Type = %q, want %s (body %s)", ct, problemContentType, rec.Body)
	}
	var p Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("invalid problem body: %v", err)
	}
	if p.Status != rec.Code || p.RequestID == "" || p.Type != "about:blank" {
		t.Errorf("problem = %+v (code %d)", p, rec.Code)
	}
	return p
}

func TestCurrentByCoordinates(t *testing.T) {
	h := newHarness(t, okService(), nil)
	rec := h.do(t, http.MethodGet, "/v1/air-quality/current?lat=52.52&lon=13.41", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	hdr := rec.Header()
	for k, want := range map[string]string{
		"Content-Type":           "application/json; charset=utf-8",
		"Cache-Control":          "public, max-age=600",
		"X-Cache":                "MISS",
		"Cache-Status":           "airq; fwd=miss; stored; ttl=600",
		"Age":                    "0",
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
	} {
		if got := hdr.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if hdr.Get("ETag") == "" || hdr.Get("X-Request-ID") == "" {
		t.Error("missing ETag or X-Request-ID")
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["attribution"] != "test attribution" || body["location"] != nil {
		t.Errorf("body = %v", body)
	}
	cur := body["current"].(map[string]any)
	if cur["time"] != "2026-10-26T16:00:00Z" || cur["european_aqi"].(map[string]any)["category"] != "fair" {
		t.Errorf("current = %v", cur)
	}

	// Conditional request revalidates without a body.
	rec2 := h.do(t, http.MethodGet, "/v1/air-quality/current?lat=52.52&lon=13.41",
		map[string]string{"If-None-Match": `W/"nope", ` + hdr.Get("ETag")})
	if rec2.Code != http.StatusNotModified || rec2.Body.Len() != 0 {
		t.Errorf("conditional GET = %d with %d bytes", rec2.Code, rec2.Body.Len())
	}
	if rec2.Header().Get("ETag") != hdr.Get("ETag") || rec2.Header().Get("Cache-Control") == "" {
		t.Errorf("304 must keep validators and caching headers: %v", rec2.Header())
	}
	if rec3 := h.do(t, http.MethodGet, "/v1/air-quality/current?lat=52.52&lon=13.41",
		map[string]string{"If-None-Match": `"other"`}); rec3.Code != http.StatusOK || rec3.Body.Len() == 0 {
		t.Errorf("non-matching validator = %d with %d bytes", rec3.Code, rec3.Body.Len())
	}
	head := h.do(t, http.MethodHead, "/v1/air-quality/current?lat=52.52&lon=13.41", nil)
	if head.Code != http.StatusOK || head.Body.Len() != 0 || head.Header().Get("Content-Length") == "" {
		t.Errorf("HEAD = %d, %d body bytes, Content-Length %q", head.Code, head.Body.Len(), head.Header().Get("Content-Length"))
	}
}

func TestCurrentByLocationPathAndQuery(t *testing.T) {
	h := newHarness(t, okService(), nil)
	for _, target := range []string{
		"/v1/air-quality/Berlin?country=de",
		"/v1/air-quality/current?location=Berlin&country=DE",
		"/v1/air-quality/S%C3%A3o%20Paulo",
	} {
		rec := h.do(t, http.MethodGet, target, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d: %s", target, rec.Code, rec.Body)
		}
		var body struct {
			Location *airquality.Location `json:"location"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if body.Location == nil || body.Location.Name == "" {
			t.Errorf("%s: location missing from %s", target, rec.Body)
		}
	}
}

func TestForecast(t *testing.T) {
	h := newHarness(t, okService(), nil)
	rec := h.do(t, http.MethodGet, "/v1/air-quality/forecast?lat=1&lon=2&hours=6", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	var body struct{ Hourly []any }
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if len(body.Hourly) != 6 {
		t.Errorf("hourly len = %d", len(body.Hourly))
	}
	if rec.Header().Get("X-Cache") != "HIT" || rec.Header().Get("Age") != "30" || rec.Header().Get("Cache-Status") != "airq; hit; ttl=300" {
		t.Errorf("cache headers = %v", rec.Header())
	}

	if rec := h.do(t, http.MethodGet, "/v1/air-quality/forecast?lat=1&lon=2", nil); rec.Code != http.StatusOK {
		t.Errorf("default hours: %d", rec.Code)
	}
}

func TestValidation(t *testing.T) {
	h := newHarness(t, okService(), nil)
	tests := map[string]string{
		"/v1/air-quality/current":                                           "lat",
		"/v1/air-quality/current?lat=1":                                     "lon",
		"/v1/air-quality/current?lat=91&lon=0":                              "lat",
		"/v1/air-quality/current?lat=0&lon=-180.5":                          "lon",
		"/v1/air-quality/current?lat=NaN&lon=0":                             "lat",
		"/v1/air-quality/current?lat=Inf&lon=0":                             "lat",
		"/v1/air-quality/current?lat=1e400&lon=0":                           "lat",
		"/v1/air-quality/current?location=Berlin&lat=1&lon=2":               "location",
		"/v1/air-quality/current?location=B":                                "location",
		"/v1/air-quality/current?location=Berlin%3BDROP":                    "location",
		"/v1/air-quality/current?location=Berlin&country=DEU":               "country",
		"/v1/air-quality/Berlin%2C%20DE":                                    "location",
		"/v1/air-quality/" + strings.Repeat("a", 101):                       "location",
		"/v1/air-quality/forecast?lat=1&lon=1&hours=0":                      "hours",
		"/v1/air-quality/forecast?lat=1&lon=1&hours=97":                     "hours",
		"/v1/air-quality/forecast?lat=1&lon=1&hours=abc":                    "hours",
		"/v1/air-quality/current?lat=" + strings.Repeat("1", 40) + "&lon=0": "lat",
	}
	for target, field := range tests {
		t.Run(target, func(t *testing.T) {
			rec := h.do(t, http.MethodGet, target, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body)
			}
			p := decodeProblem(t, rec)
			if p.Code != "invalid_request" {
				t.Errorf("code = %s", p.Code)
			}
			found := false
			for _, fe := range p.Errors {
				found = found || fe.Field == field
			}
			if !found {
				t.Errorf("no error for field %q in %+v", field, p.Errors)
			}
		})
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		status     int
		code       string
		retryAfter string
	}{
		{"not found", airquality.ErrLocationNotFound, 404, "location_not_found", ""},
		{"circuit open", &upstream.Error{Kind: upstream.KindCircuitOpen, RetryAfter: 12500 * time.Millisecond}, 503, "upstream_unavailable", "13"},
		{"vendor 429", &upstream.Error{Kind: upstream.KindRateLimited}, 503, "upstream_unavailable", "1"},
		{"throttled", &upstream.Error{Kind: upstream.KindThrottled}, 503, "upstream_unavailable", "1"},
		{"timeout", &upstream.Error{Kind: upstream.KindTimeout}, 504, "upstream_timeout", ""},
		{"vendor 5xx", &upstream.Error{Kind: upstream.KindServer, StatusCode: 500}, 502, "upstream_error", ""},
		{"network", &upstream.Error{Kind: upstream.KindNetwork}, 502, "upstream_error", ""},
		{"vendor 4xx", &upstream.Error{Kind: upstream.KindClient, StatusCode: 400}, 502, "upstream_rejected", ""},
		{"deadline", context.DeadlineExceeded, 504, "timeout", ""},
		{"unknown", errors.New("boom"), 500, "internal_error", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := okService()
			svc.current = func(context.Context, airquality.Coordinates) (airquality.Result[airquality.Current], error) {
				return airquality.Result[airquality.Current]{}, tt.err
			}
			svc.resolve = func(context.Context, string, string) (airquality.Result[airquality.Location], error) {
				return airquality.Result[airquality.Location]{}, tt.err
			}
			h := newHarness(t, svc, nil)
			target := "/v1/air-quality/current?lat=1&lon=1"
			if tt.code == "location_not_found" {
				target = "/v1/air-quality/Atlantis"
			}
			rec := h.do(t, http.MethodGet, target, nil)
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d", rec.Code, tt.status)
			}
			if p := decodeProblem(t, rec); p.Code != tt.code {
				t.Errorf("code = %s, want %s", p.Code, tt.code)
			}
			if got := rec.Header().Get("Retry-After"); got != tt.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tt.retryAfter)
			}
			if strings.Contains(rec.Body.String(), "boom") {
				t.Error("internal error details must not leak to clients")
			}
		})
	}
}

func TestClientCancellationIsRecordedAs499(t *testing.T) {
	svc := okService()
	svc.current = func(context.Context, airquality.Coordinates) (airquality.Result[airquality.Current], error) {
		return airquality.Result[airquality.Current]{}, &upstream.Error{Kind: upstream.KindCanceled, Err: context.Canceled}
	}
	h := newHarness(t, svc, nil)
	if rec := h.do(t, http.MethodGet, "/v1/air-quality/current?lat=1&lon=1", nil); rec.Code != 499 {
		t.Fatalf("status = %d", rec.Code)
	}
	if n := testutil.CollectAndCount(h.reg, "airq_http_requests_total"); n == 0 {
		t.Fatal("no request metrics")
	}
}

func TestStaleResponseHeaders(t *testing.T) {
	svc := okService()
	svc.current = func(context.Context, airquality.Coordinates) (airquality.Result[airquality.Current], error) {
		return airquality.Result[airquality.Current]{Status: airquality.CacheStale, Age: 2 * time.Hour}, nil
	}
	rec := newHarness(t, svc, nil).do(t, http.MethodGet, "/v1/air-quality/current?lat=1&lon=1", nil)
	if rec.Code != http.StatusOK || rec.Header().Get("X-Cache") != "STALE" ||
		rec.Header().Get("Cache-Control") != "no-cache" || rec.Header().Get("Age") != "7200" ||
		!strings.Contains(rec.Header().Get("Cache-Status"), "stale-if-error") {
		t.Fatalf("status=%d headers=%v", rec.Code, rec.Header())
	}
}

func TestRequestDeadlineIsApplied(t *testing.T) {
	svc := okService()
	var deadline time.Time
	svc.current = func(ctx context.Context, _ airquality.Coordinates) (airquality.Result[airquality.Current], error) {
		deadline, _ = ctx.Deadline()
		return airquality.Result[airquality.Current]{}, nil
	}
	newHarness(t, svc, nil).do(t, http.MethodGet, "/v1/air-quality/current?lat=1&lon=1", nil)
	if deadline.IsZero() || time.Until(deadline) > time.Second {
		t.Errorf("handler context deadline = %v", deadline)
	}
}

func TestUnmatchedRoutesUseProblemDetails(t *testing.T) {
	h := newHarness(t, okService(), nil)
	rec := h.do(t, http.MethodGet, "/nope", nil)
	if rec.Code != http.StatusNotFound || decodeProblem(t, rec).Code != "not_found" {
		t.Fatalf("404: %d %s", rec.Code, rec.Body)
	}
	rec = h.do(t, http.MethodPost, "/v1/air-quality/current", nil)
	if rec.Code != http.StatusMethodNotAllowed || decodeProblem(t, rec).Code != "method_not_allowed" {
		t.Fatalf("405: %d %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Header().Get("Allow"), http.MethodGet) {
		t.Errorf("Allow = %q", rec.Header().Get("Allow"))
	}
	if got := testutil.ToFloat64(counterVec(t, h.reg, "airq_http_requests_total", map[string]string{"route": "unmatched", "code": "404"})); got != 1 {
		t.Errorf("unmatched 404 counter = %v", got)
	}
}

func TestHeadIsSupported(t *testing.T) {
	rec := newHarness(t, okService(), nil).do(t, http.MethodHead, "/v1/air-quality/current?lat=1&lon=1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD status = %d", rec.Code)
	}
}

func TestPanicRecovery(t *testing.T) {
	svc := okService()
	svc.current = func(context.Context, airquality.Coordinates) (airquality.Result[airquality.Current], error) {
		panic("kaboom")
	}
	h := newHarness(t, svc, nil)
	rec := h.do(t, http.MethodGet, "/v1/air-quality/current?lat=1&lon=1", nil)
	if rec.Code != http.StatusInternalServerError || decodeProblem(t, rec).Code != "internal_error" {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := testutil.ToFloat64(counterVec(t, h.reg, "airq_http_panics_recovered_total", nil)); got != 1 {
		t.Errorf("panics = %v", got)
	}
	if !strings.Contains(h.logs.String(), "kaboom") {
		t.Error("panic not logged")
	}
}

func TestRateLimitingPerClient(t *testing.T) {
	h := newHarness(t, okService(), func(o *Options) {
		o.RateLimitRPS = 0.001
		o.RateLimitBurst = 2
	})
	target := "/v1/air-quality/current?lat=1&lon=1"
	for i := range 2 {
		if rec := h.do(t, http.MethodGet, target, nil); rec.Code != http.StatusOK {
			t.Fatalf("request %d: %d", i, rec.Code)
		}
	}
	rec := h.do(t, http.MethodGet, target, nil)
	if rec.Code != http.StatusTooManyRequests || decodeProblem(t, rec).Code != "rate_limited" || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("third request: %d %v", rec.Code, rec.Header())
	}
	// Another client is unaffected.
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.RemoteAddr = "203.0.113.9:1234"
	other := httptest.NewRecorder()
	h.handler.ServeHTTP(other, req)
	if other.Code != http.StatusOK {
		t.Errorf("other client = %d", other.Code)
	}
	// Operational endpoints are exempt.
	for _, p := range []string{"/health", "/livez", "/readyz", "/metrics"} {
		if rec := h.do(t, http.MethodGet, p, nil); rec.Code != http.StatusOK {
			t.Errorf("%s while rate limited = %d", p, rec.Code)
		}
	}
	if got := testutil.ToFloat64(counterVec(t, h.reg, "airq_http_rejected_requests_total", map[string]string{"reason": "rate_limited"})); got != 1 {
		t.Errorf("rejected{rate_limited} = %v", got)
	}
}

func TestLoadShedding(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	svc := okService()
	svc.current = func(context.Context, airquality.Coordinates) (airquality.Result[airquality.Current], error) {
		entered <- struct{}{}
		<-release
		return airquality.Result[airquality.Current]{}, nil
	}
	h := newHarness(t, svc, func(o *Options) { o.MaxInFlight = 1 })

	done := make(chan int)
	go func() { done <- h.do(t, http.MethodGet, "/v1/air-quality/current?lat=1&lon=1", nil).Code }()
	<-entered

	rec := h.do(t, http.MethodGet, "/v1/air-quality/forecast?lat=1&lon=1", nil)
	if rec.Code != http.StatusServiceUnavailable || decodeProblem(t, rec).Code != "overloaded" || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("shed request: %d", rec.Code)
	}
	if rec := h.do(t, http.MethodGet, "/livez", nil); rec.Code != http.StatusOK {
		t.Errorf("probes must bypass shedding, got %d", rec.Code)
	}
	close(release)
	if code := <-done; code != http.StatusOK {
		t.Errorf("admitted request = %d", code)
	}
}

const (
	incomingTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	incomingParent  = "00-" + incomingTraceID + "-00f067aa0ba902b7-01"
)

func TestRequestID(t *testing.T) {
	h := newHarness(t, okService(), nil)
	rec := h.do(t, http.MethodGet, "/v1/air-quality/Berlin", map[string]string{"X-Request-ID": "abc-123"})
	if rec.Header().Get("X-Request-ID") != "abc-123" {
		t.Errorf("valid request id not honoured: %q", rec.Header().Get("X-Request-ID"))
	}
	rec = h.do(t, http.MethodGet, "/v1/air-quality/Berlin", map[string]string{"X-Request-ID": "bad id\nwith newline"})
	spans := h.spans.Ended()
	if got := rec.Header().Get("X-Request-ID"); got != spans[len(spans)-1].SpanContext().TraceID().String() {
		t.Errorf("an invalid request id should fall back to the trace id, got %q", got)
	}
	if rec := h.do(t, http.MethodGet, "/livez", nil); len(rec.Header().Get("X-Request-ID")) < 16 {
		t.Errorf("untraced routes still need a request id, got %q", rec.Header().Get("X-Request-ID"))
	}
}

func TestServerSpans(t *testing.T) {
	h := newHarness(t, okService(), nil)
	h.do(t, http.MethodGet, "/v1/air-quality/Berlin", nil)
	h.do(t, http.MethodGet, "/livez", nil)
	h.do(t, http.MethodGet, "/metrics", nil)

	spans := h.spans.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1 (probes and scrapes are not traced)", len(spans))
	}
	s := spans[0]
	if s.Name() != "GET /v1/air-quality/{location}" || s.SpanKind() != trace.SpanKindServer {
		t.Errorf("span = %q kind %v", s.Name(), s.SpanKind())
	}
	attrs := map[string]string{}
	for _, kv := range s.Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	if attrs["http.route"] != "/v1/air-quality/{location}" || attrs["airq.request_id"] == "" {
		t.Errorf("attributes = %v", attrs)
	}
	if !strings.Contains(h.logs.String(), `"trace_id":"`+s.SpanContext().TraceID().String()+`"`) {
		t.Error("access log does not carry the trace id")
	}
}

func TestIncomingTraceContextIsTrustedOnlyBehindProxy(t *testing.T) {
	t.Run("untrusted caller", func(t *testing.T) {
		h := newHarness(t, okService(), nil)
		rec := h.do(t, http.MethodGet, "/v1/air-quality/Berlin", map[string]string{"traceparent": incomingParent})
		s := h.spans.Ended()[0]
		if s.SpanContext().TraceID().String() == incomingTraceID || s.Parent().IsValid() {
			t.Error("a public endpoint must start a new trace instead of adopting the caller's")
		}
		if len(s.Links()) != 1 || s.Links()[0].SpanContext.TraceID().String() != incomingTraceID {
			t.Errorf("the caller's trace should be linked, links = %+v", s.Links())
		}
		if rec.Header().Get("X-Request-ID") == incomingTraceID {
			t.Error("request id must not be derived from an untrusted trace id")
		}
	})
	t.Run("trusted proxy", func(t *testing.T) {
		h := newHarness(t, okService(), func(o *Options) { o.TrustProxyHeaders = true })
		rec := h.do(t, http.MethodGet, "/v1/air-quality/Berlin", map[string]string{"traceparent": incomingParent})
		s := h.spans.Ended()[0]
		if s.SpanContext().TraceID().String() != incomingTraceID || s.Parent().SpanID().String() != "00f067aa0ba902b7" {
			t.Errorf("trace context not continued: %v parent %v", s.SpanContext().TraceID(), s.Parent().SpanID())
		}
		if rec.Header().Get("X-Request-ID") != incomingTraceID {
			t.Errorf("X-Request-ID = %q, want the propagated trace id", rec.Header().Get("X-Request-ID"))
		}
	})
}

func TestMetricsUseRoutePatternsNotPaths(t *testing.T) {
	h := newHarness(t, okService(), nil)
	for _, city := range []string{"Berlin", "Paris", "Rome"} {
		h.do(t, http.MethodGet, "/v1/air-quality/"+city, nil)
	}
	labels := map[string]string{"method": "GET", "route": "/v1/air-quality/{location}", "code": "200"}
	if got := testutil.ToFloat64(counterVec(t, h.reg, "airq_http_requests_total", labels)); got != 3 {
		t.Errorf("requests for pattern = %v, want 3", got)
	}
	body := h.do(t, http.MethodGet, "/metrics", nil).Body.String()
	if strings.Contains(body, `route="/v1/air-quality/Berlin"`) {
		t.Error("raw paths must never become metric labels")
	}
}

func TestIndexAndOperationalEndpoints(t *testing.T) {
	h := newHarness(t, okService(), nil)
	rec := h.do(t, http.MethodGet, "/", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "/v1/air-quality") {
		t.Errorf("index: %d %s", rec.Code, rec.Body)
	}
	if rec := h.do(t, http.MethodGet, "/health", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Errorf("health: %d %s", rec.Code, rec.Body)
	}
	if rec := h.do(t, http.MethodGet, "/metrics", nil); !strings.Contains(rec.Body.String(), "airq_http_requests_in_flight") {
		t.Error("metrics endpoint missing http metrics")
	}
}

func TestAccessLogRedactsUnknownQueryParameters(t *testing.T) {
	h := newHarness(t, okService(), nil)
	h.do(t, http.MethodGet, "/v1/air-quality/current?lat=1&lon=2&apikey=s3cr3t-key&token=s3cr3t-token", nil)
	logs := h.logs.String()
	if strings.Contains(logs, "s3cr3t") {
		t.Fatalf("secret leaked into logs: %s", logs)
	}
	if !strings.Contains(logs, `"query":"apikey=REDACTED&lat=1&lon=2&token=REDACTED"`) {
		t.Errorf("access log query not redacted as expected: %s", logs)
	}

	for raw, want := range map[string]string{
		"":               "",
		"location=Paris": "location=Paris",
		"key=a&key=b":    "key=REDACTED&key=REDACTED",
		"lat=1;secret=x": "[unparseable]",
		"bad=%zz&lat=1":  "[unparseable]",
	} {
		if got := redactQuery(raw); got != want {
			t.Errorf("redactQuery(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestClientIP(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "10.0.0.1:5555"
	r.Header.Set("X-Forwarded-For", "6.6.6.6, 198.51.100.7")
	if got := clientIP(r, false); got != "10.0.0.1" {
		t.Errorf("untrusted = %s", got)
	}
	if got := clientIP(r, true); got != "198.51.100.7" {
		t.Errorf("trusted = %s (must use the right-most entry)", got)
	}
	r.Header.Set("X-Forwarded-For", "garbage")
	if got := clientIP(r, true); got != "10.0.0.1" {
		t.Errorf("invalid XFF must fall back to RemoteAddr, got %s", got)
	}
}

func counterVec(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) prometheus.Collector {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	metric:
		for _, m := range mf.GetMetric() {
			have := map[string]string{}
			for _, l := range m.GetLabel() {
				have[l.GetName()] = l.GetValue()
			}
			for k, v := range labels {
				if have[k] != v {
					continue metric
				}
			}
			total += m.GetCounter().GetValue()
		}
	}
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_value", Help: "test"})
	g.Set(total)
	return g
}
