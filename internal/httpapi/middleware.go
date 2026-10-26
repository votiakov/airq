package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	"github.com/votiakov/airq/internal/cache"
	"github.com/votiakov/airq/internal/telemetry"
)

// responseRecorder captures the status code and size for metrics and access logs.
type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (rr *responseRecorder) WriteHeader(code int) {
	if rr.status == 0 && code >= 200 {
		rr.status = code
	} else if rr.status != 0 {
		return // superfluous call; avoid net/http's log noise
	}
	rr.ResponseWriter.WriteHeader(code)
}

func (rr *responseRecorder) Write(b []byte) (int, error) {
	if rr.status == 0 {
		rr.WriteHeader(http.StatusOK)
	}
	n, err := rr.ResponseWriter.Write(b)
	rr.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (rr *responseRecorder) Unwrap() http.ResponseWriter { return rr.ResponseWriter }

func (rr *responseRecorder) headerWritten() bool { return rr.status != 0 }

// requestContext assigns the request ID carried by every log line and audit row. A
// caller-supplied X-Request-ID is honoured only if it is short and uses a safe
// alphabet (it ends up in logs and the database); otherwise the trace ID is used, so
// one identifier finds the request in logs, traces and the database.
func requestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		span := trace.SpanFromContext(r.Context())
		if !validRequestID(id) {
			if sc := span.SpanContext(); sc.HasTraceID() {
				id = sc.TraceID().String()
			} else {
				id = rand.Text()
			}
		}
		span.SetAttributes(attribute.String("airq.request_id", id))
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(telemetry.WithRequestID(r.Context(), id)))
	})
}

func validRequestID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	return true
}

// quietRoutes are logged at debug level: probes and scrapes would drown real traffic.
var quietRoutes = map[string]bool{"/livez": true, "/readyz": true, "/metrics": true}

// observe records RED metrics and an access log line per request. The route label is
// the matched ServeMux pattern (never the raw path) so label cardinality stays bounded
// no matter what URLs clients send.
func observe(mux *http.ServeMux, m *metrics, log *slog.Logger, trustProxy bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		_, pattern := mux.Handler(r)
		route := routeLabel(pattern)
		if pattern != "" {
			// otelhttp runs before routing; name the span after the route template.
			span := trace.SpanFromContext(r.Context())
			span.SetName(r.Method + " " + route)
			span.SetAttributes(semconv.HTTPRoute(route))
		}

		rec := &responseRecorder{ResponseWriter: w}
		var ww http.ResponseWriter = rec
		if pattern == "" {
			ww = &problemRewriter{responseRecorder: rec, r: r}
		}

		m.inFlight.Inc()
		defer m.inFlight.Dec()
		next.ServeHTTP(ww, r)

		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		elapsed := time.Since(start)
		method := methodLabel(r.Method)
		code := strconv.Itoa(status)
		m.requests.WithLabelValues(method, route, code).Inc()
		telemetry.ObserveWithExemplar(r.Context(), m.duration.WithLabelValues(method, route, code), elapsed.Seconds())

		level := slog.LevelInfo
		switch {
		case status >= 500 && status != 503:
			level = slog.LevelError
		case quietRoutes[route] && status < 400:
			level = slog.LevelDebug
		}
		log.LogAttrs(r.Context(), level, "http request",
			slog.String("method", r.Method),
			slog.String("route", route),
			slog.String("path", truncate(r.URL.Path, 256)),
			slog.String("query", truncate(r.URL.RawQuery, 256)),
			slog.Int("status", status),
			slog.Float64("duration_ms", float64(elapsed.Microseconds())/1000),
			slog.Int64("bytes", rec.bytes),
			slog.String("client_ip", clientIP(r, trustProxy)),
			slog.String("user_agent", truncate(r.UserAgent(), 128)),
			slog.String("cache", w.Header().Get("X-Cache")),
		)
	})
}

// routeLabel strips the method from a ServeMux pattern ("GET /x" -> "/x").
func routeLabel(pattern string) string {
	if pattern == "" {
		return "unmatched"
	}
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		return pattern[i+1:]
	}
	return pattern
}

// methodLabel bounds the method label to known verbs.
func methodLabel(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions:
		return m
	default:
		return "OTHER"
	}
}

// problemRewriter turns ServeMux's plain-text 404/405 responses into problem+json so
// every error the API emits has the same shape.
type problemRewriter struct {
	*responseRecorder
	r        *http.Request
	replaced bool
}

func (p *problemRewriter) WriteHeader(code int) {
	if code != http.StatusNotFound && code != http.StatusMethodNotAllowed {
		p.responseRecorder.WriteHeader(code)
		return
	}
	p.replaced = true
	detail, errCode := "No route matches "+p.r.URL.Path+".", "not_found"
	if code == http.StatusMethodNotAllowed {
		detail, errCode = "Method "+p.r.Method+" is not allowed; see the Allow header.", "method_not_allowed"
	}
	body, _ := json.Marshal(newProblem(p.r, code, errCode, detail, nil))
	p.Header().Set("Content-Type", problemContentType)
	p.Header().Del("Content-Length")
	p.responseRecorder.WriteHeader(code)
	_, _ = p.responseRecorder.Write(append(body, '\n'))
}

func (p *problemRewriter) Write(b []byte) (int, error) {
	if p.replaced {
		return len(b), nil // swallow the original text body
	}
	return p.responseRecorder.Write(b)
}

// recoverer converts handler panics into 500 responses instead of dropped connections.
func recoverer(m *metrics, log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if v == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity, as net/http does
				panic(v)
			}
			m.panics.Inc()
			log.ErrorContext(r.Context(), "panic recovered", slog.String("panic", fmt.Sprint(v)), slog.String("stack", string(debug.Stack())))
			if hw, ok := w.(interface{ headerWritten() bool }); ok && hw.headerWritten() {
				return
			}
			writeProblem(w, r, http.StatusInternalServerError, "internal_error", "An unexpected error occurred.")
		}()
		next.ServeHTTP(w, r)
	})
}

// securityHeaders sets defensive headers appropriate for a JSON API. HSTS is left to
// the TLS-terminating ingress.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

// loadShedder caps concurrent API requests. Beyond the cap it rejects immediately
// with 503 rather than queueing, keeping latency bounded for admitted requests and
// letting the load balancer retry elsewhere.
type loadShedder struct {
	sem *semaphore.Weighted
	m   *metrics
}

func newLoadShedder(maxInFlight int, m *metrics) *loadShedder {
	return &loadShedder{sem: semaphore.NewWeighted(int64(maxInFlight)), m: m}
}

func (s *loadShedder) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.sem.TryAcquire(1) {
			s.m.rejected.WithLabelValues("overloaded").Inc()
			w.Header().Set("Retry-After", "1")
			writeProblem(w, r, http.StatusServiceUnavailable, "overloaded", "The service is at capacity; retry shortly.")
			return
		}
		defer s.sem.Release(1)
		next.ServeHTTP(w, r)
	})
}

// clientLimiter applies a token bucket per client IP. Buckets live in a bounded LRU
// so a flood of distinct addresses cannot grow memory without limit.
type clientLimiter struct {
	limit      rate.Limit
	burst      int
	trustProxy bool
	m          *metrics

	mu      sync.Mutex
	buckets *cache.Cache[string, *rate.Limiter]
}

const (
	maxTrackedClients = 10_000
	clientIdleTTL     = 10 * time.Minute
)

func newClientLimiter(rps float64, burst int, trustProxy bool, m *metrics) *clientLimiter {
	return &clientLimiter{
		limit: rate.Limit(rps), burst: burst, trustProxy: trustProxy, m: m,
		buckets: cache.New[string, *rate.Limiter](cache.Options{MaxEntries: maxTrackedClients}),
	}
}

func (l *clientLimiter) bucket(ip string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim := rate.NewLimiter(l.limit, l.burst)
	if e, ok := l.buckets.Get(ip); ok {
		lim = e.Value
	}
	l.buckets.Set(ip, lim, clientIdleTTL, 0) // refresh idle expiry
	return lim
}

func (l *clientLimiter) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := l.bucket(clientIP(r, l.trustProxy)).Reserve()
		if d := res.Delay(); d > 0 {
			res.Cancel()
			l.m.rejected.WithLabelValues("rate_limited").Inc()
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(d.Seconds()))))
			writeProblem(w, r, http.StatusTooManyRequests, "rate_limited", "Too many requests from this client; slow down.")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientIP returns the caller's address. X-Forwarded-For is trusted only when the
// service is known to sit behind exactly one reverse proxy, in which case the
// right-most entry (appended by that proxy) is the only one a client cannot forge.
func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			if ip := strings.TrimSpace(parts[len(parts)-1]); net.ParseIP(ip) != nil {
				return ip
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// withTimeout bounds the whole request (including retries and stale fallback).
func withTimeout(d time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}
