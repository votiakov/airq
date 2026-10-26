// Package upstream is a resilient JSON-over-HTTP client for third-party APIs.
//
// Every call passes through, in order:
//
//  1. a circuit breaker (sony/gobreaker) that fails fast while the vendor is known
//     to be down. It sees one outcome per logical call (not per retry), and opens on
//     consecutive failures or on a high failure ratio within a window;
//  2. a token-bucket quota guard (x/time/rate) so we never exceed the vendor's rate
//     limit;
//  3. a bulkhead (x/sync/semaphore) so a slow vendor cannot exhaust goroutines,
//     sockets or memory;
//  4. a per-attempt timeout, a response size cap and JSON decoding into typed values;
//  5. retries (cenkalti/backoff) with capped exponential backoff (randomised ±50%)
//     for transient failures, honouring a bounded Retry-After and never sleeping past the caller's
//     deadline.
//
// The libraries supply the mechanisms; this package owns the policy: which failures
// are retryable, which count against the breaker, and how each is classified.
// Each attempt is measured, logged and (optionally) handed to a Recorder for audit.
package upstream

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/sony/gobreaker/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	"github.com/votiakov/airq/internal/telemetry"
	"github.com/votiakov/airq/internal/version"
)

// apiKeyParam is the query parameter Open-Meteo uses for commercial API keys. It is
// never logged, recorded or included in errors.
const apiKeyParam = "apikey"

// minAttemptBudget is the least remaining deadline worth starting a retry with.
const minAttemptBudget = 100 * time.Millisecond

// errNotSent reports to the breaker that an attempt was abandoned before reaching the
// vendor (local throttling or caller cancellation), so it says nothing about the
// vendor's health.
var errNotSent = errors.New("request not sent")

// Exchange describes one attempt that reached the network. It is handed to the
// Recorder for persistence; Query never contains credentials.
type Exchange struct {
	Time       time.Time
	RequestID  string
	Upstream   string
	Endpoint   string
	Query      map[string]string
	Attempt    int
	StatusCode int // 0 when no response was received
	Outcome    string
	Duration   time.Duration
	Body       []byte // raw response body, nil on transport errors
	Error      string
	// Trace identifies the logical call's span so asynchronous consumers (the
	// recorder's batched writes) can link back to the request that caused them.
	Trace trace.SpanContext
}

// Recorder receives exchanges. Implementations must not block.
type Recorder interface {
	Record(Exchange)
}

// Config configures a Client.
type Config struct {
	// Name identifies the upstream in metrics, logs and errors (e.g. "air_quality").
	Name             string
	BaseURL          string
	APIKey           string
	HTTPClient       *http.Client
	AttemptTimeout   time.Duration
	MaxAttempts      int
	RetryBaseDelay   time.Duration // first backoff interval, doubled per retry
	RetryMaxDelay    time.Duration // cap on a single backoff interval
	BreakerThreshold int           // consecutive failed calls that open the circuit
	BreakerCooldown  time.Duration
	// BreakerFailureRatio opens the circuit when at least this fraction of calls in
	// BreakerWindow failed, so a vendor that fails intermittently (never five in a row)
	// is still detected. Zero disables the ratio trip. It needs BreakerMinCalls calls
	// in the window to avoid tripping on a handful of unlucky ones.
	BreakerFailureRatio float64
	BreakerMinCalls     int
	BreakerWindow       time.Duration
	RateLimit           rate.Limit
	RateBurst           int
	MaxConcurrent       int
	MaxResponseBytes    int64
	Metrics             *Metrics
	Recorder            Recorder // optional
	Logger              *slog.Logger
	Now                 func() time.Time
	TracerProvider      trace.TracerProvider // defaults to the global provider
}

// Client is safe for concurrent use.
type Client struct {
	cfg      Config
	base     *url.URL
	breaker  *gobreaker.TwoStepCircuitBreaker[struct{}]
	openedAt atomic.Int64 // UnixNano of the last transition to open, for Retry-After hints
	limiter  *rate.Limiter
	bulkhead *semaphore.Weighted
	log      *slog.Logger
	tracer   trace.Tracer
}

// New creates a client. BaseURL must be an absolute http(s) URL.
func New(cfg Config) (*Client, error) {
	base, err := url.Parse(cfg.BaseURL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("upstream %s: invalid base URL %q", cfg.Name, cfg.BaseURL)
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = NewHTTPClient(cfg.AttemptTimeout)
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}
	if cfg.BreakerThreshold < 1 {
		cfg.BreakerThreshold = 1
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Metrics == nil {
		return nil, errors.New("upstream: Metrics is required")
	}
	if cfg.TracerProvider == nil {
		cfg.TracerProvider = otel.GetTracerProvider()
	}
	cfg.Metrics.init(cfg.Name)

	c := &Client{
		cfg:      cfg,
		base:     base,
		limiter:  rate.NewLimiter(cfg.RateLimit, max(cfg.RateBurst, 1)),
		bulkhead: semaphore.NewWeighted(int64(cfg.MaxConcurrent)),
		log:      cfg.Logger.With(slog.String("upstream", cfg.Name)),
		tracer:   cfg.TracerProvider.Tracer("github.com/votiakov/airq/internal/upstream"),
	}
	threshold := uint32(min(cfg.BreakerThreshold, math.MaxUint32))       //nolint:gosec // bounded to [1, MaxUint32]
	minCalls := uint32(min(max(cfg.BreakerMinCalls, 1), math.MaxUint32)) //nolint:gosec // bounded to [1, MaxUint32]
	c.breaker = gobreaker.NewTwoStepCircuitBreaker[struct{}](gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: 1, // a single trial request while half-open
		Timeout:     cfg.BreakerCooldown,
		Interval:    cfg.BreakerWindow, // closed-state counters restart every window
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.ConsecutiveFailures >= threshold {
				return true
			}
			return cfg.BreakerFailureRatio > 0 && counts.Requests >= minCalls &&
				float64(counts.TotalFailures)/float64(counts.Requests) >= cfg.BreakerFailureRatio
		},
		IsExcluded: excludedFromBreaker,
		OnStateChange: func(_ string, from, to gobreaker.State) {
			if to == gobreaker.StateOpen {
				c.openedAt.Store(time.Now().UnixNano())
			}
			cfg.Metrics.circuitState.WithLabelValues(cfg.Name).Set(float64(to))
			cfg.Metrics.transitions.WithLabelValues(cfg.Name, to.String()).Inc()
			c.log.Warn("circuit breaker state changed", "from", from.String(), "to", to.String())
		},
	})
	return c, nil
}

// excludedFromBreaker reports failures that are not the vendor's fault: our own bad
// requests, departed callers and calls that never left the process (local quota or
// bulkhead refusals).
func excludedFromBreaker(err error) bool {
	if errors.Is(err, errNotSent) {
		return true
	}
	var uerr *Error
	return errors.As(err, &uerr) && (uerr.Kind == KindClient || uerr.Kind == KindCanceled || uerr.Kind == KindThrottled)
}

// NewHTTPClient returns an http.Client tuned for calling a single API host: bounded
// dial/TLS/header timeouts, a connection pool large enough to avoid churn, TLS 1.2+,
// and redirects disabled (an API redirect is unexpected and could leak the key to
// another host). There is deliberately no overall Client.Timeout; each attempt gets a
// context deadline instead.
func NewHTTPClient(responseHeaderTimeout time.Duration) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// Name returns the upstream name.
func (c *Client) Name() string { return c.cfg.Name }

// CircuitOpen reports whether the breaker is failing fast (used by health checks).
func (c *Client) CircuitOpen() bool { return c.breaker.State() == gobreaker.StateOpen }

// GetJSON performs GET base+path?query and decodes a 200 response into out.
//
// It is traced as one span for the logical call with a client span per attempt.
// Spans are created here rather than by otelhttp's transport, which records the full
// URL (including the API key) and would propagate our trace context to the vendor.
func (c *Client) GetJSON(ctx context.Context, path string, query url.Values, out any) error {
	ctx, span := c.tracer.Start(ctx, c.cfg.Name+" "+path, trace.WithAttributes(attribute.String("airq.upstream", c.cfg.Name)))
	defer span.End()

	err := c.getJSON(ctx, path, query, out)
	outcome := "success"
	var uerr *Error
	if errors.As(err, &uerr) {
		outcome = string(uerr.Kind)
		span.SetAttributes(semconv.ErrorTypeKey.String(outcome))
		span.SetStatus(codes.Error, uerr.Error())
	}
	c.cfg.Metrics.calls.WithLabelValues(c.cfg.Name, outcome).Inc()
	return err
}

func (c *Client) getJSON(ctx context.Context, path string, query url.Values, out any) error {
	target := c.base.JoinPath(path)
	if !strings.HasPrefix(target.Path, "/") {
		target.Path = "/" + target.Path // JoinPath on an empty base path drops the slash
	}
	q := url.Values{}
	for k, vs := range query {
		q[k] = vs
	}
	if c.cfg.APIKey != "" {
		q.Set(apiKeyParam, c.cfg.APIKey)
	}
	target.RawQuery = q.Encode()

	// The breaker sees one outcome per logical call, not one per attempt: retries are
	// our own amplification of a single failure, and counting them would let one slow
	// call trip the circuit and then be answered with "circuit open" mid-retry.
	done, berr := c.breaker.Allow() // ErrOpenState, or ErrTooManyRequests while a half-open trial runs
	if berr != nil {
		c.cfg.Metrics.attempts.WithLabelValues(c.cfg.Name, string(KindCircuitOpen)).Inc()
		return &Error{Upstream: c.cfg.Name, Kind: KindCircuitOpen, RetryAfter: c.circuitRetryAfter(), Err: berr}
	}
	var lastVendorErr *Error // the last failure that is the vendor's doing
	attempt := 0
	op := func() (struct{}, error) {
		attempt++
		err := c.attempt(ctx, attempt, target, out)
		if err != nil && !excludedFromBreaker(err) {
			lastVendorErr = err
		}
		switch {
		case err == nil:
			return struct{}{}, nil
		case !err.retryable():
			return struct{}{}, backoff.Permanent(err)
		case err.RetryAfter > 0:
			// The vendor's explicit hint replaces our own schedule.
			return struct{}{}, errors.Join(err, &backoff.RetryAfterError{Duration: err.RetryAfter})
		default:
			return struct{}{}, err
		}
	}
	_, err := backoff.Retry(ctx, op,
		backoff.WithBackOff(&backoff.ExponentialBackOff{
			InitialInterval:     c.cfg.RetryBaseDelay,
			RandomizationFactor: backoff.DefaultRandomizationFactor,
			Multiplier:          backoff.DefaultMultiplier,
			MaxInterval:         c.cfg.RetryMaxDelay,
		}),
		backoff.WithMaxTries(uint(c.cfg.MaxAttempts)), // clamped to >= 1 in New
		backoff.WithMaxElapsedTime(retryBudget(ctx)),
		backoff.WithNotify(func(error, time.Duration) { c.cfg.Metrics.retries.WithLabelValues(c.cfg.Name).Inc() }),
	)
	if err == nil {
		done(nil)
		return nil
	}
	var uerr *Error
	if !errors.As(err, &uerr) {
		uerr = c.contextError(ctx.Err()) // cancelled while backing off
	}
	// Report the vendor's last real failure to the breaker even when the call ended
	// because the caller left or the local quota ran out while retrying.
	if excludedFromBreaker(uerr) && lastVendorErr != nil {
		done(lastVendorErr)
	} else {
		done(uerr)
	}
	return uerr
}

// retryBudget bounds the retry loop so a backoff sleep only starts when an attempt
// could still finish before the caller's deadline. Zero means no deadline (MaxTries
// still applies).
func retryBudget(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return max(time.Until(deadline)-minAttemptBudget, time.Nanosecond)
}

func (c *Client) attempt(ctx context.Context, attempt int, target *url.URL, out any) *Error {
	name := c.cfg.Name

	if uerr := c.admit(ctx); uerr != nil {
		c.cfg.Metrics.attempts.WithLabelValues(name, string(uerr.Kind)).Inc()
		return uerr
	}
	defer c.release()

	start := c.cfg.Now()
	actx, span := c.tracer.Start(ctx, http.MethodGet, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		semconv.HTTPRequestMethodGet, semconv.URLFull(redactedURL(target)), semconv.ServerAddress(target.Hostname()),
	))
	if attempt > 1 {
		span.SetAttributes(semconv.HTTPRequestResendCount(attempt - 1))
	}
	status, body, uerr := c.do(actx, target, out)
	elapsed := c.cfg.Now().Sub(start)
	if status != 0 {
		span.SetAttributes(semconv.HTTPResponseStatusCode(status))
	}
	if uerr != nil {
		span.SetAttributes(semconv.ErrorTypeKey.String(string(uerr.Kind)))
		span.SetStatus(codes.Error, uerr.Error())
	}
	span.End()

	outcome := "success"
	if uerr != nil {
		outcome = string(uerr.Kind)
	}

	c.cfg.Metrics.attempts.WithLabelValues(name, outcome).Inc()
	telemetry.ObserveWithExemplar(ctx, c.cfg.Metrics.duration.WithLabelValues(name, outcome), elapsed.Seconds())

	attrs := []any{
		slog.String("endpoint", target.Path), slog.Int("attempt", attempt), slog.String("outcome", outcome),
		slog.Int("status", status), slog.Duration("duration", elapsed),
	}
	if uerr != nil {
		c.log.WarnContext(ctx, "upstream attempt failed", append(attrs, slog.String("error", uerr.Error()))...)
	} else {
		c.log.DebugContext(ctx, "upstream attempt succeeded", attrs...)
	}

	if c.cfg.Recorder != nil {
		ex := Exchange{
			Time:       start,
			RequestID:  telemetry.RequestID(ctx),
			Upstream:   name,
			Endpoint:   target.Path,
			Query:      redactedQuery(target.Query()),
			Attempt:    attempt,
			StatusCode: status,
			Outcome:    outcome,
			Duration:   elapsed,
			Body:       body,
			Trace:      trace.SpanContextFromContext(ctx),
		}
		if uerr != nil {
			ex.Error = uerr.Error()
		}
		c.cfg.Recorder.Record(ex)
	}
	return uerr
}

// admit waits for the quota guard and a bulkhead slot, bounded by ctx.
func (c *Client) admit(ctx context.Context) *Error {
	waitStart := time.Now()
	if err := c.limiter.Wait(ctx); err != nil {
		if ctx.Err() != nil {
			return c.contextError(ctx.Err())
		}
		// Wait fails immediately when the reservation would outlive the deadline.
		return &Error{Upstream: c.cfg.Name, Kind: KindThrottled, Err: errors.New("outbound rate limit exceeded")}
	}
	c.cfg.Metrics.limiterWait.WithLabelValues(c.cfg.Name).Observe(time.Since(waitStart).Seconds())

	if err := c.bulkhead.Acquire(ctx, 1); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return &Error{Upstream: c.cfg.Name, Kind: KindThrottled, Err: errors.New("bulkhead full")}
		}
		return c.contextError(err)
	}
	c.cfg.Metrics.inFlight.WithLabelValues(c.cfg.Name).Inc()
	return nil
}

func (c *Client) release() {
	c.bulkhead.Release(1)
	c.cfg.Metrics.inFlight.WithLabelValues(c.cfg.Name).Dec()
}

// circuitRetryAfter estimates when the open circuit will admit a trial request.
// gobreaker does not expose this, so the transition time is tracked in OnStateChange.
func (c *Client) circuitRetryAfter() time.Duration {
	remaining := c.cfg.BreakerCooldown - time.Since(time.Unix(0, c.openedAt.Load()))
	return max(remaining, time.Second)
}

// do performs a single HTTP exchange and returns the status code and raw body (for
// recording) alongside any classified error.
func (c *Client) do(ctx context.Context, target *url.URL, out any) (int, []byte, *Error) {
	name := c.cfg.Name
	actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
	defer cancel()

	actx = httptrace.WithClientTrace(actx, &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			c.cfg.Metrics.conns.WithLabelValues(name, strconv.FormatBool(info.Reused)).Inc()
		},
	})

	req, err := http.NewRequestWithContext(actx, http.MethodGet, target.String(), nil)
	if err != nil {
		return 0, nil, &Error{Upstream: name, Kind: KindClient, Err: redact(err, target)}
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "airq/"+version.Get().Version)

	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return 0, nil, c.transportError(ctx, err, target)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, c.cfg.MaxResponseBytes+1))
	if err != nil {
		return resp.StatusCode, nil, c.transportError(ctx, err, target)
	}
	if int64(len(body)) > c.cfg.MaxResponseBytes {
		return resp.StatusCode, nil, &Error{Upstream: name, Kind: KindBadResponse, StatusCode: resp.StatusCode,
			Err: fmt.Errorf("response exceeds %d bytes", c.cfg.MaxResponseBytes)}
	}

	switch {
	case resp.StatusCode == http.StatusOK:
		if err := json.Unmarshal(body, out); err != nil {
			return resp.StatusCode, body, &Error{Upstream: name, Kind: KindBadResponse, StatusCode: resp.StatusCode,
				Err: fmt.Errorf("decode response: %w", err)}
		}
		return resp.StatusCode, body, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		ra, _ := parseRetryAfter(resp.Header.Get("Retry-After"), c.cfg.Now())
		return resp.StatusCode, body, &Error{Upstream: name, Kind: KindRateLimited, StatusCode: resp.StatusCode, RetryAfter: ra}
	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooEarly:
		ra, _ := parseRetryAfter(resp.Header.Get("Retry-After"), c.cfg.Now())
		return resp.StatusCode, body, &Error{Upstream: name, Kind: KindServer, StatusCode: resp.StatusCode, RetryAfter: ra}
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden,
		resp.StatusCode == http.StatusNotFound, resp.StatusCode == http.StatusGone:
		// The vendor is refusing us (revoked key, exhausted plan, retired endpoint), not
		// complaining about one malformed request. Retrying cannot help, but the breaker
		// must see it, stale data may cover for it, and an alert must name it.
		return resp.StatusCode, body, &Error{Upstream: name, Kind: KindRejected, StatusCode: resp.StatusCode, Reason: vendorReason(body)}
	case resp.StatusCode >= 400:
		return resp.StatusCode, body, &Error{Upstream: name, Kind: KindClient, StatusCode: resp.StatusCode, Reason: vendorReason(body)}
	default:
		return resp.StatusCode, body, &Error{Upstream: name, Kind: KindBadResponse, StatusCode: resp.StatusCode,
			Err: errors.New("unexpected status")}
	}
}

func (c *Client) transportError(ctx context.Context, err error, target *url.URL) *Error {
	if ctx.Err() != nil { // the caller's context, not just this attempt's
		return c.contextError(ctx.Err())
	}
	kind := KindNetwork
	var nerr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &nerr) && nerr.Timeout()) {
		kind = KindTimeout
	}
	return &Error{Upstream: c.cfg.Name, Kind: kind, Err: redact(err, target)}
}

func (c *Client) contextError(err error) *Error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Upstream: c.cfg.Name, Kind: KindTimeout, Err: err}
	}
	return &Error{Upstream: c.cfg.Name, Kind: KindCanceled, Err: err}
}

// redact strips credentials from *url.Error, whose message otherwise embeds the full
// request URL including the API key.
func redact(err error, target *url.URL) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return &url.Error{Op: uerr.Op, URL: redactedURL(target), Err: uerr.Err}
	}
	return err
}

// redactedURL renders target without credentials, for errors and span attributes.
func redactedURL(target *url.URL) string {
	safe := *target
	safe.User = nil
	safe.RawQuery = encodeQuery(redactedQuery(target.Query()))
	return safe.String()
}

func redactedQuery(q url.Values) map[string]string {
	out := make(map[string]string, len(q))
	for k, v := range q {
		if k == apiKeyParam {
			continue
		}
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

func encodeQuery(m map[string]string) string {
	q := url.Values{}
	for k, v := range m {
		q.Set(k, v)
	}
	return q.Encode()
}

// vendorReason extracts Open-Meteo's {"error":true,"reason":"..."} message.
func vendorReason(body []byte) string {
	var v struct {
		Reason string `json:"reason"`
	}
	if json.Unmarshal(body, &v) != nil {
		return ""
	}
	const maxLen = 200
	if len(v.Reason) > maxLen {
		return strings.ToValidUTF8(v.Reason[:maxLen], "")
	}
	return v.Reason
}
