// Package upstream is a resilient JSON-over-HTTP client for third-party APIs.
//
// Every call passes through, in order:
//
//  1. a circuit breaker that fails fast while the vendor is known to be down;
//  2. a token-bucket quota guard so we never exceed the vendor's rate limit;
//  3. a bulkhead (bounded concurrency) so a slow vendor cannot exhaust goroutines,
//     sockets or memory;
//  4. a per-attempt timeout, a response size cap and strict JSON decoding;
//  5. retries with capped, fully-jittered exponential backoff for transient failures,
//     honouring Retry-After and never sleeping past the caller's deadline.
//
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
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/votiakov/airq/internal/resilience"
	"github.com/votiakov/airq/internal/telemetry"
	"github.com/votiakov/airq/internal/version"
)

// apiKeyParam is the query parameter Open-Meteo uses for commercial API keys. It is
// never logged, recorded or included in errors.
const apiKeyParam = "apikey"

// minAttemptBudget is the least remaining deadline worth starting a retry with.
const minAttemptBudget = 100 * time.Millisecond

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
	Backoff          resilience.Backoff
	BreakerThreshold int
	BreakerCooldown  time.Duration
	RateLimit        rate.Limit
	RateBurst        int
	MaxConcurrent    int
	MaxResponseBytes int64
	Metrics          *Metrics
	Recorder         Recorder // optional
	Logger           *slog.Logger
	Now              func() time.Time
}

// Client is safe for concurrent use.
type Client struct {
	cfg     Config
	base    *url.URL
	breaker *resilience.Breaker
	limiter *rate.Limiter
	sem     chan struct{}
	log     *slog.Logger
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
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Metrics == nil {
		return nil, errors.New("upstream: Metrics is required")
	}
	cfg.Metrics.init(cfg.Name)

	c := &Client{
		cfg:     cfg,
		base:    base,
		limiter: rate.NewLimiter(cfg.RateLimit, max(cfg.RateBurst, 1)),
		sem:     make(chan struct{}, cfg.MaxConcurrent),
		log:     cfg.Logger.With(slog.String("upstream", cfg.Name)),
	}
	c.breaker = resilience.NewBreaker(resilience.BreakerConfig{
		FailureThreshold: cfg.BreakerThreshold,
		Cooldown:         cfg.BreakerCooldown,
		Now:              cfg.Now,
		OnStateChange: func(from, to resilience.State) {
			cfg.Metrics.circuitState.WithLabelValues(cfg.Name).Set(float64(to))
			cfg.Metrics.transitions.WithLabelValues(cfg.Name, to.String()).Inc()
			c.log.Warn("circuit breaker state changed", "from", from.String(), "to", to.String())
		},
	})
	return c, nil
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

// BreakerState returns the circuit breaker state (used by health checks).
func (c *Client) BreakerState() resilience.State { return c.breaker.State() }

// GetJSON performs GET base+path?query and decodes a 200 response into out.
func (c *Client) GetJSON(ctx context.Context, path string, query url.Values, out any) error {
	err := c.getJSON(ctx, path, query, out)
	outcome := "success"
	var uerr *Error
	if errors.As(err, &uerr) {
		outcome = string(uerr.Kind)
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

	var lastErr *Error
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		if attempt > 1 {
			delay := max(c.cfg.Backoff.Delay(attempt-2), lastErr.RetryAfter)
			if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < delay+minAttemptBudget {
				c.log.DebugContext(ctx, "not retrying: insufficient deadline budget", "attempt", attempt, "delay", delay)
				break
			}
			c.cfg.Metrics.retries.WithLabelValues(c.cfg.Name).Inc()
			if err := resilience.Sleep(ctx, delay); err != nil {
				return c.contextError(err)
			}
		}
		err := c.attempt(ctx, attempt, target, out)
		if err == nil {
			return nil
		}
		lastErr = err
		if !err.retryable() {
			break
		}
	}
	return lastErr
}

func (c *Client) attempt(ctx context.Context, attempt int, target *url.URL, out any) *Error {
	name := c.cfg.Name
	done, err := c.breaker.Allow()
	if err != nil {
		c.cfg.Metrics.attempts.WithLabelValues(name, string(KindCircuitOpen)).Inc()
		return &Error{Upstream: name, Kind: KindCircuitOpen, RetryAfter: c.breaker.RetryAfter(), Err: err}
	}

	if uerr := c.admit(ctx); uerr != nil {
		done(resilience.Ignored)
		c.cfg.Metrics.attempts.WithLabelValues(name, string(uerr.Kind)).Inc()
		return uerr
	}
	defer c.release()

	start := c.cfg.Now()
	status, body, uerr := c.do(ctx, target, out)
	elapsed := c.cfg.Now().Sub(start)

	outcome := "success"
	brOutcome := resilience.Success
	if uerr != nil {
		outcome = string(uerr.Kind)
		brOutcome = resilience.Failure
		if uerr.Kind == KindClient || uerr.Kind == KindCanceled {
			brOutcome = resilience.Ignored // not the vendor's fault
		}
	}
	done(brOutcome)

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

	select {
	case c.sem <- struct{}{}:
		c.cfg.Metrics.inFlight.WithLabelValues(c.cfg.Name).Inc()
		return nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return &Error{Upstream: c.cfg.Name, Kind: KindThrottled, Err: errors.New("bulkhead full")}
		}
		return c.contextError(ctx.Err())
	}
}

func (c *Client) release() {
	<-c.sem
	c.cfg.Metrics.inFlight.WithLabelValues(c.cfg.Name).Dec()
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
		ra, _ := resilience.ParseRetryAfter(resp.Header.Get("Retry-After"), c.cfg.Now())
		return resp.StatusCode, body, &Error{Upstream: name, Kind: KindRateLimited, StatusCode: resp.StatusCode, RetryAfter: ra}
	case resp.StatusCode >= 500:
		ra, _ := resilience.ParseRetryAfter(resp.Header.Get("Retry-After"), c.cfg.Now())
		return resp.StatusCode, body, &Error{Upstream: name, Kind: KindServer, StatusCode: resp.StatusCode, RetryAfter: ra}
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
		safe := *target
		safe.RawQuery = encodeQuery(redactedQuery(target.Query()))
		return &url.Error{Op: uerr.Op, URL: safe.String(), Err: uerr.Err}
	}
	return err
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
