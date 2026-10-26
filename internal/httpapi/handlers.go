package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/votiakov/airq/internal/airquality"
	"github.com/votiakov/airq/internal/upstream"
)

// Service is the domain behaviour the API needs (implemented by airquality.Service).
type Service interface {
	Current(ctx context.Context, c airquality.Coordinates) (airquality.Result[airquality.Current], error)
	Forecast(ctx context.Context, c airquality.Coordinates, hours int) (airquality.Result[airquality.Forecast], error)
	ResolveLocation(ctx context.Context, name, countryCode string) (airquality.Result[airquality.Location], error)
}

const defaultForecastHours = 24

type api struct {
	svc         Service
	attribution string
	log         *slog.Logger
}

type currentResponse struct {
	Location *airquality.Location `json:"location,omitempty"`
	airquality.Current
	Attribution string `json:"attribution"`
}

type forecastResponse struct {
	Location *airquality.Location `json:"location,omitempty"`
	airquality.Forecast
	Attribution string `json:"attribution"`
}

// current serves GET /v1/air-quality/current?lat=&lon= or ?location=&country=.
func (a *api) current(w http.ResponseWriter, r *http.Request) {
	t, errs := parseTarget(r.URL.Query())
	if len(errs) > 0 {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request", "One or more query parameters are invalid.", errs...)
		return
	}
	a.serveCurrent(w, r, t)
}

// currentByLocation serves GET /v1/air-quality/{location}?country=.
func (a *api) currentByLocation(w http.ResponseWriter, r *http.Request) {
	q := url.Values{"location": {r.PathValue("location")}}
	if c := r.URL.Query().Get("country"); c != "" {
		q.Set("country", c)
	}
	t, errs := parseTarget(q)
	if len(errs) > 0 {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request", "The location is invalid.", errs...)
		return
	}
	a.serveCurrent(w, r, t)
}

func (a *api) serveCurrent(w http.ResponseWriter, r *http.Request, t target) {
	loc, coords, err := a.resolve(r.Context(), t)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	res, err := a.svc.Current(r.Context(), coords)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeResult(w, r, currentResponse{Location: loc, Current: res.Value, Attribution: a.attribution}, res.Status, res.Age, res.TTL)
}

// forecast serves GET /v1/air-quality/forecast?lat=&lon=&hours= (or location=).
func (a *api) forecast(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	t, errs := parseTarget(q)
	hours := defaultForecastHours
	if h := q.Get("hours"); h != "" {
		n, err := strconv.Atoi(h)
		if err != nil || n < 1 || n > airquality.MaxForecastHours {
			errs = append(errs, FieldError{"hours", fmt.Sprintf("must be an integer between 1 and %d", airquality.MaxForecastHours)})
		}
		hours = n
	}
	if len(errs) > 0 {
		writeProblem(w, r, http.StatusBadRequest, "invalid_request", "One or more query parameters are invalid.", errs...)
		return
	}
	loc, coords, err := a.resolve(r.Context(), t)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	res, err := a.svc.Forecast(r.Context(), coords, hours)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeResult(w, r, forecastResponse{Location: loc, Forecast: res.Value, Attribution: a.attribution}, res.Status, res.Age, res.TTL)
}

func (a *api) resolve(ctx context.Context, t target) (*airquality.Location, airquality.Coordinates, error) {
	if !t.byName {
		return nil, t.coords, nil
	}
	res, err := a.svc.ResolveLocation(ctx, t.name, t.country)
	if err != nil {
		return nil, airquality.Coordinates{}, err
	}
	return &res.Value, res.Value.Coordinates, nil
}

// fail maps domain and upstream errors to HTTP. Vendor details are logged, never
// returned: clients get a stable code and a generic message.
func (a *api) fail(w http.ResponseWriter, r *http.Request, err error) {
	var uerr *upstream.Error
	switch {
	case errors.Is(err, airquality.ErrLocationNotFound):
		writeProblem(w, r, http.StatusNotFound, "location_not_found", "No location matches the given name.")
	case errors.As(err, &uerr):
		a.failUpstream(w, r, uerr)
	case errors.Is(err, context.Canceled):
		clientGone(w)
	case errors.Is(err, context.DeadlineExceeded):
		writeProblem(w, r, http.StatusGatewayTimeout, "timeout", "The request took too long to complete.")
	default:
		a.log.ErrorContext(r.Context(), "unhandled error", slog.String("error", err.Error()))
		writeProblem(w, r, http.StatusInternalServerError, "internal_error", "An unexpected error occurred.")
	}
}

func (a *api) failUpstream(w http.ResponseWriter, r *http.Request, uerr *upstream.Error) {
	switch uerr.Kind {
	case upstream.KindCanceled:
		clientGone(w)
	case upstream.KindCircuitOpen, upstream.KindThrottled, upstream.KindRateLimited:
		retry := max(uerr.RetryAfter, time.Second)
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retry.Seconds()))))
		writeProblem(w, r, http.StatusServiceUnavailable, "upstream_unavailable",
			"The air quality provider is temporarily unavailable and no cached data exists for this location.")
	case upstream.KindTimeout:
		writeProblem(w, r, http.StatusGatewayTimeout, "upstream_timeout", "The air quality provider did not respond in time.")
	case upstream.KindClient:
		// Our validation should make this impossible; if it happens it is our bug.
		a.log.ErrorContext(r.Context(), "provider rejected a request that passed validation",
			slog.String("error", uerr.Error()))
		writeProblem(w, r, http.StatusBadGateway, "upstream_rejected", "The air quality provider rejected the request.")
	case upstream.KindNetwork, upstream.KindServer, upstream.KindBadResponse:
		writeProblem(w, r, http.StatusBadGateway, "upstream_error", "The air quality provider returned an error.")
	default:
		writeProblem(w, r, http.StatusBadGateway, "upstream_error", "The air quality provider returned an error.")
	}
}

// clientGone records 499 (nginx's "client closed request") for metrics and logs; the
// connection is already gone so nothing is actually delivered.
func clientGone(w http.ResponseWriter) { w.WriteHeader(499) }

// writeResult writes a JSON body with HTTP caching metadata: Cache-Control derived
// from the remaining TTL, Age, a strong ETag (with If-None-Match → 304), and
// X-Cache plus RFC 9211 Cache-Status describing how the response was produced.
func writeResult(w http.ResponseWriter, r *http.Request, body any, status airquality.CacheStatus, age, ttl time.Duration) {
	buf, err := json.Marshal(body)
	if err != nil {
		writeProblem(w, r, http.StatusInternalServerError, "internal_error", "Failed to encode the response.")
		return
	}
	buf = append(buf, '\n')
	hash := fnv.New64a()
	_, _ = hash.Write(buf)
	etag := fmt.Sprintf(`"%x"`, hash.Sum64())

	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("ETag", etag)
	h.Set("Age", strconv.Itoa(int(age.Seconds())))
	h.Set("X-Cache", strings.ToUpper(string(status)))
	switch status {
	case airquality.CacheStale:
		h.Set("Cache-Control", "no-cache")
		h.Set("Cache-Status", "airq; hit; detail=stale-if-error")
	case airquality.CacheMiss:
		h.Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(ttl.Seconds())))
		h.Set("Cache-Status", fmt.Sprintf("airq; fwd=miss; stored; ttl=%d", int(ttl.Seconds())))
	default:
		h.Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(ttl.Seconds())))
		h.Set("Cache-Status", fmt.Sprintf("airq; hit; ttl=%d", int(ttl.Seconds())))
	}

	if etagMatches(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf)
}

func etagMatches(header, etag string) bool {
	for candidate := range strings.SplitSeq(header, ",") {
		c := strings.TrimPrefix(strings.TrimSpace(candidate), "W/")
		if c == etag || c == "*" {
			return true
		}
	}
	return false
}

// target is a validated query subject: either coordinates or a place name.
type target struct {
	coords  airquality.Coordinates
	name    string
	country string
	byName  bool
}

const maxLocationRunes = 100

func parseTarget(q url.Values) (target, []FieldError) {
	var errs []FieldError
	name := strings.TrimSpace(q.Get("location"))
	latS, lonS := strings.TrimSpace(q.Get("lat")), strings.TrimSpace(q.Get("lon"))

	if name != "" {
		if latS != "" || lonS != "" {
			errs = append(errs, FieldError{"location", "provide either location or lat/lon, not both"})
		}
		if msg := validateLocation(name); msg != "" {
			errs = append(errs, FieldError{"location", msg})
		}
		country := strings.ToUpper(strings.TrimSpace(q.Get("country")))
		if country != "" && !validCountry(country) {
			errs = append(errs, FieldError{"country", "must be an ISO 3166-1 alpha-2 code such as DE or US"})
		}
		return target{name: name, country: country, byName: true}, errs
	}

	if latS == "" && lonS == "" {
		return target{}, []FieldError{{"lat", "lat and lon (or location) are required"}}
	}
	lat, msg := parseCoordinate(latS, 90)
	if msg != "" {
		errs = append(errs, FieldError{"lat", msg})
	}
	lon, msg := parseCoordinate(lonS, 180)
	if msg != "" {
		errs = append(errs, FieldError{"lon", msg})
	}
	return target{coords: airquality.Coordinates{Latitude: lat, Longitude: lon}}, errs
}

func parseCoordinate(s string, limit float64) (float64, string) {
	if s == "" {
		return 0, "is required"
	}
	if len(s) > 32 {
		return 0, "is too long"
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, "must be a decimal number"
	}
	if v < -limit || v > limit {
		return 0, fmt.Sprintf("must be between %g and %g", -limit, limit)
	}
	return v, ""
}

// validateLocation allows letters (any script), combining marks, digits, spaces and
// the punctuation found in real place names. Everything else is rejected before it
// can reach the vendor, the cache key space or the logs.
func validateLocation(s string) string {
	n := utf8.RuneCountInString(s)
	if n < 2 || n > maxLocationRunes {
		return fmt.Sprintf("must be between 2 and %d characters", maxLocationRunes)
	}
	for _, r := range s {
		switch {
		case unicode.IsLetter(r), unicode.IsMark(r), unicode.IsDigit(r):
		case r == ' ', r == '-', r == '\'', r == '’', r == '.':
		default:
			return fmt.Sprintf("contains an unsupported character %q; use the country parameter instead of commas", r)
		}
	}
	return ""
}

func validCountry(s string) bool {
	return len(s) == 2 && s[0] >= 'A' && s[0] <= 'Z' && s[1] >= 'A' && s[1] <= 'Z'
}
