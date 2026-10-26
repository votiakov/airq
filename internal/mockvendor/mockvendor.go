// Package mockvendor emulates the subset of the Open-Meteo Air Quality and Geocoding
// APIs used by airq, with runtime fault injection. It exists for end-to-end tests,
// CI smoke tests and chaos drills (watch the circuit breaker open and stale data being
// served) without depending on, or spending quota against, the real vendor.
//
// Faults are changed at runtime:
//
//	curl -X POST 'localhost:8090/__faults?failure_rate=1&status=503'
//	curl -X POST 'localhost:8090/__faults?latency=4s'
//	curl -X DELETE localhost:8090/__faults
package mockvendor

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Faults configures injected failures.
type Faults struct {
	FailureRate float64       `json:"failure_rate"` // 0..1 probability of returning Status
	Status      int           `json:"status"`       // HTTP status for injected failures
	Latency     time.Duration `json:"latency_ns"`   // added to every vendor response
}

// Server is the mock vendor.
type Server struct {
	mu     sync.Mutex
	faults Faults
	calls  map[string]int
	now    func() time.Time
}

// New returns a healthy mock vendor.
func New() *Server {
	return &Server{faults: Faults{Status: http.StatusServiceUnavailable}, calls: map[string]int{}, now: time.Now}
}

// SetFaults replaces the injected faults.
func (s *Server) SetFaults(f Faults) {
	if f.Status == 0 {
		f.Status = http.StatusServiceUnavailable
	}
	s.mu.Lock()
	s.faults = f
	s.mu.Unlock()
}

// Calls returns how many vendor requests were received per path.
func (s *Server) Calls(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[path]
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/air-quality", s.vendor(s.airQuality))
	mux.HandleFunc("GET /v1/search", s.vendor(s.search))
	mux.HandleFunc("GET /__faults", s.getFaults)
	mux.HandleFunc("POST /__faults", s.postFaults)
	mux.HandleFunc("DELETE /__faults", func(w http.ResponseWriter, _ *http.Request) {
		s.SetFaults(Faults{})
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	return mux
}

func (s *Server) vendor(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		f := s.faults
		s.calls[r.URL.Path]++
		s.mu.Unlock()

		if f.Latency > 0 {
			select {
			case <-time.After(f.Latency):
			case <-r.Context().Done():
				return
			}
		}
		if f.FailureRate > 0 && rand.Float64() < f.FailureRate { //nolint:gosec // fault injection, not security
			w.Header().Set("Content-Type", "application/json")
			if f.Status == http.StatusTooManyRequests {
				w.Header().Set("Retry-After", "1")
			}
			w.WriteHeader(f.Status)
			_, _ = fmt.Fprintf(w, `{"error":true,"reason":"injected fault (HTTP %d)"}`, f.Status)
			return
		}
		next(w, r)
	}
}

var variables = []string{
	"european_aqi", "us_aqi", "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
	"sulphur_dioxide", "ozone", "ammonia", "dust", "uv_index",
}

func (s *Server) airQuality(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	lat, err1 := strconv.ParseFloat(q.Get("latitude"), 64)
	lon, err2 := strconv.ParseFloat(q.Get("longitude"), 64)
	if err1 != nil || err2 != nil || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		vendorError(w, "Latitude must be in range of -90 to 90°.")
		return
	}
	hour := s.now().UTC().Truncate(time.Hour)
	body := map[string]any{
		"latitude": math.Round(lat*10) / 10, "longitude": math.Round(lon*10) / 10,
		"generationtime_ms": 0.1, "utc_offset_seconds": 0, "timezone": "GMT",
		"timezone_abbreviation": "GMT", "elevation": 42.0,
	}
	units := map[string]string{"time": "unixtime"}
	for _, v := range variables {
		units[v] = unitFor(v)
	}

	if q.Get("current") != "" {
		point := map[string]any{"time": hour.Unix(), "interval": 3600}
		for _, v := range variables {
			point[v] = value(v, lat, lon, hour)
		}
		body["current_units"], body["current"] = units, point
	}
	if q.Get("hourly") != "" {
		hours, _ := strconv.Atoi(q.Get("forecast_hours"))
		hours = min(max(hours, 1), 168)
		series := map[string]any{}
		times := make([]int64, hours)
		for i := range hours {
			times[i] = hour.Add(time.Duration(i) * time.Hour).Unix()
		}
		series["time"] = times
		for _, v := range variables {
			col := make([]any, hours)
			for i := range hours {
				col[i] = value(v, lat, lon, hour.Add(time.Duration(i)*time.Hour))
			}
			series[v] = col
		}
		body["hourly_units"], body["hourly"] = units, series
	}
	writeJSON(w, body)
}

// value is deterministic for (variable, location, hour) so tests are stable while
// the data still varies realistically across places and time.
func value(variable string, lat, lon float64, t time.Time) any {
	h := fnv.New32a()
	_, _ = fmt.Fprintf(h, "%s|%.2f|%.2f|%d", variable, lat, lon, t.Unix())
	x := float64(h.Sum32()%1000) / 1000
	switch variable {
	case "european_aqi":
		return math.Round(5 + x*70)
	case "us_aqi":
		return math.Round(5 + x*120)
	case "ammonia":
		if lat < 35 || lat > 72 || lon < -25 || lon > 45 {
			return nil // like the real API, only available in the European domain
		}
		return round1(x * 10)
	case "carbon_monoxide":
		return round1(100 + x*300)
	case "uv_index":
		return round1(x * 8)
	default:
		return round1(x * 60)
	}
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }

func unitFor(v string) string {
	switch v {
	case "european_aqi":
		return "EAQI"
	case "us_aqi":
		return "USAQI"
	case "uv_index":
		return ""
	default:
		return "μg/m³"
	}
}

type place struct {
	Name, Country, Code, Admin1, TZ string
	Lat, Lon                        float64
	Population                      int
}

var places = []place{
	{"Berlin", "Germany", "DE", "State of Berlin", "Europe/Berlin", 52.52437, 13.41053, 3426354},
	{"London", "United Kingdom", "GB", "England", "Europe/London", 51.50853, -0.12574, 8961989},
	{"Paris", "France", "FR", "Île-de-France", "Europe/Paris", 48.85341, 2.3488, 2138551},
	{"Paris", "United States", "US", "Texas", "America/Chicago", 33.66094, -95.55551, 24782},
	{"New York", "United States", "US", "New York", "America/New_York", 40.71427, -74.00597, 8804190},
	{"Tokyo", "Japan", "JP", "Tokyo", "Asia/Tokyo", 35.6895, 139.69171, 9733276},
	{"Delhi", "India", "IN", "Delhi", "Asia/Kolkata", 28.65195, 77.23149, 11034555},
	{"São Paulo", "Brazil", "BR", "São Paulo", "America/Sao_Paulo", -23.5475, -46.63611, 10021295},
	{"Sydney", "Australia", "AU", "New South Wales", "Australia/Sydney", -33.86785, 151.20732, 4627345},
	{"Kyiv", "Ukraine", "UA", "Kyiv City", "Europe/Kyiv", 50.45466, 30.5238, 2797553},
}

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := strings.ToLower(strings.TrimSpace(q.Get("name")))
	country := strings.ToUpper(q.Get("countryCode"))
	var results []map[string]any
	for i, p := range places {
		if strings.ToLower(p.Name) != name || (country != "" && p.Code != country) {
			continue
		}
		results = append(results, map[string]any{
			"id": 1000 + i, "name": p.Name, "latitude": p.Lat, "longitude": p.Lon,
			"country_code": p.Code, "country": p.Country, "admin1": p.Admin1,
			"timezone": p.TZ, "population": p.Population,
		})
	}
	body := map[string]any{"generationtime_ms": 0.1}
	if len(results) > 0 {
		body["results"] = results[:1]
	}
	writeJSON(w, body)
}

func (s *Server) getFaults(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	f := s.faults
	s.mu.Unlock()
	writeJSON(w, f)
}

func (s *Server) postFaults(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	s.mu.Lock()
	f := s.faults
	s.mu.Unlock()
	if v := q.Get("failure_rate"); v != "" {
		rate, err := strconv.ParseFloat(v, 64)
		if err != nil || rate < 0 || rate > 1 {
			http.Error(w, "failure_rate must be between 0 and 1", http.StatusBadRequest)
			return
		}
		f.FailureRate = rate
	}
	if v := q.Get("status"); v != "" {
		code, err := strconv.Atoi(v)
		if err != nil || code < 400 || code > 599 {
			http.Error(w, "status must be 4xx or 5xx", http.StatusBadRequest)
			return
		}
		f.Status = code
	}
	if v := q.Get("latency"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 || d > time.Minute {
			http.Error(w, "latency must be a duration up to 1m", http.StatusBadRequest)
			return
		}
		f.Latency = d
	}
	s.SetFaults(f)
	s.getFaults(w, r)
}

func vendorError(w http.ResponseWriter, reason string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": true, "reason": reason})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}
