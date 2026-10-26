package config

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(nil))
	if err != nil {
		t.Fatalf("Load with empty env: %v", err)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr = %q, want :8080", cfg.HTTP.Addr)
	}
	if cfg.Log.Level != slog.LevelInfo || cfg.Log.Format != "json" {
		t.Errorf("unexpected log defaults: %+v", cfg.Log)
	}
	if cfg.Upstream.AirQualityURL != "https://air-quality-api.open-meteo.com" {
		t.Errorf("unexpected air quality URL %q", cfg.Upstream.AirQualityURL)
	}
	if cfg.DB.URL != "" {
		t.Errorf("DB.URL should default to empty (persistence disabled)")
	}
	if cfg.HTTP.WriteTimeout <= cfg.HTTP.RequestTimeout {
		t.Errorf("default write timeout must exceed request timeout")
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"LOG_LEVEL":                "debug",
		"LOG_FORMAT":               "text",
		"HTTP_ADDR":                ":9999",
		"HTTP_REQUEST_TIMEOUT":     "2s",
		"HTTP_RATE_LIMIT_RPS":      "2.5",
		"HTTP_TRUST_PROXY_HEADERS": "true",
		"UPSTREAM_AIR_QUALITY_URL": "http://mock:8090",
		"UPSTREAM_ATTEMPT_TIMEOUT": "1s",
		"CACHE_CURRENT_TTL":        " 90s ", // whitespace is tolerated
		"DATABASE_URL":             "postgres://u:p@db:5432/air",
		"DB_RETENTION":             "0s",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Log.Level != slog.LevelDebug || cfg.Log.Format != "text" {
		t.Errorf("log = %+v", cfg.Log)
	}
	if cfg.HTTP.Addr != ":9999" || cfg.HTTP.RequestTimeout != 2*time.Second || cfg.HTTP.RateLimitRPS != 2.5 {
		t.Errorf("http = %+v", cfg.HTTP)
	}
	if !cfg.HTTP.TrustProxyHeaders {
		t.Error("TrustProxyHeaders not applied")
	}
	if cfg.Cache.CurrentTTL != 90*time.Second {
		t.Errorf("CurrentTTL = %s", cfg.Cache.CurrentTTL)
	}
	if cfg.DB.Retention != 0 {
		t.Errorf("Retention = %s, want 0 (disabled)", cfg.DB.Retention)
	}
}

func TestLoadCollectsAllErrors(t *testing.T) {
	_, err := Load(env(map[string]string{
		"LOG_LEVEL":                "loud",
		"HTTP_READ_TIMEOUT":        "soon",
		"HTTP_MAX_IN_FLIGHT":       "0",
		"HTTP_RATE_LIMIT_RPS":      "NaN",
		"UPSTREAM_AIR_QUALITY_URL": "ftp://example.com",
		"UPSTREAM_GEOCODING_URL":   "https://user:pw@example.com",
		"DB_MIGRATE_ON_START":      "maybe",
		"DB_MAX_IDLE_CONNS":        "50",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{
		"LOG_LEVEL", "HTTP_READ_TIMEOUT", "HTTP_MAX_IN_FLIGHT", "HTTP_RATE_LIMIT_RPS",
		"UPSTREAM_AIR_QUALITY_URL", "UPSTREAM_GEOCODING_URL", "DB_MIGRATE_ON_START", "DB_MAX_IDLE_CONNS",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%v", want, err)
		}
	}
}

func TestLoadCrossFieldValidation(t *testing.T) {
	tests := map[string]map[string]string{
		"write timeout below request timeout": {"HTTP_WRITE_TIMEOUT": "5s", "HTTP_REQUEST_TIMEOUT": "10s"},
		"request timeout below attempt":       {"HTTP_REQUEST_TIMEOUT": "1s", "UPSTREAM_ATTEMPT_TIMEOUT": "2s"},
		"retry max below base":                {"UPSTREAM_RETRY_BASE_DELAY": "2s", "UPSTREAM_RETRY_MAX_DELAY": "1s"},
		"too many attempts":                   {"UPSTREAM_MAX_ATTEMPTS": "11"},
		"batch too large":                     {"RECORDER_BATCH_SIZE": "5000"},
		"negative stale window":               {"CACHE_STALE_FOR": "-1s"},
	}
	for name, vars := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(env(vars)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestLogValueRedactsSecrets(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"DATABASE_URL":     "postgres://app:s3cr3t-pass@db:5432/air?sslmode=disable",
		"UPSTREAM_API_KEY": "super-secret-key",
	}))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("config", "config", cfg)
	out := buf.String()
	for _, secret := range []string{"s3cr3t-pass", "super-secret-key"} {
		if strings.Contains(out, secret) {
			t.Errorf("log output leaks %q: %s", secret, out)
		}
	}
	if !strings.Contains(out, "db:5432") {
		t.Errorf("redacted DSN should keep the host for debugging: %s", out)
	}
}

func TestRedactDSN(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"postgres://u:pw@h:5432/db", "postgres://u:xxxxx@h:5432/db"},
		{"postgres://u@h/db?password=pw", "postgres://u@h/db?password=xxxxx"},
		{"host=h user=u password=pw", "[redacted]"},
	}
	for _, tt := range tests {
		if got := RedactDSN(tt.in); got != tt.want {
			t.Errorf("RedactDSN(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
