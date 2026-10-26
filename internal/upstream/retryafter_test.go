package upstream

import (
	"net/http"
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"", 0, false},
		{"5", 5 * time.Second, true},
		{" 0 ", 0, true},
		{"-1", 0, false},
		{"soon", 0, false},
		{now.Add(30 * time.Second).Format(http.TimeFormat), 30 * time.Second, true},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0, true},
		// A hostile or buggy vendor cannot make us wait, or tell clients to wait, for long.
		{"86400", MaxRetryAfter, true},
		{"9223372036", MaxRetryAfter, true},
		{"9223372037", MaxRetryAfter, true}, // would overflow time.Duration if multiplied first
		{"99999999999999999999", 0, false},  // beyond int: malformed
		{now.Add(48 * time.Hour).Format(http.TimeFormat), MaxRetryAfter, true},
	}
	for _, tt := range tests {
		got, ok := parseRetryAfter(tt.in, now)
		if got != tt.want || ok != tt.ok {
			t.Errorf("parseRetryAfter(%q) = %s,%v want %s,%v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}
