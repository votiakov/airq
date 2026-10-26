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
	}
	for _, tt := range tests {
		got, ok := parseRetryAfter(tt.in, now)
		if got != tt.want || ok != tt.ok {
			t.Errorf("parseRetryAfter(%q) = %s,%v want %s,%v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}
