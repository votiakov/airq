package upstream

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// parseRetryAfter interprets a Retry-After header value, which is either a number of
// seconds or an HTTP date. It returns false when the header is absent or malformed.
func parseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(t.Sub(now), 0), true
	}
	return 0, false
}
