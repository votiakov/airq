package upstream

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// MaxRetryAfter caps any Retry-After hint taken from the vendor. Whatever the vendor
// asks for, we never sleep, or tell our own clients to wait, longer than this.
const MaxRetryAfter = 2 * time.Minute

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
		return min(time.Duration(min(secs, int(MaxRetryAfter/time.Second)))*time.Second, MaxRetryAfter), true
	}
	if t, err := http.ParseTime(v); err == nil {
		return min(max(t.Sub(now), 0), MaxRetryAfter), true
	}
	return 0, false
}
