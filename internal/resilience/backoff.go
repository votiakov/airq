package resilience

import (
	"context"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Backoff computes capped exponential backoff with full jitter
// (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/), which
// spreads retries from many clients evenly and avoids synchronised retry storms.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
	// Rand returns a float in [0,1); defaults to math/rand/v2.Float64.
	Rand func() float64
}

// Delay returns the wait before retry number n (0 for the first retry).
func (b Backoff) Delay(n int) time.Duration {
	ceiling := b.Max
	if n < 62 { // guard the shift against overflow
		if d := b.Base << n; d > 0 && d < b.Max {
			ceiling = d
		}
	}
	r := b.Rand
	if r == nil {
		r = rand.Float64
	}
	return time.Duration(r() * float64(ceiling))
}

// ParseRetryAfter interprets a Retry-After header value, which is either a number of
// seconds or an HTTP date. It returns false when the header is absent or malformed.
func ParseRetryAfter(v string, now time.Time) (time.Duration, bool) {
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

// Sleep waits for d or until ctx is done, whichever is first.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
