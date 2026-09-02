package httpapi

import (
	"cmp"
	"maps"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	loginWindow      = 5 * time.Minute
	loginMaxAttempts = 10
	throttleSweepAt  = 1024
)

type window struct {
	start    time.Time
	attempts int
}

type loginThrottle struct {
	mu      sync.Mutex
	windows map[string]window
	sweepAt int
}

func newLoginThrottle() *loginThrottle {
	return &loginThrottle{windows: make(map[string]window), sweepAt: throttleSweepAt}
}

func (t *loginThrottle) allow(key string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.windows) > t.sweepAt {
		maps.DeleteFunc(t.windows, func(_ string, w window) bool { return now.Sub(w.start) >= loginWindow })
		t.sweepAt = max(throttleSweepAt, 2*len(t.windows))
	}
	w := t.windows[key]
	if now.Sub(w.start) >= loginWindow {
		w = window{start: now}
	}
	w.attempts++
	t.windows[key] = w
	return w.attempts <= loginMaxAttempts
}

func (t *loginThrottle) reset(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.windows, key)
}

func (s *Server) clientIP(r *http.Request) string {
	if s.trustedProxy {
		forwarded, _, _ := strings.Cut(r.Header.Get("X-Forwarded-For"), ",")
		if ip := cmp.Or(strings.TrimSpace(forwarded), strings.TrimSpace(r.Header.Get("X-Real-IP"))); ip != "" {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
