package httpapi

import (
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	loginWindow      = 5 * time.Minute
	loginMaxAttempts = 10
	// sweep stale windows once tracked keys pass this bound
	throttleSweepAt = 1024
)

type loginThrottle struct {
	mu      sync.Mutex
	windows map[string]window
}

type window struct {
	start    time.Time
	attempts int
}

func newLoginThrottle() *loginThrottle {
	return &loginThrottle{windows: make(map[string]window)}
}

func (t *loginThrottle) allow(key string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.windows) > throttleSweepAt {
		for k, w := range t.windows {
			if now.Sub(w.start) >= loginWindow {
				delete(t.windows, k)
			}
		}
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

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
