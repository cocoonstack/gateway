// Package httpapi serves the browser-facing control-plane API and static UI.
package httpapi

import (
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/gateway/control-plane/internal/gateway"
	"github.com/cocoonstack/gateway/control-plane/internal/kv"
	"github.com/cocoonstack/gateway/control-plane/internal/user"
)

const (
	sessionCookie = "cp_session"
	maxJSONBody   = 1 << 20
	maxConfigBody = 4 << 20
)

type principal struct {
	User    user.User
	Session kv.Session
}

type principalKey struct{}

type Server struct {
	users        user.Store
	sessions     kv.Sessions
	gateway      gateway.Client
	sessionTTL   time.Duration
	cookieSecure bool
	webDir       string
	throttle     *loginThrottle
}

func New(
	users user.Store,
	sessions kv.Sessions,
	gw gateway.Client,
	sessionTTL time.Duration,
	cookieSecure bool,
	webDir string,
) *Server {
	return &Server{
		users:        users,
		sessions:     sessions,
		gateway:      gw,
		sessionTTL:   sessionTTL,
		cookieSecure: cookieSecure,
		webDir:       webDir,
		throttle:     newLoginThrottle(),
	}
}

// Routes are mirrored in api/openapi.yaml — update both together.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", s.health)
	mux.HandleFunc("POST /api/v1/auth/login", s.login)
	mux.Handle("POST /api/v1/auth/logout", s.requireAuth(http.HandlerFunc(s.logout)))
	mux.Handle("GET /api/v1/session", s.requireAuth(http.HandlerFunc(s.session)))
	mux.Handle("GET /api/v1/overview", s.requireAuth(http.HandlerFunc(s.overview)))
	mux.Handle("GET /api/v1/usage", s.requireAuth(http.HandlerFunc(s.usage)))
	mux.Handle("GET /api/v1/usage/series", s.requireAuth(http.HandlerFunc(s.usageSeries)))
	mux.Handle("GET /api/v1/models/status", s.requireAuth(http.HandlerFunc(s.models)))
	mux.Handle("GET /api/v1/admin/instances", s.requireSystem(http.HandlerFunc(s.instances)))
	mux.Handle("GET /api/v1/admin/users", s.requireSystem(http.HandlerFunc(s.listUsers)))
	mux.Handle("POST /api/v1/admin/users", s.requireSystem(http.HandlerFunc(s.createUser)))
	mux.Handle("PATCH /api/v1/admin/users/{id}", s.requireSystem(http.HandlerFunc(s.patchUser)))
	mux.Handle("GET /api/v1/admin/keys", s.requireAdmin(http.HandlerFunc(s.listKeys)))
	mux.Handle("POST /api/v1/admin/keys", s.requireAdmin(http.HandlerFunc(s.createKey)))
	mux.Handle("PATCH /api/v1/admin/keys/{ak}", s.requireAdmin(http.HandlerFunc(s.patchKey)))
	mux.Handle("DELETE /api/v1/admin/keys/{ak}", s.requireAdmin(http.HandlerFunc(s.deleteKey)))
	mux.Handle("GET /api/v1/admin/config", s.requireSystem(http.HandlerFunc(s.getConfig)))
	mux.Handle("POST /api/v1/admin/config/validate", s.requireSystem(http.HandlerFunc(s.validateConfig)))
	mux.Handle("PUT /api/v1/admin/config", s.requireSystem(http.HandlerFunc(s.publishConfig)))
	mux.Handle("GET /api/v1/admin/config/versions", s.requireSystem(http.HandlerFunc(s.configVersions)))
	mux.Handle("POST /api/v1/admin/config/versions/{id}/rollback", s.requireSystem(http.HandlerFunc(s.rollbackConfig)))
	mux.Handle("GET /api/v1/admin/audit", s.requireAdmin(http.HandlerFunc(s.audit)))
	mux.HandleFunc("/", s.serveWeb)
	return s.accessLog(mux)
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(sessionCookie)
		if err != nil || cookie.Value == "" {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		session, err := s.sessions.Get(r.Context(), cookie.Value)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		u, err := s.users.ByID(r.Context(), session.UserID)
		if err != nil || u.Disabled || u.PasswordChangedAt > 0 && session.IssuedAt <= u.PasswordChangedAt {
			_ = s.sessions.Delete(r.Context(), session.ID)
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if mutating(r.Method) &&
			subtle.ConstantTimeCompare([]byte(r.Header.Get("X-CSRF-Token")), []byte(session.CSRFToken)) != 1 {
			writeError(w, http.StatusForbidden, "invalid CSRF token")
			return
		}
		ctx := context.WithValue(r.Context(), principalKey{}, principal{User: u, Session: session})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) requireAdmin(next http.Handler) http.Handler {
	return s.requireAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if role := current(r).User.Role; role != user.RoleTenantAdmin && role != user.RoleSystemAdmin {
			writeError(w, http.StatusForbidden, "admin role required")
			return
		}
		next.ServeHTTP(w, r)
	}))
}

func (s *Server) requireSystem(next http.Handler) http.Handler {
	return s.requireAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if current(r).User.Role != user.RoleSystemAdmin {
			writeError(w, http.StatusForbidden, "system admin role required")
			return
		}
		next.ServeHTTP(w, r)
	}))
}

func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		wrapped := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		log.WithFunc("httpapi.access").Infof(
			r.Context(),
			"%s %s status=%d duration_ms=%d",
			r.Method,
			r.URL.Path,
			wrapped.status,
			time.Since(started).Milliseconds(),
		)
	})
}

func (s *Server) serveWeb(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	path := filepath.Join(s.webDir, filepath.Clean("/"+r.URL.Path))
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		http.ServeFile(w, r, path)
		return
	}
	index := filepath.Join(s.webDir, "index.html")
	if _, err := os.Stat(index); err != nil {
		writeError(w, http.StatusNotFound, "web assets are not built")
		return
	}
	http.ServeFile(w, r, index)
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func current(r *http.Request) principal {
	value, _ := r.Context().Value(principalKey{}).(principal)
	return value
}

func scopeFor(u user.User) gateway.Scope {
	switch u.Role {
	case user.RoleSystemAdmin:
		return gateway.Scope{}
	case user.RoleTenantAdmin:
		return gateway.Scope{Tenant: u.Tenant}
	default:
		return gateway.Scope{Tenant: u.Tenant, User: cmp.Or(u.GatewayUserID, u.ID)}
	}
}

func scopedTenant(p principal, requested string) string {
	if p.User.Role == user.RoleTenantAdmin {
		return p.User.Tenant
	}
	return requested
}

func period(r *http.Request) (int64, int64, string, error) {
	now := time.Now().Unix()
	since := queryInt(r, "since", now-29*86_400)
	until := queryInt(r, "until", now)
	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		bucket = "day"
	}
	if since < 0 || until < since {
		return 0, 0, "", fmt.Errorf("since/until must be a valid non-negative range")
	}
	if bucket != "hour" && bucket != "day" {
		return 0, 0, "", fmt.Errorf("bucket must be hour or day")
	}
	return since, until, bucket, nil
}

func queryInt(r *http.Request, name string, fallback int64) int64 {
	value, err := strconv.ParseInt(r.URL.Query().Get(name), 10, 64)
	if err != nil {
		return fallback
	}
	return value
}

func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, value any) bool {
	decoder := json.NewDecoder(io.LimitReader(r.Body, limit))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// the status line is already committed; an encode error is a gone client
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"message": message}})
}

func publicUser(u user.User) user.User {
	u.PasswordHash = ""
	return u
}

func mutating(method string) bool {
	return method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions
}

// auditLog names the principal behind a mutating admin action; accessLog lines carry only the route.
func auditLog(r *http.Request, action, target string) {
	p := current(r)
	log.WithFunc("httpapi.audit").Infof(
		r.Context(),
		"actor=%s role=%s action=%s target=%s ip=%s",
		p.User.Email, p.User.Role, action, target, clientIP(r),
	)
}

func mapError(ctx context.Context, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, user.ErrNotFound), errors.Is(err, gateway.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, user.ErrConflict), errors.Is(err, gateway.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	default:
		log.WithFunc("httpapi.mapError").Error(ctx, err, "request failed")
		writeError(w, http.StatusBadGateway, err.Error())
	}
}
