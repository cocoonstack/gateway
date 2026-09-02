package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/cocoonstack/gateway/control-plane/internal/auth"
	"github.com/cocoonstack/gateway/control-plane/internal/gateway"
	kvmemory "github.com/cocoonstack/gateway/control-plane/internal/kv/memory"
	"github.com/cocoonstack/gateway/control-plane/internal/user"
	usermemory "github.com/cocoonstack/gateway/control-plane/internal/user/memory"
)

func TestMemberIsScopedAndCannotReadAdminSurfaces(t *testing.T) {
	handler := testServer(t)
	member := loginAs(t, handler, "user@example.com")

	rec := request(t, handler, member, http.MethodGet, "/api/v1/usage?user=bob", nil, false)
	if rec.Code != http.StatusOK {
		t.Fatalf("usage status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var usage struct {
		Rows []gateway.UsageRow `json:"usage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &usage); err != nil {
		t.Fatalf("decode usage: %v", err)
	}
	for _, row := range usage.Rows {
		if row.UserID != "alice" {
			t.Errorf("row user = %q, want session user alice", row.UserID)
		}
		if row.VendorCostMicros != 0 {
			t.Errorf("member vendor cost = %d, want hidden", row.VendorCostMicros)
		}
	}

	rec = request(t, handler, member, http.MethodGet, "/api/v1/admin/instances", nil, false)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("instances status = %d, want 403", rec.Code)
	}
}

func TestSystemAdminConfigAndInstances(t *testing.T) {
	handler := testServer(t)
	admin := loginAs(t, handler, "admin@example.com")

	rec := request(t, handler, admin, http.MethodGet, "/api/v1/admin/instances", nil, false)
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte("gw-a")) {
		t.Fatalf("instances status = %d, body = %s", rec.Code, rec.Body.String())
	}

	body := map[string]string{"yaml": "listen: {host: h, port: 1}\nmodels: []\n"}
	rec = request(t, handler, admin, http.MethodPost, "/api/v1/admin/config/validate", body, false)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("missing csrf status = %d, want 403", rec.Code)
	}
	rec = request(t, handler, admin, http.MethodPost, "/api/v1/admin/config/validate", body, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("validate status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestTenantAdminCannotMutateAnotherTenantKey(t *testing.T) {
	handler := testServer(t)
	manager := loginAs(t, handler, "manager@example.com")
	rec := request(
		t,
		handler,
		manager,
		http.MethodPatch,
		"/api/v1/admin/keys/ak-labs-paused",
		map[string]any{"banned": false},
		true,
	)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant patch status = %d, want 404; body = %s", rec.Code, rec.Body.String())
	}
}

func TestTenantAdminCannotHijackForeignKeyViaCreate(t *testing.T) {
	handler := testServer(t)
	manager := loginAs(t, handler, "manager@example.com")
	rec := request(
		t,
		handler,
		manager,
		http.MethodPost,
		"/api/v1/admin/keys",
		map[string]any{"ak": "ak-labs-paused", "product": "standard"},
		true,
	)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("foreign-ak create status = %d, want 404 like the gateway's scoped_key; body = %s", rec.Code, rec.Body.String())
	}
	rec = request(
		t,
		handler,
		manager,
		http.MethodPost,
		"/api/v1/admin/keys",
		map[string]any{"ak": "ak-acme-new", "product": "standard"},
		true,
	)
	if rec.Code != http.StatusCreated {
		t.Fatalf("own-tenant create status = %d, want 201; body = %s", rec.Code, rec.Body.String())
	}
}

func TestLoginThrottleLocksAfterRepeatedFailures(t *testing.T) {
	handler := testServer(t)
	body, _ := json.Marshal(map[string]string{"email": "admin@example.com", "password": "wrong-password"})
	var last int
	for range loginMaxAttempts + 1 {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		last = rec.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("attempt %d status = %d, want 429", loginMaxAttempts+1, last)
	}
}

func TestRequestIDEchoedOrGenerated(t *testing.T) {
	handler := testServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.Header.Set("X-Request-ID", "trace-abc.123")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "trace-abc.123" {
		t.Errorf("request id = %q, want caller value echoed", got)
	}

	for _, inbound := range []string{"", "bad id\nwith newline"} {
		req = httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
		if inbound != "" {
			req.Header.Set("X-Request-ID", inbound)
		}
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		got := rec.Header().Get("X-Request-ID")
		if got == "" || got == inbound {
			t.Errorf("request id = %q for inbound %q, want a fresh generated id", got, inbound)
		}
	}
}

func TestPasswordResetEvictsExistingSessions(t *testing.T) {
	handler := testServer(t)
	admin := loginAs(t, handler, "admin@example.com")
	member := loginAs(t, handler, "user@example.com")

	rec := request(t, handler, member, http.MethodGet, "/api/v1/session", nil, false)
	if rec.Code != http.StatusOK {
		t.Fatalf("pre-reset session status = %d, want 200", rec.Code)
	}
	rec = request(
		t,
		handler,
		admin,
		http.MethodPatch,
		"/api/v1/admin/users/member",
		map[string]any{"password": "brand-new-pass-1!"},
		true,
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("password reset status = %d, body = %s", rec.Code, rec.Body.String())
	}
	rec = request(t, handler, member, http.MethodGet, "/api/v1/session", nil, false)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("post-reset session status = %d, want 401", rec.Code)
	}
}

func TestClientIPHonoursForwardedHeadersOnlyWhenProxyIsTrusted(t *testing.T) {
	for _, tt := range []struct {
		name         string
		trustedProxy bool
		forwardedFor string
		realIP       string
		want         string
	}{
		{"untrusted ignores forwarded", false, "203.0.113.7, 10.0.0.1", "203.0.113.9", "192.0.2.1"},
		{"trusted takes first forwarded hop", true, "203.0.113.7, 10.0.0.1", "203.0.113.9", "203.0.113.7"},
		{"trusted falls back to real ip", true, "", "203.0.113.9", "203.0.113.9"},
		{"trusted falls back to remote addr", true, "", "", "192.0.2.1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestServer(t, usermemory.New(), tt.trustedProxy)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
			req.RemoteAddr = "192.0.2.1:34567"
			if tt.forwardedFor != "" {
				req.Header.Set("X-Forwarded-For", tt.forwardedFor)
			}
			if tt.realIP != "" {
				req.Header.Set("X-Real-IP", tt.realIP)
			}
			if got := s.clientIP(req); got != tt.want {
				t.Errorf("clientIP = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLoginThrottleSweepStaysAmortisedAndReclaimsExpired(t *testing.T) {
	throttle := newLoginThrottle()
	now := time.Now()
	for i := range throttleSweepAt * 3 {
		throttle.allow(strconv.Itoa(i), now)
	}
	if len(throttle.windows) != throttleSweepAt*3 {
		t.Fatalf("live windows = %d, want %d", len(throttle.windows), throttleSweepAt*3)
	}
	if throttle.sweepAt <= throttleSweepAt {
		t.Fatalf("sweepAt = %d, want it raised above %d so live windows do not force a scan per login", throttle.sweepAt, throttleSweepAt)
	}
	for i := range throttleSweepAt * 3 {
		throttle.allow("fresh-"+strconv.Itoa(i), now.Add(2*loginWindow))
	}
	if _, ok := throttle.windows["0"]; ok {
		t.Error("expired window survived every sweep")
	}
}

type loginState struct {
	cookie *http.Cookie
	csrf   string
}

func testServer(t *testing.T) http.Handler {
	t.Helper()
	store := usermemory.New()
	for _, seed := range []struct {
		id, email, tenant, gatewayUserID string
		role                             user.Role
	}{
		{"admin", "admin@example.com", "", "", user.RoleSystemAdmin},
		{"manager", "manager@example.com", "acme", "", user.RoleTenantAdmin},
		{"member", "user@example.com", "acme", "alice", user.RoleMember},
	} {
		hash, err := auth.HashPassword("password123!")
		if err != nil {
			t.Fatalf("hash password: %v", err)
		}
		now := time.Now().Unix()
		if err := store.Create(t.Context(), user.User{
			ID: seed.id, Email: seed.email, DisplayName: seed.id, PasswordHash: hash,
			Tenant: seed.tenant, GatewayUserID: seed.gatewayUserID, Role: seed.role,
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("seed user: %v", err)
		}
	}
	return newTestServer(t, store, false).Handler()
}

func newTestServer(t *testing.T, store user.Store, trustedProxy bool) *Server {
	t.Helper()
	return New(Options{
		Users:        store,
		Sessions:     kvmemory.New(),
		Gateway:      gateway.NewMock(),
		SessionTTL:   time.Hour,
		TrustedProxy: trustedProxy,
		WebDir:       t.TempDir(),
	})
}

func loginAs(t *testing.T, handler http.Handler, email string) loginState {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"email": email, "password": "password123!"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		CSRF string `json:"csrf_token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode login: %v", err)
	}
	return loginState{cookie: rec.Result().Cookies()[0], csrf: response.CSRF}
}

func request(t *testing.T, handler http.Handler, state loginState, method, path string, body any, csrf bool) *httptest.ResponseRecorder {
	t.Helper()
	var payload bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&payload).Encode(body); err != nil {
			t.Fatalf("encode request: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &payload)
	req.AddCookie(state.cookie)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if csrf {
		req.Header.Set("X-CSRF-Token", state.csrf)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}
