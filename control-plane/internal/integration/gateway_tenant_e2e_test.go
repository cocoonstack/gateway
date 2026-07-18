//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cocoonstack/gateway/control-plane/internal/auth"
	"github.com/cocoonstack/gateway/control-plane/internal/gateway"
	"github.com/cocoonstack/gateway/control-plane/internal/httpapi"
	kvmemory "github.com/cocoonstack/gateway/control-plane/internal/kv/memory"
	"github.com/cocoonstack/gateway/control-plane/internal/user"
	usermemory "github.com/cocoonstack/gateway/control-plane/internal/user/memory"
)

const gatewayConfTemplate = `listen: {host: 127.0.0.1, port: %d}
admin: {token_env: GW_E2E_GLOBAL_TOKEN}
storage:
  postgres_url: "%s"
  redis_url: "%s"
models:
  - name: m1
    protocol: openai-chat
    input_price_per_1k_micros: 5000
    output_price_per_1k_micros: 5000
accounts:
  - name: acct-mock
    provider: openai
    protocols: [openai-chat]
    cost_input_price_per_1k_micros: 1000
    cost_output_price_per_1k_micros: 1000
tenants:
  - name: acme
    admin_token_env: GW_E2E_ACME_TOKEN
    models: [m1]
  - name: labs
    models: [m1]
access_keys: []
`

func TestTenantTokenChain(t *testing.T) {
	gwBin := os.Getenv("CP_TEST_GW_BIN")
	pgURL := os.Getenv("CP_TEST_PG_URL")
	redisURL := os.Getenv("CP_TEST_REDIS_URL")
	if gwBin == "" || pgURL == "" || redisURL == "" {
		t.Skip("CP_TEST_GW_BIN, CP_TEST_PG_URL and CP_TEST_REDIS_URL are required")
	}
	gwURL := startGateway(t, gwBin, pgURL, redisURL)
	cp := startControlPlane(t, gwURL)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	acmeKey, labsKey := "ak-e2e-acme-"+suffix, "ak-e2e-labs-"+suffix

	root := login(t, cp, "root@example.com")
	acme := login(t, cp, "acme@example.com")
	labs := login(t, cp, "labs@example.com")

	rec := send(t, cp, root, http.MethodPost, "/api/v1/admin/keys",
		map[string]any{"ak": labsKey, "product": "p", "tenant": "labs", "qps": 5, "daily_token_quota": 1_000_000})
	if rec.StatusCode != http.StatusCreated {
		t.Fatalf("system admin create labs key = %d, body %s", rec.StatusCode, rec.Body)
	}
	rec = send(t, cp, acme, http.MethodPost, "/api/v1/admin/keys",
		map[string]any{"ak": acmeKey, "product": "p", "qps": 5, "daily_token_quota": 1_000_000})
	if rec.StatusCode != http.StatusCreated {
		t.Fatalf("tenant admin create own key = %d, body %s", rec.StatusCode, rec.Body)
	}

	rec = send(t, cp, acme, http.MethodGet, "/api/v1/admin/keys", nil)
	if rec.StatusCode != http.StatusOK || !strings.Contains(rec.Body, acmeKey) || strings.Contains(rec.Body, labsKey) {
		t.Fatalf("tenant admin list = %d, want only own tenant's keys; body %s", rec.StatusCode, rec.Body)
	}

	rec = send(t, cp, acme, http.MethodPatch, "/api/v1/admin/keys/"+labsKey, map[string]any{"banned": true})
	if rec.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant patch = %d, want 404 from the gateway; body %s", rec.StatusCode, rec.Body)
	}
	rec = send(t, cp, acme, http.MethodPost, "/api/v1/admin/keys",
		map[string]any{"ak": labsKey, "product": "p", "qps": 5, "daily_token_quota": 1_000_000})
	if rec.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant create takeover = %d, want 404 from the gateway; body %s", rec.StatusCode, rec.Body)
	}

	rec = send(t, cp, labs, http.MethodPatch, "/api/v1/admin/keys/"+labsKey, map[string]any{"banned": true})
	if rec.StatusCode != http.StatusBadGateway || !strings.Contains(rec.Body, "no gateway admin token") {
		t.Fatalf("mutation without tenant token = %d, want fail-closed 502; body %s", rec.StatusCode, rec.Body)
	}

	gwGet := func(path, token string) (int, string) {
		req, err := http.NewRequest(http.MethodGet, gwURL+path, nil)
		if err != nil {
			t.Fatalf("build gateway request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("call gateway %s: %v", path, err)
		}
		defer resp.Body.Close()
		var body bytes.Buffer
		_, _ = body.ReadFrom(resp.Body)
		return resp.StatusCode, body.String()
	}

	if _, body := gwGet("/admin/keys?ak="+labsKey, "acme-e2e-token"); !strings.Contains(body, `"count":0`) {
		t.Fatalf("ak filter under tenant token leaked a foreign key: %s", body)
	}
	if _, body := gwGet("/admin/keys?ak="+labsKey, "root-e2e-token"); !strings.Contains(body, `"count":1`) {
		t.Fatalf("ak filter under global token missed the key: %s", body)
	}

	chat, err := json.Marshal(map[string]any{
		"model": "m1", "messages": []map[string]string{{"role": "user", "content": "hello e2e"}},
	})
	if err != nil {
		t.Fatalf("encode chat: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, gwURL+"/v1/chat/completions", bytes.NewReader(chat))
	if err != nil {
		t.Fatalf("build chat request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+acmeKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("chat through gateway: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("chat status = %d", resp.StatusCode)
	}

	type usageRow struct {
		CostMicros       int64 `json:"cost_micros"`
		VendorCostMicros int64 `json:"vendor_cost_micros"`
	}
	decodeUsage := func(token string) []usageRow {
		_, body := gwGet("/admin/usage/users?since=0&until="+strconv.FormatInt(time.Now().Unix()+60, 10), token)
		var view struct {
			Usage []usageRow `json:"usage"`
		}
		if err := json.Unmarshal([]byte(body), &view); err != nil {
			t.Fatalf("decode usage view: %v; body %s", err, body)
		}
		return view.Usage
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		tenantView := decodeUsage("acme-e2e-token")
		if slices.ContainsFunc(tenantView, func(r usageRow) bool { return r.CostMicros > 0 }) {
			for _, row := range tenantView {
				if row.VendorCostMicros != 0 {
					t.Fatalf("tenant token saw vendor cost %d", row.VendorCostMicros)
				}
			}
			globalView := decodeUsage("root-e2e-token")
			if !slices.ContainsFunc(globalView, func(r usageRow) bool { return r.VendorCostMicros > 0 }) {
				t.Fatal("global token saw no vendor cost; margin basis lost")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("billing row never appeared in gateway usage")
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func startGateway(t *testing.T, bin, pgURL, redisURL string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick gateway port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	conf := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(conf, fmt.Appendf(nil, gatewayConfTemplate, port, pgURL, redisURL), 0o600); err != nil {
		t.Fatalf("write gateway config: %v", err)
	}
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"GW_CONFIG="+conf,
		"GW_TRANSPORT=mock",
		"GW_E2E_GLOBAL_TOKEN=root-e2e-token",
		"GW_E2E_ACME_TOKEN=acme-e2e-token",
	)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get(url + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return url
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway never became healthy: %v", err)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func startControlPlane(t *testing.T, gwURL string) *httptest.Server {
	t.Helper()
	client, err := gateway.NewHTTP("gw="+gwURL, "root-e2e-token", map[string]string{"acme": "acme-e2e-token"})
	if err != nil {
		t.Fatalf("build gateway client: %v", err)
	}
	store := usermemory.New()
	for _, seed := range []struct {
		id, email, tenant string
		role              user.Role
	}{
		{"root", "root@example.com", "", user.RoleSystemAdmin},
		{"acme", "acme@example.com", "acme", user.RoleTenantAdmin},
		{"labs", "labs@example.com", "labs", user.RoleTenantAdmin},
	} {
		hash, err := auth.HashPassword("password123!")
		if err != nil {
			t.Fatalf("hash password: %v", err)
		}
		now := time.Now().Unix()
		if err := store.Create(t.Context(), user.User{
			ID: seed.id, Email: seed.email, DisplayName: seed.id, PasswordHash: hash,
			Tenant: seed.tenant, Role: seed.role, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("seed user: %v", err)
		}
	}
	server := httptest.NewServer(httpapi.New(store, kvmemory.New(), client, time.Hour, false, t.TempDir()).Handler())
	t.Cleanup(server.Close)
	return server
}

type browserSession struct {
	cookie *http.Cookie
	csrf   string
}

func login(t *testing.T, cp *httptest.Server, email string) browserSession {
	t.Helper()
	body, err := json.Marshal(map[string]string{"email": email, "password": "password123!"})
	if err != nil {
		t.Fatalf("encode login: %v", err)
	}
	resp, err := http.Post(cp.URL+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("login %s: %v", email, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login %s status = %d", email, resp.StatusCode)
	}
	var payload struct {
		CSRF string `json:"csrf_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode login: %v", err)
	}
	return browserSession{cookie: resp.Cookies()[0], csrf: payload.CSRF}
}

type wireResponse struct {
	StatusCode int
	Body       string
}

func send(t *testing.T, cp *httptest.Server, session browserSession, method, path string, body any) wireResponse {
	t.Helper()
	var payload bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&payload).Encode(body); err != nil {
			t.Fatalf("encode request: %v", err)
		}
	}
	req, err := http.NewRequest(method, cp.URL+path, &payload)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.AddCookie(session.cookie)
	req.Header.Set("X-CSRF-Token", session.csrf)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var out bytes.Buffer
	_, _ = out.ReadFrom(resp.Body)
	return wireResponse{StatusCode: resp.StatusCode, Body: out.String()}
}
