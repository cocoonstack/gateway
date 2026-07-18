package gateway

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

var _ Client = (*MockClient)(nil)

type MockClient struct {
	mu       sync.RWMutex
	yaml     string
	version  int64
	versions []ConfigVersion
	keys     map[string]Key
	audit    []AuditEntry
}

func NewMock() *MockClient {
	now := time.Now().Unix()
	owner := "alice"
	expires := now + 30*86_400
	return &MockClient{
		yaml:    "listen: {host: 0.0.0.0, port: 8080}\nstorage: {}\nmodels:\n  - {name: gpt-4o, protocol: openai-chat}\naccounts:\n  - {name: primary-openai, provider: openai, protocols: [openai-chat]}\ntenants:\n  - {name: acme}\naccess_keys: []\n",
		version: 3,
		versions: []ConfigVersion{
			{ID: 3, CreatedAtEpochSecs: now - 300},
			{ID: 2, CreatedAtEpochSecs: now - 3_600},
			{ID: 1, CreatedAtEpochSecs: now - 86_400},
		},
		keys: map[string]Key{
			"ak-acme-alice": {
				AK: "ak-acme-alice", Product: "standard", Tenant: "acme", Owner: &owner,
				QPS: 10, DailyTokenQuota: 1_000_000, Status: "active", Available: true,
			},
			"ak-acme-batch": {
				AK: "ak-acme-batch", Product: "batch", Tenant: "acme", QPS: 2,
				DailyTokenQuota: 500_000, ExpiresAtEpochSecs: &expires, Status: "active", Available: true,
			},
			"ak-labs-paused": {
				AK: "ak-labs-paused", Product: "research", Tenant: "labs", QPS: 1,
				DailyTokenQuota: 100_000, Banned: true, Status: "banned", Available: false,
			},
		},
		audit: []AuditEntry{
			{CreatedAtEpochSecs: now - 300, Actor: "global", Scope: "global", Action: "config_publish", Target: "3", SourceIP: "127.0.0.1"},
			{CreatedAtEpochSecs: now - 900, Actor: "global", Scope: "global", Action: "key_patch", Target: "ak-labs-paused", SourceIP: "127.0.0.1"},
		},
	}
}

func (m *MockClient) Usage(_ context.Context, scope Scope, _, _ int64) ([]UsageRow, error) {
	userID := scope.User
	if userID == "" {
		userID = "alice"
	}
	rows := []UsageRow{
		{UserID: userID, Model: "gpt-4o", Requests: 184, PromptTokens: 128_400, CompletionTokens: 42_700, TotalTokens: 171_100, CostMicros: 748_000, VendorCostMicros: 422_000},
		{UserID: userID, Model: "claude-sonnet", Requests: 62, PromptTokens: 84_200, CompletionTokens: 19_600, TotalTokens: 103_800, CostMicros: 512_000, VendorCostMicros: 331_000},
	}
	if scope.User == "" {
		rows = append(rows, UsageRow{UserID: "bob", Model: "gpt-4o-mini", Requests: 323, PromptTokens: 212_000, CompletionTokens: 31_000, TotalTokens: 243_000, CostMicros: 192_000, VendorCostMicros: 89_000})
	}
	return rows, nil
}

func (m *MockClient) UsageSeries(_ context.Context, _ Scope, bucket string, since, until int64) (Series, error) {
	seconds := int64(86_400)
	if bucket == "hour" {
		seconds = 3_600
	}
	first := since - since%seconds
	points := make([]SeriesPoint, 0)
	for start, idx := first, int64(0); start <= until && len(points) < 400; start, idx = start+seconds, idx+1 {
		requests := 18 + (idx*7)%19
		tokens := requests * (730 + (idx%5)*120)
		points = append(points, SeriesPoint{
			Start: start, End: min(start+seconds-1, until), Requests: requests,
			PromptTokens: tokens * 3 / 4, CompletionTokens: tokens / 4, TotalTokens: tokens,
			CostMicros: tokens * 4, VendorCostMicros: tokens * 2,
		})
	}
	return Series{Bucket: bucket, Since: since, Until: until, Points: points}, nil
}

func (m *MockClient) Models(_ context.Context, _ Scope) ([]ModelStatus, error) {
	return []ModelStatus{
		{Model: "gpt-4o", State: "available", Requests: 986, Errors: 4, WindowMinutes: 15},
		{Model: "gpt-4o-mini", State: "available", Requests: 1_422, Errors: 8, WindowMinutes: 15},
		{Model: "claude-sonnet", State: "unstable", Requests: 412, Errors: 57, WindowMinutes: 15},
		{Model: "realtime", State: "no_data", Requests: 0, Errors: 0, WindowMinutes: 15},
	}, nil
}

func (m *MockClient) Keys(_ context.Context, tenant string) ([]Key, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]Key, 0, len(m.keys))
	for _, key := range m.keys {
		if tenant == "" || key.Tenant == tenant {
			keys = append(keys, cloneJSON(key))
		}
	}
	slices.SortFunc(keys, func(a, b Key) int { return cmp.Compare(a.AK, b.AK) })
	return keys, nil
}

func (m *MockClient) CreateKey(_ context.Context, key Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if key.AK == "" || key.Product == "" || key.Tenant == "" {
		return fmt.Errorf("ak, product and tenant are required")
	}
	key.Status = "active"
	key.Available = true
	m.keys[key.AK] = cloneJSON(key)
	m.record("key_create", key.AK)
	return nil
}

func (m *MockClient) PatchKey(_ context.Context, ak string, patch map[string]any) (Key, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key, ok := m.keys[ak]
	if !ok {
		return Key{}, fmt.Errorf("key %s: %w", ak, ErrNotFound)
	}
	if value, ok := patch["qps"].(float64); ok {
		key.QPS = value
	}
	if value, ok := patch["daily_token_quota"].(float64); ok {
		key.DailyTokenQuota = int64(value)
	}
	if value, ok := patch["banned"].(bool); ok {
		key.Banned = value
		key.Available = !value
		if value {
			key.Status = "banned"
		} else {
			key.Status = "active"
		}
	}
	m.keys[ak] = key
	m.record("key_patch", ak)
	return cloneJSON(key), nil
}

func (m *MockClient) DeleteKey(_ context.Context, ak string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.keys[ak]; !ok {
		return fmt.Errorf("key %s: %w", ak, ErrNotFound)
	}
	delete(m.keys, ak)
	m.record("key_delete", ak)
	return nil
}

func (m *MockClient) Instances(context.Context) ([]Instance, error) {
	return []Instance{
		{ID: "gw-a", URL: "http://gw-a:8080", Status: "available", LatencyMS: 8, Accounts: []Account{{Name: "openai-primary", Provider: "openai", Tier: "paygo", Health: "healthy", Protocols: []string{"openai-chat"}}}},
		{ID: "gw-b", URL: "http://gw-b:8080", Status: "available", LatencyMS: 11, Accounts: []Account{{Name: "anthropic-primary", Provider: "anthropic", Tier: "paygo", Health: "healthy", Protocols: []string{"anthropic-messages"}}}},
	}, nil
}

func (m *MockClient) Config(context.Context) (ConfigDocument, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return ConfigDocument{Version: m.version, YAML: m.yaml}, nil
}

func (m *MockClient) ValidateConfig(_ context.Context, yaml string) (map[string]any, error) {
	if !strings.Contains(yaml, "listen:") || !strings.Contains(yaml, "models:") {
		return nil, fmt.Errorf("invalid config: listen and models are required")
	}
	return map[string]any{"valid": true, "models": strings.Count(yaml, "name:")}, nil
}

func (m *MockClient) PublishConfig(ctx context.Context, yaml string, expectedVersion int64) (int64, error) {
	if _, err := m.ValidateConfig(ctx, yaml); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if expectedVersion > 0 && expectedVersion != m.version {
		return 0, fmt.Errorf("config head is at version %d: %w", m.version, ErrConflict)
	}
	m.version++
	m.yaml = yaml
	m.versions = append([]ConfigVersion{{ID: m.version, CreatedAtEpochSecs: time.Now().Unix()}}, m.versions...)
	m.record("config_publish", fmt.Sprint(m.version))
	return m.version, nil
}

func (m *MockClient) ConfigVersions(context.Context) ([]ConfigVersion, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]ConfigVersion(nil), m.versions...), nil
}

func (m *MockClient) RollbackConfig(_ context.Context, id int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !slices.ContainsFunc(m.versions, func(v ConfigVersion) bool { return v.ID == id }) {
		return 0, fmt.Errorf("config version %d: %w", id, ErrNotFound)
	}
	m.version++
	m.versions = append([]ConfigVersion{{ID: m.version, CreatedAtEpochSecs: time.Now().Unix()}}, m.versions...)
	m.record("config_rollback", fmt.Sprint(m.version))
	return m.version, nil
}

func (m *MockClient) Audit(context.Context) ([]AuditEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries := append([]AuditEntry(nil), m.audit...)
	slices.SortFunc(entries, func(a, b AuditEntry) int { return cmp.Compare(b.CreatedAtEpochSecs, a.CreatedAtEpochSecs) })
	return entries, nil
}

func (m *MockClient) SecurityEvents(_ context.Context, tenant string) ([]SecurityEvent, error) {
	now := time.Now().Unix()
	if tenant == "" {
		tenant = "acme"
	}
	return []SecurityEvent{
		{CreatedAtEpochSecs: now - 120, RequestID: "req-42", AK: "ak-acme-alice", UserID: "alice", Tenant: tenant, Surface: "chat", Rule: "dlp", Action: "redact", Hits: 1},
		{CreatedAtEpochSecs: now - 500, RequestID: "req-39", AK: "ak-acme-batch", UserID: "bob", Tenant: tenant, Surface: "batch", Rule: "blocklist", Action: "flag", Hits: 2},
	}, nil
}

func (m *MockClient) record(action, target string) {
	m.audit = append(m.audit, AuditEntry{
		CreatedAtEpochSecs: time.Now().Unix(), Actor: "control-plane", Scope: "global",
		Action: action, Target: target, SourceIP: "127.0.0.1",
	})
}

func cloneJSON[T any](value T) T {
	body, _ := json.Marshal(value)
	var out T
	_ = json.Unmarshal(body, &out)
	return out
}
