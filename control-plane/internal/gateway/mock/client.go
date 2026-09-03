// Package mock serves the control-plane surfaces without a running gateway.
package mock

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cocoonstack/gateway/control-plane/internal/gateway"
)

var _ gateway.Client = (*Client)(nil)

type Client struct {
	mu       sync.RWMutex
	yaml     string
	version  int64
	versions []gateway.ConfigVersion
	keys     map[string]gateway.Key
	audit    []gateway.AuditEntry
}

func New() *Client {
	now := time.Now().Unix()
	owner := "alice"
	expires := now + 30*86_400
	return &Client{
		yaml:    "listen: {host: 0.0.0.0, port: 8080}\nstorage: {}\nmodels:\n  - {name: gpt-4o, protocol: openai-chat}\naccounts:\n  - {name: primary-openai, provider: openai, protocols: [openai-chat]}\ntenants:\n  - {name: acme}\naccess_keys: []\n",
		version: 3,
		versions: []gateway.ConfigVersion{
			{ID: 3, CreatedAtEpochSecs: now - 300},
			{ID: 2, CreatedAtEpochSecs: now - 3_600},
			{ID: 1, CreatedAtEpochSecs: now - 86_400},
		},
		keys: map[string]gateway.Key{
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
		audit: []gateway.AuditEntry{
			{CreatedAtEpochSecs: now - 300, Actor: "global", Scope: "global", Action: "config_publish", Target: "3", SourceIP: "127.0.0.1"},
			{CreatedAtEpochSecs: now - 900, Actor: "global", Scope: "global", Action: "key_patch", Target: "ak-labs-paused", SourceIP: "127.0.0.1"},
		},
	}
}

func (c *Client) Usage(_ context.Context, scope gateway.Scope, _, _ int64) ([]gateway.UsageRow, error) {
	userID := scope.User
	if userID == "" {
		userID = "alice"
	}
	rows := []gateway.UsageRow{
		{UserID: userID, Model: "gpt-4o", Requests: 184, PromptTokens: 128_400, CompletionTokens: 42_700, TotalTokens: 171_100, CostMicros: 748_000, VendorCostMicros: 422_000},
		{UserID: userID, Model: "claude-sonnet", Requests: 62, PromptTokens: 84_200, CompletionTokens: 19_600, TotalTokens: 103_800, CostMicros: 512_000, VendorCostMicros: 331_000},
	}
	if scope.User == "" {
		rows = append(rows, gateway.UsageRow{UserID: "bob", Model: "gpt-4o-mini", Requests: 323, PromptTokens: 212_000, CompletionTokens: 31_000, TotalTokens: 243_000, CostMicros: 192_000, VendorCostMicros: 89_000})
	}
	return rows, nil
}

func (c *Client) UsageSeries(_ context.Context, _ gateway.Scope, bucket string, since, until int64) (gateway.Series, error) {
	seconds := int64(86_400)
	if bucket == "hour" {
		seconds = 3_600
	}
	first := since - since%seconds
	points := make([]gateway.SeriesPoint, 0)
	for start, idx := first, int64(0); start <= until && len(points) < 400; start, idx = start+seconds, idx+1 {
		requests := 18 + (idx*7)%19
		tokens := requests * (730 + (idx%5)*120)
		points = append(points, gateway.SeriesPoint{
			Start: start, End: min(start+seconds-1, until), Requests: requests,
			PromptTokens: tokens * 3 / 4, CompletionTokens: tokens / 4, TotalTokens: tokens,
			CostMicros: tokens * 4, VendorCostMicros: tokens * 2,
		})
	}
	return gateway.Series{Bucket: bucket, Since: since, Until: until, Points: points}, nil
}

func (c *Client) Models(_ context.Context, _ gateway.Scope) ([]gateway.ModelStatus, error) {
	return []gateway.ModelStatus{
		{Model: "gpt-4o", State: "available", Requests: 986, Errors: 4, WindowMinutes: 15},
		{Model: "gpt-4o-mini", State: "available", Requests: 1_422, Errors: 8, WindowMinutes: 15},
		{Model: "claude-sonnet", State: "unstable", Requests: 412, Errors: 57, WindowMinutes: 15},
		{Model: "realtime", State: "no_data", Requests: 0, Errors: 0, WindowMinutes: 15},
	}, nil
}

func (c *Client) Keys(_ context.Context, tenant string, offset, limit int64) ([]gateway.Key, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]gateway.Key, 0, len(c.keys))
	for _, key := range c.keys {
		if tenant == "" || key.Tenant == tenant {
			keys = append(keys, cloneJSON(key))
		}
	}
	slices.SortFunc(keys, func(a, b gateway.Key) int { return cmp.Compare(a.AK, b.AK) })
	if offset > 0 {
		keys = keys[min(offset, int64(len(keys))):]
	}
	if limit > 0 && int64(len(keys)) > limit {
		keys = keys[:limit]
	}
	return keys, nil
}

func (c *Client) CreateKey(_ context.Context, actingTenant string, key gateway.Key) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key.AK == "" || key.Product == "" || key.Tenant == "" {
		return errors.New("ak, product and tenant are required")
	}
	// the real gateway answers an uncovered existing ak with 404 (scoped_key anti-probing), never 409
	if existing, ok := c.keys[key.AK]; ok && actingTenant != "" && existing.Tenant != actingTenant {
		return fmt.Errorf("key %s: %w", key.AK, gateway.ErrNotFound)
	}
	key.Status = "active"
	key.Available = true
	c.keys[key.AK] = cloneJSON(key)
	c.record("key_create", key.AK)
	return nil
}

func (c *Client) PatchKey(_ context.Context, actingTenant, ak string, patch map[string]any) (gateway.Key, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key, ok := c.keys[ak]
	if !ok || (actingTenant != "" && key.Tenant != actingTenant) {
		return gateway.Key{}, fmt.Errorf("key %s: %w", ak, gateway.ErrNotFound)
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
	c.keys[ak] = key
	c.record("key_patch", ak)
	return cloneJSON(key), nil
}

func (c *Client) DeleteKey(_ context.Context, actingTenant, ak string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key, ok := c.keys[ak]; !ok || (actingTenant != "" && key.Tenant != actingTenant) {
		return fmt.Errorf("key %s: %w", ak, gateway.ErrNotFound)
	}
	delete(c.keys, ak)
	c.record("key_delete", ak)
	return nil
}

func (c *Client) Instances(context.Context) ([]gateway.Instance, error) {
	return []gateway.Instance{
		{ID: "gw-a", URL: "http://gw-a:8080", Status: "available", LatencyMS: 8, Accounts: []gateway.Account{{Name: "openai-primary", Provider: "openai", Tier: "paygo", Health: "healthy", Protocols: []string{"openai-chat"}}}},
		{ID: "gw-b", URL: "http://gw-b:8080", Status: "available", LatencyMS: 11, Accounts: []gateway.Account{{Name: "anthropic-primary", Provider: "anthropic", Tier: "paygo", Health: "healthy", Protocols: []string{"anthropic-messages"}}}},
	}, nil
}

func (c *Client) Config(context.Context) (gateway.ConfigDocument, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return gateway.ConfigDocument{Version: c.version, YAML: c.yaml}, nil
}

func (c *Client) ValidateConfig(_ context.Context, yaml string) (map[string]any, error) {
	if !strings.Contains(yaml, "listen:") || !strings.Contains(yaml, "models:") {
		return nil, errors.New("invalid config: listen and models are required")
	}
	return map[string]any{"valid": true, "models": strings.Count(yaml, "name:")}, nil
}

func (c *Client) PublishConfig(ctx context.Context, yaml string, expectedVersion int64) (int64, error) {
	if _, err := c.ValidateConfig(ctx, yaml); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if expectedVersion > 0 && expectedVersion != c.version {
		return 0, fmt.Errorf("config head is at version %d: %w", c.version, gateway.ErrConflict)
	}
	c.version++
	c.yaml = yaml
	c.versions = append([]gateway.ConfigVersion{{ID: c.version, CreatedAtEpochSecs: time.Now().Unix()}}, c.versions...)
	c.record("config_publish", fmt.Sprint(c.version))
	return c.version, nil
}

func (c *Client) ConfigVersions(context.Context) ([]gateway.ConfigVersion, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Clone(c.versions), nil
}

func (c *Client) RollbackConfig(_ context.Context, id int64) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !slices.ContainsFunc(c.versions, func(v gateway.ConfigVersion) bool { return v.ID == id }) {
		return 0, fmt.Errorf("config version %d: %w", id, gateway.ErrNotFound)
	}
	c.version++
	c.versions = append([]gateway.ConfigVersion{{ID: c.version, CreatedAtEpochSecs: time.Now().Unix()}}, c.versions...)
	c.record("config_rollback", fmt.Sprint(c.version))
	return c.version, nil
}

func (c *Client) Audit(context.Context) ([]gateway.AuditEntry, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entries := slices.Clone(c.audit)
	slices.SortFunc(entries, func(a, b gateway.AuditEntry) int { return cmp.Compare(b.CreatedAtEpochSecs, a.CreatedAtEpochSecs) })
	return entries, nil
}

func (c *Client) SecurityEvents(_ context.Context, tenant string) ([]gateway.SecurityEvent, error) {
	now := time.Now().Unix()
	if tenant == "" {
		tenant = "acme"
	}
	return []gateway.SecurityEvent{
		{CreatedAtEpochSecs: now - 120, RequestID: "req-42", AK: "ak-acme-alice", UserID: "alice", Tenant: tenant, Surface: "chat", Rule: "dlp", Action: "redact", Hits: 1},
		{CreatedAtEpochSecs: now - 500, RequestID: "req-39", AK: "ak-acme-batch", UserID: "bob", Tenant: tenant, Surface: "batch", Rule: "blocklist", Action: "flag", Hits: 2},
	}, nil
}

func (c *Client) record(action, target string) {
	c.audit = append(c.audit, gateway.AuditEntry{
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
