package gateway

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Target struct {
	ID  string
	URL string
}

var _ Client = (*HTTPClient)(nil)

type HTTPClient struct {
	targets    []Target
	adminToken string
	client     *http.Client
}

func NewHTTP(rawTargets, adminToken string) (*HTTPClient, error) {
	targets, err := parseTargets(rawTargets)
	if err != nil {
		return nil, err
	}
	return &HTTPClient{
		targets:    targets,
		adminToken: adminToken,
		client:     &http.Client{Timeout: 8 * time.Second},
	}, nil
}

func (c *HTTPClient) Usage(ctx context.Context, scope Scope, since, until int64) ([]UsageRow, error) {
	q := scopeQuery(scope)
	q.Set("since", strconv.FormatInt(since, 10))
	q.Set("until", strconv.FormatInt(until, 10))
	var resp struct {
		Usage []UsageRow `json:"usage"`
	}
	if err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/usage/users?"+q.Encode(), nil, &resp, true); err != nil {
		return nil, err
	}
	return resp.Usage, nil
}

func (c *HTTPClient) UsageSeries(ctx context.Context, scope Scope, bucket string, since, until int64) (Series, error) {
	q := scopeQuery(scope)
	q.Set("bucket", bucket)
	q.Set("since", strconv.FormatInt(since, 10))
	q.Set("until", strconv.FormatInt(until, 10))
	var series Series
	err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/usage/series?"+q.Encode(), nil, &series, true)
	return series, err
}

func (c *HTTPClient) Models(ctx context.Context, scope Scope) ([]ModelStatus, error) {
	q := scopeQuery(scope)
	path := "/admin/models/status"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	var resp struct {
		Models []ModelStatus `json:"models"`
	}
	if err := c.doJSON(ctx, c.primary(), http.MethodGet, path, nil, &resp, true); err != nil {
		return nil, err
	}
	return resp.Models, nil
}

func (c *HTTPClient) Keys(ctx context.Context, tenant string) ([]Key, error) {
	q := make(url.Values)
	q.Set("limit", "1000")
	if tenant != "" {
		q.Set("tenant", tenant)
	}
	var resp struct {
		Keys []Key `json:"keys"`
	}
	if err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/keys?"+q.Encode(), nil, &resp, true); err != nil {
		return nil, err
	}
	return resp.Keys, nil
}

func (c *HTTPClient) CreateKey(ctx context.Context, key Key) error {
	return c.doJSON(ctx, c.primary(), http.MethodPost, "/admin/keys", key, nil, true)
}

func (c *HTTPClient) PatchKey(ctx context.Context, ak string, patch map[string]any) (Key, error) {
	var key Key
	err := c.doJSON(ctx, c.primary(), http.MethodPatch, "/admin/keys/"+url.PathEscape(ak), patch, &key, true)
	return key, err
}

func (c *HTTPClient) DeleteKey(ctx context.Context, ak string) error {
	return c.doJSON(ctx, c.primary(), http.MethodDelete, "/admin/keys/"+url.PathEscape(ak), nil, nil, true)
}

func (c *HTTPClient) Instances(ctx context.Context) ([]Instance, error) {
	ch := make(chan Instance, len(c.targets))
	for _, target := range c.targets {
		go func() {
			started := time.Now()
			instance := Instance{ID: target.ID, URL: target.URL, Status: "unavailable", Accounts: []Account{}}
			var health struct {
				Status string `json:"status"`
			}
			if err := c.doJSON(ctx, target, http.MethodGet, "/health", nil, &health, false); err != nil {
				instance.Error = err.Error()
				instance.LatencyMS = time.Since(started).Milliseconds()
				ch <- instance
				return
			}
			var accounts struct {
				Accounts []Account `json:"accounts"`
			}
			if err := c.doJSON(ctx, target, http.MethodGet, "/internal/accounts", nil, &accounts, false); err != nil {
				instance.Status = "degraded"
				instance.Error = err.Error()
			} else {
				instance.Status = "available"
				instance.Accounts = accounts.Accounts
			}
			instance.LatencyMS = time.Since(started).Milliseconds()
			ch <- instance
		}()
	}
	instances := make([]Instance, 0, len(c.targets))
	for range c.targets {
		instances = append(instances, <-ch)
	}
	slices.SortFunc(instances, func(a, b Instance) int { return cmp.Compare(a.ID, b.ID) })
	return instances, nil
}

func (c *HTTPClient) Config(ctx context.Context) (ConfigDocument, error) {
	var doc ConfigDocument
	err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/config", nil, &doc, true)
	return doc, err
}

func (c *HTTPClient) ValidateConfig(ctx context.Context, yaml string) (map[string]any, error) {
	var result map[string]any
	err := c.doText(ctx, c.primary(), http.MethodPost, "/admin/config/validate", yaml, &result)
	return result, err
}

func (c *HTTPClient) PublishConfig(ctx context.Context, yaml string, expectedVersion int64) (int64, error) {
	path := "/admin/config"
	if expectedVersion > 0 {
		path += "?expected_version=" + strconv.FormatInt(expectedVersion, 10)
	}
	var result struct {
		Version int64 `json:"version"`
	}
	err := c.doText(ctx, c.primary(), http.MethodPut, path, yaml, &result)
	return result.Version, err
}

func (c *HTTPClient) ConfigVersions(ctx context.Context) ([]ConfigVersion, error) {
	var result struct {
		Versions []ConfigVersion `json:"versions"`
	}
	err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/config/versions", nil, &result, true)
	return result.Versions, err
}

func (c *HTTPClient) RollbackConfig(ctx context.Context, id int64) (int64, error) {
	var result struct {
		Version int64 `json:"version"`
	}
	path := fmt.Sprintf("/admin/config/versions/%d/rollback", id)
	err := c.doJSON(ctx, c.primary(), http.MethodPost, path, nil, &result, true)
	return result.Version, err
}

func (c *HTTPClient) Audit(ctx context.Context) ([]AuditEntry, error) {
	var result struct {
		Entries []AuditEntry `json:"entries"`
	}
	err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/audit/ops?limit=200", nil, &result, true)
	return result.Entries, err
}

func (c *HTTPClient) SecurityEvents(ctx context.Context, tenant string) ([]SecurityEvent, error) {
	q := make(url.Values)
	q.Set("limit", "200")
	if tenant != "" {
		q.Set("tenant", tenant)
	}
	var result struct {
		Events []SecurityEvent `json:"events"`
	}
	err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/audit/events?"+q.Encode(), nil, &result, true)
	return result.Events, err
}

func (c *HTTPClient) primary() Target { return c.targets[0] }

func (c *HTTPClient) doJSON(ctx context.Context, target Target, method, path string, input, output any, admin bool) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode gateway request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, target.URL+path, body)
	if err != nil {
		return fmt.Errorf("create gateway request: %w", err)
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if admin {
		req.Header.Set("Authorization", "Bearer "+c.adminToken)
	}
	return c.send(req, output)
}

func (c *HTTPClient) doText(ctx context.Context, target Target, method, path, input string, output any) error {
	req, err := http.NewRequestWithContext(ctx, method, target.URL+path, strings.NewReader(input))
	if err != nil {
		return fmt.Errorf("create gateway request: %w", err)
	}
	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("Authorization", "Bearer "+c.adminToken)
	return c.send(req, output)
}

func (c *HTTPClient) send(req *http.Request, output any) error {
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request gateway: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read gateway response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var envelope struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(body, &envelope)
		message := strings.TrimSpace(envelope.Error.Message)
		if message == "" {
			message = strings.TrimSpace(string(body))
		}
		switch resp.StatusCode {
		case http.StatusNotFound:
			return fmt.Errorf("%w: %s", ErrNotFound, message)
		case http.StatusConflict:
			return fmt.Errorf("%w: %s", ErrConflict, message)
		}
		return fmt.Errorf("gateway %s: %s", resp.Status, message)
	}
	if output != nil && len(body) > 0 {
		if err := json.Unmarshal(body, output); err != nil {
			return fmt.Errorf("decode gateway response: %w", err)
		}
	}
	return nil
}

func parseTargets(raw string) ([]Target, error) {
	parts := strings.Split(raw, ",")
	targets := make([]Target, 0, len(parts))
	seen := make(map[string]struct{})
	for _, part := range parts {
		id, endpoint, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || endpoint == "" {
			return nil, fmt.Errorf("CP_GATEWAY_TARGETS entries must be id=url")
		}
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("gateway target %s has an invalid URL", id)
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("duplicate gateway target %s", id)
		}
		seen[id] = struct{}{}
		targets = append(targets, Target{ID: id, URL: strings.TrimRight(endpoint, "/")})
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("at least one gateway target is required")
	}
	return targets, nil
}

func scopeQuery(scope Scope) url.Values {
	q := make(url.Values)
	if scope.Tenant != "" {
		q.Set("tenant", scope.Tenant)
	}
	if scope.User != "" {
		q.Set("user", scope.User)
	}
	return q
}
