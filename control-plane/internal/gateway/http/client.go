// Package http reaches the Rust gateway admin API over HTTP.
package http

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cocoonstack/gateway/control-plane/internal/gateway"
)

const (
	requestTimeout      = 8 * time.Second
	maxIdleConns        = 32
	maxIdleConnsPerHost = 8
)

type Target struct {
	ID  string
	URL string
}

var _ gateway.Client = (*Client)(nil)

type Client struct {
	targets      []Target
	adminToken   string
	tenantTokens map[string]string
	client       *http.Client
}

func New(rawTargets, adminToken string, tenantTokens map[string]string) (*Client, error) {
	targets, err := parseTargets(rawTargets)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxIdleConns
	transport.MaxIdleConnsPerHost = maxIdleConnsPerHost
	return &Client{
		targets:      targets,
		adminToken:   adminToken,
		tenantTokens: tenantTokens,
		client:       &http.Client{Timeout: requestTimeout, Transport: transport},
	}, nil
}

func (c *Client) Usage(ctx context.Context, scope gateway.Scope, since, until int64) ([]gateway.UsageRow, error) {
	q := scopeQuery(scope)
	q.Set("since", strconv.FormatInt(since, 10))
	q.Set("until", strconv.FormatInt(until, 10))
	var resp struct {
		Usage []gateway.UsageRow `json:"usage"`
	}
	if err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/usage/users?"+q.Encode(), nil, &resp, c.readBearer(scope.Tenant)); err != nil {
		return nil, err
	}
	return resp.Usage, nil
}

func (c *Client) UsageSeries(ctx context.Context, scope gateway.Scope, bucket string, since, until int64) (gateway.Series, error) {
	q := scopeQuery(scope)
	q.Set("bucket", bucket)
	q.Set("since", strconv.FormatInt(since, 10))
	q.Set("until", strconv.FormatInt(until, 10))
	var series gateway.Series
	err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/usage/series?"+q.Encode(), nil, &series, c.readBearer(scope.Tenant))
	return series, err
}

func (c *Client) Models(ctx context.Context, scope gateway.Scope) ([]gateway.ModelStatus, error) {
	q := scopeQuery(scope)
	path := "/admin/models/status"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	var resp struct {
		Models []gateway.ModelStatus `json:"models"`
	}
	if err := c.doJSON(ctx, c.primary(), http.MethodGet, path, nil, &resp, c.readBearer(scope.Tenant)); err != nil {
		return nil, err
	}
	return resp.Models, nil
}

func (c *Client) Keys(ctx context.Context, tenant string, offset, limit int64) ([]gateway.Key, error) {
	q := make(url.Values)
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.FormatInt(limit, 10))
	}
	if tenant != "" {
		q.Set("tenant", tenant)
	}
	var resp struct {
		Keys []gateway.Key `json:"keys"`
	}
	if err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/keys?"+q.Encode(), nil, &resp, c.readBearer(tenant)); err != nil {
		return nil, err
	}
	return resp.Keys, nil
}

func (c *Client) CreateKey(ctx context.Context, actingTenant string, key gateway.Key) error {
	bearer, err := c.mutateBearer(actingTenant)
	if err != nil {
		return err
	}
	return c.doJSON(ctx, c.primary(), http.MethodPost, "/admin/keys", key, nil, bearer)
}

func (c *Client) PatchKey(ctx context.Context, actingTenant, ak string, patch map[string]any) (gateway.Key, error) {
	bearer, err := c.mutateBearer(actingTenant)
	if err != nil {
		return gateway.Key{}, err
	}
	var key gateway.Key
	err = c.doJSON(ctx, c.primary(), http.MethodPatch, "/admin/keys/"+url.PathEscape(ak), patch, &key, bearer)
	return key, err
}

func (c *Client) DeleteKey(ctx context.Context, actingTenant, ak string) error {
	bearer, err := c.mutateBearer(actingTenant)
	if err != nil {
		return err
	}
	return c.doJSON(ctx, c.primary(), http.MethodDelete, "/admin/keys/"+url.PathEscape(ak), nil, nil, bearer)
}

func (c *Client) Instances(ctx context.Context) ([]gateway.Instance, error) {
	ch := make(chan gateway.Instance, len(c.targets))
	for _, target := range c.targets {
		go func() {
			started := time.Now()
			instance := gateway.Instance{ID: target.ID, URL: target.URL, Status: "unavailable", Accounts: []gateway.Account{}}
			var health struct {
				Status string `json:"status"`
			}
			if err := c.doJSON(ctx, target, http.MethodGet, "/health", nil, &health, ""); err != nil {
				instance.Error = err.Error()
				instance.LatencyMS = time.Since(started).Milliseconds()
				ch <- instance
				return
			}
			var accounts struct {
				Accounts []gateway.Account `json:"accounts"`
			}
			if err := c.doJSON(ctx, target, http.MethodGet, "/internal/accounts", nil, &accounts, c.adminToken); err != nil {
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
	instances := make([]gateway.Instance, 0, len(c.targets))
	for range c.targets {
		instances = append(instances, <-ch)
	}
	slices.SortFunc(instances, func(a, b gateway.Instance) int { return cmp.Compare(a.ID, b.ID) })
	return instances, nil
}

func (c *Client) Config(ctx context.Context) (gateway.ConfigDocument, error) {
	var doc gateway.ConfigDocument
	err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/config", nil, &doc, c.adminToken)
	return doc, err
}

func (c *Client) ValidateConfig(ctx context.Context, yaml string) (map[string]any, error) {
	var result map[string]any
	err := c.doText(ctx, c.primary(), http.MethodPost, "/admin/config/validate", yaml, &result)
	return result, err
}

func (c *Client) PublishConfig(ctx context.Context, yaml string, expectedVersion int64) (int64, error) {
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

func (c *Client) ConfigVersions(ctx context.Context) ([]gateway.ConfigVersion, error) {
	var result struct {
		Versions []gateway.ConfigVersion `json:"versions"`
	}
	err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/config/versions", nil, &result, c.adminToken)
	return result.Versions, err
}

func (c *Client) RollbackConfig(ctx context.Context, id int64) (int64, error) {
	var result struct {
		Version int64 `json:"version"`
	}
	path := fmt.Sprintf("/admin/config/versions/%d/rollback", id)
	err := c.doJSON(ctx, c.primary(), http.MethodPost, path, nil, &result, c.adminToken)
	return result.Version, err
}

func (c *Client) Audit(ctx context.Context) ([]gateway.AuditEntry, error) {
	var result struct {
		Entries []gateway.AuditEntry `json:"entries"`
	}
	err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/audit/ops?limit=200", nil, &result, c.adminToken)
	return result.Entries, err
}

func (c *Client) SecurityEvents(ctx context.Context, tenant string) ([]gateway.SecurityEvent, error) {
	q := make(url.Values)
	q.Set("limit", "200")
	if tenant != "" {
		q.Set("tenant", tenant)
	}
	var result struct {
		Events []gateway.SecurityEvent `json:"events"`
	}
	err := c.doJSON(ctx, c.primary(), http.MethodGet, "/admin/audit/events?"+q.Encode(), nil, &result, c.readBearer(tenant))
	return result.Events, err
}

func (c *Client) primary() Target { return c.targets[0] }

func (c *Client) readBearer(tenant string) string {
	if token, ok := c.tenantTokens[tenant]; ok {
		return token
	}
	return c.adminToken
}

// mutateBearer fails closed: a global-token fallback would erase the tenant boundary the gateway enforces.
func (c *Client) mutateBearer(actingTenant string) (string, error) {
	if actingTenant == "" {
		return c.adminToken, nil
	}
	token, ok := c.tenantTokens[actingTenant]
	if !ok {
		return "", fmt.Errorf("no gateway admin token configured for tenant %s", actingTenant)
	}
	return token, nil
}

func (c *Client) doJSON(ctx context.Context, target Target, method, path string, input, output any, bearer string) error {
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
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if rid := gateway.RequestIDFrom(ctx); rid != "" {
		req.Header.Set("X-Request-ID", rid)
	}
	return c.send(req, output)
}

func (c *Client) doText(ctx context.Context, target Target, method, path, input string, output any) error {
	req, err := http.NewRequestWithContext(ctx, method, target.URL+path, strings.NewReader(input))
	if err != nil {
		return fmt.Errorf("create gateway request: %w", err)
	}
	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("Authorization", "Bearer "+c.adminToken)
	if rid := gateway.RequestIDFrom(ctx); rid != "" {
		req.Header.Set("X-Request-ID", rid)
	}
	return c.send(req, output)
}

func (c *Client) send(req *http.Request, output any) (err error) {
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request gateway: %w", err)
	}
	defer func() { err = errors.Join(err, resp.Body.Close()) }()
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
			return fmt.Errorf("%w: %s", gateway.ErrNotFound, message)
		case http.StatusConflict:
			return fmt.Errorf("%w: %s", gateway.ErrConflict, message)
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
			return nil, errors.New("CP_GATEWAY_TARGETS entries must be id=url")
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
		return nil, errors.New("at least one gateway target is required")
	}
	return targets, nil
}

func scopeQuery(scope gateway.Scope) url.Values {
	q := make(url.Values)
	if scope.Tenant != "" {
		q.Set("tenant", scope.Tenant)
	}
	if scope.User != "" {
		q.Set("user", scope.User)
	}
	return q
}
