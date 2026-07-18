// Package gateway adapts the Rust gateway admin API for the control plane.
package gateway

import (
	"context"
	"errors"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

type UsageRow struct {
	UserID           string `json:"user_id"`
	Model            string `json:"model"`
	Requests         int64  `json:"requests"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	TotalTokens      int64  `json:"total_tokens"`
	CostMicros       int64  `json:"cost_micros"`
	VendorCostMicros int64  `json:"vendor_cost_micros"`
}

type SeriesPoint struct {
	Start            int64 `json:"start"`
	End              int64 `json:"end"`
	Requests         int64 `json:"requests"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	CostMicros       int64 `json:"cost_micros"`
	VendorCostMicros int64 `json:"vendor_cost_micros"`
}

type Series struct {
	Bucket string        `json:"bucket"`
	Since  int64         `json:"since"`
	Until  int64         `json:"until"`
	Points []SeriesPoint `json:"series"`
}

type ModelStatus struct {
	Model         string `json:"model"`
	State         string `json:"state"`
	Requests      int64  `json:"requests"`
	Errors        int64  `json:"errors"`
	WindowMinutes int64  `json:"window_minutes"`
}

type Key struct {
	AK                      string           `json:"ak"`
	Product                 string           `json:"product"`
	Tenant                  string           `json:"tenant"`
	Owner                   *string          `json:"owner"`
	QPS                     float64          `json:"qps"`
	DailyTokenQuota         int64            `json:"daily_token_quota"`
	TokensPerMinute         *int64           `json:"tokens_per_minute"`
	ExpiresAtEpochSecs      *int64           `json:"expires_at_epoch_secs"`
	Banned                  bool             `json:"banned"`
	SuspendedUntilEpochSecs *int64           `json:"suspended_until_epoch_secs"`
	Status                  string           `json:"status"`
	Available               bool             `json:"available"`
	ModelQuotas             map[string]int64 `json:"model_quotas,omitempty"`
}

type Account struct {
	Name      string   `json:"name"`
	Provider  string   `json:"provider"`
	Priority  int64    `json:"priority"`
	Tier      string   `json:"tier"`
	Health    string   `json:"health"`
	Protocols []string `json:"protocols"`
}

type Instance struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Status    string    `json:"status"`
	LatencyMS int64     `json:"latency_ms"`
	Error     string    `json:"error,omitempty"`
	Accounts  []Account `json:"accounts"`
}

type ConfigDocument struct {
	Version int64  `json:"version"`
	YAML    string `json:"yaml"`
}

type ConfigVersion struct {
	ID                 int64 `json:"id"`
	CreatedAtEpochSecs int64 `json:"created_at_epoch_secs"`
}

type AuditEntry struct {
	CreatedAtEpochSecs int64  `json:"created_at_epoch_secs"`
	Actor              string `json:"actor"`
	Scope              string `json:"scope"`
	Action             string `json:"action"`
	Target             string `json:"target"`
	Summary            string `json:"summary"`
	SourceIP           string `json:"source_ip"`
}

type SecurityEvent struct {
	CreatedAtEpochSecs int64  `json:"created_at_epoch_secs"`
	RequestID          string `json:"request_id"`
	AK                 string `json:"ak"`
	UserID             string `json:"user_id"`
	Tenant             string `json:"tenant"`
	Surface            string `json:"surface"`
	Rule               string `json:"rule"`
	Action             string `json:"action"`
	Hits               int64  `json:"hits"`
}

type Scope struct {
	Tenant string
	User   string
}

// Client is the control plane's only dependency on the Rust gateway.
type Client interface {
	Usage(context.Context, Scope, int64, int64) ([]UsageRow, error)
	UsageSeries(context.Context, Scope, string, int64, int64) (Series, error)
	Models(context.Context, Scope) ([]ModelStatus, error)
	Keys(context.Context, string) ([]Key, error)
	CreateKey(context.Context, Key) error
	PatchKey(context.Context, string, map[string]any) (Key, error)
	DeleteKey(context.Context, string) error
	Instances(context.Context) ([]Instance, error)
	Config(context.Context) (ConfigDocument, error)
	ValidateConfig(context.Context, string) (map[string]any, error)
	PublishConfig(context.Context, string, int64) (int64, error)
	ConfigVersions(context.Context) ([]ConfigVersion, error)
	RollbackConfig(context.Context, int64) (int64, error)
	Audit(context.Context) ([]AuditEntry, error)
	SecurityEvents(context.Context, string) ([]SecurityEvent, error)
}
