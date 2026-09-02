// Package config loads control-plane runtime configuration from environment.
package config

import (
	"cmp"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr          string
	StoreDriver         string
	DatabaseURL         string
	KVDriver            string
	RedisURL            string
	GatewayMode         string
	GatewayTargets      string
	GatewayAdminToken   string
	GatewayTenantTokens map[string]string
	WebDir              string
	SessionTTL          time.Duration
	CookieSecure        bool
	TrustedProxy        bool
	DevSeed             bool
	BootstrapEmail      string
	BootstrapPassword   string
	LogLevel            string
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddr:        cmp.Or(os.Getenv("CP_LISTEN"), "127.0.0.1:8090"),
		StoreDriver:       cmp.Or(os.Getenv("CP_STORE"), "memory"),
		DatabaseURL:       os.Getenv("CP_DATABASE_URL"),
		KVDriver:          cmp.Or(os.Getenv("CP_KV"), "memory"),
		RedisURL:          os.Getenv("CP_REDIS_URL"),
		GatewayMode:       cmp.Or(os.Getenv("CP_GATEWAY_MODE"), "mock"),
		GatewayTargets:    cmp.Or(os.Getenv("CP_GATEWAY_TARGETS"), "local=http://127.0.0.1:8080"),
		GatewayAdminToken: os.Getenv("CP_GATEWAY_ADMIN_TOKEN"),
		WebDir:            cmp.Or(os.Getenv("CP_WEB_DIR"), "web/dist"),
		SessionTTL:        12 * time.Hour,
		CookieSecure:      envBool("CP_COOKIE_SECURE", false),
		TrustedProxy:      envBool("CP_TRUSTED_PROXY", false),
		DevSeed:           envBool("CP_DEV_SEED", false),
		BootstrapEmail:    os.Getenv("CP_BOOTSTRAP_ADMIN_EMAIL"),
		BootstrapPassword: os.Getenv("CP_BOOTSTRAP_ADMIN_PASSWORD"),
		LogLevel:          cmp.Or(os.Getenv("CP_LOG_LEVEL"), "info"),
	}
	if raw := os.Getenv("CP_SESSION_TTL"); raw != "" {
		ttl, err := time.ParseDuration(raw)
		if err != nil || ttl <= 0 {
			return Config{}, errors.New("CP_SESSION_TTL must be a positive duration")
		}
		cfg.SessionTTL = ttl
	}
	if cfg.StoreDriver != "memory" && cfg.StoreDriver != "postgres" {
		return Config{}, errors.New("CP_STORE must be memory or postgres")
	}
	if cfg.StoreDriver == "postgres" && cfg.DatabaseURL == "" {
		return Config{}, errors.New("CP_DATABASE_URL is required for postgres")
	}
	if cfg.KVDriver != "memory" && cfg.KVDriver != "redis" {
		return Config{}, errors.New("CP_KV must be memory or redis")
	}
	if cfg.KVDriver == "redis" && cfg.RedisURL == "" {
		return Config{}, errors.New("CP_REDIS_URL is required for redis")
	}
	if cfg.GatewayMode != "mock" && cfg.GatewayMode != "http" {
		return Config{}, errors.New("CP_GATEWAY_MODE must be mock or http")
	}
	if cfg.GatewayMode == "http" && strings.TrimSpace(cfg.GatewayAdminToken) == "" {
		return Config{}, errors.New("CP_GATEWAY_ADMIN_TOKEN is required for http gateway mode")
	}
	tenantTokens, err := parseTenantTokens(os.Getenv("CP_GATEWAY_TENANT_TOKENS"))
	if err != nil {
		return Config{}, err
	}
	cfg.GatewayTenantTokens = tenantTokens
	if (cfg.BootstrapEmail == "") != (cfg.BootstrapPassword == "") {
		return Config{}, errors.New("bootstrap admin email and password must be set together")
	}
	return cfg, nil
}

func parseTenantTokens(raw string) (map[string]string, error) {
	tokens := make(map[string]string)
	for part := range strings.SplitSeq(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		tenant, token, ok := strings.Cut(part, "=")
		if !ok || tenant == "" || token == "" {
			return nil, errors.New("CP_GATEWAY_TENANT_TOKENS entries must be tenant=token")
		}
		tokens[tenant] = token
	}
	return tokens, nil
}

func envBool(name string, fallback bool) bool {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	return err == nil && v
}
