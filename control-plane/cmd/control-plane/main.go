package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/types"

	"github.com/cocoonstack/gateway/control-plane/internal/auth"
	"github.com/cocoonstack/gateway/control-plane/internal/config"
	"github.com/cocoonstack/gateway/control-plane/internal/gateway"
	"github.com/cocoonstack/gateway/control-plane/internal/httpapi"
	"github.com/cocoonstack/gateway/control-plane/internal/kv"
	kvmemory "github.com/cocoonstack/gateway/control-plane/internal/kv/memory"
	kvredis "github.com/cocoonstack/gateway/control-plane/internal/kv/redis"
	storememory "github.com/cocoonstack/gateway/control-plane/internal/store/memory"
	storepostgres "github.com/cocoonstack/gateway/control-plane/internal/store/postgres"
	"github.com/cocoonstack/gateway/control-plane/internal/user"
)

func main() {
	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := log.SetupLog(ctx, &types.ServerLogConfig{Level: cfg.LogLevel}, ""); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		log.WithFunc("main.run").Error(ctx, err, "control plane stopped")
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config) error {
	users, closeUsers, err := buildUserStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeUsers()
	sessions, err := buildSessionStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer sessions.Close()
	gw, err := buildGateway(cfg)
	if err != nil {
		return err
	}
	if err := seedUsers(ctx, users, cfg); err != nil {
		return err
	}

	api := httpapi.New(users, sessions, gw, cfg.SessionTTL, cfg.CookieSecure, cfg.WebDir)
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.WithFunc("main.shutdown").Error(shutdownCtx, err, "drain HTTP server")
		}
	}()
	log.WithFunc("main.run").Infof(ctx, "control plane listening on http://%s", cfg.ListenAddr)
	err = server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve control plane: %w", err)
	}
	<-done
	return nil
}

func buildUserStore(ctx context.Context, cfg config.Config) (user.Store, func(), error) {
	if cfg.StoreDriver == "memory" {
		return storememory.New(), func() {}, nil
	}
	store, err := storepostgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, err
	}
	return store, store.Close, nil
}

func buildSessionStore(ctx context.Context, cfg config.Config) (kv.Sessions, error) {
	if cfg.KVDriver == "memory" {
		return kvmemory.New(), nil
	}
	return kvredis.Connect(ctx, cfg.RedisURL)
}

func buildGateway(cfg config.Config) (gateway.Client, error) {
	if cfg.GatewayMode == "mock" {
		return gateway.NewMock(), nil
	}
	return gateway.NewHTTP(cfg.GatewayTargets, cfg.GatewayAdminToken)
}

func seedUsers(ctx context.Context, store user.Store, cfg config.Config) error {
	if cfg.DevSeed {
		seeds := []userSeed{
			{"dev-admin", "admin@example.com", "System Admin", "admin12345!", "", "", user.RoleSystemAdmin},
			{"dev-tenant-admin", "manager@example.com", "Acme Admin", "manager123!", "acme", "", user.RoleTenantAdmin},
			{"dev-member", "user@example.com", "Alice Chen", "user12345!", "acme", "alice", user.RoleMember},
		}
		for _, seed := range seeds {
			if err := ensureUser(ctx, store, seed); err != nil {
				return err
			}
		}
	}
	if cfg.BootstrapEmail != "" {
		seed := userSeed{"bootstrap-admin", cfg.BootstrapEmail, "System Admin", cfg.BootstrapPassword, "", "", user.RoleSystemAdmin}
		if err := ensureUser(ctx, store, seed); err != nil {
			return err
		}
	}
	return nil
}

type userSeed struct {
	id, email, displayName, password, tenant, gatewayUserID string
	role                                                    user.Role
}

func ensureUser(ctx context.Context, store user.Store, seed userSeed) error {
	if _, err := store.ByEmail(ctx, seed.email); err == nil {
		return nil
	} else if !errors.Is(err, user.ErrNotFound) {
		return err
	}
	hash, err := auth.HashPassword(seed.password)
	if err != nil {
		return fmt.Errorf("hash seed password: %w", err)
	}
	now := time.Now().Unix()
	if err := store.Create(ctx, user.User{
		ID: seed.id, Email: seed.email, DisplayName: seed.displayName, PasswordHash: hash,
		Tenant: seed.tenant, GatewayUserID: seed.gatewayUserID, Role: seed.role,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return fmt.Errorf("seed user %s: %w", seed.email, err)
	}
	return nil
}
