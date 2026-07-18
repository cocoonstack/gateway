//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cocoonstack/gateway/control-plane/internal/kv"
	kvredis "github.com/cocoonstack/gateway/control-plane/internal/kv/redis"
	"github.com/cocoonstack/gateway/control-plane/internal/user"
	userpostgres "github.com/cocoonstack/gateway/control-plane/internal/user/postgres"
)

func TestPostgresIdentityAndRedisSession(t *testing.T) {
	pgURL := os.Getenv("CP_TEST_PG_URL")
	redisURL := os.Getenv("CP_TEST_REDIS_URL")
	if pgURL == "" || redisURL == "" {
		t.Skip("CP_TEST_PG_URL and CP_TEST_REDIS_URL are required")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	users, err := userpostgres.Connect(ctx, pgURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer users.Close()
	sessions, err := kvredis.Connect(ctx, redisURL)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	defer sessions.Close()

	suffix := time.Now().UnixNano()
	u := user.User{
		ID: fmt.Sprintf("integration-%d", suffix), Email: fmt.Sprintf("integration-%d@example.com", suffix),
		DisplayName: "Integration User", PasswordHash: "test-hash", Tenant: "integration",
		GatewayUserID: "integration-user", Role: user.RoleMember,
		CreatedAt: time.Now().Unix(), UpdatedAt: time.Now().Unix(),
	}
	if err := users.Create(ctx, u); err != nil {
		t.Fatalf("create user: %v", err)
	}
	loaded, err := users.ByEmail(ctx, u.Email)
	if err != nil || loaded.ID != u.ID {
		t.Fatalf("read user: got=%+v err=%v", loaded, err)
	}

	session := kv.Session{
		ID: fmt.Sprintf("session-%d", suffix), UserID: u.ID, CSRFToken: "csrf-integration",
		ExpiresAt: time.Now().Add(time.Minute).Unix(),
	}
	if err := sessions.Put(ctx, session); err != nil {
		t.Fatalf("put session: %v", err)
	}
	loadedSession, err := sessions.Get(ctx, session.ID)
	if err != nil || loadedSession != session {
		t.Fatalf("read session: got=%+v err=%v", loadedSession, err)
	}
	if err := sessions.Delete(ctx, session.ID); err != nil {
		t.Fatalf("delete session: %v", err)
	}
	if _, err := sessions.Get(ctx, session.ID); !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("deleted session error = %v, want %v", err, kv.ErrNotFound)
	}
}
