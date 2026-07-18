package memory

import (
	"errors"
	"testing"

	"github.com/cocoonstack/gateway/control-plane/internal/user"
)

func TestStoreContract(t *testing.T) {
	ctx := t.Context()
	store := New()
	alice := user.User{
		ID: "u1", Email: "ALICE@example.com", DisplayName: "Alice", PasswordHash: "hash",
		Tenant: "acme", GatewayUserID: "alice", Role: user.RoleMember,
		CreatedAt: 1, UpdatedAt: 1,
	}
	if err := store.Create(ctx, alice); err != nil {
		t.Fatalf("create user: %v", err)
	}
	got, err := store.ByEmail(ctx, " alice@EXAMPLE.com ")
	if err != nil {
		t.Fatalf("find by normalized email: %v", err)
	}
	if got.ID != alice.ID || got.Email != "alice@example.com" {
		t.Errorf("got %+v, want normalized alice", got)
	}
	if err := store.Create(ctx, user.User{ID: "u2", Email: "alice@example.com", PasswordHash: "hash", Tenant: "acme", Role: user.RoleMember}); !errors.Is(err, user.ErrConflict) {
		t.Fatalf("duplicate email error = %v, want conflict", err)
	}
	got.Disabled = true
	got.UpdatedAt = 2
	if err := store.Update(ctx, got); err != nil {
		t.Fatalf("update user: %v", err)
	}
	updated, err := store.ByID(ctx, alice.ID)
	if err != nil || !updated.Disabled {
		t.Fatalf("updated user = %+v, %v", updated, err)
	}
	users, err := store.List(ctx)
	if err != nil || len(users) != 1 {
		t.Fatalf("users = %+v, %v", users, err)
	}
}
