// Package user defines control-plane identities and their persistence seam.
package user

import (
	"context"
	"errors"
	"strings"
)

type Role string

const (
	RoleMember      Role = "member"
	RoleTenantAdmin Role = "tenant_admin"
	RoleSystemAdmin Role = "system_admin"
)

var (
	ErrNotFound = errors.New("user not found")
	ErrConflict = errors.New("user already exists")
)

// User is one human control-plane identity. Non-system users belong to one
// gateway tenant; GatewayUserID is the billing attribution key.
type User struct {
	ID            string `json:"id"`
	Email         string `json:"email"`
	DisplayName   string `json:"display_name"`
	PasswordHash  string `json:"-"`
	Tenant        string `json:"tenant"`
	GatewayUserID string `json:"gateway_user_id"`
	Role          Role   `json:"role"`
	Disabled      bool   `json:"disabled"`
	// PasswordChangedAt evicts sessions issued at or before it (epoch secs);
	// zero = never reset.
	PasswordChangedAt int64 `json:"-"`
	CreatedAt         int64 `json:"created_at"`
	UpdatedAt         int64 `json:"updated_at"`
}

func (u User) Validate() error {
	if strings.TrimSpace(u.ID) == "" {
		return errors.New("id is required")
	}
	if NormalizeEmail(u.Email) == "" || !strings.Contains(u.Email, "@") {
		return errors.New("valid email is required")
	}
	if u.PasswordHash == "" {
		return errors.New("password hash is required")
	}
	switch u.Role {
	case RoleMember, RoleTenantAdmin:
		if strings.TrimSpace(u.Tenant) == "" {
			return errors.New("tenant is required for non-system users")
		}
	case RoleSystemAdmin:
	default:
		return errors.New("invalid role")
	}
	return nil
}

func NormalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// Store persists human identities. Implementations must enforce unique email.
type Store interface {
	Create(context.Context, User) error
	ByID(context.Context, string) (User, error)
	ByEmail(context.Context, string) (User, error)
	List(context.Context) ([]User, error)
	Update(context.Context, User) error
}
