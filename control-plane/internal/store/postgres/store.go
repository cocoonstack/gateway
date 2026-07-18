// Package postgres provides the production identity store.
package postgres

import (
	"cmp"
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cocoonstack/gateway/control-plane/internal/user"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const userSelect = `SELECT id, email, display_name, password_hash, tenant,
 gateway_user_id, role, disabled, password_changed_at, created_at, updated_at FROM users`

var _ user.Store = (*Store)(nil)

type Store struct {
	pool *pgxpool.Pool
}

func Connect(ctx context.Context, rawURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, rawURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"); err != nil {
		return fmt.Errorf("create schema migrations: %w", err)
	}
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return cmp.Compare(a.Name(), b.Name()) })
	for _, entry := range entries {
		name := entry.Name()
		var applied bool
		if err := s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = $1)", name).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if applied {
			continue
		}
		body, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (name) VALUES ($1)", name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}
	return nil
}

func (s *Store) Create(ctx context.Context, u user.User) error {
	if err := u.Validate(); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO users (id, email, display_name, password_hash, tenant,
		 gateway_user_id, role, disabled, password_changed_at, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		u.ID, user.NormalizeEmail(u.Email), u.DisplayName, u.PasswordHash, u.Tenant,
		u.GatewayUserID, u.Role, u.Disabled, u.PasswordChangedAt, u.CreatedAt, u.UpdatedAt,
	)
	if err != nil {
		if isDuplicate(err) {
			return user.ErrConflict
		}
		return fmt.Errorf("create user: %w", err)
	}
	return nil
}

func (s *Store) ByID(ctx context.Context, id string) (user.User, error) {
	return scanUser(s.pool.QueryRow(ctx, userSelect+" WHERE id = $1", id))
}

func (s *Store) ByEmail(ctx context.Context, email string) (user.User, error) {
	return scanUser(s.pool.QueryRow(ctx, userSelect+" WHERE email = $1", user.NormalizeEmail(email)))
}

func (s *Store) List(ctx context.Context) ([]user.User, error) {
	rows, err := s.pool.Query(ctx, userSelect+" ORDER BY email")
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()
	users := make([]user.User, 0)
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate users: %w", err)
	}
	return users, nil
}

func (s *Store) Update(ctx context.Context, u user.User) error {
	if err := u.Validate(); err != nil {
		return err
	}
	result, err := s.pool.Exec(ctx,
		`UPDATE users SET email=$2, display_name=$3, password_hash=$4, tenant=$5,
		 gateway_user_id=$6, role=$7, disabled=$8, password_changed_at=$9, updated_at=$10 WHERE id=$1`,
		u.ID, user.NormalizeEmail(u.Email), u.DisplayName, u.PasswordHash, u.Tenant,
		u.GatewayUserID, u.Role, u.Disabled, u.PasswordChangedAt, u.UpdatedAt,
	)
	if err != nil {
		if isDuplicate(err) {
			return user.ErrConflict
		}
		return fmt.Errorf("update user: %w", err)
	}
	if result.RowsAffected() == 0 {
		return user.ErrNotFound
	}
	return nil
}

type scanner interface {
	Scan(...any) error
}

func isDuplicate(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func scanUser(row scanner) (user.User, error) {
	var u user.User
	if err := row.Scan(
		&u.ID, &u.Email, &u.DisplayName, &u.PasswordHash, &u.Tenant,
		&u.GatewayUserID, &u.Role, &u.Disabled, &u.PasswordChangedAt, &u.CreatedAt, &u.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return user.User{}, user.ErrNotFound
		}
		return user.User{}, fmt.Errorf("scan user: %w", err)
	}
	return u, nil
}
