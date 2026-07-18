// Package memory provides the dependency-free development identity store.
package memory

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"github.com/cocoonstack/gateway/control-plane/internal/user"
)

var _ user.Store = (*Store)(nil)

type Store struct {
	mu      sync.RWMutex
	byID    map[string]user.User
	byEmail map[string]string
}

func New() *Store {
	return &Store{
		byID:    make(map[string]user.User),
		byEmail: make(map[string]string),
	}
}

func (s *Store) Create(_ context.Context, u user.User) error {
	if err := u.Validate(); err != nil {
		return err
	}
	u.Email = user.NormalizeEmail(u.Email)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[u.ID]; ok {
		return user.ErrConflict
	}
	if _, ok := s.byEmail[u.Email]; ok {
		return user.ErrConflict
	}
	s.byID[u.ID] = u
	s.byEmail[u.Email] = u.ID
	return nil
}

func (s *Store) ByID(_ context.Context, id string) (user.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.byID[id]
	if !ok {
		return user.User{}, user.ErrNotFound
	}
	return u, nil
}

func (s *Store) ByEmail(_ context.Context, email string) (user.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byEmail[user.NormalizeEmail(email)]
	if !ok {
		return user.User{}, user.ErrNotFound
	}
	return s.byID[id], nil
}

func (s *Store) List(_ context.Context) ([]user.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	users := make([]user.User, 0, len(s.byID))
	for _, u := range s.byID {
		users = append(users, u)
	}
	slices.SortFunc(users, func(a, b user.User) int { return cmp.Compare(a.Email, b.Email) })
	return users, nil
}

func (s *Store) Update(_ context.Context, u user.User) error {
	if err := u.Validate(); err != nil {
		return err
	}
	u.Email = user.NormalizeEmail(u.Email)
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.byID[u.ID]
	if !ok {
		return user.ErrNotFound
	}
	if id, ok := s.byEmail[u.Email]; ok && id != u.ID {
		return user.ErrConflict
	}
	delete(s.byEmail, old.Email)
	s.byID[u.ID] = u
	s.byEmail[u.Email] = u.ID
	return nil
}
