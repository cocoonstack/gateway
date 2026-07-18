// Package kv defines the control-plane session seam.
package kv

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("session not found")

type Session struct {
	ID        string `json:"id"`
	UserID    string `json:"user_id"`
	CSRFToken string `json:"csrf_token"`
	IssuedAt  int64  `json:"issued_at"`
	ExpiresAt int64  `json:"expires_at"`
}

type Sessions interface {
	Put(context.Context, Session) error
	Get(context.Context, string) (Session, error)
	Delete(context.Context, string) error
	Close() error
}
