package httpapi

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/cocoonstack/gateway/control-plane/internal/auth"
	"github.com/cocoonstack/gateway/control-plane/internal/kv"
	"github.com/cocoonstack/gateway/control-plane/internal/user"
)

// dummyHash keeps unknown-email logins as expensive as real ones, so response
// time does not reveal which emails exist.
var dummyHash = sync.OnceValue(func() string {
	hash, err := auth.HashPassword("control-plane-timing-decoy")
	if err != nil {
		return ""
	}
	return hash
})

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "gateway-control-plane"})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if !decodeJSON(w, r, maxJSONBody, &body) {
		return
	}
	throttleKey := clientIP(r) + "|" + user.NormalizeEmail(body.Email)
	if !s.throttle.allow(throttleKey, time.Now()) {
		writeError(w, http.StatusTooManyRequests, "too many login attempts; retry later")
		return
	}
	u, err := s.users.ByEmail(r.Context(), body.Email)
	if err != nil {
		auth.VerifyPassword(dummyHash(), body.Password)
		writeError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}
	if !auth.VerifyPassword(u.PasswordHash, body.Password) || u.Disabled {
		writeError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}
	s.throttle.reset(throttleKey)
	sessionID, err := auth.RandomToken(32)
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	csrf, err := auth.RandomToken(24)
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	now := time.Now()
	session := kv.Session{
		ID: sessionID, UserID: u.ID, CSRFToken: csrf,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(s.sessionTTL).Unix(),
	}
	if err := s.sessions.Put(r.Context(), session); err != nil {
		mapError(r.Context(), w, err)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: session.ID, Path: "/", HttpOnly: true,
		Secure: s.cookieSecure, SameSite: http.SameSiteLaxMode,
		MaxAge: int(s.sessionTTL.Seconds()), Expires: time.Unix(session.ExpiresAt, 0),
	})
	writeJSON(w, http.StatusOK, map[string]any{"user": publicUser(u), "csrf_token": csrf})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	p := current(r)
	if err := s.sessions.Delete(r.Context(), p.Session.ID); err != nil && !errors.Is(err, kv.ErrNotFound) {
		mapError(r.Context(), w, err)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Path: "/", HttpOnly: true, Secure: s.cookieSecure,
		SameSite: http.SameSiteLaxMode, MaxAge: -1, Expires: time.Unix(1, 0),
	})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) session(w http.ResponseWriter, r *http.Request) {
	p := current(r)
	writeJSON(w, http.StatusOK, map[string]any{
		"user": publicUser(p.User), "csrf_token": p.Session.CSRFToken,
	})
}
