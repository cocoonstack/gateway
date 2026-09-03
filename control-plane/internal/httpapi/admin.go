package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/cocoonstack/gateway/control-plane/internal/auth"
	"github.com/cocoonstack/gateway/control-plane/internal/gateway"
	"github.com/cocoonstack/gateway/control-plane/internal/user"
)

var patchableKeyFields = map[string]bool{
	"qps": true, "daily_token_quota": true, "tokens_per_minute": true,
	"expires_at_epoch_secs": true, "banned": true, "suspended_until_epoch_secs": true,
}

type userCreate struct {
	Email         string    `json:"email"`
	DisplayName   string    `json:"display_name"`
	Password      string    `json:"password"`
	Tenant        string    `json:"tenant"`
	GatewayUserID string    `json:"gateway_user_id"`
	Role          user.Role `json:"role"`
}

type configPayload struct {
	YAML            string `json:"yaml"`
	ExpectedVersion int64  `json:"expected_version"`
}

func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	users, err := s.users.List(r.Context())
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	for idx := range users {
		users[idx] = publicUser(users[idx])
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	var body userCreate
	if !decodeJSON(w, r, maxJSONBody, &body) {
		return
	}
	hash, err := auth.HashPassword(body.Password)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id, err := auth.RandomToken(12)
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	now := time.Now().Unix()
	u := user.User{
		ID: id, Email: body.Email, DisplayName: body.DisplayName, PasswordHash: hash,
		Tenant: body.Tenant, GatewayUserID: body.GatewayUserID, Role: body.Role,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.users.Create(r.Context(), u); err != nil {
		writeUserSaveError(w, err)
		return
	}
	s.auditLog(r, "user_create", u.Email)
	writeJSON(w, http.StatusCreated, publicUser(u))
}

func (s *Server) patchUser(w http.ResponseWriter, r *http.Request) {
	u, err := s.users.ByID(r.Context(), r.PathValue("id"))
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	var body struct {
		Email         *string    `json:"email"`
		DisplayName   *string    `json:"display_name"`
		Password      *string    `json:"password"`
		Tenant        *string    `json:"tenant"`
		GatewayUserID *string    `json:"gateway_user_id"`
		Role          *user.Role `json:"role"`
		Disabled      *bool      `json:"disabled"`
	}
	if !decodeJSON(w, r, maxJSONBody, &body) {
		return
	}
	if body.Email != nil {
		u.Email = *body.Email
	}
	if body.DisplayName != nil {
		u.DisplayName = *body.DisplayName
	}
	if body.Password != nil && *body.Password != "" {
		u.PasswordHash, err = auth.HashPassword(*body.Password)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		// evict sessions issued before the reset so a stolen cookie dies too
		u.PasswordChangedAt = time.Now().Unix()
	}
	if body.Tenant != nil {
		u.Tenant = *body.Tenant
	}
	if body.GatewayUserID != nil {
		u.GatewayUserID = *body.GatewayUserID
	}
	if body.Role != nil {
		u.Role = *body.Role
	}
	if body.Disabled != nil {
		u.Disabled = *body.Disabled
	}
	u.UpdatedAt = time.Now().Unix()
	if err := s.users.Update(r.Context(), u); err != nil {
		writeUserSaveError(w, err)
		return
	}
	s.auditLog(r, "user_patch", u.ID)
	writeJSON(w, http.StatusOK, publicUser(u))
}

func (s *Server) listKeys(w http.ResponseWriter, r *http.Request) {
	p := current(r)
	tenant := scopedTenant(p, r.URL.Query().Get("tenant"))
	keys, err := s.gateway.Keys(r.Context(), tenant, queryInt(r, "offset", 0), queryInt(r, "limit", 1000))
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

func (s *Server) createKey(w http.ResponseWriter, r *http.Request) {
	var key gateway.Key
	if !decodeJSON(w, r, maxJSONBody, &key) {
		return
	}
	p := current(r)
	key.Tenant = scopedTenant(p, key.Tenant)
	if key.AK == "" || key.Product == "" || key.Tenant == "" {
		writeError(w, http.StatusBadRequest, "ak, product and tenant are required")
		return
	}
	if err := s.gateway.CreateKey(r.Context(), actingTenant(p), key); err != nil {
		mapError(r.Context(), w, err)
		return
	}
	s.auditLog(r, "key_create", key.AK)
	writeJSON(w, http.StatusCreated, map[string]string{"status": "created", "ak": key.AK})
}

func (s *Server) patchKey(w http.ResponseWriter, r *http.Request) {
	ak := r.PathValue("ak")
	var patch map[string]any
	if !decodeJSON(w, r, maxJSONBody, &patch) {
		return
	}
	for field := range patch {
		if !patchableKeyFields[field] {
			writeError(w, http.StatusBadRequest, "unsupported key field: "+field)
			return
		}
	}
	key, err := s.gateway.PatchKey(r.Context(), actingTenant(current(r)), ak, patch)
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	s.auditLog(r, "key_patch", ak)
	writeJSON(w, http.StatusOK, key)
}

func (s *Server) deleteKey(w http.ResponseWriter, r *http.Request) {
	ak := r.PathValue("ak")
	if err := s.gateway.DeleteKey(r.Context(), actingTenant(current(r)), ak); err != nil {
		mapError(r.Context(), w, err)
		return
	}
	s.auditLog(r, "key_delete", ak)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getConfig(w http.ResponseWriter, r *http.Request) {
	doc, err := s.gateway.Config(r.Context())
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func (s *Server) validateConfig(w http.ResponseWriter, r *http.Request) {
	body, ok := configBody(w, r)
	if !ok {
		return
	}
	result, err := s.gateway.ValidateConfig(r.Context(), body.YAML)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) publishConfig(w http.ResponseWriter, r *http.Request) {
	body, ok := configBody(w, r)
	if !ok {
		return
	}
	version, err := s.gateway.PublishConfig(r.Context(), body.YAML, body.ExpectedVersion)
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	s.auditLog(r, "config_publish", strconv.FormatInt(version, 10))
	writeJSON(w, http.StatusOK, map[string]any{"status": "published", "version": version})
}

func (s *Server) configVersions(w http.ResponseWriter, r *http.Request) {
	versions, err := s.gateway.ConfigVersions(r.Context())
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"versions": versions})
}

func (s *Server) rollbackConfig(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid config version")
		return
	}
	version, err := s.gateway.RollbackConfig(r.Context(), id)
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	s.auditLog(r, "config_rollback", strconv.FormatInt(version, 10))
	writeJSON(w, http.StatusOK, map[string]any{"status": "rolled_back", "version": version})
}

func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	p := current(r)
	kind := r.URL.Query().Get("kind")
	if kind == "" {
		kind = "ops"
	}
	if kind == "ops" {
		if p.User.Role != user.RoleSystemAdmin {
			writeError(w, http.StatusForbidden, "system admin role required")
			return
		}
		entries, err := s.gateway.Audit(r.Context())
		if err != nil {
			mapError(r.Context(), w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
		return
	}
	if kind != "security" {
		writeError(w, http.StatusBadRequest, "kind must be ops or security")
		return
	}
	events, err := s.gateway.SecurityEvents(r.Context(), scopedTenant(p, r.URL.Query().Get("tenant")))
	if err != nil {
		mapError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func writeUserSaveError(w http.ResponseWriter, err error) {
	if errors.Is(err, user.ErrConflict) {
		writeError(w, http.StatusConflict, "email already exists")
		return
	}
	writeError(w, http.StatusBadRequest, err.Error())
}

func configBody(w http.ResponseWriter, r *http.Request) (configPayload, bool) {
	var body configPayload
	if !decodeJSON(w, r, maxConfigBody, &body) {
		return configPayload{}, false
	}
	if body.YAML == "" {
		writeError(w, http.StatusBadRequest, "yaml is required")
		return configPayload{}, false
	}
	return body, true
}
