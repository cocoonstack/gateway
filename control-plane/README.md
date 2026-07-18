# Gateway control plane

The control plane is a small Go BFF with a React/TypeScript browser UI. It owns
human identities and browser sessions; it does **not** read or mutate the Rust
gateway's Postgres tables or Redis keys. Usage, billing, model status, access
keys, audit data and configuration all cross the Rust gateway admin HTTP API.

## Roles

| Role | Scope | UI and API access |
| --- | --- | --- |
| `member` | One gateway tenant and user ID | Own usage, charges and model availability |
| `tenant_admin` | One gateway tenant | Tenant usage, key lifecycle and security events |
| `system_admin` | Global | Fleet economics, instances/accounts, users, keys, audit and configuration |

The Go service derives tenant/user filters from the authenticated session. It
never trusts browser-supplied scope for a member or tenant administrator, and it
removes vendor cost from non-system responses.

## Gateway instances

`CP_GATEWAY_TARGETS` is a deliberately simple ordered list such as:

```text
gw-a=http://gateway-a:8080,gw-b=http://gateway-b:8080
```

The first target handles shared admin reads and mutations. Gateway configuration
and keys already converge through the gateway's own Postgres/Redis mechanisms.
The control plane polls every configured target in parallel for `/health` and
`/internal/accounts`, so an operator can distinguish instance reachability from
account-pool health and client-visible model availability. There is no second
service-discovery or leader-election system in this project.

## Local development

The default configuration uses an in-memory identity store, in-memory session
KV and a deterministic gateway mock:

```sh
cd control-plane
CP_DEV_SEED=true go run ./cmd/control-plane
```

In another terminal:

```sh
cd control-plane/web
npm install
npm run dev
```

Open <http://127.0.0.1:5173>. Development accounts are:

| Role | Email | Password |
| --- | --- | --- |
| System admin | `admin@example.com` | `admin12345!` |
| Tenant admin | `manager@example.com` | `manager123!` |
| Member | `user@example.com` | `user12345!` |

These accounts exist only when `CP_DEV_SEED=true` is set explicitly — the
default is always false, for every store backend.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `CP_LISTEN` | `127.0.0.1:8090` | HTTP listen address |
| `CP_STORE` | `memory` | `memory` or `postgres` for control-plane users |
| `CP_DATABASE_URL` | — | Control-plane Postgres URL |
| `CP_KV` | `memory` | `memory` or `redis` for browser sessions |
| `CP_REDIS_URL` | — | Control-plane Redis URL |
| `CP_DEV_SEED` | `false` | Seed fixed demo accounts (never enable in production) |
| `CP_GATEWAY_MODE` | `mock` | `mock` or `http` |
| `CP_GATEWAY_TARGETS` | `local=http://127.0.0.1:8080` | Comma-separated `id=url` targets |
| `CP_GATEWAY_ADMIN_TOKEN` | — | Global Rust gateway admin bearer token |
| `CP_WEB_DIR` | `web/dist` | Built browser assets |
| `CP_SESSION_TTL` | `12h` | Browser session lifetime |
| `CP_COOKIE_SECURE` | `false` | Secure cookie flag; enable behind HTTPS |
| `CP_BOOTSTRAP_ADMIN_EMAIL` | — | Optional first Postgres system admin |
| `CP_BOOTSTRAP_ADMIN_PASSWORD` | — | Password paired with bootstrap email |

The Postgres schema contains only `users`. Redis uses only
`gateway:control-plane:session:*`. Neither connection points at gateway-owned
state.

## Tests and builds

```sh
make test                 # Go unit tests
make web-test             # Vitest plus production UI build
make test-integration     # Local Postgres 16 + Redis 7 containers
make build                # Browser assets plus Go binary
```

The browser E2E suite runs the in-memory/mock stack:

```sh
cd web
npx playwright install chromium
npm run e2e
```

The BFF contract is in [`api/openapi.yaml`](api/openapi.yaml). Rust admin API
details remain in [`../docs/api.md`](../docs/api.md).
