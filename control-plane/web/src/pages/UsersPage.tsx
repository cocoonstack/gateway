import { type FormEvent, type ReactElement, useState } from "react";
import { useAuth } from "../App";
import { api, jsonBody } from "../api";
import { dateTime, roleLabel } from "../format";
import { useAPI, useAction } from "../hooks";
import type { Cap, Role, User, UserBudget } from "../types";
import { Card, ErrorNotice, FormModal, Loading, PageHeader, Status } from "../components/UI";

type CapMode = "inherit" | "limit" | "unlimited";
type CapDraft = { mode: CapMode; amount: string };

export default function UsersPage(): ReactElement {
  const { session } = useAuth();
  const system = session.user.role === "system_admin";
  const { data, error, reload } = useAPI<{ users: User[] }>("/api/v1/admin/users");
  const [creating, setCreating] = useState(false);
  const [budgetFor, setBudgetFor] = useState<User | null>(null);
  const action = useAction();

  function toggle(user: User) {
    void action.run(async () => {
      await api(`/api/v1/admin/users/${user.id}`, { method: "PATCH", ...jsonBody({ disabled: !user.disabled }) });
      reload();
    });
  }

  return (
    <>
      <PageHeader eyebrow="Identity" title="Users & roles" description="Human access to the control plane and each user's gateway budget. Gateway API keys remain a separate credential domain." actions={system && <button className="button primary" onClick={() => setCreating(true)}>Add user</button>} />
      {(error || action.error) && <ErrorNotice message={error || action.error} />}
      {creating && <CreateUser onClose={() => setCreating(false)} onCreated={() => { setCreating(false); reload(); }} />}
      {budgetFor && <EditBudget user={budgetFor} onClose={() => setBudgetFor(null)} />}
      {!data ? <Loading /> : (
        <Card><div className="table-wrap"><table><thead><tr><th>User</th><th>Role</th><th>Tenant</th><th>Gateway identity</th><th>Status</th><th>Created</th><th /></tr></thead><tbody>
          {data.users.map((user) => <tr key={user.id}><td><strong>{user.display_name}</strong><small className="cell-sub">{user.email}</small></td><td>{roleLabel(user.role)}</td><td>{user.tenant || "Global"}</td><td>{user.gateway_user_id || "—"}</td><td><Status value={user.disabled ? "disabled" : "active"} /></td><td>{dateTime(user.created_at)}</td><td><div className="row-actions">{user.tenant && <button onClick={() => setBudgetFor(user)}>Budget</button>}{system && <button onClick={() => toggle(user)}>{user.disabled ? "Enable" : "Disable"}</button>}</div></td></tr>)}
        </tbody></table></div></Card>
      )}
    </>
  );
}

function CreateUser({ onClose, onCreated }: { onClose: () => void; onCreated: () => void }) {
  const [form, setForm] = useState<{ email: string; display_name: string; password: string; role: Role; tenant: string; gateway_user_id: string }>({ email: "", display_name: "", password: "", role: "member", tenant: "", gateway_user_id: "" });
  const { run, busy, error } = useAction();
  function submit(event: FormEvent) {
    event.preventDefault();
    void run(async () => {
      await api("/api/v1/admin/users", { method: "POST", ...jsonBody(form) });
      onCreated();
    });
  }
  const system = form.role === "system_admin";
  return (
    <FormModal eyebrow="Identity" title="Add control-plane user" busy={busy} error={error} submitLabel="Add user" busyLabel="Creating…" onClose={onClose} onSubmit={submit}>
      <label>Display name<input value={form.display_name} onChange={(event) => setForm({ ...form, display_name: event.target.value })} required /></label>
      <label>Email<input type="email" value={form.email} onChange={(event) => setForm({ ...form, email: event.target.value })} required /></label>
      <label>Password<input type="password" minLength={10} value={form.password} onChange={(event) => setForm({ ...form, password: event.target.value })} required /></label>
      <label>Role<select value={form.role} onChange={(event) => setForm({ ...form, role: event.target.value as Role })}><option value="member">Member</option><option value="tenant_admin">Tenant admin</option><option value="system_admin">System admin</option></select></label>
      <label>Tenant<input disabled={system} value={form.tenant} onChange={(event) => setForm({ ...form, tenant: event.target.value })} required={!system} /></label>
      <label>Gateway user id<input disabled={system} value={form.gateway_user_id} onChange={(event) => setForm({ ...form, gateway_user_id: event.target.value })} placeholder="Billing attribution" /></label>
    </FormModal>
  );
}

function EditBudget({ user, onClose }: { user: User; onClose: () => void }) {
  const { data, error } = useAPI<UserBudget>(`/api/v1/admin/users/${encodeURIComponent(user.id)}/budget`);
  if (!data) {
    return <FormModal eyebrow="Budget" title={user.display_name} busy error={error} submitLabel="Save" busyLabel="Loading…" onClose={onClose} onSubmit={(event) => event.preventDefault()}><Loading /></FormModal>;
  }
  return <BudgetForm user={user} budget={data} onClose={onClose} />;
}

function BudgetForm({ user, budget, onClose }: { user: User; budget: UserBudget; onClose: () => void }) {
  const [daily, setDaily] = useState(draft(budget.daily_cost_quota_micros, 1_000_000));
  const [monthly, setMonthly] = useState(draft(budget.monthly_cost_quota_micros, 1_000_000));
  const [tokens, setTokens] = useState(draft(budget.daily_token_quota, 1));
  const { run, busy, error } = useAction();
  const path = `/api/v1/admin/users/${encodeURIComponent(user.id)}/budget`;

  function submit(event: FormEvent) {
    event.preventDefault();
    void run(async () => {
      await api(path, { method: "PUT", ...jsonBody({ daily_cost_quota_micros: cap(daily, 1_000_000), monthly_cost_quota_micros: cap(monthly, 1_000_000), daily_token_quota: cap(tokens, 1) }) });
      onClose();
    });
  }

  function reset() {
    void run(async () => {
      await api(path, { method: "DELETE" });
      onClose();
    });
  }

  return (
    <FormModal eyebrow="Budget" title={`${user.display_name} (${budget.user})`} busy={busy} error={error} submitLabel="Save budget" busyLabel="Saving…" onClose={onClose} onSubmit={submit}>
      <CapField label="Daily cost (USD)" value={daily} step="0.01" onChange={setDaily} />
      <CapField label="Monthly cost (USD)" value={monthly} step="0.01" onChange={setMonthly} />
      <CapField label="Daily tokens" value={tokens} step="1" onChange={setTokens} />
      <p className="cell-sub">Inherit uses the tenant default. Tenant pool and per-key caps still apply; the tightest cap wins.</p>
      <button type="button" className="button secondary" disabled={busy} onClick={reset}>Reset to tenant defaults</button>
    </FormModal>
  );
}

function CapField({ label, value, step, onChange }: { label: string; value: CapDraft; step: string; onChange: (value: CapDraft) => void }) {
  return (
    <fieldset className="cap-field">
      <legend>{label}</legend>
      <select aria-label={`${label} mode`} value={value.mode} onChange={(event) => onChange({ ...value, mode: event.target.value as CapMode })}>
        <option value="inherit">Inherit</option>
        <option value="limit">Limit</option>
        <option value="unlimited">Unlimited</option>
      </select>
      {value.mode === "limit" && <input aria-label={`${label} limit`} type="number" min="0" step={step} value={value.amount} onChange={(event) => onChange({ ...value, amount: event.target.value })} required />}
    </fieldset>
  );
}

function draft(value: Cap, scale: number): CapDraft {
  if (value === null) return { mode: "inherit", amount: "" };
  if (value === "unlimited") return { mode: "unlimited", amount: "" };
  return { mode: "limit", amount: String(value / scale) };
}

function cap(value: CapDraft, scale: number): Cap {
  if (value.mode === "inherit") return null;
  if (value.mode === "unlimited") return "unlimited";
  return Math.round(Number(value.amount) * scale);
}
