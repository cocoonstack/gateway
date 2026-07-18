import { FormEvent, useState } from "react";
import { api, jsonBody } from "../api";
import { dateTime, roleLabel } from "../format";
import { useAPI } from "../hooks";
import type { Role, User } from "../types";
import { Card, ErrorNotice, Loading, PageHeader, Status } from "../components/UI";

export default function UsersPage() {
  const { data, error, reload } = useAPI<{ users: User[] }>("/api/v1/admin/users");
  const [creating, setCreating] = useState(false);
  const [actionError, setActionError] = useState("");

  async function toggle(user: User) {
    setActionError("");
    try {
      await api(`/api/v1/admin/users/${user.id}`, { method: "PATCH", ...jsonBody({ disabled: !user.disabled }) });
      reload();
    } catch (err) { setActionError(err instanceof Error ? err.message : "User update failed"); }
  }

  return <><PageHeader eyebrow="Identity" title="Users & roles" description="Human access to the control plane. Gateway API keys remain a separate credential domain." actions={<button className="button primary" onClick={() => setCreating(true)}>Add user</button>} />{(error || actionError) && <ErrorNotice message={error || actionError} />}{creating && <CreateUser onClose={() => setCreating(false)} onCreated={() => { setCreating(false); reload(); }} />}{!data ? <Loading /> : <Card><div className="table-wrap"><table><thead><tr><th>User</th><th>Role</th><th>Tenant</th><th>Gateway identity</th><th>Status</th><th>Created</th><th /></tr></thead><tbody>{data.users.map((user) => <tr key={user.id}><td><strong>{user.display_name}</strong><small className="cell-sub">{user.email}</small></td><td>{roleLabel(user.role)}</td><td>{user.tenant || "Global"}</td><td>{user.gateway_user_id || "—"}</td><td><Status value={user.disabled ? "disabled" : "active"} /></td><td>{dateTime(user.created_at)}</td><td><button className="table-button" onClick={() => void toggle(user)}>{user.disabled ? "Enable" : "Disable"}</button></td></tr>)}</tbody></table></div></Card>}</>;
}

function CreateUser({ onClose, onCreated }: { onClose: () => void; onCreated: () => void }) {
  const [form, setForm] = useState<{ email: string; display_name: string; password: string; role: Role; tenant: string; gateway_user_id: string }>({ email: "", display_name: "", password: "", role: "member", tenant: "", gateway_user_id: "" });
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  async function submit(event: FormEvent) { event.preventDefault(); setBusy(true); setError(""); try { await api("/api/v1/admin/users", { method: "POST", ...jsonBody(form) }); onCreated(); } catch (err) { setError(err instanceof Error ? err.message : "Create failed"); } finally { setBusy(false); } }
  const system = form.role === "system_admin";
  return <div className="modal-backdrop" role="presentation" onMouseDown={onClose}><div className="modal" role="dialog" aria-modal="true" aria-labelledby="new-user-title" onMouseDown={(event) => event.stopPropagation()}><div className="modal-head"><div><p className="eyebrow">Identity</p><h2 id="new-user-title">Add control-plane user</h2></div><button className="icon-button" aria-label="Close" onClick={onClose}>×</button></div><form className="form-grid" onSubmit={submit}><label>Display name<input value={form.display_name} onChange={(event) => setForm({ ...form, display_name: event.target.value })} required /></label><label>Email<input type="email" value={form.email} onChange={(event) => setForm({ ...form, email: event.target.value })} required /></label><label>Password<input type="password" minLength={10} value={form.password} onChange={(event) => setForm({ ...form, password: event.target.value })} required /></label><label>Role<select value={form.role} onChange={(event) => setForm({ ...form, role: event.target.value as Role })}><option value="member">Member</option><option value="tenant_admin">Tenant admin</option><option value="system_admin">System admin</option></select></label><label>Tenant<input disabled={system} value={form.tenant} onChange={(event) => setForm({ ...form, tenant: event.target.value })} required={!system} /></label><label>Gateway user id<input disabled={system} value={form.gateway_user_id} onChange={(event) => setForm({ ...form, gateway_user_id: event.target.value })} placeholder="Billing attribution" /></label>{error && <ErrorNotice message={error} />}<div className="form-actions"><button type="button" className="button secondary" onClick={onClose}>Cancel</button><button className="button primary" disabled={busy}>{busy ? "Creating…" : "Add user"}</button></div></form></div></div>;
}
