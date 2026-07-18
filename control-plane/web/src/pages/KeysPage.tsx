import { FormEvent, useState } from "react";
import { useAuth } from "../App";
import { api, jsonBody } from "../api";
import { compact, dateTime } from "../format";
import { useAPI } from "../hooks";
import type { AccessKey } from "../types";
import { Card, Empty, ErrorNotice, Loading, PageHeader, Status } from "../components/UI";

export default function KeysPage() {
  const { session } = useAuth();
  const [tenant, setTenant] = useState(session.user.role === "tenant_admin" ? session.user.tenant : "");
  const path = `/api/v1/admin/keys${tenant ? `?tenant=${encodeURIComponent(tenant)}` : ""}`;
  const { data, error, reload } = useAPI<{ keys: AccessKey[] }>(path);
  const [creating, setCreating] = useState(false);
  const [actionError, setActionError] = useState("");

  async function toggle(key: AccessKey) {
    setActionError("");
    try {
      await api(`/api/v1/admin/keys/${encodeURIComponent(key.ak)}`, { method: "PATCH", ...jsonBody({ banned: !key.banned }) });
      reload();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Key update failed");
    }
  }

  async function remove(key: AccessKey) {
    if (!window.confirm(`Revoke ${key.ak}? Requests using it will stop authenticating.`)) return;
    setActionError("");
    try {
      await api(`/api/v1/admin/keys/${encodeURIComponent(key.ak)}`, { method: "DELETE" });
      reload();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Key revoke failed");
    }
  }

  return (
    <>
      <PageHeader eyebrow="Credentials" title="Access keys" description="Lifecycle and governance state from the gateway's live key store." actions={<button className="button primary" onClick={() => setCreating(true)}>New key</button>} />
      {session.user.role === "system_admin" && <div className="filter-bar"><label>Tenant filter<input placeholder="All tenants" value={tenant} onChange={(event) => setTenant(event.target.value)} /></label></div>}
      {(error || actionError) && <ErrorNotice message={error || actionError} />}
      {creating && <CreateKey tenant={tenant || session.user.tenant} tenantLocked={session.user.role === "tenant_admin"} onClose={() => setCreating(false)} onCreated={() => { setCreating(false); reload(); }} />}
      {!data ? <Loading /> : data.keys.length === 0 ? <Empty>No keys match this tenant.</Empty> : (
        <Card><div className="table-wrap"><table><thead><tr><th>Key</th><th>Tenant / owner</th><th>Status</th><th>QPS</th><th>Daily quota</th><th>Expires</th><th /></tr></thead><tbody>
          {data.keys.map((key) => <tr key={key.ak}><td><code className="key-code">{key.ak}</code><small className="cell-sub">{key.product}</small></td><td>{key.tenant}<small className="cell-sub">{key.owner || "Shared key"}</small></td><td><Status value={key.status} /></td><td>{key.qps}</td><td>{compact(key.daily_token_quota)}</td><td>{key.expires_at_epoch_secs ? dateTime(key.expires_at_epoch_secs) : "Never"}</td><td><div className="row-actions"><button onClick={() => void toggle(key)}>{key.banned ? "Unban" : "Ban"}</button><button className="danger-link" onClick={() => void remove(key)}>Revoke</button></div></td></tr>)}
        </tbody></table></div></Card>
      )}
    </>
  );
}

function CreateKey({ tenant, tenantLocked, onClose, onCreated }: { tenant: string; tenantLocked: boolean; onClose: () => void; onCreated: () => void }) {
  const [form, setForm] = useState({ ak: "", product: "standard", tenant, owner: "", qps: 10, daily_token_quota: 1_000_000 });
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  async function submit(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError("");
    try {
      await api("/api/v1/admin/keys", { method: "POST", ...jsonBody({ ...form, owner: form.owner || null }) });
      onCreated();
    } catch (err) { setError(err instanceof Error ? err.message : "Create failed"); } finally { setBusy(false); }
  }
  return <div className="modal-backdrop" role="presentation" onMouseDown={onClose}><div className="modal" role="dialog" aria-modal="true" aria-labelledby="new-key-title" onMouseDown={(event) => event.stopPropagation()}><div className="modal-head"><div><p className="eyebrow">Credential</p><h2 id="new-key-title">Create access key</h2></div><button className="icon-button" aria-label="Close" onClick={onClose}>×</button></div><form className="form-grid" onSubmit={submit}><label>Key<input value={form.ak} onChange={(event) => setForm({ ...form, ak: event.target.value })} placeholder="ak-team-name" required /></label><label>Product<input value={form.product} onChange={(event) => setForm({ ...form, product: event.target.value })} required /></label><label>Tenant<input value={form.tenant} disabled={tenantLocked} onChange={(event) => setForm({ ...form, tenant: event.target.value })} required /></label><label>Owner<input value={form.owner} onChange={(event) => setForm({ ...form, owner: event.target.value })} placeholder="Optional user id" /></label><label>QPS<input type="number" min="0.1" step="0.1" value={form.qps} onChange={(event) => setForm({ ...form, qps: Number(event.target.value) })} required /></label><label>Daily token quota<input type="number" min="0" value={form.daily_token_quota} onChange={(event) => setForm({ ...form, daily_token_quota: Number(event.target.value) })} required /></label>{error && <ErrorNotice message={error} />}<div className="form-actions"><button type="button" className="button secondary" onClick={onClose}>Cancel</button><button className="button primary" disabled={busy}>{busy ? "Creating…" : "Create key"}</button></div></form></div></div>;
}
