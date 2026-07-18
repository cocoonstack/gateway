import { useAuth } from "../App";
import { compact } from "../format";
import { useAPI } from "../hooks";
import type { Instance, ModelStatus } from "../types";
import { Card, ErrorNotice, Loading, PageHeader, Status } from "../components/UI";

export default function AvailabilityPage() {
  const { session } = useAuth();
  const isSystem = session.user.role === "system_admin";
  const models = useAPI<{ models: ModelStatus[] }>("/api/v1/models/status");
  const instances = useAPI<{ instances: Instance[] }>(isSystem ? "/api/v1/admin/instances" : null);

  return (
    <>
      <PageHeader
        eyebrow="Operations"
        title="Availability"
        description={isSystem ? "Instance reachability, upstream account health and client-visible model outcomes." : "Client-visible model outcomes for your entitled model set."}
        actions={<button className="button secondary" onClick={() => { models.reload(); if (isSystem) instances.reload(); }}>Refresh</button>}
      />
      {(models.error || (isSystem && instances.error)) && <ErrorNotice message={models.error || instances.error} />}
      {!models.data ? <Loading /> : (
        <Card>
          <div className="card-heading"><div><p className="eyebrow">Shared service view</p><h2>Models</h2></div><span className="muted">Recent gateway window</span></div>
          <div className="model-grid">{models.data.models.map((model) => (
            <div className="model-card" key={model.model}><div className="model-card-head"><strong>{model.model}</strong><Status value={model.state} /></div><div className="model-stats"><span><b>{compact(model.requests)}</b> requests</span><span><b>{model.errors}</b> errors</span><span><b>{model.window_minutes}m</b> window</span></div></div>
          ))}</div>
        </Card>
      )}
      {isSystem && (
        !instances.data ? <Loading label="Checking instances" /> : (
          <Card>
            <div className="card-heading"><div><p className="eyebrow">Configured targets</p><h2>Gateway instances</h2></div><span className="muted">Health and local account-pool view</span></div>
            <div className="instance-list">{instances.data.instances.map((instance) => (
              <article key={instance.id} className="instance-card">
                <div className="instance-top"><div><strong>{instance.id}</strong><code>{instance.url}</code></div><Status value={instance.status} /></div>
                <div className="instance-meta"><span>Latency <b>{instance.latency_ms} ms</b></span><span>Accounts <b>{instance.accounts.length}</b></span>{instance.error && <span className="error-text">{instance.error}</span>}</div>
                <div className="account-list">{instance.accounts.map((account) => <div key={account.name}><span className={`health-dot health-${account.health}`} /><div><strong>{account.name}</strong><small>{account.provider} · {account.tier} · {account.protocols.join(", ")}</small></div><span>{account.health}</span></div>)}</div>
              </article>
            ))}</div>
          </Card>
        )
      )}
    </>
  );
}
