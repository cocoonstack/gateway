import { useState } from "react";
import { Card, Empty, ErrorNotice, Loading, PageHeader, Status } from "../components/UI";
import { dateTime } from "../format";
import { useAPI } from "../hooks";
import { useAuth } from "../App";
import type { AuditEntry, SecurityEvent } from "../types";

export default function AuditPage() {
  const { session } = useAuth();
  const system = session.user.role === "system_admin";
  const [kind, setKind] = useState<"ops" | "security">(system ? "ops" : "security");
  const ops = useAPI<{ entries: AuditEntry[] }>(kind === "ops" ? "/api/v1/admin/audit?kind=ops" : null);
  const security = useAPI<{ events: SecurityEvent[] }>(kind === "security" ? "/api/v1/admin/audit?kind=security" : null);

  return (
    <>
      <PageHeader
        eyebrow="Operations"
        title="Audit"
        description="Review gateway configuration changes and request security signals."
        actions={
          <div className="tab-list" role="tablist">
            {system && <button className={kind === "ops" ? "active" : ""} onClick={() => setKind("ops")}>Operations</button>}
            <button className={kind === "security" ? "active" : ""} onClick={() => setKind("security")}>Security</button>
          </div>
        }
      />

      {kind === "ops" ? (
        <Card>
          <div className="card-heading"><div><h2>Operational changes</h2><p>Configuration and administrative actions recorded by the gateway.</p></div></div>
          {ops.loading && <Loading />}
          {ops.error && <ErrorNotice message={ops.error} />}
          {ops.data?.entries.length === 0 && <Empty>No operational audit entries in this period.</Empty>}
          {ops.data && ops.data.entries.length > 0 && (
            <div className="table-wrap"><table><thead><tr><th>Time</th><th>Actor</th><th>Action</th><th>Target</th><th>Summary</th></tr></thead><tbody>
              {ops.data.entries.map((entry, index) => <tr key={`${entry.created_at_epoch_secs}-${index}`}>
                <td className="cell-sub">{dateTime(entry.created_at_epoch_secs)}</td><td>{entry.actor}</td><td><Status value={entry.action} /></td><td>{entry.target}</td><td>{entry.summary}</td>
              </tr>)}
            </tbody></table></div>
          )}
        </Card>
      ) : (
        <Card>
          <div className="card-heading"><div><h2>Security events</h2><p>Rule matches and actions, scoped to the tenants you manage.</p></div></div>
          {security.loading && <Loading />}
          {security.error && <ErrorNotice message={security.error} />}
          {security.data?.events.length === 0 && <Empty>No security events in this period.</Empty>}
          {security.data && security.data.events.length > 0 && (
            <div className="table-wrap"><table><thead><tr><th>Time</th><th>Tenant / user</th><th>Surface</th><th>Rule</th><th>Action</th><th>Hits</th></tr></thead><tbody>
              {security.data.events.map((event, index) => <tr key={`${event.created_at_epoch_secs}-${index}`}>
                <td className="cell-sub">{dateTime(event.created_at_epoch_secs)}</td><td><strong>{event.tenant || "—"}</strong><span className="cell-sub">{event.user_id || event.ak}</span></td><td>{event.surface}</td><td>{event.rule}</td><td><Status value={event.action} /></td><td>{event.hits}</td>
              </tr>)}
            </tbody></table></div>
          )}
        </Card>
      )}
    </>
  );
}
