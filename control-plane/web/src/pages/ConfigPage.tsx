import { useEffect, useState } from "react";
import { api } from "../api";
import { Card, ErrorNotice, Loading, PageHeader } from "../components/UI";
import { dateTime } from "../format";
import { useAPI } from "../hooks";
import type { ConfigDocument, ConfigVersion } from "../types";

interface VersionList {
  versions: ConfigVersion[];
}

export default function ConfigPage() {
  const current = useAPI<ConfigDocument>("/api/v1/admin/config");
  const history = useAPI<VersionList>("/api/v1/admin/config/versions");
  const [yaml, setYAML] = useState("");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    if (current.data) setYAML(current.data.yaml);
  }, [current.data]);

  async function act(kind: "validate" | "publish") {
    setBusy(true);
    setError("");
    setMessage("");
    try {
      if (kind === "validate") {
        await api("/api/v1/admin/config/validate", {
          method: "POST",
          body: JSON.stringify({ yaml }),
        });
        setMessage("Configuration is valid and ready to publish.");
      } else {
        const result = await api<{ version: number }>("/api/v1/admin/config", {
          method: "PUT",
          body: JSON.stringify({ yaml }),
        });
        setMessage(`Published as version ${result.version}.`);
        current.reload();
        history.reload();
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Configuration request failed");
    } finally {
      setBusy(false);
    }
  }

  async function rollback(version: number) {
    if (!window.confirm(`Roll back to version ${version}? A new version will be published.`)) return;
    setBusy(true);
    setError("");
    setMessage("");
    try {
      const result = await api<{ version: number }>(`/api/v1/admin/config/versions/${version}/rollback`, {
        method: "POST",
      });
      setMessage(`Version ${version} restored as version ${result.version}.`);
      current.reload();
      history.reload();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Rollback failed");
    } finally {
      setBusy(false);
    }
  }

  if (current.loading) return <Loading label="Loading gateway configuration" />;

  return (
    <>
      <PageHeader
        eyebrow="System administration"
        title="Configuration"
        description="Validate and publish the gateway configuration through the gateway admin API."
        actions={<span className="period-chip">Current version {current.data?.version ?? "—"}</span>}
      />
      {current.error && <ErrorNotice message={current.error} />}
      {error && <ErrorNotice message={error} />}
      {message && <div className="notice notice-success">{message}</div>}

      <div className="content-grid config-grid">
        <Card className="config-editor-card">
          <div className="card-heading">
            <div><h2>Gateway YAML</h2><p>Changes take effect only after a successful publish.</p></div>
          </div>
          <label className="editor-label" htmlFor="config-yaml">Configuration document</label>
          <textarea
            id="config-yaml"
            className="config-editor"
            value={yaml}
            spellCheck={false}
            onChange={(event) => setYAML(event.target.value)}
          />
          <div className="form-actions">
            <button className="button secondary" disabled={busy || !yaml} onClick={() => void act("validate")}>Validate</button>
            <button className="button primary" disabled={busy || !yaml} onClick={() => void act("publish")}>Publish configuration</button>
          </div>
        </Card>

        <Card className="version-card">
          <div className="card-heading">
            <div><h2>Version history</h2><p>Recent configurations retained by the gateway.</p></div>
          </div>
          {history.loading && <Loading label="Loading versions" />}
          {history.error && <ErrorNotice message={history.error} />}
          <div className="version-list">
            {history.data?.versions.map((version) => (
              <div className="version-row" key={version.id}>
                <div><strong>Version {version.id}</strong><span>{dateTime(version.created_at_epoch_secs)}</span></div>
                {version.id === current.data?.version
                  ? <span className="current-tag">Current</span>
                  : <button className="table-button" disabled={busy} onClick={() => void rollback(version.id)}>Restore</button>}
              </div>
            ))}
          </div>
        </Card>
      </div>
    </>
  );
}
