import { useAuth } from "../App";
import { compact, money } from "../format";
import { useAPI } from "../hooks";
import type { Overview } from "../types";
import { Card, ErrorNotice, LineChart, Loading, Metric, PageHeader, Status } from "../components/UI";

export default function OverviewPage() {
  const { session } = useAuth();
  const { data, error } = useAPI<Overview>("/api/v1/overview?bucket=day");
  const isSystem = session.user.role === "system_admin";

  if (error) return <ErrorNotice message={error} />;
  if (!data) return <Loading label="Loading overview" />;

  const available = data.models.filter((model) => model.state === "available").length;
  const vendorCost = data.totals.vendor_cost_micros ?? 0;
  const margin = vendorCost > 0 ? data.totals.cost_micros - vendorCost : 0;

  return (
    <>
      <PageHeader
        eyebrow={isSystem ? "Fleet overview" : session.user.tenant || "Personal workspace"}
        title={`Good ${greeting()}, ${session.user.display_name.split(" ")[0]}.`}
        description={isSystem ? "The operational and commercial pulse of your gateway fabric." : "Your model usage and estimated charges for the last 30 days."}
        actions={<span className="period-chip">Last 30 days</span>}
      />
      <div className="metric-grid">
        <Metric label="Requests" value={compact(data.totals.requests)} detail="Completed model calls" />
        <Metric label="Total tokens" value={compact(data.totals.total_tokens)} detail="Prompt + completion" />
        <Metric label={isSystem ? "Revenue" : "Estimated cost"} value={money(data.totals.cost_micros)} detail="Calculated from model rates" tone="positive" />
        {isSystem
          ? <Metric label="Gross margin" value={money(margin)} detail={`${available}/${data.models.length} models available`} tone={margin >= 0 ? "positive" : "warning"} />
          : <Metric label="Model health" value={`${available}/${data.models.length}`} detail="Currently available" tone="positive" />}
      </div>
      <div className="content-grid two-thirds">
        <Card>
          <div className="card-heading"><div><p className="eyebrow">Consumption</p><h2>Token trend</h2></div><span className="legend"><i /> Total tokens</span></div>
          <LineChart data={data.series.series} value="total_tokens" />
        </Card>
        <Card>
          <div className="card-heading"><div><p className="eyebrow">Live window</p><h2>Model status</h2></div></div>
          <div className="status-list">
            {data.models.map((model) => (
              <div key={model.model}><div><strong>{model.model}</strong><span>{compact(model.requests)} requests · {model.errors} errors</span></div><Status value={model.state} /></div>
            ))}
          </div>
        </Card>
      </div>
      <Card>
        <div className="card-heading"><div><p className="eyebrow">Breakdown</p><h2>Usage by model</h2></div></div>
        <div className="table-wrap">
          <table><thead><tr><th>Model</th>{isSystem && <th>User</th>}<th>Requests</th><th>Tokens</th><th>Charge</th>{isSystem && <th>Vendor cost</th>}</tr></thead>
            <tbody>{data.usage.map((row) => <tr key={`${row.user_id}-${row.model}`}><td><strong>{row.model}</strong></td>{isSystem && <td>{row.user_id || "Anonymous"}</td>}<td>{compact(row.requests)}</td><td>{compact(row.total_tokens)}</td><td>{money(row.cost_micros)}</td>{isSystem && <td>{money(row.vendor_cost_micros)}</td>}</tr>)}</tbody>
          </table>
        </div>
      </Card>
    </>
  );
}

function greeting() {
  const hour = new Date().getHours();
  if (hour < 12) return "morning";
  if (hour < 18) return "afternoon";
  return "evening";
}
