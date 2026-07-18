import { useState } from "react";
import { useAuth } from "../App";
import { compact, dateTime, money } from "../format";
import { useAPI } from "../hooks";
import type { UsageRow, UsageSeries } from "../types";
import { Card, ErrorNotice, LineChart, Loading, PageHeader } from "../components/UI";

export default function UsagePage() {
  const { session } = useAuth();
  const [days, setDays] = useState(30);
  const until = Math.floor(Date.now() / 1000);
  const since = until - (days - 1) * 86_400;
  const { data: usageData, error: usageError } = useAPI<{ usage: UsageRow[] }>(`/api/v1/usage?since=${since}&until=${until}`);
  const { data: series, error: seriesError } = useAPI<UsageSeries>(`/api/v1/usage/series?bucket=day&since=${since}&until=${until}`);
  const isSystem = session.user.role === "system_admin";

  return (
    <>
      <PageHeader
        eyebrow="Metering"
        title="Usage & cost"
        description="Billing-period totals are derived from the gateway ledger and durable rollups."
        actions={<select aria-label="Usage period" value={days} onChange={(event) => setDays(Number(event.target.value))}><option value={7}>7 days</option><option value={30}>30 days</option><option value={90}>90 days</option></select>}
      />
      {(usageError || seriesError) && <ErrorNotice message={usageError || seriesError} />}
      {!usageData || !series ? <Loading label="Loading usage" /> : (
        <>
          <div className="content-grid halves">
            <Card><div className="card-heading"><div><p className="eyebrow">Daily volume</p><h2>Requests</h2></div></div><LineChart data={series.series} value="requests" /></Card>
            <Card><div className="card-heading"><div><p className="eyebrow">Daily charge</p><h2>Cost</h2></div></div><LineChart data={series.series} value="cost_micros" format={money} /></Card>
          </div>
          <Card>
            <div className="card-heading"><div><p className="eyebrow">Billing dimensions</p><h2>Model detail</h2></div><span className="muted">Through {dateTime(until)}</span></div>
            <div className="table-wrap"><table><thead><tr>{isSystem && <th>User</th>}<th>Model</th><th>Requests</th><th>Prompt</th><th>Completion</th><th>Total tokens</th><th>Charge</th>{isSystem && <th>Vendor</th>}</tr></thead>
              <tbody>{usageData.usage.map((row) => <tr key={`${row.user_id}-${row.model}`}>{isSystem && <td>{row.user_id || "Anonymous"}</td>}<td><strong>{row.model}</strong></td><td>{compact(row.requests)}</td><td>{compact(row.prompt_tokens)}</td><td>{compact(row.completion_tokens)}</td><td>{compact(row.total_tokens)}</td><td>{money(row.cost_micros)}</td>{isSystem && <td>{money(row.vendor_cost_micros)}</td>}</tr>)}</tbody>
            </table></div>
          </Card>
        </>
      )}
    </>
  );
}
