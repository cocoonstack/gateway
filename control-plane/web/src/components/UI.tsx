import type { ReactNode } from "react";
import { compact, money } from "../format";

export function PageHeader({
  eyebrow,
  title,
  description,
  actions,
}: {
  eyebrow: string;
  title: string;
  description: string;
  actions?: ReactNode;
}) {
  return (
    <header className="page-header">
      <div>
        <p className="eyebrow">{eyebrow}</p>
        <h1>{title}</h1>
        <p className="page-description">{description}</p>
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </header>
  );
}

export function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <section className={`card ${className}`}>{children}</section>;
}

export function Metric({
  label,
  value,
  detail,
  tone = "neutral",
}: {
  label: string;
  value: string;
  detail: string;
  tone?: "neutral" | "positive" | "warning";
}) {
  return (
    <Card className={`metric metric-${tone}`}>
      <p>{label}</p>
      <strong>{value}</strong>
      <span>{detail}</span>
    </Card>
  );
}

export function Status({ value }: { value: string }) {
  return (
    <span className={`status status-${value}`}>
      <span aria-hidden="true" />
      {value.replace("_", " ")}
    </span>
  );
}

export function Loading({ label = "Loading" }: { label?: string }) {
  return (
    <div className="loading" role="status">
      <span />
      {label}
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>;
}

export function ErrorNotice({ message }: { message: string }) {
  return <div className="notice notice-error">{message}</div>;
}

export function LineChart({
  data,
  value,
  format = compact,
}: {
  data: { start: number }[];
  value: string;
  format?: (value: number) => string;
}) {
  const width = 720;
  const height = 230;
  const padding = 18;
  const values = data.map((item) => (item as Record<string, number>)[value] ?? 0);
  const max = Math.max(...values, 1);
  const points = values
    .map((item, index) => {
      const x = padding + (index / Math.max(values.length - 1, 1)) * (width - padding * 2);
      const y = height - padding - (item / max) * (height - padding * 2);
      return `${x},${y}`;
    })
    .join(" ");
  return (
    <div className="chart-wrap">
      <div className="chart-scale">
        <span>{format(max)}</span>
        <span>{format(max / 2)}</span>
        <span>0</span>
      </div>
      <svg className="line-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${value} trend`}>
        <defs>
          <linearGradient id={`fill-${value}`} x1="0" x2="0" y1="0" y2="1">
            <stop offset="0" stopColor="#6fe0bf" stopOpacity="0.32" />
            <stop offset="1" stopColor="#6fe0bf" stopOpacity="0" />
          </linearGradient>
        </defs>
        <line x1={padding} y1={height / 2} x2={width - padding} y2={height / 2} className="grid-line" />
        {points && (
          <>
            <polygon points={`${padding},${height - padding} ${points} ${width - padding},${height - padding}`} fill={`url(#fill-${value})`} />
            <polyline points={points} className="trend-line" />
          </>
        )}
      </svg>
    </div>
  );
}

export function Cost({ micros }: { micros: number }) {
  return <>{money(micros)}</>;
}
