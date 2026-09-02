import type { FormEvent, ReactElement, ReactNode } from "react";
import { compact } from "../format";

type NumericKey<T> = { [K in keyof T]: T[K] extends number ? K : never }[keyof T] & string;

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
}): ReactElement {
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

export function Card({ children, className = "" }: { children: ReactNode; className?: string }): ReactElement {
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
}): ReactElement {
  return (
    <Card className={`metric metric-${tone}`}>
      <p>{label}</p>
      <strong>{value}</strong>
      <span>{detail}</span>
    </Card>
  );
}

export function Status({ value }: { value: string }): ReactElement {
  return (
    <span className={`status status-${value}`}>
      <span aria-hidden="true" />
      {value.replaceAll("_", " ")}
    </span>
  );
}

export function Loading({ label = "Loading" }: { label?: string }): ReactElement {
  return (
    <div className="loading" role="status">
      <span />
      {label}
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }): ReactElement {
  return <div className="empty">{children}</div>;
}

export function ErrorNotice({ message }: { message: string }): ReactElement {
  return <div className="notice notice-error">{message}</div>;
}

export function FormModal({
  eyebrow,
  title,
  busy,
  error,
  submitLabel,
  busyLabel,
  onClose,
  onSubmit,
  children,
}: {
  eyebrow: string;
  title: string;
  busy: boolean;
  error: string;
  submitLabel: string;
  busyLabel: string;
  onClose: () => void;
  onSubmit: (event: FormEvent) => void;
  children: ReactNode;
}): ReactElement {
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby="form-modal-title" onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-head">
          <div>
            <p className="eyebrow">{eyebrow}</p>
            <h2 id="form-modal-title">{title}</h2>
          </div>
          <button className="icon-button" aria-label="Close" onClick={onClose}>×</button>
        </div>
        <form className="form-grid" onSubmit={onSubmit}>
          {children}
          {error && <ErrorNotice message={error} />}
          <div className="form-actions">
            <button type="button" className="button secondary" onClick={onClose}>Cancel</button>
            <button className="button primary" disabled={busy}>{busy ? busyLabel : submitLabel}</button>
          </div>
        </form>
      </div>
    </div>
  );
}

export function LineChart<T extends { start: number }>({
  data,
  value,
  format = compact,
}: {
  data: T[];
  value: NumericKey<T>;
  format?: (value: number) => string;
}): ReactElement {
  const width = 720;
  const height = 230;
  const padding = 18;
  const values = data.map((item) => item[value] as number);
  const max = values.reduce((highest, item) => Math.max(highest, item), 1);
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
