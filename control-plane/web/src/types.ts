export type Role = "member" | "tenant_admin" | "system_admin";

export interface User {
  id: string;
  email: string;
  display_name: string;
  tenant: string;
  gateway_user_id: string;
  role: Role;
  disabled: boolean;
  created_at: number;
  updated_at: number;
}

export interface Session {
  user: User;
  csrf_token: string;
}

export interface UsageRow {
  user_id: string;
  model: string;
  requests: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  cost_micros: number;
  vendor_cost_micros: number;
}

export interface SeriesPoint {
  start: number;
  end: number;
  requests: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  cost_micros: number;
  vendor_cost_micros: number;
}

export interface UsageSeries {
  bucket: "hour" | "day";
  since: number;
  until: number;
  series: SeriesPoint[];
}

export interface ModelStatus {
  model: string;
  state: "available" | "unstable" | "unavailable" | "no_data";
  requests: number;
  errors: number;
  window_minutes: number;
}

export interface Overview {
  totals: {
    requests: number;
    total_tokens: number;
    cost_micros: number;
    vendor_cost_micros?: number;
  };
  usage: UsageRow[];
  series: UsageSeries;
  models: ModelStatus[];
}

export interface Account {
  name: string;
  provider: string;
  priority: number;
  tier: string;
  health: string;
  protocols: string[];
}

export interface Instance {
  id: string;
  url: string;
  status: "available" | "degraded" | "unavailable";
  latency_ms: number;
  error?: string;
  accounts: Account[];
}

export interface AccessKey {
  ak: string;
  product: string;
  tenant: string;
  owner?: string | null;
  qps: number;
  daily_token_quota: number;
  tokens_per_minute?: number | null;
  expires_at_epoch_secs?: number | null;
  banned: boolean;
  suspended_until_epoch_secs?: number | null;
  status: "active" | "banned" | "expired" | "suspended";
  available: boolean;
}

export interface ConfigDocument {
  version: number;
  yaml: string;
}

export interface ConfigVersion {
  id: number;
  created_at_epoch_secs: number;
}

export interface AuditEntry {
  created_at_epoch_secs: number;
  actor: string;
  scope: string;
  action: string;
  target: string;
  summary: string;
  source_ip: string;
}

export interface SecurityEvent {
  created_at_epoch_secs: number;
  request_id: string;
  ak: string;
  user_id: string;
  tenant: string;
  surface: string;
  rule: string;
  action: string;
  hits: number;
}
