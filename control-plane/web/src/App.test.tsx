import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { Role, Session } from "./types";

const overview = {
  totals: { requests: 12, total_tokens: 4200, billed_units: 63, cost_micros: 20000, vendor_cost_micros: 12000 },
  usage: [
    { user_id: "", model: "tts-1", requests: 1, prompt_tokens: 0, completion_tokens: 0, total_tokens: 0, cost_micros: 945, vendor_cost_micros: 252, billed_units: 63 },
  ],
  series: { bucket: "day", since: 1, until: 2, series: [] },
  models: [{ model: "gpt-test", state: "available", requests: 12, errors: 0, window_minutes: 15 }],
};

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("role navigation", () => {
  it("keeps a member on usage and availability surfaces", async () => {
    mockAPI("member");
    render(<MemoryRouter><App /></MemoryRouter>);

    expect(await screen.findByRole("link", { name: /usage & cost/i })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /availability/i })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /access keys/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /configuration/i })).not.toBeInTheDocument();
  });

  it("exposes fleet and configuration surfaces to a system admin", async () => {
    mockAPI("system_admin");
    render(<MemoryRouter><App /></MemoryRouter>);

    expect(await screen.findByRole("link", { name: /access keys/i })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /users & roles/i })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /configuration/i })).toBeInTheDocument();
    expect(await screen.findByText("Revenue")).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Units" })).toBeInTheDocument();
    expect(screen.getByRole("cell", { name: "63" })).toBeInTheDocument();
  });
});

describe("user budgets", () => {
  it("lets a tenant admin set a user's daily cap in dollars", async () => {
    const users = { users: [{ id: "u-1", email: "carol@example.com", display_name: "Carol", tenant: "tenant-a", gateway_user_id: "carol", role: "member", disabled: false, created_at: 1, updated_at: 1 }] };
    const budget = { user: "carol", daily_cost_quota_micros: null, monthly_cost_quota_micros: "unlimited", daily_token_quota: null };
    const calls = mockAPI("tenant_admin", (path) => (path.endsWith("/budget") ? budget : path.startsWith("/api/v1/admin/users") ? users : undefined));
    render(<MemoryRouter initialEntries={["/users"]}><App /></MemoryRouter>);

    fireEvent.click(await screen.findByRole("button", { name: "Budget" }));
    expect(screen.queryByRole("button", { name: "Add user" })).not.toBeInTheDocument();
    fireEvent.change(await screen.findByRole("combobox", { name: "Daily cost (USD) mode" }), { target: { value: "limit" } });
    fireEvent.change(screen.getByRole("spinbutton", { name: "Daily cost (USD) limit" }), { target: { value: "2" } });
    fireEvent.click(screen.getByRole("button", { name: "Save budget" }));

    await waitFor(() => expect(calls.some((c) => c.method === "PUT")).toBe(true));
    const put = calls.find((c) => c.method === "PUT");
    expect(put?.path).toBe("/api/v1/admin/users/u-1/budget");
    expect(JSON.parse(put?.body ?? "{}")).toEqual({ daily_cost_quota_micros: 2_000_000, monthly_cost_quota_micros: "unlimited", daily_token_quota: null });
  });
});

function mockAPI(role: Role, route: (path: string) => unknown = () => undefined) {
  const session: Session = {
    csrf_token: "csrf-test",
    user: {
      id: "user-1",
      email: "person@example.com",
      display_name: role === "system_admin" ? "System Admin" : "Member User",
      tenant: role === "system_admin" ? "" : "tenant-a",
      gateway_user_id: role === "system_admin" ? "" : "gateway-user-1",
      role,
      disabled: false,
      created_at: 1,
      updated_at: 1,
    },
  };
  const calls: { method: string; path: string; body: string }[] = [];
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = typeof input === "string" ? input : input.toString();
    calls.push({ method: init?.method ?? "GET", path, body: typeof init?.body === "string" ? init.body : "" });
    const body = path.startsWith("/api/v1/session") ? session : route(path) ?? overview;
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }));
  return calls;
}
