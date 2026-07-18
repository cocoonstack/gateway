import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { Role, Session } from "./types";

const overview = {
  totals: { requests: 12, total_tokens: 4200, cost_micros: 20000, vendor_cost_micros: 12000 },
  usage: [],
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
  });
});

function mockAPI(role: Role) {
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
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
    const path = typeof input === "string" ? input : input.toString();
    const body = path.startsWith("/api/v1/session") ? session : overview;
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }));
}
