import { FormEvent, useState } from "react";
import { api, jsonBody } from "../api";
import { useAction } from "../hooks";
import type { Session } from "../types";

export default function LoginPage({ onLogin }: { onLogin: (session: Session) => void }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const { run, busy, error } = useAction("Sign in failed");

  function submit(event: FormEvent) {
    event.preventDefault();
    void run(async () => {
      const session = await api<Session>("/api/v1/auth/login", {
        method: "POST",
        ...jsonBody({ email, password }),
      });
      onLogin(session);
    });
  }

  return (
    <div className="login-page">
      <div className="login-art" aria-hidden="true">
        <div className="orb orb-one" />
        <div className="orb orb-two" />
        <div className="login-copy">
          <span className="brand-mark large">C</span>
          <p className="eyebrow">Cocoonstack</p>
          <h1>Operate every model<br />from one calm surface.</h1>
          <p>Usage, cost, availability and fleet configuration—scoped to the person signing in.</p>
          <div className="signal-row"><i /><span>Gateway fabric online</span></div>
        </div>
      </div>
      <section className="login-panel">
        <form onSubmit={submit}>
          <p className="eyebrow">Gateway control</p>
          <h2>Welcome back</h2>
          <p className="muted">Use your control-plane account to continue.</p>
          <label>Email<input autoFocus type="email" autoComplete="username" value={email} onChange={(event) => setEmail(event.target.value)} required /></label>
          <label>Password<input type="password" autoComplete="current-password" value={password} onChange={(event) => setPassword(event.target.value)} required /></label>
          {error && <div className="notice notice-error">{error}</div>}
          <button className="button primary wide" disabled={busy}>{busy ? "Signing in…" : "Sign in"}</button>
        </form>
        <p className="login-foot">Protected by an opaque, server-side session.</p>
      </section>
    </div>
  );
}
