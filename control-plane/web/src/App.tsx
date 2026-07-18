import { createContext, useContext, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { Navigate, NavLink, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { api, APIError, setCSRF, setUnauthorizedHandler } from "./api";
import { roleLabel } from "./format";
import type { Role, Session } from "./types";
import { Loading } from "./components/UI";
import LoginPage from "./pages/LoginPage";
import OverviewPage from "./pages/OverviewPage";
import UsagePage from "./pages/UsagePage";
import AvailabilityPage from "./pages/AvailabilityPage";
import KeysPage from "./pages/KeysPage";
import UsersPage from "./pages/UsersPage";
import ConfigPage from "./pages/ConfigPage";
import AuditPage from "./pages/AuditPage";

interface AuthState {
  session: Session;
  setSession: (session: Session) => void;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthState | null>(null);

export function useAuth(): AuthState {
  const value = useContext(AuthContext);
  if (!value) throw new Error("AuthContext is unavailable");
  return value;
}

export default function App() {
  const [session, updateSession] = useState<Session | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setUnauthorizedHandler(() => {
      setCSRF("");
      updateSession(null);
    });
    api<Session>("/api/v1/session")
      .then((value) => {
        setCSRF(value.csrf_token);
        updateSession(value);
      })
      .catch((error: unknown) => {
        if (!(error instanceof APIError) || error.status !== 401) {
          console.error(error);
        }
      })
      .finally(() => setLoading(false));
  }, []);

  const auth = useMemo<AuthState | null>(() => {
    if (!session) return null;
    return {
      session,
      setSession(value) {
        setCSRF(value.csrf_token);
        updateSession(value);
      },
      async logout() {
        await api<void>("/api/v1/auth/logout", { method: "POST" });
        setCSRF("");
        updateSession(null);
      },
    };
  }, [session]);

  if (loading) return <Loading label="Opening control plane" />;
  if (!auth) return <LoginPage onLogin={(value) => {
    setCSRF(value.csrf_token);
    updateSession(value);
  }} />;

  return (
    <AuthContext.Provider value={auth}>
      <Shell />
    </AuthContext.Provider>
  );
}

interface NavItem {
  to: string;
  label: string;
  icon: string;
  minRole: Role;
  page: ReactNode;
  end?: boolean;
}

const roleRank: Record<Role, number> = { member: 0, tenant_admin: 1, system_admin: 2 };

const navItems: NavItem[] = [
  { to: "/", label: "Overview", icon: "◫", end: true, minRole: "member", page: <OverviewPage /> },
  { to: "/usage", label: "Usage & cost", icon: "⌁", minRole: "member", page: <UsagePage /> },
  { to: "/availability", label: "Availability", icon: "◉", minRole: "member", page: <AvailabilityPage /> },
  { to: "/keys", label: "Access keys", icon: "⌘", minRole: "tenant_admin", page: <KeysPage /> },
  { to: "/audit", label: "Audit", icon: "≣", minRole: "tenant_admin", page: <AuditPage /> },
  { to: "/users", label: "Users & roles", icon: "♙", minRole: "system_admin", page: <UsersPage /> },
  { to: "/configuration", label: "Configuration", icon: "⌗", minRole: "system_admin", page: <ConfigPage /> },
];

function Shell() {
  const { session, logout } = useAuth();
  const role = session.user.role;
  const location = useLocation();
  const navigate = useNavigate();
  const links = navItems.filter((item) => roleRank[role] >= roleRank[item.minRole]);

  useEffect(() => {
    window.scrollTo({ top: 0 });
  }, [location.pathname]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark">C</span>
          <div><strong>Cocoon</strong><small>Gateway control</small></div>
        </div>
        <nav aria-label="Main navigation">
          {links.map((link) => (
            <NavLink key={link.to} to={link.to} end={link.end} className={({ isActive }) => isActive ? "active" : ""}>
              <span aria-hidden="true">{link.icon}</span>{link.label}
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-foot">
          <div className="avatar">{session.user.display_name.slice(0, 1).toUpperCase()}</div>
          <div className="identity">
            <strong>{session.user.display_name}</strong>
            <span>{roleLabel(role)}</span>
          </div>
          <button className="icon-button" aria-label="Sign out" title="Sign out" onClick={() => void logout().then(() => navigate("/"))}>↗</button>
        </div>
      </aside>
      <main className="main-content">
        <Routes>
          {navItems.map((item) => (
            <Route key={item.to} path={item.to} element={roleRank[role] >= roleRank[item.minRole] ? item.page : <Navigate to="/" />} />
          ))}
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}
