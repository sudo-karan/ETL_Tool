import { NavLink, Outlet } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";

const NAV = [
  { to: "/pipelines", label: "Pipelines" },
  { to: "/diagnostics", label: "Diagnostics" },
  { to: "/schedules", label: "Schedules" },
  { to: "/secrets", label: "Secrets" },
];

export function AppLayout() {
  const { user, logout } = useAuth();
  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">⚙ ETL Tool</div>
        <nav className="nav">
          {NAV.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
        <div className="topbar-right">
          <span className="muted">{user?.email}</span>
          <button className="btn btn-ghost" onClick={logout}>
            Log out
          </button>
        </div>
      </header>
      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}
