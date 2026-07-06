import { Navigate, Route, Routes } from "react-router-dom";
import { useAuth } from "./auth/AuthContext";
import { AppLayout } from "./components/AppLayout";
import { LoginPage } from "./auth/LoginPage";
import { PipelineListPage } from "./pipelines/PipelineListPage";
import { EditorPage } from "./pipelines/editor/EditorPage";
import { DiagnosticsPage } from "./diagnostics/DiagnosticsPage";
import { SchedulesPage } from "./schedules/SchedulesPage";
import { SecretsPage } from "./secrets/SecretsPage";
import type { ReactNode } from "react";

function RequireAuth({ children }: { children: ReactNode }) {
  const { token, loading } = useAuth();
  if (loading) return <div className="center muted">Loading…</div>;
  if (!token) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

export function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        element={
          <RequireAuth>
            <AppLayout />
          </RequireAuth>
        }
      >
        <Route path="/" element={<Navigate to="/pipelines" replace />} />
        <Route path="/pipelines" element={<PipelineListPage />} />
        <Route path="/pipelines/:id" element={<EditorPage />} />
        <Route path="/diagnostics" element={<DiagnosticsPage />} />
        <Route path="/schedules" element={<SchedulesPage />} />
        <Route path="/secrets" element={<SecretsPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
