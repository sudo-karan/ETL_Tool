import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { api, ApiError, setAuthToken } from "../api/client";
import type { UserRead } from "../api/types";

const TOKEN_KEY = "etl_token";

interface AuthState {
  user: UserRead | null;
  token: string | null;
  loading: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, password: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthState | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem(TOKEN_KEY));
  const [user, setUser] = useState<UserRead | null>(null);
  const [loading, setLoading] = useState<boolean>(!!token);

  useEffect(() => {
    setAuthToken(token);
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);

    if (!token) {
      setUser(null);
      setLoading(false);
      return;
    }
    let active = true;
    setLoading(true);
    api
      .me()
      .then((u) => active && setUser(u))
      .catch((err) => {
        if (active && err instanceof ApiError && err.status === 401) setToken(null);
      })
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [token]);

  const login = async (email: string, password: string) => {
    const t = await api.login(email, password);
    setToken(t.access_token);
  };

  const register = async (email: string, password: string) => {
    await api.register(email, password);
    const t = await api.login(email, password);
    setToken(t.access_token);
  };

  const logout = () => setToken(null);

  return (
    <AuthContext.Provider value={{ user, token, loading, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthState {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
