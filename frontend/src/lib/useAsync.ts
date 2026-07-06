import { useCallback, useEffect, useState } from "react";

interface AsyncState<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  reload: () => void;
  setData: (data: T) => void;
}

/** Run an async loader on mount and whenever `deps` change; expose reload. */
export function useAsync<T>(loader: () => Promise<T>, deps: unknown[]): AsyncState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [nonce, setNonce] = useState(0);

  // eslint-disable-next-line react-hooks/exhaustive-deps
  const runner = useCallback(loader, deps);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
    runner()
      .then((d) => active && setData(d))
      .catch((e) => active && setError(e?.message ?? String(e)))
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [runner, nonce]);

  const reload = useCallback(() => setNonce((n) => n + 1), []);
  return { data, error, loading, reload, setData };
}
