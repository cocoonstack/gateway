import { useCallback, useEffect, useState } from "react";
import { api } from "./api";

export function useAPI<T>(path: string | null) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState("");
  const [version, setVersion] = useState(0);

  const reload = useCallback(() => setVersion((value) => value + 1), []);

  useEffect(() => {
    if (!path) return;
    let active = true;
    setError("");
    api<T>(path)
      .then((value) => active && setData(value))
      .catch((err: unknown) => active && setError(errorMessage(err, "Request failed")));
    return () => { active = false; };
  }, [path, version]);

  return { data, error, loading: path !== null && data === null && error === "", reload, setData };
}

export function useAction(fallback = "Request failed") {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const run = useCallback(async (fn: () => Promise<void>) => {
    setBusy(true);
    setError("");
    try {
      await fn();
    } catch (err) {
      setError(errorMessage(err, fallback));
    } finally {
      setBusy(false);
    }
  }, [fallback]);
  return { run, busy, error };
}

function errorMessage(err: unknown, fallback: string): string {
  return err instanceof Error ? err.message : fallback;
}
