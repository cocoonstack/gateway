import { useCallback, useEffect, useState } from "react";
import { api } from "./api";

export interface APIState<T> {
  data: T | null;
  error: string;
  loading: boolean;
  reload: () => void;
}

export interface ActionState {
  run: (fn: () => Promise<void>) => Promise<void>;
  busy: boolean;
  error: string;
}

interface Result<T> {
  path: string;
  data: T | null;
  error: string;
}

export function useAPI<T>(path: string | null): APIState<T> {
  const [result, setResult] = useState<Result<T> | null>(null);
  const [version, setVersion] = useState(0);

  const reload = useCallback(() => setVersion((value) => value + 1), []);

  useEffect(() => {
    if (!path) return;
    let active = true;
    api<T>(path)
      .then((value) => active && setResult({ path, data: value, error: "" }))
      .catch((err: unknown) => active && setResult({ path, data: null, error: errorMessage(err, "Request failed") }));
    return () => { active = false; };
  }, [path, version]);

  const fresh = result && result.path === path ? result : null;
  return { data: fresh?.data ?? null, error: fresh?.error ?? "", loading: path !== null && fresh === null, reload };
}

export function useAction(fallback = "Request failed"): ActionState {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const run = useCallback(async (fn: () => Promise<void>) => {
    setBusy(true);
    setError("");
    try {
      await fn();
    } catch (err: unknown) {
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
