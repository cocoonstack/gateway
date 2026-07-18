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
      .catch((err: unknown) => active && setError(err instanceof Error ? err.message : "Request failed"));
    return () => { active = false; };
  }, [path, version]);

  return { data, error, loading: path !== null && data === null && error === "", reload, setData };
}
