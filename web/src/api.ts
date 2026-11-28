// Thin typed client for the FastAPI backend. All calls are same-origin ("/api/...");
// in dev Vite proxies them to FastAPI (see vite.config.ts).
import { useEffect, useState } from "react";

export type Health = {
  db: string;
  last_collector_run_at: string | null;
  last_collector_status: string | null;
  last_collector_success_at: string | null;
  latest_stream_event_time: string | null;
  latest_stream_ingested_at: string | null;
  stream_events_total: number;
};
export type Video = {
  video_id: string; title: string; channel_title: string; is_short: boolean;
  views: number; likes: number | null; comments: number; engagement_rate: number | null; views_rank: number;
};
export type DailyRow = { snapshot_date: string; views: number; views_gained: number | null; title: string };
export type Mention = { event_hour: string; source: string; mentions: number };
export type Keyword = { topic_group: string; keyword: string; mentions: number };
export type SpikeRow = {
  topic_group: string; event_hour: string; mentions: number; trailing_mean: number | null;
  trailing_stddev: number | null; z_score: number | null; is_spike: boolean; history_hours?: number;
};
export type Spikes = { spikes: SpikeRow[]; highest_z: SpikeRow[] };
export type Cell = string | number | boolean | null;
export type AskResult = {
  question: string; sql: string; columns: string[]; rows: Cell[][];
  chart_hint: "bar" | "line" | "table"; retries: number; elapsed_ms: number;
};

export class ApiError extends Error {
  attempts: { sql: string | null; error: string }[];
  constructor(message: string, attempts: { sql: string | null; error: string }[] = []) {
    super(message);
    this.attempts = attempts;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  if (!res.ok) {
    let detail: unknown = null;
    try { detail = (await res.json()).detail; } catch { /* not JSON */ }
    if (detail && typeof detail === "object" && "error" in detail) {
      const d = detail as { error: string; attempts?: ApiError["attempts"] };
      throw new ApiError(d.error, d.attempts ?? []);
    }
    throw new ApiError(typeof detail === "string" ? detail : `request failed (HTTP ${res.status})`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  health: () => request<Health>("/api/health"),
  topVideos: (limit = 10) => request<Video[]>(`/api/gta/top-videos?limit=${limit}`),
  daily: (id: string) => request<DailyRow[]>(`/api/gta/daily?video_id=${encodeURIComponent(id)}`),
  mentions: (topic: string, hours = 72) => request<Mention[]>(`/api/mentions/hourly?topic_group=${topic}&hours=${hours}`),
  keywords: (topic: string, days = 7) => request<Keyword[]>(`/api/keywords?topic_group=${topic}&days=${days}&limit=10`),
  spikes: () => request<Spikes>("/api/spikes"),
  ask: (question: string) =>
    request<AskResult>("/api/ask", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ question }),
    }),
};

export type Loadable<T> = { data: T | null; error: string | null; loading: boolean };

/** Run an async loader whenever `key` changes; returns {data, error, loading}. A newer call wins over a stale one. */
export function useLoad<T>(loader: (() => Promise<T>) | null, key: string): Loadable<T> {
  const [state, setState] = useState<Loadable<T>>({ data: null, error: null, loading: !!loader });
  useEffect(() => {
    if (!loader) return;
    let cancelled = false;
    setState((s) => ({ ...s, loading: true, error: null }));
    loader()
      .then((data) => { if (!cancelled) setState({ data, error: null, loading: false }); })
      .catch((e: Error) => { if (!cancelled) setState({ data: null, error: e.message, loading: false }); });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);
  return state;
}
