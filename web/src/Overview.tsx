import { useState } from "react";
import { api, useLoad, type Health, type SpikeRow } from "./api";
import { AutoChart, fmtInt, fmtPct } from "./charts";

function ago(iso: string | null): string {
  if (!iso) return "never";
  const mins = Math.max(0, (Date.now() - new Date(iso).getTime()) / 60000);
  if (mins < 90) return `${Math.round(mins)} min ago`;
  if (mins < 48 * 60) return `${(mins / 60).toFixed(1)} h ago`;
  return `${Math.round(mins / 1440)} days ago`;
}
const hoursSince = (iso: string | null) => (iso ? (Date.now() - new Date(iso).getTime()) / 3.6e6 : Infinity);

function Card({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <section className="card">
      <h2>{title}</h2>
      {subtitle && <p className="sub">{subtitle}</p>}
      {children}
    </section>
  );
}

function Status({ state }: { state: ReturnType<typeof useLoad<Health>> }) {
  if (state.loading) return <p className="muted">Checking pipeline…</p>;
  if (state.error || !state.data) return <p className="error">API/database unreachable: {state.error}</p>;
  const h = state.data;
  const items = [
    { label: "Database", value: h.db === "ok" ? "connected" : h.db, ok: h.db === "ok" },
    { label: "YouTube collector", value: `${h.last_collector_status ?? "no runs"}, ${ago(h.last_collector_success_at)}`, ok: h.last_collector_status === "success" && hoursSince(h.last_collector_success_at) < 36 },
    { label: "Newest stream event", value: ago(h.latest_stream_event_time), ok: hoursSince(h.latest_stream_event_time) < 2, hint: "stream collectors run on demand" },
    { label: "Stream events stored", value: fmtInt(h.stream_events_total), ok: true },
  ];
  return (
    <div className="status-grid">
      {items.map((i) => (
        <div key={i.label} className="status">
          <span className={`dot ${i.ok ? "ok" : "warn"}`} />
          <div><div className="label">{i.label}</div><div className="value">{i.value}</div></div>
        </div>
      ))}
    </div>
  );
}

function SpikeList({ spikes, highest }: { spikes: SpikeRow[]; highest: SpikeRow[] }) {
  const row = (s: SpikeRow, key: string) => (
    <tr key={key}>
      <td><span className={`pill ${s.topic_group}`}>{s.topic_group}</span></td>
      <td>{s.event_hour.slice(0, 16).replace("T", " ")} UTC</td>
      <td className="num">{s.mentions}</td>
      <td className="num">{s.trailing_mean ?? "-"}</td>
      <td className="num">{s.z_score ?? "-"}</td>
    </tr>
  );
  const head = <thead><tr><th>topic</th><th>hour</th><th>mentions</th><th>24h mean</th><th>z</th></tr></thead>;
  return (
    <>
      {spikes.length ? (
        <div className="table-wrap"><table>{head}<tbody>{spikes.map((s, i) => row(s, `s${i}`))}</tbody></table></div>
      ) : (
        <p className="ok-text">No spikes right now. An hour is a spike when z ≥ 3, at least 10 mentions and 6+ hours of history.</p>
      )}
      {highest.length > 0 && (
        <details>
          <summary>Highest z-scores (not flagged)</summary>
          <div className="table-wrap"><table>{head}<tbody>{highest.map((s, i) => row(s, `h${i}`))}</tbody></table></div>
        </details>
      )}
    </>
  );
}

export default function Overview() {
  const health = useLoad(api.health, "health");
  const videos = useLoad(() => api.topVideos(10), "videos");
  const [picked, setPicked] = useState<string | null>(null);
  const selected = picked ?? videos.data?.[0]?.video_id ?? null;
  const daily = useLoad(selected ? () => api.daily(selected) : null, `daily:${selected}`);
  const mentions = useLoad(() => api.mentions("ai_tech", 72), "mentions");
  const keywords = useLoad(() => api.keywords("ai_tech", 7), "keywords");
  const spikes = useLoad(api.spikes, "spikes");

  const gained = (daily.data ?? []).filter((d) => d.views_gained != null);
  const selTitle = videos.data?.find((v) => v.video_id === selected)?.title;

  return (
    <div className="page">
      <Card title="Pipeline status" subtitle="Live from the database (view analytics.mart_pipeline_status)">
        <Status state={health} />
      </Card>

      <div className="two-col">
        <Card title="GTA 6 on YouTube" subtitle="Top 10 videos by views (latest daily snapshot). Click a row to see its daily views gained.">
          {videos.error && <p className="error">{videos.error}</p>}
          {videos.loading && <p className="muted">Loading…</p>}
          {videos.data && (
            <div className="table-wrap">
              <table className="clickable">
                <thead><tr><th>#</th><th>title</th><th>views</th><th>engagement</th></tr></thead>
                <tbody>
                  {videos.data.map((v) => (
                    <tr key={v.video_id} className={v.video_id === selected ? "selected" : ""} onClick={() => setPicked(v.video_id)}>
                      <td className="num">{v.views_rank}</td>
                      <td className="title" title={v.title}>{v.title}{v.is_short && <span className="pill short">short</span>}</td>
                      <td className="num">{fmtInt(v.views)}</td>
                      <td className="num">{fmtPct(v.engagement_rate)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <h3>Views gained per day{selTitle ? `: ${selTitle.slice(0, 48)}` : ""}</h3>
          {daily.loading && <p className="muted">Loading…</p>}
          {daily.error && <p className="error">{daily.error}</p>}
          {!daily.loading && daily.data && (gained.length ? (
            <AutoChart kind="bar" columns={["day", "views gained"]} rows={gained.map((d) => [d.snapshot_date, d.views_gained])} height={220} />
          ) : (
            <p className="muted">Only one snapshot so far, so there is nothing to subtract yet.</p>
          ))}
        </Card>

        <div className="stack">
          <Card title="AI: mentions per hour" subtitle="Bluesky, Wikipedia and Hacker News, newest 72 hours of data">
            {mentions.error && <p className="error">{mentions.error}</p>}
            {mentions.loading && <p className="muted">Loading…</p>}
            {mentions.data && (mentions.data.length ? (
              <AutoChart kind="line" columns={["hour", "source", "mentions"]} rows={mentions.data.map((m) => [m.event_hour, m.source, m.mentions])} height={240} />
            ) : <p className="muted">No AI mentions in range.</p>)}
          </Card>
          <Card title="AI: top keywords" subtitle="Last 7 days of data">
            {keywords.error && <p className="error">{keywords.error}</p>}
            {keywords.loading && <p className="muted">Loading…</p>}
            {keywords.data && (
              <AutoChart kind="bar" columns={["keyword", "mentions"]} rows={keywords.data.map((k) => [k.keyword, k.mentions])} height={240} />
            )}
          </Card>
        </div>
      </div>

      <Card title="Spikes" subtitle="Hourly mentions compared with the trailing 24 hours (z-score)">
        {spikes.error && <p className="error">{spikes.error}</p>}
        {spikes.loading && <p className="muted">Loading…</p>}
        {spikes.data && <SpikeList spikes={spikes.data.spikes} highest={spikes.data.highest_z} />}
      </Card>
    </div>
  );
}
