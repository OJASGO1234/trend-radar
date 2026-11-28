import {
  Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import type { Cell } from "./api";

export const PALETTE = ["#4f8cff", "#f59e0b", "#10b981", "#ef4444", "#a855f7", "#14b8a6"];

const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });
export const fmtInt = (n: number | null | undefined) => (n == null ? "-" : Math.round(n).toLocaleString("en-US"));
export const fmtPct = (x: number | null | undefined) => (x == null ? "-" : `${(x * 100).toFixed(2)}%`);
const isTime = (v: Cell) => typeof v === "string" && /^\d{4}-\d{2}-\d{2}/.test(v);
const shortTime = (v: string) => (v.length > 10 ? v.slice(5, 16).replace("T", " ") : v.slice(5));
const isNum = (v: Cell) => typeof v === "number";

type Point = Record<string, string | number | null>;

/** Turn query result rows into recharts data. 3 columns (time, category, number) become one line per category. */
function shape(columns: string[], rows: Cell[][], mode: "line" | "bar") {
  const numCols = columns.map((_, i) => i).filter((i) => rows.some((r) => r[i] != null) && rows.every((r) => r[i] == null || isNum(r[i])));
  const xIdx = mode === "line" ? 0 : columns.findIndex((_, i) => !numCols.includes(i));
  const valueCols = numCols.filter((i) => i !== xIdx);
  const catIdx = columns.findIndex((_, i) => i !== xIdx && !numCols.includes(i));

  if (mode === "line" && catIdx >= 0 && valueCols.length) {
    const series = [...new Set(rows.map((r) => String(r[catIdx])))];
    const byX = new Map<string, Point>();
    for (const r of rows) {
      const x = String(r[xIdx]);
      const p = byX.get(x) ?? { x: isTime(r[xIdx]) ? shortTime(x) : x };
      p[String(r[catIdx])] = r[valueCols[0]] as number;
      byX.set(x, p);
    }
    return { data: [...byX.values()], keys: series };
  }
  const data = rows.map((r) => {
    const p: Point = { x: isTime(r[xIdx]) ? shortTime(String(r[xIdx])) : String(r[xIdx]) };
    for (const i of valueCols) p[columns[i]] = (r[i] as number) ?? null;
    return p;
  });
  return { data, keys: valueCols.map((i) => columns[i]) };
}

export function AutoChart({ columns, rows, kind, height = 280 }: { columns: string[]; rows: Cell[][]; kind: "line" | "bar"; height?: number }) {
  const { data, keys } = shape(columns, rows, kind);
  if (!data.length || !keys.length) return null;
  const axis = { stroke: "var(--muted)", fontSize: 12 };
  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        {kind === "line" ? (
          <LineChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="var(--grid)" strokeDasharray="3 3" />
            <XAxis dataKey="x" {...axis} minTickGap={24} />
            <YAxis {...axis} width={52} tickFormatter={(v) => compact.format(Number(v))} />
            <Tooltip contentStyle={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 8 }} />
            {keys.length > 1 && <Legend />}
            {keys.map((k, i) => (
              <Line key={k} type="monotone" dataKey={k} stroke={PALETTE[i % PALETTE.length]} strokeWidth={2} dot={data.length < 20} connectNulls isAnimationActive={false} />
            ))}
          </LineChart>
        ) : (
          <BarChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="var(--grid)" strokeDasharray="3 3" />
            <XAxis dataKey="x" {...axis} interval={0} tickFormatter={(v: string) => (v.length > 14 ? v.slice(0, 13) + "…" : v)} angle={data.length > 5 ? -30 : 0} textAnchor={data.length > 5 ? "end" : "middle"} height={data.length > 5 ? 70 : 30} />
            <YAxis {...axis} width={52} tickFormatter={(v) => compact.format(Number(v))} />
            <Tooltip contentStyle={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 8 }} />
            {keys.length > 1 && <Legend />}
            {keys.map((k, i) => <Bar key={k} dataKey={k} fill={PALETTE[i % PALETTE.length]} radius={[4, 4, 0, 0]} isAnimationActive={false} />)}
          </BarChart>
        )}
      </ResponsiveContainer>
    </div>
  );
}

export function ResultTable({ columns, rows }: { columns: string[]; rows: Cell[][] }) {
  const show = (v: Cell) => (v == null ? "NULL" : typeof v === "number" ? (Number.isInteger(v) ? v.toLocaleString("en-US") : String(Math.round(v * 10000) / 10000)) : String(v));
  return (
    <div className="table-wrap">
      <table>
        <thead><tr>{columns.map((c) => <th key={c}>{c}</th>)}</tr></thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>{r.map((v, j) => <td key={j} className={typeof v === "number" ? "num" : ""}>{show(v)}</td>)}</tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
