import { useEffect, useState } from "react";
import { api, ApiError, type AskResult } from "./api";
import { AutoChart, ResultTable } from "./charts";

const EXAMPLES = [
  "Which 5 videos have the highest engagement rate among those with over 1 million views?",
  "How many views did the most viewed video gain each day?",
  "Compare GTA 6 mentions per hour by source",
  "Which AI keywords are mentioned most?",
];

export default function Ask() {
  const [question, setQuestion] = useState("");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<AskResult | null>(null);
  const [error, setError] = useState<ApiError | null>(null);

  async function submit(q: string) {
    const text = q.trim();
    if (text.length < 3 || busy) return;
    setQuestion(text);
    setBusy(true);
    setError(null);
    setResult(null);
    try {
      setResult(await api.ask(text));
    } catch (e) {
      setError(e instanceof ApiError ? e : new ApiError((e as Error).message));
    } finally {
      setBusy(false);
    }
  }

  // Deep link: #/ask?q=how many videos  runs that question on load
  useEffect(() => {
    const q = new URLSearchParams(window.location.hash.split("?")[1] ?? "").get("q");
    if (q) void submit(q);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="page narrow">
      <section className="card">
        <h2>Ask the data</h2>
        <p className="sub">Plain English in, read-only SQL out. The query is validated before it runs: one SELECT, analytics tables only, max 200 rows.</p>
        <form onSubmit={(e) => { e.preventDefault(); void submit(question); }} className="ask-form">
          <input
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            placeholder="e.g. Which video gained the most views yesterday?"
            maxLength={500}
            autoFocus
          />
          <button type="submit" disabled={busy || question.trim().length < 3}>{busy ? "Thinking…" : "Ask"}</button>
        </form>
        <div className="chips">
          {EXAMPLES.map((ex) => (
            <button key={ex} className="chip" disabled={busy} onClick={() => void submit(ex)}>{ex}</button>
          ))}
        </div>
      </section>

      {busy && <section className="card"><p className="muted">Writing and checking the SQL… (this takes about 5-15 seconds)</p></section>}

      {error && (
        <section className="card">
          <p className="error"><strong>{error.message}</strong></p>
          {error.attempts.map((a, i) => (
            <div key={i}>
              <p className="muted">Attempt {i + 1} was rejected: {a.error}</p>
              {a.sql && <pre className="sql"><code>{a.sql}</code></pre>}
            </div>
          ))}
        </section>
      )}

      {result && (
        <>
          <section className="card">
            <h3>Generated SQL</h3>
            <pre className="sql"><code>{result.sql}</code></pre>
            <p className="muted">
              {result.rows.length} row{result.rows.length === 1 ? "" : "s"} · {(result.elapsed_ms / 1000).toFixed(1)} s
              {result.retries > 0 && " · fixed after 1 retry"}
            </p>
          </section>
          {result.chart_hint !== "table" && (
            <section className="card">
              <AutoChart kind={result.chart_hint} columns={result.columns} rows={result.rows} />
            </section>
          )}
          <section className="card">
            <h3>Result</h3>
            {result.rows.length ? <ResultTable columns={result.columns} rows={result.rows} /> : <p className="muted">The query returned no rows.</p>}
          </section>
        </>
      )}
    </div>
  );
}
