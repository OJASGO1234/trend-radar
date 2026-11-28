import { useEffect, useState } from "react";
import Overview from "./Overview";
import Ask from "./Ask";

// Two pages, so a tiny hash router is enough (#/ and #/ask): no extra dependency.
const pageFromHash = () => (window.location.hash.startsWith("#/ask") ? "ask" : "overview");

export default function App() {
  const [page, setPage] = useState<"overview" | "ask">(pageFromHash());
  useEffect(() => {
    const on = () => setPage(pageFromHash());
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);

  return (
    <>
      <header className="topbar">
        <div className="brand"><span className="logo">◎</span> Trend Radar</div>
        <nav>
          <a href="#/" className={page === "overview" ? "active" : ""}>Overview</a>
          <a href="#/ask" className={page === "ask" ? "active" : ""}>Ask</a>
        </nav>
      </header>
      <main>{page === "overview" ? <Overview /> : <Ask />}</main>
    </>
  );
}
