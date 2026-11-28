"""Score the Text-to-SQL pipeline on api/eval/questions.yaml.

Run:  python -m scripts.run_eval                 (calls the LLM for every question)
      python -m scripts.run_eval --dry-run       (no LLM: checks that every reference query validates and runs)
      python -m scripts.run_eval --ids q01,q05   (subset)    --limit 3   (first N)

Scoring compares RESULTS of the reference SQL and the model's SQL (see the header of questions.yaml).
"""
from __future__ import annotations

import argparse
import itertools
import logging
import sys
import time
from pathlib import Path

import yaml

from api.db import run_query
from api.llm import LLMError, get_provider
from api.schema_context import get_context
from api.sql_guard import validate_and_limit
from api.text_to_sql import AskFailed, NotAnswerable, ask

QUESTIONS = Path(__file__).resolve().parents[1] / "api" / "eval" / "questions.yaml"
PAUSE_SECONDS = 2.0   # be gentle with free-tier rate limits


def norm(v):
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, (int, float)):
        return round(float(v), 3)
    return str(v).strip()


def columns_of(cols: list[str], rows: list[list]) -> list[list]:
    return [[norm(r[i]) for r in rows] for i in range(len(cols))]


def results_match(exp: tuple[list[str], list[list]], got: tuple[list[str], list[list]], ordered: bool) -> tuple[bool, str]:
    (ecols, erows), (gcols, grows) = exp, got
    if len(erows) != len(grows):
        return False, f"row count {len(grows)} != expected {len(erows)}"
    e_by_col, g_by_col = columns_of(ecols, erows), columns_of(gcols, grows)
    key = (lambda c: c) if ordered else (lambda c: sorted(c, key=repr))
    candidates = [[j for j, g in enumerate(g_by_col) if key(g) == key(e)] for e in e_by_col]
    if any(not c for c in candidates):
        missing = [ecols[i] for i, c in enumerate(candidates) if not c]
        return False, f"expected column(s) {missing} not found with matching values"
    for pick in itertools.product(*candidates):          # try the possible column mappings
        if len(set(pick)) < len(pick):
            continue
        e_rows = [tuple(c[i] for c in e_by_col) for i in range(len(erows))]
        g_rows = [tuple(g_by_col[j][i] for j in pick) for i in range(len(grows))]
        if (e_rows == g_rows) if ordered else (sorted(e_rows, key=repr) == sorted(g_rows, key=repr)):
            return True, ""
    return False, "columns match individually but rows do not line up"


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")   # titles contain emoji; Windows consoles default to cp1252
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="no LLM: run only the reference queries")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--ids", default="")
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s %(message)s")

    items = yaml.safe_load(QUESTIONS.read_text(encoding="utf-8"))
    if args.ids:
        items = [q for q in items if q["id"] in args.ids.split(",")]
    if args.limit:
        items = items[: args.limit]
    _, tables = get_context()
    llm = None if args.dry_run else get_provider()

    results = []
    for q in items:
        qid, question, t0 = q["id"], " ".join(q["question"].split()), time.monotonic()
        expect, ordered = q.get("expect"), bool(q.get("ordered"))
        status, detail, sql, retries = "FAIL", "", "", 0

        if args.dry_run:
            if expect:
                status, detail = "SKIP", f"expect: {expect}"
            else:
                safe = validate_and_limit(q["expected_sql"], set(tables))
                cols, rows = run_query(safe)
                status, detail, sql = "PASS", f"reference ok: {len(rows)} row(s), first={rows[0] if rows else None}", safe
        else:
            try:
                out = ask(question, llm)
                sql, retries = out["sql"], out["retries"]
                if expect == "refusal":
                    detail = "it answered with a query instead of declining"
                elif expect == "blocked":
                    # any result is only acceptable if it came purely from analytics.* (the guard guarantees that)
                    ok = "raw." not in sql.lower() and "drop" not in sql.lower()
                    status, detail = ("PASS", "answered safely from analytics only") if ok else ("FAIL", "touched forbidden objects")
                else:
                    exp = run_query(validate_and_limit(q["expected_sql"], set(tables)))
                    ok, why = results_match(exp, (out["columns"], out["rows"]), ordered)
                    status, detail = ("PASS", "") if ok else ("FAIL", why)
            except NotAnswerable as exc:
                status, detail = ("PASS", f"declined: {exc}") if expect == "refusal" else ("FAIL", f"declined: {exc}")
                if expect == "blocked":
                    status = "PASS"
            except AskFailed as exc:
                last = exc.attempts[-1] if exc.attempts else {}
                status = "PASS" if expect == "blocked" else "FAIL"
                detail = f"gave up after {len(exc.attempts)} attempts; last error: {last.get('error')}"
                sql = last.get("sql") or ""
            except LLMError as exc:
                status, detail = "ERROR", str(exc)[:160]
            time.sleep(PAUSE_SECONDS)
        results.append({"id": qid, "status": status, "retries": retries, "ms": int((time.monotonic() - t0) * 1000)})
        print(f"[{status:<5}] {qid} (retries={retries}, {results[-1]['ms']} ms) {question[:70]}")
        if sql:
            print(f"         sql: {' '.join(sql.split())[:230]}")
        if detail and (status != "PASS" or args.dry_run):
            print(f"         why: {detail}")

    scored = [r for r in results if r["status"] in ("PASS", "FAIL", "ERROR")]
    passed = [r for r in scored if r["status"] == "PASS"]
    if scored:
        first_try = [r for r in passed if r["retries"] == 0]
        print(f"\nACCURACY: {len(passed)}/{len(scored)} = {100 * len(passed) / len(scored):.0f}%"
              f"   (passed on the first try: {len(first_try)}/{len(scored)})")
    return 0 if len(passed) == len(scored) else 1


if __name__ == "__main__":
    sys.exit(main())
