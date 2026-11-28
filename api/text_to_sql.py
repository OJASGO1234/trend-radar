"""question -> SQL (LLM) -> validate -> run read-only -> {sql, columns, rows, chart_hint}.

Safety layers, in the order a query meets them:
  (1) read-only DB role            api/db.py  + scripts/create_readonly_role.py
  (2) sqlglot validation           api/sql_guard.py: one SELECT, analytics.* only, no dangerous functions
  (3) forced LIMIT 200             api/sql_guard.py
  (4) statement_timeout 5s         api/db.py (SET LOCAL) and the role default
If validation or execution fails, the error text goes back to the LLM ONCE; then we give up with a clear error.
"""
from __future__ import annotations

import json
import logging
import re
import time

import psycopg

from .chart import pick_chart
from .db import run_query
from .llm import LLMProvider, Message
from .schema_context import get_context
from .sql_guard import SqlRejected, validate_and_limit

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """You translate questions about two datasets into ONE PostgreSQL SELECT query.
Dataset 1: GTA 6 YouTube videos (daily stats) and online buzz about GTA 6. Dataset 2: online buzz about AI (Bluesky, Wikipedia, Hacker News).

Answer with JSON only, in exactly this shape:
  {{"sql": "<one SELECT query>", "chart_hint": "bar" | "line" | "table"}}
If the question cannot be answered from the tables below, answer instead:
  {{"sql": null, "reason": "<one short sentence>"}}

Rules:
- Use ONLY the tables listed below, always written as analytics.<table>. Never use any other schema.
- Exactly one SELECT statement (WITH ... SELECT is fine). Never modify data. No semicolon needed.
- All timestamps are UTC. "Latest" or "last N hours/days" must be anchored on the newest data, NOT on now():
  e.g. WHERE event_hour > (SELECT max(event_hour) FROM analytics.mart_mentions_hourly) - interval '24 hours'
  (the stream collectors do not run all the time, so now() would often return nothing).
- Use ILIKE '%text%' for searching titles. Give result columns short readable aliases.
- Rates are fractions (0.02 = 2%). When asked for a percentage, multiply by 100.
- Add ORDER BY, and LIMIT when the question asks for a top N.
- In PostgreSQL ORDER BY ... DESC puts NULLs FIRST. When ranking by a nullable column (likes, engagement_rate,
  views_gained, z_score) add NULLS LAST or filter IS NOT NULL.
- chart_hint: "line" when the first column is a time/date and another column is a number (a third text column
  may split the line into series); "bar" for a category column plus a number (at most ~25 rows); otherwise "table".

TABLES
{schema}

EXAMPLES
Q: Which 5 videos have the most views?
{{"sql": "SELECT title, views FROM analytics.mart_top_videos_latest ORDER BY views_rank LIMIT 5", "chart_hint": "bar"}}

Q: How many views did the most viewed video gain each day?
{{"sql": "SELECT d.snapshot_date, d.views_gained FROM analytics.mart_gta_video_daily d WHERE d.video_id = (SELECT video_id FROM analytics.mart_top_videos_latest ORDER BY views_rank LIMIT 1) AND d.views_gained IS NOT NULL ORDER BY d.snapshot_date", "chart_hint": "line"}}

Q: GTA 6 mentions per hour by source in the last 12 hours of data
{{"sql": "SELECT event_hour, source, SUM(mentions) AS mentions FROM analytics.mart_mentions_hourly WHERE topic_group = 'gta6' AND event_hour > (SELECT max(event_hour) FROM analytics.mart_mentions_hourly) - interval '12 hours' GROUP BY event_hour, source ORDER BY event_hour", "chart_hint": "line"}}

Q: Which AI keywords are mentioned most?
{{"sql": "SELECT keyword, SUM(mentions) AS mentions FROM analytics.mart_keyword_daily WHERE topic_group = 'ai_tech' GROUP BY keyword ORDER BY mentions DESC LIMIT 10", "chart_hint": "bar"}}
"""


class AskFailed(RuntimeError):
    """Could not produce a valid, runnable query. .attempts lists every try with its error."""

    def __init__(self, message: str, attempts: list[dict]):
        super().__init__(message)
        self.attempts = attempts


class NotAnswerable(RuntimeError):
    """The model says the question is out of scope for the tables."""


def build_system_prompt() -> str:
    schema, _ = get_context()
    return SYSTEM_PROMPT.format(schema=schema)


def _parse_llm_json(raw: str) -> tuple[str | None, str | None, str | None]:
    """Returns (sql, chart_hint, reason). Tolerates ```json fences."""
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip())
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        raise SqlRejected("the answer was not valid JSON; reply with only the JSON object") from None
    if not isinstance(data, dict):
        raise SqlRejected("the answer must be a JSON object with keys 'sql' and 'chart_hint'")
    sql = data.get("sql")
    if sql is None:
        return None, None, str(data.get("reason") or "the question cannot be answered from the available tables")
    if not isinstance(sql, str):
        raise SqlRejected("'sql' must be a string")
    return sql, data.get("chart_hint"), None


def ask(question: str, llm: LLMProvider) -> dict:
    started = time.monotonic()
    _, tables = get_context()
    system = build_system_prompt()
    messages = [Message("user", question)]
    attempts: list[dict] = []

    for attempt in range(2):                      # first try + ONE retry with the error fed back
        raw = llm.generate(system, messages)
        sql = None
        try:
            sql, hint, reason = _parse_llm_json(raw)
            if sql is None:
                raise NotAnswerable(reason)
            safe_sql = validate_and_limit(sql, set(tables))
            columns, rows = run_query(safe_sql)
        except NotAnswerable:
            raise
        except (SqlRejected, psycopg.Error) as exc:
            msg = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
            log.warning("attempt %d failed: %s | sql=%r", attempt + 1, msg, (sql or "")[:200])
            attempts.append({"sql": sql, "error": msg})
            messages += [
                Message("model", raw),
                Message("user", f"That query failed: {msg}. Return corrected JSON in the same shape. "
                                f"Only analytics.* tables, one SELECT."),
            ]
            continue
        return {
            "question": question,
            "sql": safe_sql,
            "columns": columns,
            "rows": rows,
            "chart_hint": pick_chart(hint, columns, rows),
            "retries": attempt,
            "elapsed_ms": int((time.monotonic() - started) * 1000),
        }
    raise AskFailed("I could not turn that question into a valid query. Try rephrasing it.", attempts)
