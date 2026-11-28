"""Database access for the API. One short-lived read-only connection per call.

Safety layers that live here:
  (1) the connection logs in as the SELECT-only role api_readonly (DATABASE_URL_READONLY)
  (4) every query runs in a READ ONLY transaction with SET LOCAL statement_timeout
Short-lived connections also keep Neon free-tier compute usage low (it can suspend between requests).
"""
from __future__ import annotations

import datetime as dt
import decimal
from typing import Any

import psycopg

from .config import load_settings


class DbNotConfigured(RuntimeError):
    pass


def _json_safe(v: Any) -> Any:
    if isinstance(v, decimal.Decimal):
        # SUM()/COUNT() of integers come back as NUMERIC: show whole numbers as ints (2, not 2.0)
        return int(v) if v == v.to_integral_value() and v.as_tuple().exponent >= 0 else float(v)
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, (bool, int, float, str)) or v is None:
        return v
    return str(v)


def run_query(sql: str, params: tuple | dict | None = None, max_rows: int = 200) -> tuple[list[str], list[list[Any]]]:
    """Run one SELECT and return (column names, rows) with JSON-safe values."""
    cfg = load_settings()
    if not cfg.database_url_readonly:
        raise DbNotConfigured("DATABASE_URL_READONLY is not set (run: python -m scripts.create_readonly_role)")
    with psycopg.connect(cfg.database_url_readonly, connect_timeout=15) as conn:
        conn.read_only = True                       # BEGIN READ ONLY
        with conn.transaction():
            conn.execute(f"SET LOCAL statement_timeout = '{cfg.statement_timeout}'")
            cur = conn.execute(sql, params)
            columns = [d.name for d in cur.description or []]
            rows = [[_json_safe(v) for v in r] for r in cur.fetchmany(max_rows)]
    return columns, rows


def query_dicts(sql: str, params: tuple | dict | None = None, max_rows: int = 500) -> list[dict[str, Any]]:
    columns, rows = run_query(sql, params, max_rows)
    return [dict(zip(columns, r)) for r in rows]
