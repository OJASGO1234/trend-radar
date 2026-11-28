"""Create (or refresh) the Neon role `api_readonly` used by the API, and write DATABASE_URL_READONLY to .env.

What the role can do:   SELECT on every table/view in schema `analytics`. Nothing else.
What it can NOT do:     read schema raw, write anything, create objects.
Defaults:               statement_timeout = 5s, default_transaction_read_only = on.

Run once (safe to re-run; re-running rotates the password):
    python -m scripts.create_readonly_role

If your Neon user may not create roles, the script prints the manual console steps instead.
"""
from __future__ import annotations

import os
import re
import secrets
import sys
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

import psycopg
from dotenv import load_dotenv
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
ROLE = "api_readonly"
ENV_FILE = ROOT / ".env"

MANUAL_STEPS = f"""
Could not create the role from SQL. Do it by hand in the Neon console:
  1. console.neon.tech -> your project -> Branch -> Roles & Databases -> "New Role"
     name: {ROLE}  (copy the generated password)
  2. SQL Editor, run (as the owner):
       ALTER ROLE {ROLE} SET statement_timeout = '5s';
       ALTER ROLE {ROLE} SET default_transaction_read_only = on;
       GRANT USAGE ON SCHEMA analytics TO {ROLE};
       GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO {ROLE};
       ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO {ROLE};
  3. Put this in .env (same host/db as DATABASE_URL, only user and password change):
       DATABASE_URL_READONLY=postgresql://{ROLE}:<password>@<host>/<db>?sslmode=require
  4. Re-run this script's checks:  python -m scripts.create_readonly_role --verify-only
"""


def readonly_url(owner_url: str, password: str) -> str:
    parts = urlsplit(owner_url)
    host = parts.netloc.rsplit("@", 1)[1]  # drop the owner's user:password
    return urlunsplit((parts.scheme, f"{ROLE}:{quote(password, safe='')}@{host}", parts.path, parts.query, ""))


def write_env(key: str, value: str) -> None:
    text = ENV_FILE.read_text(encoding="utf-8") if ENV_FILE.exists() else ""
    line = f"{key}={value}"
    if re.search(rf"^{key}=.*$", text, flags=re.M):
        text = re.sub(rf"^{key}=.*$", lambda _: line, text, flags=re.M)
    else:
        text = text.rstrip("\n") + ("\n" if text else "") + line + "\n"
    ENV_FILE.write_text(text, encoding="utf-8")


def create_role(owner_url: str) -> str:
    password = secrets.token_urlsafe(24)  # url-safe characters only: [A-Za-z0-9_-]
    with psycopg.connect(owner_url, connect_timeout=30, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (ROLE,)).fetchone()
        verb = "ALTER" if exists else "CREATE"
        # utility statements cannot take bind parameters, so the password is quoted as a literal
        conn.execute(sql.SQL(f"{verb} ROLE {ROLE} LOGIN PASSWORD {{}}").format(sql.Literal(password)))
        for stmt in (
            f"ALTER ROLE {ROLE} SET statement_timeout = '5s'",
            f"ALTER ROLE {ROLE} SET default_transaction_read_only = on",
            f"GRANT USAGE ON SCHEMA analytics TO {ROLE}",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO {ROLE}",
            # dbt drops and recreates tables on every build; without this the grant would vanish each time
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO {ROLE}",
        ):
            conn.execute(stmt)
        print(f"{'updated' if exists else 'created'} role {ROLE}; grants applied")
    return password


def verify(url: str) -> bool:
    """Prove the role is locked down: allowed read, blocked everything else."""
    ok = True

    def check(label: str, sql_text: str, should_work: bool) -> None:
        nonlocal ok
        try:
            with psycopg.connect(url, connect_timeout=30) as conn:
                row = conn.execute(sql_text).fetchone()
            result, detail = True, str(row)[:60]
        except psycopg.Error as exc:
            result, detail = False, type(exc).__name__ + ": " + str(exc).splitlines()[0][:70]
        good = result == should_work
        ok &= good
        print(f"  [{'OK' if good else 'FAIL'}] {label:<46} -> {'worked' if result else 'blocked'}  {detail}")

    check("SELECT from analytics.mart_spikes", "SELECT count(*) FROM analytics.mart_spikes", True)
    check("SELECT from raw.stream_events", "SELECT count(*) FROM raw.stream_events", False)
    check("CREATE TABLE analytics.x", "CREATE TABLE analytics.x (a int)", False)
    check("DROP TABLE analytics.mart_spikes", "DROP TABLE analytics.mart_spikes", False)
    check("INSERT into raw.collector_runs", "INSERT INTO raw.collector_runs (source) VALUES ('x')", False)
    check("SHOW statement_timeout (expect 5s)", "SELECT current_setting('statement_timeout')", True)
    check("pg_sleep(8) is cut at the 5 s timeout", "SELECT pg_sleep(8)", False)
    return ok


def main() -> int:
    load_dotenv(ENV_FILE)
    owner_url = os.environ.get("DATABASE_URL")
    if not owner_url:
        print("DATABASE_URL is not set in .env", file=sys.stderr)
        return 1
    if "--verify-only" in sys.argv:
        url = os.environ.get("DATABASE_URL_READONLY")
        if not url:
            print("DATABASE_URL_READONLY is not set", file=sys.stderr)
            return 1
        return 0 if verify(url) else 1
    try:
        password = create_role(owner_url)
    except psycopg.errors.InsufficientPrivilege:
        print(MANUAL_STEPS)
        return 2
    write_env("DATABASE_URL_READONLY", readonly_url(owner_url, password))
    print("wrote DATABASE_URL_READONLY to .env (gitignored)")
    print("verifying the role:")
    return 0 if verify(os.environ.get("DATABASE_URL_READONLY") or readonly_url(owner_url, password)) else 1


if __name__ == "__main__":
    sys.exit(main())
