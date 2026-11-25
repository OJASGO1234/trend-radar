"""Apply sql/*.sql to Neon in filename order.

Every file uses IF NOT EXISTS, so re-running is safe. Each file runs in its own
transaction; one short-lived connection, closed when done (Neon compute hours).
"""
import argparse
import logging
import os
import sys
from pathlib import Path

import psycopg
import psycopg.sql
from dotenv import load_dotenv

log = logging.getLogger("apply_schema")
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="list files, don't connect")
    ap.add_argument("--limit", type=int, default=None, help="apply only the first N files")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    files = sorted(SQL_DIR.glob("*.sql"))[: args.limit]
    if args.dry_run:
        for f in files:
            log.info("would apply %s", f.name)
        return 0

    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        log.error("DATABASE_URL is not set (put it in .env)")
        return 1

    with psycopg.connect(url) as conn:
        for f in files:
            log.info("applying %s", f.name)
            conn.execute(f.read_text(encoding="utf-8"))
            conn.commit()
        rows = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema = 'raw' ORDER BY 1, 2"
        ).fetchall()
        # Names come from information_schema (not user input), but quote them anyway.
        for schema, table in rows:
            n = conn.execute(
                psycopg.sql.SQL("SELECT count(*) FROM {}.{}").format(
                    psycopg.sql.Identifier(schema), psycopg.sql.Identifier(table)
                )
            ).fetchone()[0]
            log.info("table: %s.%s rows=%d", schema, table, n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
