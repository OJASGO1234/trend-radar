"""Row counts in raw.stream_events by source / topic_group (short-lived connection).

Run:  python -m scripts.counts
"""
import os

import psycopg

from streaming.common.config import load_settings  # noqa: F401  (importing it loads .env)

with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=15) as conn:
    print(f"{'source':<12}{'topic_group':<13}{'rows':>6}  {'first event':<21}  last event")
    for r in conn.execute("SELECT source, topic_group, count(*), min(event_time), max(event_time) "
                          "FROM raw.stream_events GROUP BY 1,2 ORDER BY 1,2"):
        print(f"{r[0]:<12}{r[1]:<13}{r[2]:>6}  {r[3]:%Y-%m-%d %H:%M:%S}    {r[4]:%Y-%m-%d %H:%M:%S}")
    print("total", conn.execute("SELECT count(*) FROM raw.stream_events").fetchone()[0])
