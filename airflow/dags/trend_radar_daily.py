"""trend_radar_daily: the transform side of the pipeline, once a day.

   wait_for_collector  ->  dbt_source_freshness  ->  dbt_build  ->  log_mart_row_counts

The YouTube collector itself is NOT scheduled here: GitHub Actions runs it at 00:30 UTC
(works with the laptop off). This DAG starts at 01:30 UTC and first WAITS until the
collector has really finished successfully, then transforms the raw data with dbt.
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

log = logging.getLogger(__name__)

DBT = os.environ.get("DBT_BIN", "dbt")
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/repo/dbt")
MARTS = [
    "mart_gta_video_daily", "mart_top_videos_latest", "mart_mentions_hourly",
    "mart_keyword_daily", "mart_spikes",
]


def _connect():
    import psycopg2  # ships with the Airflow image
    return psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=30)


def collector_succeeded_today(data_interval_end, **_) -> bool:
    """True if raw.collector_runs has a 'success' started on the run's UTC date.

    A scheduled run at 01:30 UTC on day D has data_interval_end = D 01:30, so we look for a
    success since D 00:00 UTC. A manually triggered run uses "now", so "today" works too.
    """
    day_start = data_interval_end.in_timezone("UTC").start_of("day")
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT count(*), max(started_at) FROM raw.collector_runs
                   WHERE source = 'youtube_gta6' AND status = 'success'
                     AND started_at >= %s AND started_at < %s""",
                (day_start, day_start.add(days=1)),
            )
            n, last = cur.fetchone()
    finally:
        conn.close()   # short-lived connection: Neon can suspend between checks
    log.info("collector successes since %s: %s (latest %s)", day_start, n, last)
    return n > 0


def log_mart_row_counts() -> dict:
    counts = {}
    conn = _connect()
    try:
        with conn.cursor() as cur:
            for m in MARTS:
                cur.execute(f"SELECT count(*) FROM analytics.{m}")  # names come from the constant above
                counts[m] = cur.fetchone()[0]
    finally:
        conn.close()
    for m, n in counts.items():
        log.info("mart row count: %-24s %d", m, n)
    empty = [m for m, n in counts.items() if n == 0]
    if empty:
        raise ValueError(f"empty marts: {empty}")
    return counts


with DAG(
    dag_id="trend_radar_daily",
    description="Wait for the collector, check source freshness, dbt build, log mart counts",
    schedule="30 1 * * *",                       # 01:30 UTC, an hour after the GitHub Actions collector
    start_date=pendulum.datetime(2026, 11, 20, tz="UTC"),
    catchup=False,                               # do not backfill every missed day
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["trend-radar", "dbt"],
) as dag:
    wait_for_collector = PythonSensor(
        task_id="wait_for_collector",
        python_callable=collector_succeeded_today,
        mode="reschedule",                       # free the worker slot between checks
        poke_interval=10 * 60,                   # look every 10 minutes
        timeout=2 * 60 * 60,                     # give up after 2 hours (collector clearly failed)
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f'cd "{DBT_DIR}" && "{DBT}" source freshness',
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f'cd "{DBT_DIR}" && "{DBT}" build',
    )

    log_counts = PythonOperator(
        task_id="log_mart_row_counts",
        python_callable=log_mart_row_counts,
    )

    wait_for_collector >> dbt_source_freshness >> dbt_build >> log_counts
