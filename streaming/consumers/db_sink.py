"""Kafka (raw.bluesky, raw.wikipedia, raw.hackernews) -> Neon table raw.stream_events.

Delivery guarantee: AT-LEAST-ONCE.
  1. poll messages into an in-memory batch (<= 500 rows or 5 s)
  2. INSERT the batch ... ON CONFLICT (source, event_id) DO NOTHING, and COMMIT in Postgres
  3. only then commit the Kafka offsets
A crash between 2 and 3 means the batch is read again after restart; the primary key
turns the replay into harmless "duplicates". Nothing is ever lost.

Poison messages (not JSON, wrong shape, bad values) are logged with topic/partition/offset,
counted, skipped, and their offsets are committed with the rest so we never loop on them.

Run:  python -m streaming.consumers.db_sink [--dry-run] [--limit N] [--duration S]
Stop: Ctrl+C (flushes the batch, commits, closes) or create .state/db_sink.stop
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from psycopg.types.json import Jsonb

from streaming.common.backoff import Backoff
from streaming.common.config import load_settings
from streaming.common.envelope import SOURCES, topic_for
from streaming.common.log import setup_logging

log = logging.getLogger("db_sink")

GROUP_ID = "db_sink"
TOPICS = [topic_for(s) for s in SOURCES]
TOPIC_GROUPS = ("gta6", "ai_tech")        # mirrors the CHECK constraint in sql/002_stream_schema.sql
BATCH_MAX_ROWS = 500
BATCH_MAX_SECONDS = 5.0
STATS_EVERY = 30.0
RETRY_CAP_SECONDS = 30.0

COLUMNS = (
    "source", "event_id", "event_time", "text", "url", "matched_keywords",
    "topic_group", "payload", "kafka_topic", "kafka_partition", "kafka_offset",
)


class Poison(Exception):
    """The message can never be inserted, no matter how often we retry."""


@dataclass(frozen=True)
class Row:
    source: str
    event_id: str
    event_time: datetime
    text: str | None
    url: str | None
    matched_keywords: list[str]
    topic_group: str
    payload: dict
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int

    def params(self) -> tuple:
        return (self.source, self.event_id, self.event_time, self.text, self.url,
                self.matched_keywords, self.topic_group, Jsonb(self.payload),
                self.kafka_topic, self.kafka_partition, self.kafka_offset)


def parse_message(topic: str, partition: int, offset: int, value: bytes | None) -> Row:
    """Turn one Kafka message into a Row, or raise Poison with the reason."""
    if value is None:
        raise Poison("empty value")
    try:
        env = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Poison(f"not JSON ({type(exc).__name__}) head={value[:60]!r}") from None
    if not isinstance(env, dict):
        raise Poison(f"JSON is {type(env).__name__}, expected object")

    source, event_id = env.get("source"), env.get("event_id")
    if source not in SOURCES:
        raise Poison(f"bad source {source!r}")
    if topic_for(source) != topic:
        raise Poison(f"source {source!r} does not belong on topic {topic}")
    if not isinstance(event_id, str) or not event_id:
        raise Poison("missing event_id")
    if env.get("topic_group") not in TOPIC_GROUPS:
        raise Poison(f"bad topic_group {env.get('topic_group')!r}")
    try:
        event_time = datetime.fromisoformat(env["event_ts"])
    except (KeyError, TypeError, ValueError):
        raise Poison(f"bad event_ts {env.get('event_ts')!r}") from None
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)

    payload = env.get("payload")
    if not isinstance(payload, dict):
        raise Poison("payload is not an object")
    text, url = payload.get("text"), payload.get("url")
    keywords = payload.get("matched_keywords", [])
    if text is not None and not isinstance(text, str):
        raise Poison("payload.text is not a string")
    if url is not None and not isinstance(url, str):
        raise Poison("payload.url is not a string")
    if not isinstance(keywords, list) or not all(isinstance(k, str) for k in keywords):
        raise Poison("payload.matched_keywords is not a list of strings")

    return Row(source, event_id, event_time, text, url, keywords, env["topic_group"],
               payload, topic, partition, offset)


# ---------------------------------------------------------------- database ----

def _insert_sql(n_rows: int) -> str:
    one = "(" + ", ".join(["%s"] * len(COLUMNS)) + ")"
    return (f"INSERT INTO raw.stream_events ({', '.join(COLUMNS)}) VALUES "
            + ", ".join([one] * n_rows)
            + " ON CONFLICT (source, event_id) DO NOTHING")


def insert_batch(conn: psycopg.Connection, rows: list[Row]) -> int:
    """One multi-row INSERT in one transaction. Returns rows actually inserted."""
    flat: list[Any] = []
    for r in rows:
        flat.extend(r.params())
    with conn.transaction():  # commits on exit, rolls back on exception
        cur = conn.execute(_insert_sql(len(rows)), flat)
        return cur.rowcount


def insert_isolating(conn: psycopg.Connection, rows: list[Row]) -> tuple[int, int]:
    """Fallback after a data error: one transaction per row so a single bad row
    (e.g. a NUL character, which Postgres rejects) cannot block the other 499.
    Returns (inserted, poison)."""
    inserted = poison = 0
    for r in rows:
        try:
            inserted += insert_batch(conn, [r])
        except psycopg.DataError as exc:
            poison += 1
            log.error("poison (database rejected row) topic=%s partition=%d offset=%d event_id=%s err=%s",
                      r.kafka_topic, r.kafka_partition, r.kafka_offset, r.event_id, str(exc).splitlines()[0])
    return inserted, poison


# -------------------------------------------------------------------- sink ----

class Sink:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.stop = threading.Event()
        self.settings = load_settings()
        self.stop_file = self.settings.state_dir / "db_sink.stop"
        self.database_url = os.environ.get("DATABASE_URL")
        # counters (total / since last summary)
        self.consumed = self.inserted = self.duplicates = self.poison = 0
        self._last = (0, 0, 0, 0)
        # current batch
        self.rows: list[Row] = []
        self.offsets: dict[tuple[str, int], int] = {}   # (topic, partition) -> highest offset seen
        self.batch_started = 0.0
        self.consumer = Consumer({
            "bootstrap.servers": self.settings.kafka_bootstrap,
            # dry-run must not steal partitions from (or share offsets with) the real sink
            "group.id": GROUP_ID + ("-dryrun" if args.dry_run else ""),
            "enable.auto.commit": False,          # WE decide when a message counts as done
            "auto.offset.reset": "earliest",      # first run (no committed offset) reads from the start
            "max.poll.interval.ms": 600000,       # tolerate a long Neon outage without being kicked out
            "client.id": "db_sink",
        })

    # ---- main loop ---------------------------------------------------------
    def run(self) -> None:
        if not self.args.dry_run and not self.database_url:
            raise SystemExit("DATABASE_URL is not set (put it in .env)")
        self.stop_file.unlink(missing_ok=True)
        self.consumer.subscribe(TOPICS)
        log.info("started group=%s topics=%s dry_run=%s limit=%s duration=%s batch=%d rows/%.0fs",
                 GROUP_ID, TOPICS, self.args.dry_run, self.args.limit, self.args.duration,
                 BATCH_MAX_ROWS, BATCH_MAX_SECONDS)
        deadline = time.monotonic() + self.args.duration if self.args.duration else None
        next_stats = time.monotonic() + STATS_EVERY
        try:
            while not self.stop.is_set():
                if self.stop_file.exists() or (deadline and time.monotonic() >= deadline):
                    break
                if self.args.limit and self.consumed >= self.args.limit:
                    break
                msg = self.consumer.poll(1.0)
                if msg is not None:
                    self._handle(msg)
                if self._batch_due():
                    self._flush()
                if time.monotonic() >= next_stats:
                    self._log_stats("interval")
                    next_stats = time.monotonic() + STATS_EVERY
        finally:
            log.info("shutting down: flushing %d pending rows", len(self.rows))
            self._flush(shutting_down=True)
            self.consumer.close()
            self._log_stats("final")

    def _handle(self, msg) -> None:
        err = msg.error()
        if err is not None:
            if err.code() != KafkaError._PARTITION_EOF:
                log.error("kafka error %s", err)
                if err.fatal():
                    raise KafkaException(err)
            return
        self.consumed += 1
        topic, part, off = msg.topic(), msg.partition(), msg.offset()
        if not self.rows and not self.offsets:
            self.batch_started = time.monotonic()
        # Even a poison message advances the offset: it is "done" once we've logged it.
        key = (topic, part)
        self.offsets[key] = max(self.offsets.get(key, -1), off)
        try:
            self.rows.append(parse_message(topic, part, off, msg.value()))
        except Poison as why:
            self.poison += 1
            log.warning("poison topic=%s partition=%d offset=%d reason=%s", topic, part, off, why)

    def _batch_due(self) -> bool:
        if not self.offsets:
            return False
        return len(self.rows) >= BATCH_MAX_ROWS or time.monotonic() - self.batch_started >= BATCH_MAX_SECONDS

    # ---- flush = DB commit, then Kafka commit --------------------------------
    def _flush(self, shutting_down: bool = False) -> None:
        if not self.offsets:
            return
        if self.rows and not self.args.dry_run:
            if not self._write_with_retry(shutting_down):
                return   # shutdown interrupted a retry: offsets stay uncommitted, rows replay next start
        elif self.rows:
            self.inserted += len(self.rows)   # dry-run: pretend, nothing is written
            log.info("DRY-RUN would insert %d rows", len(self.rows))
        if not self.args.dry_run:
            self._commit_offsets()
        self.rows, self.offsets = [], {}

    def _write_with_retry(self, shutting_down: bool) -> bool:
        backoff = Backoff(base=1, cap=RETRY_CAP_SECONDS)
        while True:
            try:
                # Short-lived connection: Neon can scale to zero while we're idle.
                with psycopg.connect(self.database_url, connect_timeout=15) as conn:
                    try:
                        n = insert_batch(conn, self.rows)
                        poison = 0
                    except psycopg.DataError as exc:
                        log.warning("batch rejected (%s); retrying row by row", str(exc).splitlines()[0])
                        n, poison = insert_isolating(conn, self.rows)
                self.inserted += n
                self.poison += poison
                self.duplicates += len(self.rows) - n - poison
                log.info("batch written rows=%d inserted=%d duplicates=%d poison=%d",
                         len(self.rows), n, len(self.rows) - n - poison, poison)
                return True
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                # Neon down / network / timeout. NOT committing offsets, so nothing can be lost.
                delay = backoff.next_delay()
                log.error("database unavailable err=%s retry_in=%.1fs pending_rows=%d",
                          str(exc).splitlines()[0] if str(exc) else type(exc).__name__, delay, len(self.rows))
                if shutting_down and backoff.attempt >= 3:
                    log.error("giving up on shutdown flush; %d rows will be re-read on next start", len(self.rows))
                    return False
                if self.stop.wait(delay) and not shutting_down:
                    # Ctrl+C during an outage: take one last attempt through the shutdown path
                    shutting_down = True

    def _commit_offsets(self) -> None:
        # Kafka's committed offset is "the NEXT message to read", hence +1.
        to_commit = [TopicPartition(t, p, o + 1) for (t, p), o in self.offsets.items()]
        for attempt in range(1, 4):
            try:
                self.consumer.commit(offsets=to_commit, asynchronous=False)
                return
            except KafkaException as exc:
                log.error("offset commit failed attempt=%d err=%s", attempt, exc.args[0])
                time.sleep(attempt)
        log.error("giving up committing offsets; rows are already in the DB so a replay is harmless")

    # ---- stats -----------------------------------------------------------------
    def _log_stats(self, kind: str) -> None:
        cur = (self.consumed, self.inserted, self.duplicates, self.poison)
        d = [c - p for c, p in zip(cur, self._last)]
        self._last = cur
        log.info("stats kind=%s window_consumed=%d window_inserted=%d window_duplicates=%d window_poison=%d | "
                 "total_consumed=%d total_inserted=%d total_duplicates=%d total_poison=%d",
                 kind, *d, *cur)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dry-run", action="store_true", help="parse and validate only: no DB writes, no offset commits")
    p.add_argument("--limit", type=int, default=0, help="stop after consuming N messages (0 = no limit)")
    p.add_argument("--duration", type=int, default=0, help="stop after N seconds (0 = run forever)")
    args = p.parse_args()
    setup_logging()
    sink = Sink(args)
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):   # SIGBREAK exists on Windows only
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), lambda *_: sink.stop.set())
    sink.run()


if __name__ == "__main__":
    main()
