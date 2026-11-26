"""Shared producer scaffolding: CLI flags, stop signal, 30 s stats log, emit()."""
from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
import time
from pathlib import Path

from .config import Settings, load_settings
from .envelope import topic_for
from .kafka_helper import EventProducer, Stats
from .log import setup_logging

log = logging.getLogger(__name__)
STATS_EVERY = 30  # seconds


def parse_args(description: str) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--dry-run", action="store_true", help="filter and log events, do not touch Kafka")
    p.add_argument("--limit", type=int, default=0, help="stop after this many KEPT events (0 = no limit)")
    p.add_argument("--duration", type=int, default=0, help="stop after N seconds (0 = run forever)")
    p.add_argument("--no-state", action="store_true", help="ignore and do not write the local cursor/state file")
    return p.parse_args()


class Runtime:
    """Use as `with Runtime(source, args) as rt:`, then call rt.seen() / rt.emit(event)."""

    def __init__(self, source: str, args: argparse.Namespace):
        setup_logging()
        self.source, self.args = source, args
        self.settings: Settings = load_settings()
        self.topic = topic_for(source)
        self.stats = Stats()
        self.stop = threading.Event()
        self._deadline = time.monotonic() + args.duration if args.duration else None
        self._producer = (
            None if args.dry_run
            else EventProducer(self.settings.kafka_bootstrap, f"producer-{source}", self.stats)
        )
        self._reporter = threading.Thread(target=self._report_loop, daemon=True)
        self._last = (0, 0, 0, 0)

    # --- lifecycle -------------------------------------------------------
    def __enter__(self) -> "Runtime":
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        self._reporter.start()
        log.info("started source=%s topic=%s dry_run=%s limit=%s duration=%s",
                 self.source, self.topic, self.args.dry_run, self.args.limit, self.args.duration)
        return self

    def __exit__(self, *exc) -> None:
        self.stop.set()
        remaining = self.flush()
        self._log_stats("final")
        if remaining:
            log.error("%d messages were NOT delivered before exit", remaining)

    # --- API for producers -------------------------------------------------
    def seen(self, n: int = 1) -> None:
        self.stats.add(seen=n)

    def emit(self, event: dict) -> None:
        self.stats.add(kept=1)
        if self._producer is None:
            log.info("DRY-RUN event %s", json.dumps(event, ensure_ascii=False)[:400])
        else:
            self._producer.send(self.topic, event)
        if self.args.limit and self.stats.snapshot()[1] >= self.args.limit:
            self.stop.set()

    def flush(self) -> int:
        """Block until everything emitted so far is confirmed by the broker."""
        return self._producer.flush() if self._producer else 0

    def should_stop(self) -> bool:
        if self._deadline and time.monotonic() >= self._deadline:
            self.stop.set()
        return self.stop.is_set()

    def state_path(self, name: str) -> Path:
        return self.settings.state_dir / name

    # --- stats -------------------------------------------------------------
    def _report_loop(self) -> None:
        while not self.stop.wait(STATS_EVERY):
            self._log_stats("interval")

    def _log_stats(self, kind: str) -> None:
        cur = self.stats.snapshot()
        d = [c - p for c, p in zip(cur, self._last)]
        self._last = cur
        log.info("stats kind=%s window_seen=%d window_kept=%d window_sent=%d | "
                 "total_seen=%d total_kept=%d total_sent=%d total_failed=%d",
                 kind, d[0], d[1], d[2], *cur)
