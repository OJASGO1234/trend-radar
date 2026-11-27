"""Prometheus metrics for producers and the sink (prometheus_client).

Prometheus PULLS: each process serves plain text at http://<host>:<port>/metrics and the
Prometheus server scrapes it every 15 s. We only ever increment / set values here.
"""
from __future__ import annotations

import logging

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

log = logging.getLogger(__name__)

# Seconds. Wide on purpose: live events take ~1 s, a replayed backlog takes hours.
LATENCY_BUCKETS = (0.5, 1, 2, 5, 10, 30, 60, 120, 300, 900, 3600, 21600, 86400)
INSERT_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)


def serve(addr: str, port: int, registry: CollectorRegistry) -> bool:
    """Start the /metrics HTTP server in a background thread. Never fatal: a busy port
    must not take the pipeline down, we just lose monitoring."""
    try:
        start_http_server(port, addr=addr, registry=registry)
        log.info("metrics listening on http://%s:%d/metrics", addr, port)
        return True
    except OSError as exc:
        log.error("could not start metrics server on %s:%d err=%r (continuing without it)", addr, port, exc)
        return False


class ProducerMetrics:
    """Counters carry a `source` label so one dashboard query covers all producers."""

    def __init__(self, source: str):
        self.source = source
        self.registry = r = CollectorRegistry()  # own registry: several can coexist (tests)
        self._seen = Counter("events_seen", "Events read from the source", ["source"], registry=r)
        self._kept = Counter("events_kept", "Events that passed the keyword filter", ["source"], registry=r)
        self._sent = Counter("events_sent", "Events confirmed by the Kafka broker", ["source"], registry=r)
        self._failed = Counter("events_failed", "Events the broker rejected or timed out", ["source"], registry=r)
        self._latency = Histogram(
            "event_latency_seconds", "ingested_ts minus event_ts (source -> our producer)",
            ["source"], buckets=LATENCY_BUCKETS, registry=r)
        # touch the label so the series exists (value 0) from the first scrape
        for c in (self._seen, self._kept, self._sent, self._failed):
            c.labels(source)

    def seen(self, n: int = 1) -> None:
        self._seen.labels(self.source).inc(n)

    def kept(self) -> None:
        self._kept.labels(self.source).inc()

    def sent(self) -> None:
        self._sent.labels(self.source).inc()

    def failed(self) -> None:
        self._failed.labels(self.source).inc()

    def latency(self, seconds: float) -> None:
        self._latency.labels(self.source).observe(max(seconds, 0.0))


class SinkMetrics:
    def __init__(self):
        self.registry = r = CollectorRegistry()
        self.consumed = Counter("messages_consumed", "Kafka messages polled by db_sink", ["topic"], registry=r)
        self.inserted = Counter("rows_inserted", "Rows newly inserted into raw.stream_events", registry=r)
        self.duplicate = Counter("rows_duplicate", "Rows skipped by ON CONFLICT DO NOTHING", registry=r)
        self.poison = Counter("poison", "Messages/rows that can never be inserted", ["topic"], registry=r)
        self.batch_seconds = Histogram("batch_insert_seconds", "Connect + INSERT + COMMIT time per batch",
                                       buckets=INSERT_BUCKETS, registry=r)
        self.lag = Gauge("consumer_lag", "High watermark minus committed offset", ["topic", "partition"], registry=r)
        self.last_commit = Gauge("last_commit_timestamp_seconds", "Unix time of the last successful offset commit", registry=r)
