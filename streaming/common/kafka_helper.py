"""Kafka producer wrapper: acks=all + idempotence, key = event_id, delivery counting."""
from __future__ import annotations

import json
import logging
import threading

from confluent_kafka import KafkaError, Message, Producer

log = logging.getLogger(__name__)


class Stats:
    """Thread-safe counters: seen (read from source), kept (passed filter),
    sent (broker confirmed), failed (broker rejected / timed out)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.seen = self.kept = self.sent = self.failed = 0

    def add(self, **inc: int) -> None:
        with self._lock:
            for k, v in inc.items():
                setattr(self, k, getattr(self, k) + v)

    def snapshot(self) -> tuple[int, int, int, int]:
        with self._lock:
            return self.seen, self.kept, self.sent, self.failed


class EventProducer:
    def __init__(self, bootstrap: str, client_id: str, stats: Stats, metrics=None):
        self.stats = stats
        self.metrics = metrics  # ProducerMetrics or None
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": client_id,
                "acks": "all",                 # wait for all in-sync replicas
                "enable.idempotence": True,    # broker drops retry duplicates within a session
                "linger.ms": 50,               # small batches = fewer requests
                "compression.type": "lz4",
                "delivery.timeout.ms": 60000,  # give up on a message after 60 s
            }
        )

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.stats.add(failed=1)
            if self.metrics:
                self.metrics.failed()
            log.error("delivery failed key=%s err=%s", msg.key(), err)
        else:
            self.stats.add(sent=1)
            if self.metrics:
                self.metrics.sent()

    def send(self, topic: str, event: dict) -> None:
        value = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        key = event["event_id"].encode("utf-8")
        while True:
            try:
                self._producer.produce(topic, value=value, key=key, on_delivery=self._on_delivery)
                break
            except BufferError:  # local queue full: let delivery callbacks drain it
                self._producer.poll(0.5)
        self._producer.poll(0)  # serve delivery callbacks without blocking

    def flush(self, timeout: float = 15.0) -> int:
        """Wait for outstanding messages; returns how many are still unsent."""
        return self._producer.flush(timeout)
