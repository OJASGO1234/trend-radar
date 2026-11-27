"""Prove the sink lost nothing and duplicated nothing: compare Kafka with raw.stream_events.

Reads every message in the 3 topics (offset 0 to the current end, using assign(): no
group, no commits) and checks:
  1. no loss   : every VALID Kafka message has a row in Neon with the same (source, event_id)
  2. no dups   : rows in Neon == distinct valid (source, event_id) in Kafka
  3. no orphan : every Neon row's kafka_topic/partition/offset really exists in Kafka
  4. lag       : end offsets minus committed offsets of group db_sink
Run:  python -m scripts.verify_sink      Exit code 0 = all good.
"""
from __future__ import annotations

import os
import sys
from collections import Counter

import psycopg
from confluent_kafka import Consumer, TopicPartition

from streaming.common.config import load_settings
from streaming.common.log import setup_logging
from streaming.consumers.db_sink import GROUP_ID, TOPICS, Poison, parse_message


def main() -> int:
    setup_logging()
    st = load_settings()
    c = Consumer({"bootstrap.servers": st.kafka_bootstrap, "group.id": GROUP_ID,
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    ends: dict[tuple[str, int], int] = {}
    for t in TOPICS:
        for p in c.list_topics(t, timeout=10).topics[t].partitions:
            ends[(t, p)] = c.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1]
    # committed() only reads; we never commit from this script
    committed = c.committed([TopicPartition(t, p) for (t, p) in ends], timeout=10)
    c.assign([TopicPartition(t, p, 0) for (t, p) in ends])

    valid: dict[tuple[str, str], tuple[str, int, int]] = {}
    kafka_msgs: Counter = Counter()
    poison: Counter = Counter()
    positions: set[tuple[str, int, int]] = set()
    done = {k: end == 0 for k, end in ends.items()}
    while not all(done.values()):
        m = c.poll(2.0)
        if m is None or m.error():
            continue
        k = (m.topic(), m.partition())
        kafka_msgs[m.topic()] += 1
        positions.add((m.topic(), m.partition(), m.offset()))
        try:
            r = parse_message(m.topic(), m.partition(), m.offset(), m.value())
            valid.setdefault((r.source, r.event_id), (m.topic(), m.partition(), m.offset()))
        except Poison:
            poison[m.topic()] += 1
        if m.offset() >= ends[k] - 1:
            done[k] = True
    c.close()

    with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=15) as conn:
        rows = conn.execute("SELECT source, event_id, kafka_topic, kafka_partition, kafka_offset "
                            "FROM raw.stream_events").fetchall()
    db_keys = {(r[0], r[1]) for r in rows}
    missing = set(valid) - db_keys
    extra = db_keys - set(valid)
    orphans = [r for r in rows if (r[2], r[3], r[4]) not in positions]
    lag = {f"{tp.topic}[{tp.partition}]": ends[(tp.topic, tp.partition)] - max(tp.offset, 0) for tp in committed}

    print(f"{'topic':<16}{'kafka msgs':>11}{'poison':>8}{'valid':>7}")
    for t in TOPICS:
        v = sum(1 for k in valid if f"raw.{k[0]}" == t)
        print(f"{t:<16}{kafka_msgs[t]:>11}{poison[t]:>8}{v:>7}")
    print(f"\nkafka: distinct valid (source,event_id)  : {len(valid)}")
    print(f"neon : rows in raw.stream_events         : {len(rows)}")
    print(f"MISSING in DB (loss)                     : {len(missing)}")
    print(f"EXTRA in DB (not in Kafka)               : {len(extra)}")
    print(f"rows pointing at a non-existent offset   : {len(orphans)}")
    print(f"consumer lag per partition               : {lag}")
    ok = not missing and not extra and not orphans and len(rows) == len(valid)
    print("\nRESULT:", "OK - no loss, no duplicates" if ok else "PROBLEM")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
