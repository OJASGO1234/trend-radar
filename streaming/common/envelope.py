"""The common event envelope every producer emits (see CLAUDE.md)."""
from __future__ import annotations

from datetime import datetime, timezone

SOURCES = ("bluesky", "wikipedia", "hackernews")


def topic_for(source: str) -> str:
    return f"raw.{source}"


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def from_unix(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def make_event(
    *,
    source: str,
    event_id: str,
    topic_group: str,
    event_ts: datetime,
    text: str,
    url: str,
    matched_keywords: list[str],
    raw: dict,
) -> dict:
    """Envelope: event_id, source, topic_group, event_ts, ingested_ts, payload.

    payload always carries text/url/matched_keywords (the sink maps these to
    columns of raw.stream_events) plus a trimmed `raw` copy of the source record.
    """
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}")
    return {
        "event_id": event_id,
        "source": source,
        "topic_group": topic_group,
        "event_ts": iso(event_ts),
        "ingested_ts": iso(datetime.now(timezone.utc)),
        "payload": {
            "text": text,
            "url": url,
            "matched_keywords": matched_keywords,
            "raw": raw,
        },
    }
