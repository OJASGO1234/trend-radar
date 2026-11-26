"""Bluesky Jetstream -> Kafka topic raw.bluesky.

Push source: one long-lived WebSocket. We keep only English posts that match
config/keywords.yaml. The last time_us (microsecond timestamp of the Jetstream
event) is saved to .state/bluesky_cursor.json so a restart resumes where it stopped.

Run:  python -m streaming.producers.bluesky [--dry-run] [--limit N] [--duration S]
"""
from __future__ import annotations

import json
import logging
import time
from urllib.parse import urlencode

from websockets.sync.client import connect

from streaming.common.backoff import Backoff
from streaming.common.envelope import from_unix, make_event
from streaming.common.keywords import KeywordFilter
from streaming.common.runtime import Runtime, parse_args
from streaming.common.state import load_state, save_state

log = logging.getLogger("bluesky")

JETSTREAM = "wss://jetstream2.us-east.bsky.network/subscribe"
REWIND_US = 2_000_000       # on resume, step back 2 s; duplicates are harmless (DB is idempotent)
CHECKPOINT_EVERY = 10       # seconds between flush + cursor save


def build_url(cursor: int | None) -> str:
    params = {"wantedCollections": "app.bsky.feed.post"}
    if cursor:
        params["cursor"] = str(cursor - REWIND_US)
    return f"{JETSTREAM}?{urlencode(params)}"


def to_event(msg: dict, kf: KeywordFilter) -> dict | None:
    """Return an envelope if msg is a new English post matching a keyword, else None."""
    commit = msg.get("commit")
    if msg.get("kind") != "commit" or not commit or commit.get("operation") != "create":
        return None
    record = commit.get("record") or {}
    langs = [str(x).split("-")[0].lower() for x in record.get("langs") or []]
    if "en" not in langs:
        return None
    text = record.get("text", "")
    hit = kf.match(text)
    if hit is None:
        return None
    group, words = hit
    did, rkey = msg["did"], commit["rkey"]
    return make_event(
        source="bluesky",
        event_id=f"{did}/{rkey}",  # a post is uniquely identified by author DID + record key
        topic_group=group,
        event_ts=from_unix(msg["time_us"] / 1_000_000),
        text=text,
        url=f"https://bsky.app/profile/{did}/post/{rkey}",
        matched_keywords=words,
        raw={"did": did, "rkey": rkey, "cid": commit.get("cid"), "langs": record.get("langs"),
             "createdAt": record.get("createdAt"), "reply": "reply" in record},
    )


def main() -> None:
    args = parse_args(__doc__.splitlines()[0])
    with Runtime("bluesky", args) as rt:
        kf = KeywordFilter.from_yaml(rt.settings.keywords_path)
        cursor_file = rt.state_path("bluesky_cursor.json")
        cursor = None if args.no_state else load_state(cursor_file).get("time_us")
        log.info("resume cursor=%s", cursor)
        backoff = Backoff(base=1, cap=60)
        last_checkpoint = time.monotonic()

        def checkpoint() -> None:
            # Flush FIRST: the cursor may only move past events Kafka has confirmed.
            if rt.flush() == 0 and cursor and not args.no_state and not args.dry_run:
                save_state(cursor_file, {"time_us": cursor})

        while not rt.should_stop():
            url = build_url(cursor)
            try:
                with connect(url, max_size=2**20, open_timeout=15, ping_interval=20) as ws:
                    log.info("connected url=%s", url)
                    while not rt.should_stop():
                        try:
                            raw = ws.recv(timeout=5)
                        except TimeoutError:
                            raw = None
                        if raw is not None:
                            backoff.reset()
                            rt.seen()
                            msg = json.loads(raw)
                            cursor = msg.get("time_us", cursor)
                            event = to_event(msg, kf)
                            if event:
                                rt.emit(event)
                        if time.monotonic() - last_checkpoint >= CHECKPOINT_EVERY:
                            checkpoint()
                            last_checkpoint = time.monotonic()
            except Exception as exc:  # network drop, server restart, bad frame...
                delay = backoff.next_delay()
                log.warning("connection lost err=%r reconnect_in=%.1fs cursor=%s", exc, delay, cursor)
                rt.stop.wait(delay)
        checkpoint()


if __name__ == "__main__":
    main()
