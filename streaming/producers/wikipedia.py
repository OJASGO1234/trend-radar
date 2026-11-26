"""Wikipedia EventStreams (recentchange, SSE) -> Kafka topic raw.wikipedia.

Push source over plain HTTP: the server keeps the response open and writes
"event/id/data" blocks. We keep enwiki edits whose TITLE matches keywords.yaml.
The SSE `id:` of the last event is saved to .state/wikipedia_last_event_id.json and
sent back as the Last-Event-ID header on reconnect, so Wikimedia replays what we missed.

Run:  python -m streaming.producers.wikipedia [--dry-run] [--limit N] [--duration S]
"""
from __future__ import annotations

import json
import logging
import time
from urllib.parse import quote

import requests

from streaming.common.backoff import Backoff
from streaming.common.envelope import from_unix, make_event
from streaming.common.keywords import KeywordFilter
from streaming.common.runtime import Runtime, parse_args
from streaming.common.state import load_state, save_state

log = logging.getLogger("wikipedia")

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
CHECKPOINT_EVERY = 10


def to_event(data: dict, kf: KeywordFilter) -> dict | None:
    if data.get("wiki") != "enwiki" or data.get("type") not in ("edit", "new"):
        return None
    title = data.get("title", "")
    hit = kf.match(title)
    if hit is None:
        return None
    group, words = hit
    return make_event(
        source="wikipedia",
        event_id=data["meta"]["id"],  # UUID Wikimedia assigns to every event
        topic_group=group,
        event_ts=from_unix(data["timestamp"]),
        text=title,
        url=data.get("notify_url") or f"https://en.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}",
        matched_keywords=words,
        raw={k: data.get(k) for k in ("title", "type", "user", "bot", "minor", "comment", "length", "revision")},
    )


def sse_events(resp: requests.Response):
    """Minimal SSE parser: yields (event_id, data_str) for each complete event block."""
    event_id, data_lines = None, []
    for line in resp.iter_lines(decode_unicode=True):
        if line is None:
            continue
        if line == "":                       # blank line = end of one event
            if data_lines:
                yield event_id, "\n".join(data_lines)
            data_lines = []                  # (event_id is kept: SSE says it persists)
        elif line.startswith(":"):           # comment / keep-alive
            continue
        elif line.startswith("id:"):
            event_id = line[3:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())


def main() -> None:
    args = parse_args(__doc__.splitlines()[0])
    with Runtime("wikipedia", args) as rt:
        kf = KeywordFilter.from_yaml(rt.settings.keywords_path)
        state_file = rt.state_path("wikipedia_last_event_id.json")
        last_id = None if args.no_state else load_state(state_file).get("last_event_id")
        log.info("resume last_event_id=%s", last_id)
        backoff = Backoff(base=1, cap=60)
        last_checkpoint = time.monotonic()
        session = requests.Session()

        def checkpoint() -> None:
            if rt.flush() == 0 and last_id and not args.no_state and not args.dry_run:
                save_state(state_file, {"last_event_id": last_id})

        while not rt.should_stop():
            headers = {"User-Agent": rt.settings.wiki_user_agent, "Accept": "text/event-stream"}
            if last_id:
                headers["Last-Event-ID"] = last_id
            try:
                # (connect timeout, read timeout): the server sends keep-alives, so 30 s of
                # silence means the connection is dead.
                with session.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 30)) as resp:
                    resp.raise_for_status()
                    log.info("connected status=%s last_event_id=%s", resp.status_code, last_id)
                    resp.encoding = "utf-8"
                    for event_id, data_str in sse_events(resp):
                        backoff.reset()
                        rt.seen()
                        if event_id:
                            last_id = event_id
                        try:
                            data = json.loads(data_str)
                        except json.JSONDecodeError:
                            continue
                        event = to_event(data, kf)
                        if event:
                            rt.emit(event)
                        if time.monotonic() - last_checkpoint >= CHECKPOINT_EVERY:
                            checkpoint()
                            last_checkpoint = time.monotonic()
                        if rt.should_stop():
                            break
            except Exception as exc:
                delay = backoff.next_delay()
                log.warning("stream error err=%r reconnect_in=%.1fs", exc, delay)
                rt.stop.wait(delay)
        checkpoint()


if __name__ == "__main__":
    main()
