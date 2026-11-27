"""Hacker News (Firebase API, polled) -> Kafka topic raw.hackernews.

Pull source: HN has no push API. Every HN_POLL_SECONDS we ask for `maxitem` (the newest
item id), fetch every id between our last seen id and that, and keep stories/comments that
match keywords.yaml. The last seen id is saved to .state/hackernews_last_id.json.

Run:  python -m streaming.producers.hackernews [--dry-run] [--limit N] [--duration S]
"""
from __future__ import annotations

import html
import logging
import re
from concurrent.futures import ThreadPoolExecutor

import requests

from streaming.common.envelope import from_unix, make_event
from streaming.common.keywords import KeywordFilter
from streaming.common.runtime import Runtime, parse_args
from streaming.common.state import load_state, save_state

log = logging.getLogger("hackernews")

API = "https://hacker-news.firebaseio.com/v0"
MAX_CATCHUP = 500   # after a long outage, only fetch the newest 500 items (protects API + time)
WORKERS = 8
TAG_RE = re.compile(r"<[^>]+>")


def clean(text: str | None) -> str:
    """HN comment text is HTML: drop tags, decode &amp; and friends."""
    return html.unescape(TAG_RE.sub(" ", text or "")).strip()


def to_event(item: dict, kf: KeywordFilter) -> dict | None:
    if item.get("type") not in ("story", "comment") or item.get("deleted") or item.get("dead"):
        return None
    parts = [item.get("title"), clean(item.get("text"))]
    text = "\n".join(p for p in parts if p)
    hit = kf.match(text)
    if hit is None:
        return None
    group, words = hit
    # skip `kids` (can be a huge id list) and the raw HTML `text` (we already store it cleaned)
    raw = {k: v for k, v in item.items() if k not in ("kids", "text")}
    return make_event(
        source="hackernews",
        event_id=str(item["id"]),
        topic_group=group,
        event_ts=from_unix(item["time"]),
        text=text,
        url=f"https://news.ycombinator.com/item?id={item['id']}",
        matched_keywords=words,
        raw=raw,
    )


def main() -> None:
    args = parse_args(__doc__.splitlines()[0])
    with Runtime("hackernews", args) as rt:
        kf = KeywordFilter.from_yaml(rt.settings.keywords_path)
        state_file = rt.state_path("hackernews_last_id.json")
        last_id = None if args.no_state else load_state(state_file).get("last_id")
        poll = rt.settings.hn_poll_seconds
        log.info("resume last_id=%s poll_seconds=%d", last_id, poll)
        session = requests.Session()

        def fetch(item_id: int) -> dict | None:
            r = session.get(f"{API}/item/{item_id}.json", timeout=10)
            r.raise_for_status()
            return r.json()  # None (JSON null) = item not available (yet); treated as seen

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            while not rt.should_stop():
                try:
                    r = session.get(f"{API}/maxitem.json", timeout=10)
                    r.raise_for_status()
                    max_id = int(r.json())
                    if last_id is None:          # first ever run: start from "now", no backfill
                        last_id = max_id
                        log.info("first run, starting at maxitem=%d", max_id)
                    first = max(last_id + 1, max_id - MAX_CATCHUP + 1)
                    ids = list(range(first, max_id + 1))
                    new_last = last_id
                    # map() returns in order; stop advancing at the first failed fetch so that
                    # item is retried on the next poll instead of being skipped forever.
                    for item_id, fut in zip(ids, [pool.submit(fetch, i) for i in ids]):
                        try:
                            item = fut.result()
                        except Exception as exc:
                            log.warning("fetch failed id=%d err=%r (will retry next poll)", item_id, exc)
                            break
                        new_last = item_id
                        rt.seen()
                        if item:
                            event = to_event(item, kf)
                            if event:
                                rt.emit(event)
                    last_id = max(new_last, last_id) if ids else last_id
                    if rt.checkpoint_ok() and not args.no_state and not args.dry_run:
                        save_state(state_file, {"last_id": last_id})
                    log.info("poll done fetched=%d last_id=%s", len(ids), last_id)
                except requests.RequestException as exc:
                    log.warning("poll failed err=%r", exc)
                rt.stop.wait(poll)


if __name__ == "__main__":
    main()
