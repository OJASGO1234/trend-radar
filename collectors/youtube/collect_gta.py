"""Daily YouTube collector for the GTA 6 theme.

Steps (quota units in brackets):
  1. search.list x2 ("GTA 6", "GTA VI"), last 24h, order=viewCount   [100 each]
     -> upsert raw.yt_videos
  2. videos.list in batches of 50 for ALL tracked videos              [1 per batch]
     -> insert one snapshot per video per day into raw.yt_video_stats
  3. commentThreads.list for the top videos by views                  [1 per video]
     -> upsert raw.yt_comments
Every run is logged in raw.collector_runs. Safe to re-run the same day.
"""
import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.types.json import Jsonb
from dotenv import load_dotenv

log = logging.getLogger("yt_collect")

API = "https://www.googleapis.com/youtube/v3"
QUERIES = ["GTA 6", "GTA VI"]
SEARCH_COST = 100          # search.list costs 100 units per call
LIST_COST = 1              # videos.list / commentThreads.list cost 1 unit per call
QUOTA_BUDGET = 3000        # our self-imposed ceiling (YouTube allows 10,000/day)
COMMENT_VIDEOS = 10        # fetch comments only for the top N videos
COMMENTS_PER_VIDEO = 50


class Quota:
    """Counts estimated units and refuses to go over our budget."""

    def __init__(self, budget: int):
        self.budget, self.used = budget, 0

    def spend(self, units: int) -> None:
        if self.used + units > self.budget:
            raise RuntimeError(f"quota budget {self.budget} would be exceeded (used {self.used})")
        self.used += units


def api_get(endpoint: str, key: str, **params) -> dict:
    """GET one YouTube API endpoint. Never log the URL: it contains the API key."""
    url = f"{API}/{endpoint}?{urllib.parse.urlencode({**params, 'key': key})}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        body = json.loads(e.read() or b"{}")
        err = body.get("error", {})
        reason = (err.get("errors") or [{}])[0].get("reason", "")
        raise YouTubeError(e.code, reason, err.get("message", "")) from None


class YouTubeError(Exception):
    def __init__(self, status: int, reason: str, message: str):
        super().__init__(f"YouTube API {status} {reason}: {message}")
        self.status, self.reason = status, reason


@contextmanager
def db(url: str):
    """Short-lived connection (Neon bills compute time; don't hold it open while idle)."""
    with psycopg.connect(url) as conn:
        yield conn


def search_videos(key, quota, limit):
    """Return {video_id: (title, channel_id, channel_title, published_at, source_query)}."""
    after = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    found = {}
    for q in QUERIES:
        quota.spend(SEARCH_COST)
        data = api_get("search", key, part="snippet", q=q, type="video", order="viewCount",
                       publishedAfter=after, maxResults=min(50, limit or 50))
        for item in data.get("items", []):
            s = item["snippet"]
            # setdefault: if both queries return a video, keep the first query as source_query
            found.setdefault(item["id"]["videoId"],
                             (s["title"], s["channelId"], s.get("channelTitle"), s["publishedAt"], q))
        log.info("search %r -> %d items", q, len(data.get("items", [])))
    return found


def fetch_stats(key, quota, video_ids):
    """videos.list in batches of 50. Returns {video_id: (views, likes, comments)}."""
    stats = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        quota.spend(LIST_COST)
        data = api_get("videos", key, part="statistics", id=",".join(batch), maxResults=50)
        for item in data.get("items", []):
            st = item["statistics"]
            # YouTube returns counts as strings, and omits a field when it is hidden (e.g. likes)
            to_int = lambda k: int(st[k]) if k in st else None  # noqa: E731
            stats[item["id"]] = (to_int("viewCount"), to_int("likeCount"), to_int("commentCount"))
    log.info("stats: asked %d ids, got %d (rest deleted/private)", len(video_ids), len(stats))
    return stats


def fetch_comments(key, quota, video_id):
    """Top comments of one video. Returns rows, or [] if comments are disabled."""
    quota.spend(LIST_COST)
    try:
        data = api_get("commentThreads", key, part="snippet", videoId=video_id,
                       order="relevance", maxResults=COMMENTS_PER_VIDEO, textFormat="plainText")
    except YouTubeError as e:
        if e.reason in ("commentsDisabled", "forbidden", "videoNotFound"):
            log.warning("no comments for %s (%s)", video_id, e.reason)
            return []
        raise
    rows = []
    for item in data.get("items", []):
        c = item["snippet"]["topLevelComment"]
        s = c["snippet"]
        rows.append((c["id"], video_id, s["textDisplay"], s.get("likeCount", 0), s["publishedAt"]))
    return rows


def run(args) -> int:
    load_dotenv()
    key = os.environ.get("YOUTUBE_API_KEY")
    dburl = os.environ.get("DATABASE_URL")
    if not key or (not dburl and not args.dry_run):
        log.error("YOUTUBE_API_KEY and DATABASE_URL must be set (see .env.example)")
        return 1

    quota = Quota(QUOTA_BUDGET)
    today = datetime.now(timezone.utc).date()  # snapshot_date is the UTC day
    counts = {"yt_videos": 0, "yt_video_stats": 0, "yt_comments": 0}
    run_id = None

    if args.dry_run:
        # No DB at all in dry-run: only the videos found by search are used for stats/comments.
        found = search_videos(key, quota, args.limit)
        ids = list(found)[: args.limit] if args.limit else list(found)
        stats = fetch_stats(key, quota, ids)
        top = sorted(stats, key=lambda v: stats[v][0] or 0, reverse=True)[:COMMENT_VIDEOS]
        n_comments = sum(len(fetch_comments(key, quota, v)) for v in top)
        log.info("DRY RUN: would upsert %d videos, insert %d stats, upsert %d comments; quota ~%d",
                 len(found), len(stats), n_comments, quota.used)
        return 0

    try:
        with db(dburl) as conn:
            run_id = conn.execute(
                "INSERT INTO raw.collector_runs (source) VALUES ('youtube_gta6') RETURNING run_id"
            ).fetchone()[0]
            conn.commit()
        log.info("run_id=%d started", run_id)

        # 1) discover new videos (no DB connection open during API calls)
        found = search_videos(key, quota, args.limit)
        with db(dburl) as conn:
            conn.cursor().executemany(
                """INSERT INTO raw.yt_videos
                       (video_id, title, channel_id, channel_title, published_at, source_query)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (video_id) DO UPDATE
                     SET title = EXCLUDED.title, channel_title = EXCLUDED.channel_title""",
                [(vid, *v) for vid, v in found.items()],
            )
            counts["yt_videos"] = len(found)
            # 2) every video we have ever tracked, not just today's
            q = "SELECT video_id FROM raw.yt_videos ORDER BY first_seen_at"
            tracked = [r[0] for r in conn.execute(q).fetchall()]
            conn.commit()
        if args.limit:
            tracked = tracked[: args.limit]

        stats = fetch_stats(key, quota, tracked)
        top = sorted(stats, key=lambda v: stats[v][0] or 0, reverse=True)[:COMMENT_VIDEOS]
        comments = []
        for vid in top:
            comments += fetch_comments(key, quota, vid)

        # 3) write snapshots + comments in one short transaction
        with db(dburl) as conn:
            cur = conn.cursor()
            # same-day re-run overwrites that day's snapshot instead of duplicating it
            cur.executemany(
                """INSERT INTO raw.yt_video_stats
                       (video_id, snapshot_date, view_count, like_count, comment_count)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (video_id, snapshot_date) DO UPDATE
                     SET view_count = EXCLUDED.view_count, like_count = EXCLUDED.like_count,
                         comment_count = EXCLUDED.comment_count, fetched_at = now()""",
                [(vid, today, *s) for vid, s in stats.items()],
            )
            counts["yt_video_stats"] = len(stats)
            cur.executemany(
                """INSERT INTO raw.yt_comments
                       (comment_id, video_id, text, like_count, published_at)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (comment_id) DO UPDATE
                     SET like_count = EXCLUDED.like_count, text = EXCLUDED.text, fetched_at = now()""",
                comments,
            )
            counts["yt_comments"] = len(comments)
            conn.execute(
                """UPDATE raw.collector_runs
                   SET finished_at = now(), status = 'success', rows_written = %s, details = %s
                   WHERE run_id = %s""",
                (sum(counts.values()), Jsonb({"rows": counts, "quota_units": quota.used,
                                              "queries": QUERIES}), run_id),
            )
            conn.commit()
        log.info("run_id=%d success rows=%s quota_units=%d", run_id, counts, quota.used)
        return 0
    except Exception as e:  # noqa: BLE001 - we log the failure in the DB, then re-raise
        log.error("run failed: %s", e)
        if run_id is not None:
            with db(dburl) as conn:
                conn.execute(
                    """UPDATE raw.collector_runs
                       SET finished_at = now(), status = 'failed', error = %s,
                           rows_written = %s, details = %s WHERE run_id = %s""",
                    (str(e), sum(counts.values()),
                     Jsonb({"rows": counts, "quota_units": quota.used}), run_id),
                )
                conn.commit()
        return 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--dry-run", action="store_true",
                    help="call the API (spends quota) but don't touch the database")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap results per search and number of videos for stats (cheap testing)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
