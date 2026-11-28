"""FastAPI app. Run:  uvicorn api.main:app --reload --port 8000   (or scripts/run_app.ps1)

Every endpoint reads through api/db.py, i.e. as the SELECT-only role api_readonly.
"""
from __future__ import annotations

import logging
from typing import Literal

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .db import DbNotConfigured, query_dicts
from .llm import LLMError, get_provider
from .text_to_sql import AskFailed, NotAnswerable, ask

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("api")

app = FastAPI(title="Trend Radar API", version="1.0")

TopicGroup = Literal["gta6", "ai_tech"]
Source = Literal["bluesky", "wikipedia", "hackernews"]


@app.exception_handler(DbNotConfigured)
def _db_not_configured(_, exc: DbNotConfigured):
    return JSONResponse(status_code=503, content={"detail": str(exc)})


@app.exception_handler(psycopg.OperationalError)
def _db_down(_, exc: psycopg.OperationalError):
    log.error("database unavailable: %s", str(exc).splitlines()[0])
    return JSONResponse(status_code=503, content={"detail": "database unavailable, try again in a moment"})


@app.get("/api/health")
def health():
    """DB reachable + last collector run + newest streamed event (from the live view mart_pipeline_status)."""
    try:
        row = query_dicts("SELECT * FROM analytics.mart_pipeline_status")[0]
    except psycopg.Error as exc:
        return JSONResponse(status_code=503, content={"db": "error", "detail": str(exc).splitlines()[0]})
    return {"db": "ok", **row}


@app.get("/api/gta/top-videos")
def top_videos(limit: int = Query(10, ge=1, le=100)):
    return query_dicts(
        """SELECT video_id, title, channel_title, is_short, views, likes, comments, engagement_rate,
                  views_rank, latest_snapshot_date
           FROM analytics.mart_top_videos_latest ORDER BY views_rank, video_id LIMIT %s""", (limit,))


@app.get("/api/gta/daily")
def video_daily(video_id: str = Query(..., min_length=3, max_length=40)):
    rows = query_dicts(
        """SELECT snapshot_date, views, likes, comments, views_gained, days_since_prev, engagement_rate, title
           FROM analytics.mart_gta_video_daily WHERE video_id = %s ORDER BY snapshot_date""", (video_id,))
    if not rows:
        raise HTTPException(404, f"no data for video_id {video_id!r}")
    return rows


@app.get("/api/mentions/hourly")
def mentions_hourly(topic_group: TopicGroup, source: Source | None = None,
                    hours: int = Query(72, ge=1, le=24 * 30)):
    """Mentions per hour (and source) for the newest `hours` hours of DATA (not of wall-clock time)."""
    return query_dicts(
        """SELECT event_hour, source, mentions FROM analytics.mart_mentions_hourly
           WHERE topic_group = %(t)s AND (%(s)s::text IS NULL OR source = %(s)s::text)
             AND event_hour > (SELECT max(event_hour) FROM analytics.mart_mentions_hourly) - make_interval(hours => %(h)s)
           ORDER BY event_hour, source""", {"t": topic_group, "s": source, "h": hours})


@app.get("/api/keywords")
def keywords(days: int = Query(7, ge=1, le=90), topic_group: TopicGroup | None = None,
             limit: int = Query(15, ge=1, le=50)):
    """Top keywords over the newest `days` days of data."""
    return query_dicts(
        """SELECT topic_group, keyword, SUM(mentions)::int AS mentions FROM analytics.mart_keyword_daily
           WHERE (%(t)s::text IS NULL OR topic_group = %(t)s::text)
             AND event_date > (SELECT max(event_date) FROM analytics.mart_keyword_daily) - %(d)s
           GROUP BY topic_group, keyword ORDER BY mentions DESC LIMIT %(l)s""",
        {"t": topic_group, "d": days, "l": limit})


@app.get("/api/spikes")
def spikes(limit: int = Query(20, ge=1, le=100)):
    """Flagged spikes, plus the highest z-scores that were NOT flagged (so an empty list is explainable)."""
    flagged = query_dicts(
        """SELECT topic_group, event_hour, mentions, trailing_mean, trailing_stddev, z_score, is_spike
           FROM analytics.mart_spikes WHERE is_spike ORDER BY event_hour DESC LIMIT %s""", (limit,))
    highest = query_dicts(
        """SELECT topic_group, event_hour, mentions, trailing_mean, trailing_stddev, history_hours, z_score, is_spike
           FROM analytics.mart_spikes WHERE z_score IS NOT NULL AND history_hours >= 6
           ORDER BY z_score DESC LIMIT 5""")
    return {"spikes": flagged, "highest_z": highest}


class AskRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500)


@app.post("/api/ask")
def ask_endpoint(req: AskRequest):
    try:
        return ask(req.question.strip(), get_provider())
    except NotAnswerable as exc:
        return JSONResponse(status_code=422, content={"detail": {"error": str(exc), "attempts": []}})
    except AskFailed as exc:
        return JSONResponse(status_code=422, content={"detail": {"error": str(exc), "attempts": exc.attempts}})
    except LLMError as exc:
        log.error("llm error: %s", exc)
        return JSONResponse(status_code=502, content={"detail": {"error": f"The language model is unavailable: {exc}", "attempts": []}})
