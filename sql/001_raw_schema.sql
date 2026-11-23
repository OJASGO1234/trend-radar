-- Trend Radar: raw layer. Collectors write here exactly what the APIs return.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.yt_videos (
  video_id      TEXT PRIMARY KEY,
  title         TEXT NOT NULL,
  channel_id    TEXT NOT NULL,
  channel_title TEXT,
  published_at  TIMESTAMPTZ NOT NULL,
  source_query  TEXT,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.yt_video_stats (
  video_id      TEXT   NOT NULL REFERENCES raw.yt_videos(video_id),
  snapshot_date DATE   NOT NULL,
  view_count    BIGINT,
  like_count    BIGINT,
  comment_count BIGINT,
  fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (video_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS raw.yt_comments (
  comment_id   TEXT PRIMARY KEY,
  video_id     TEXT NOT NULL REFERENCES raw.yt_videos(video_id),
  text         TEXT NOT NULL,
  like_count   INTEGER NOT NULL DEFAULT 0,
  published_at TIMESTAMPTZ NOT NULL,
  fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.collector_runs (
  run_id       BIGSERIAL PRIMARY KEY,
  source       TEXT NOT NULL,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at  TIMESTAMPTZ,
  rows_written INTEGER,
  status       TEXT NOT NULL DEFAULT 'running'
               CHECK (status IN ('running', 'success', 'failed')),
  error        TEXT
);
