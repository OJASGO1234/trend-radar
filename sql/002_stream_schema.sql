-- Trend Radar: real-time events from Kafka (Bluesky, Wikipedia, Hacker News).

CREATE TABLE IF NOT EXISTS raw.stream_events (
  source           TEXT        NOT NULL CHECK (source IN ('bluesky', 'wikipedia', 'hackernews')),
  event_id         TEXT        NOT NULL,
  event_time       TIMESTAMPTZ NOT NULL,
  text             TEXT,
  url              TEXT,
  matched_keywords TEXT[]      NOT NULL DEFAULT '{}',
  topic_group      TEXT        NOT NULL CHECK (topic_group IN ('gta6', 'ai_tech')),
  payload          JSONB       NOT NULL,
  kafka_topic      TEXT        NOT NULL,
  kafka_partition  INTEGER     NOT NULL,
  kafka_offset     BIGINT      NOT NULL,
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source, event_id)
);

CREATE INDEX IF NOT EXISTS idx_stream_events_time
  ON raw.stream_events (topic_group, event_time);
