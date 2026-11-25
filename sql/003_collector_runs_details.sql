-- Per-run details: rows written per table, estimated quota units, search queries used.
ALTER TABLE raw.collector_runs ADD COLUMN IF NOT EXISTS details JSONB;
