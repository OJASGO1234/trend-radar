# Trend Radar — project context for Claude Code

## What this is
A resume project by Ojas Gore. One system, two themes:
- **GTA 6 Launch Pulse** — how the internet reacts to GTA 6 (YouTube, Bluesky, Wikipedia, HN)
- **AI Pulse** — what is trending in AI (Bluesky, HN, GitHub, arXiv)

Deadline: 5 days. Ojas must understand every line, so explanations matter as much as code.

## Hard constraints
- Laptop: Windows, 8 GB RAM. Docker gets 3 GB via `.wslconfig`. Keep containers lean.
- Kafka: single broker, KRaft mode (no ZooKeeper), `KAFKA_HEAP_OPTS=-Xmx512m -Xms512m`.
- Database: **PostgreSQL on Neon only.** No local Postgres container. Connection string comes from `DATABASE_URL` (with `sslmode=require`).
- Neon free tier has little storage and limited compute hours: filter streams by keywords before writing, batch the inserts, and don't keep connections open when idle.
- Secrets live only in `.env` (gitignored) and GitHub Actions secrets. Keep `.env.example` up to date. Never commit keys.
- Python 3.12, venv at `.venv`. Commands must work in PowerShell.

## Sources
- Real-time: Bluesky Jetstream (WebSocket), Wikipedia EventStreams `recentchange` (SSE, needs a User-Agent header), Hacker News (Firebase API, polled)
- Batch: YouTube Data API v3 (GTA 6), GitHub, arXiv

## Stack, and why each tool is here
| Tool | Why it's here |
|---|---|
| Kafka (KRaft) | Decouples 3 bursty producers from the DB writer; can replay events; lets new consumers (spike detector, LLM enrich) attach without touching the producers |
| PostgreSQL (Neon) | One SQL store for raw + modeled data; serverless, so no RAM on the laptop |
| GitHub Actions | Free scheduler for the daily batch collector, and CI/CD later |
| Airflow (Day 3) | Orchestrates the batch jobs and dbt with dependencies and retries |
| dbt (Day 3) | Tested, versioned SQL models from raw → staging → marts |
| Prometheus + Grafana (Day 2) | Pipeline health: events/sec, consumer lag, error counts |
| FastAPI + Text-to-SQL + React (Day 4) | Ask questions in plain English over the marts |
| Docker, k3d, Helm (Day 5) | Reproducible deploy; k8s practice on a laptop |

## Repo layout
```
sql/                      001_raw_schema.sql, 002_stream_schema.sql (already exist; don't edit, add 00N_*.sql migrations)
config/keywords.yaml      keyword lists per topic (gta6, ai)
collectors/youtube/       daily GTA 6 collector
streaming/common/         config, kafka helpers, keyword filter, event envelope
streaming/producers/      bluesky.py, wikipedia.py, hackernews.py
streaming/consumers/      db_sink.py
infra/                    docker-compose.kafka.yml, docker-compose.monitoring.yml, prometheus/, grafana/
dbt/                      dbt project (staging views + marts tables in schema `analytics`); run via `python -m scripts.dbt_cli <args>`
airflow/dags/             trend_radar_daily.py (sensor -> dbt freshness -> dbt build -> row counts); infra/docker-compose.airflow.yml
scripts/                  run_stream.ps1 (start/stop all), verify_sink.py, counts.py
.github/workflows/        yt_daily.yml
docs/learning/            dayN.md, the Hinglish explanations for Ojas
```

## Conventions
- Kafka topics: `raw.bluesky`, `raw.wikipedia`, `raw.hackernews`. Common event envelope: `event_id, source, topic_group, event_ts, ingested_ts, payload (json)`. `topic_group` values are `gta6` and `ai_tech` (the CHECK constraint in `sql/002_stream_schema.sql` is the source of truth).
- Delivery: at-least-once. The consumer commits offsets only after the DB insert succeeds; the DB stays idempotent through the primary key `(source, event_id)` + `ON CONFLICT (source, event_id) DO NOTHING`.
- Use structured logging (no prints). Every script gets `--dry-run` and `--limit` flags for testing.
- Small commits with clear messages.

## How to work with Ojas
- After each task, write `docs/learning/dayN.md` in **Hinglish**:
  1. what was built and why (one para)
  2. file-by-file walkthrough of the important lines
  3. a **dry run**: follow one real example record from source → Kafka → DB, with actual values
  4. 5 likely **interview questions** with crisp answers
- Ask before adding a new tool or dependency that isn't in this file.
