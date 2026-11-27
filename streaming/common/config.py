"""Settings from environment variables (.env is loaded if present)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    hn_poll_seconds: int
    state_dir: Path
    wiki_user_agent: str
    keywords_path: Path
    metrics_addr: str
    metrics_ports: dict


def load_settings() -> Settings:
    state_dir = Path(os.getenv("STATE_DIR", ".state"))
    if not state_dir.is_absolute():
        state_dir = REPO_ROOT / state_dir
    return Settings(
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        hn_poll_seconds=int(os.getenv("HN_POLL_SECONDS", "60")),
        state_dir=state_dir,
        # Wikimedia asks every client to identify itself. Put a contact in .env.
        wiki_user_agent=os.getenv(
            "WIKI_USER_AGENT", "trend-radar/0.1 (resume project; contact not set)"
        ),
        keywords_path=REPO_ROOT / "config" / "keywords.yaml",
        # 0.0.0.0 so Prometheus inside Docker can reach us via host.docker.internal
        metrics_addr=os.getenv("METRICS_ADDR", "0.0.0.0"),
        metrics_ports={
            "bluesky": int(os.getenv("METRICS_PORT_BLUESKY", "8001")),
            "wikipedia": int(os.getenv("METRICS_PORT_WIKIPEDIA", "8002")),
            "hackernews": int(os.getenv("METRICS_PORT_HACKERNEWS", "8003")),
            "db_sink": int(os.getenv("METRICS_PORT_DB_SINK", "8004")),
        },
    )
