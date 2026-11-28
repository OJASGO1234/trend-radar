"""API settings from environment variables (.env is loaded if present)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    database_url_readonly: str
    llm_provider: str
    llm_model: str
    gemini_api_key: str
    marts_yaml: Path
    statement_timeout: str = "5s"


def load_settings() -> Settings:
    return Settings(
        # NOTE: the API never uses DATABASE_URL (the owner). Only the SELECT-only role.
        database_url_readonly=os.environ.get("DATABASE_URL_READONLY", ""),
        llm_provider=os.environ.get("LLM_PROVIDER", "gemini"),
        llm_model=os.environ.get("LLM_MODEL", "gemini-2.5-flash"),
        gemini_api_key=os.environ.get("GEMINI_API_KEY", ""),
        marts_yaml=REPO_ROOT / "dbt" / "models" / "marts" / "_marts.yml",
    )
