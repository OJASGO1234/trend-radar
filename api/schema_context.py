"""Build the schema text that goes into the Text-to-SQL prompt.

Names and descriptions come from dbt's schema file (dbt/models/marts/_marts.yml), so the
documentation people read in dbt docs is exactly what the LLM reads. Column TYPES come from
the live database (information_schema), so they can never drift from reality.
"""
from __future__ import annotations

import logging
from functools import lru_cache

import yaml

from .config import load_settings
from .db import query_dicts

log = logging.getLogger(__name__)


def _load_models() -> list[dict]:
    with open(load_settings().marts_yaml, encoding="utf-8") as f:
        return yaml.safe_load(f)["models"]


def _live_types() -> dict[tuple[str, str], str]:
    rows = query_dicts(
        "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'analytics'"
    )
    return {(r["table_name"], r["column_name"]): r["data_type"] for r in rows}


@lru_cache(maxsize=1)
def get_context() -> tuple[str, frozenset[str]]:
    """Returns (schema text for the prompt, set of table names the model may use)."""
    models = _load_models()
    try:
        types = _live_types()
    except Exception as exc:  # DB hiccup must not take the prompt down; types are a nicety
        log.warning("could not read column types from the database: %r", exc)
        types = {}
    lines: list[str] = []
    for m in models:
        lines.append(f"TABLE analytics.{m['name']}")
        lines.append(f"  purpose: {' '.join(m.get('description', '').split())}")
        lines.append("  columns:")
        for c in m.get("columns", []):
            t = types.get((m["name"], c["name"]), "")
            desc = " ".join(c.get("description", "").split())
            lines.append(f"    - {c['name']} {t}: {desc}".rstrip())
        lines.append("")
    return "\n".join(lines), frozenset(m["name"] for m in models)
