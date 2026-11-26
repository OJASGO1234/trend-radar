"""Tiny JSON state files (cursor / last seen id) written atomically."""
from __future__ import annotations

import json
import os
from pathlib import Path


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}  # missing or half-written file = start fresh


def save_state(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    os.replace(tmp, path)  # atomic: a crash never leaves a half-written state file
