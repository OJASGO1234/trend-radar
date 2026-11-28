"""Decide how to show a result: line, bar or table. The LLM suggests; the actual data decides."""
from __future__ import annotations

import re
from typing import Any

_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _col(rows: list[list[Any]], i: int) -> list[Any]:
    return [r[i] for r in rows if r[i] is not None]


def pick_chart(hint: str | None, columns: list[str], rows: list[list[Any]]) -> str:
    if len(rows) < 2 or len(columns) < 2:
        return "table"
    numeric = [i for i in range(len(columns)) if (c := _col(rows, i)) and all(_is_num(v) for v in c)]
    first = _col(rows, 0)
    first_is_time = bool(first) and all(isinstance(v, str) and _TIME.match(v) for v in first)
    has_category = any(i not in numeric for i in range(len(columns)))

    can_line = first_is_time and 1 in range(len(columns)) and bool(numeric) and 0 not in numeric
    can_bar = bool(numeric) and has_category and len(rows) <= 30 and 0 not in numeric
    if hint == "line" and can_line:
        return "line"
    if hint == "bar" and can_bar:
        return "bar"
    if can_line:
        return "line"
    if can_bar:
        return "bar"
    return "table"
