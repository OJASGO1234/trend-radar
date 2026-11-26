"""Keyword filter: text in -> (topic_group, matched keywords) out, or None."""
from __future__ import annotations

import re
from pathlib import Path

import yaml


class KeywordFilter:
    def __init__(self, groups: dict[str, list[str]]):
        # One compiled regex per topic_group. Keywords match as whole words/phrases,
        # case-insensitive, so "rag" does not fire inside "storage" or "fragile".
        self._patterns: dict[str, re.Pattern[str]] = {}
        for group, words in groups.items():
            # longest first so "gta 6 trailer" wins over "gta 6" in the alternation
            ordered = sorted({w.lower() for w in words}, key=len, reverse=True)
            alts = [r"\s+".join(re.escape(part) for part in w.split()) for w in ordered]
            self._patterns[group] = re.compile(
                r"(?<!\w)(?:" + "|".join(alts) + r")(?!\w)", re.IGNORECASE
            )

    @classmethod
    def from_yaml(cls, path: Path) -> "KeywordFilter":
        with open(path, encoding="utf-8") as f:
            return cls(yaml.safe_load(f))

    def match(self, text: str | None) -> tuple[str, list[str]] | None:
        """Return (topic_group, sorted matched keywords) or None.

        If a text hits both groups, the group with more distinct keywords wins;
        a tie goes to the group listed first in keywords.yaml.
        """
        if not text:
            return None
        best: tuple[str, list[str]] | None = None
        for group, pattern in self._patterns.items():
            found = sorted({" ".join(m.lower().split()) for m in pattern.findall(text)})
            if found and (best is None or len(found) > len(best[1])):
                best = (group, found)
        return best
