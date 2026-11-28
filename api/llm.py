"""LLM provider behind a tiny interface, so Gemini can be swapped for another model by adding one class.

Only the question text and the table schema are ever sent to the provider. No database rows.
"""
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

import requests

from .config import load_settings

log = logging.getLogger(__name__)


class LLMError(RuntimeError):
    """The provider could not answer (network, quota, bad key...)."""


@dataclass
class Message:
    role: str   # "user" or "model"
    text: str


class LLMProvider(ABC):
    name: str

    @abstractmethod
    def generate(self, system: str, messages: list[Message]) -> str:
        """Return the model's text answer (we ask for JSON)."""


class GeminiProvider(LLMProvider):
    name = "gemini"
    _URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    def __init__(self, api_key: str, model: str):
        if not api_key:
            raise LLMError("GEMINI_API_KEY is not set in .env")
        self.api_key, self.model = api_key, model

    def generate(self, system: str, messages: list[Message]) -> str:
        body = {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": m.role, "parts": [{"text": m.text}]} for m in messages],
            "generationConfig": {"temperature": 0, "responseMimeType": "application/json", "maxOutputTokens": 4096},
        }
        last = ""
        for attempt in range(1, 5):
            try:
                # key goes in a header, never in the URL (URLs end up in logs)
                r = requests.post(self._URL.format(model=self.model), json=body,
                                  headers={"x-goog-api-key": self.api_key}, timeout=60)
            except requests.RequestException as exc:
                last = f"network error: {exc.__class__.__name__}"
            else:
                if r.status_code == 200:
                    try:
                        cand = r.json()["candidates"][0]
                        # thinking models may return several parts; keep the visible text, skip "thought" parts
                        parts = cand["content"]["parts"]
                        text = "".join(p.get("text", "") for p in parts if not p.get("thought"))
                    except (KeyError, IndexError, ValueError):
                        raise LLMError(f"empty Gemini response (finishReason={r.json()['candidates'][0].get('finishReason')})") from None
                    if not text.strip():
                        raise LLMError("empty Gemini response")
                    return text
                last = f"HTTP {r.status_code}: {r.text[:160]}"
                if r.status_code not in (429, 500, 502, 503, 504):
                    break  # bad key / bad request: retrying will not help
            wait = 2 ** attempt * (3 if "429" in last else 1)  # free tier rate limits need a longer pause
            log.warning("gemini attempt %d failed (%s); retrying in %ds", attempt, last, wait)
            time.sleep(wait)
        if last.startswith("HTTP 429"):
            raise LLMError("the Gemini free-tier quota is used up for now. Wait a minute and try again.")
        raise LLMError(f"Gemini request failed: {last}")


def get_provider() -> LLMProvider:
    cfg = load_settings()
    if cfg.llm_provider == "gemini":
        return GeminiProvider(cfg.gemini_api_key, cfg.llm_model)
    raise LLMError(f"unknown LLM_PROVIDER {cfg.llm_provider!r}")
