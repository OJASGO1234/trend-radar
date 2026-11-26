"""Exponential backoff with full jitter, for reconnect loops."""
import random


class Backoff:
    def __init__(self, base: float = 1.0, cap: float = 60.0):
        self.base, self.cap, self.attempt = base, cap, 0

    def next_delay(self) -> float:
        self.attempt += 1
        return random.uniform(0, min(self.cap, self.base * 2 ** (self.attempt - 1)))

    def reset(self) -> None:
        self.attempt = 0
