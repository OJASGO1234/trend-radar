"""Run:  python -m unittest tests.test_checkpoint -v

Proves Runtime.checkpoint_ok() refuses to move the cursor once a delivery has failed.
No Kafka needed: a stub stands in for the confluent producer.
"""
import argparse
import tempfile
import unittest
from pathlib import Path

from streaming.common.runtime import Runtime
from streaming.common.state import load_state, save_state


class StubProducer:
    """Mimics EventProducer.flush(): returns how many messages are still unsent."""
    def __init__(self, unsent: int = 0):
        self.unsent = unsent

    def flush(self, timeout: float = 15.0) -> int:
        return self.unsent


def make_runtime() -> Runtime:
    rt = Runtime("bluesky", argparse.Namespace(dry_run=True, limit=0, duration=0, no_state=False))
    rt._producer = StubProducer()  # dry_run left it None; inject the stub
    return rt


class CheckpointGuard(unittest.TestCase):
    def test_clean_run_moves_state(self):
        rt = make_runtime()
        rt.stats.add(kept=5, sent=5)
        self.assertTrue(rt.checkpoint_ok())

    def test_unsent_messages_block_state(self):
        rt = make_runtime()
        rt._producer.unsent = 3
        self.assertFalse(rt.checkpoint_ok())

    def test_failure_holds_state_back_and_stays_held(self):
        rt = make_runtime()
        rt.stats.add(kept=10, sent=10)
        self.assertTrue(rt.checkpoint_ok())      # checkpoint 1: all good
        rt.stats.add(kept=1, failed=1)           # one delivery times out
        self.assertFalse(rt.checkpoint_ok())     # checkpoint 2: flush() returned 0, but failed went up
        rt.stats.add(kept=50, sent=50)           # later events deliver fine ...
        self.assertFalse(rt.checkpoint_ok())     # ... still held: the cursor must not jump past the failed one

    def test_state_file_not_overwritten_on_failure(self):
        """Same pattern the producers use: save_state only `if rt.checkpoint_ok()`."""
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "cursor.json"
            save_state(f, {"time_us": 100})
            rt = make_runtime()
            rt.stats.add(failed=1)
            if rt.checkpoint_ok():
                save_state(f, {"time_us": 200})
            self.assertEqual(load_state(f), {"time_us": 100})


if __name__ == "__main__":
    unittest.main()
