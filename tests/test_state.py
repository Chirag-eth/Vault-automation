import json
import tempfile
import unittest
from pathlib import Path

from pred_polymarket_sync.state import StateStore


class StateStoreTests(unittest.TestCase):
    def test_load_latest_orderbooks_returns_latest_per_market(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_dir = Path(temp_dir)
            orderbooks_path = state_dir / "orderbooks.jsonl"
            orderbooks_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "polymarket_market_id": "market_1",
                                "best_bid_yes": "0.41",
                                "orderbook_snapshot": {
                                    "yes": {"timestamp": "2026-04-01T10:00:00+00:00"}
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "polymarket_market_id": "market_1",
                                "best_bid_yes": "0.44",
                                "orderbook_snapshot": {
                                    "yes": {"timestamp": "2026-04-01T10:02:00+00:00"}
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "polymarket_market_id": "market_2",
                                "best_bid_yes": "0.52",
                                "orderbook_snapshot": {
                                    "yes": {"timestamp": "2026-04-01T10:01:00+00:00"}
                                },
                            }
                        ),
                    ]
                ),
                encoding="utf-8",
            )
            store = StateStore(state_dir)
            latest = store.load_latest_orderbooks()
            self.assertEqual(len(latest), 2)
            self.assertEqual(latest[0]["polymarket_market_id"], "market_1")
            self.assertEqual(latest[0]["best_bid_yes"], "0.44")


if __name__ == "__main__":
    unittest.main()
