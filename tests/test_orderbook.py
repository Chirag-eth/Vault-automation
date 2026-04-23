import unittest

from pred_polymarket_sync.models import MappingRecord, OrderBookState
from pred_polymarket_sync.orderbook import apply_price_change, build_envelope


class OrderBookTests(unittest.TestCase):
    def test_applies_price_change_and_removal(self):
        book = OrderBookState(
            asset_id="yes",
            market="poly",
            bids={"0.45": "10"},
            asks={"0.55": "12"},
        )
        apply_price_change(
            book,
            {"asset_id": "yes", "price": "0.46", "size": "5", "side": "BUY"},
        )
        self.assertEqual(book.bids["0.46"], "5")
        apply_price_change(
            book,
            {"asset_id": "yes", "price": "0.45", "size": "0", "side": "BUY"},
        )
        self.assertNotIn("0.45", book.bids)

    def test_builds_orderbook_envelope(self):
        mapping = MappingRecord(
            pred_market_id="pred",
            pred_parent_market_id="parent",
            pred_fixture_id="fixture",
            pred_league_id="league",
            pred_home_team_id="home",
            pred_away_team_id="away",
            polymarket_market_id="poly",
            yes_token_id="yes",
            no_token_id="no",
            home_team_id="514",
            home_team_name="Liverpool",
            away_team_id="513",
            away_team_name="PSG",
            league_id="1",
            league_name="UCL",
            game_id="109256",
            outcome_label="Liverpool",
            match_score=100,
            match_reason="Exact match",
        )
        envelope = build_envelope(
            mapping,
            {
                "yes": OrderBookState(
                    asset_id="yes",
                    market="poly",
                    bids={"0.48": "10"},
                    asks={"0.52": "9"},
                ),
                "no": OrderBookState(
                    asset_id="no",
                    market="poly",
                    bids={"0.47": "11"},
                    asks={"0.53": "8"},
                ),
            },
        )
        self.assertEqual(envelope.best_bid_yes, "0.48")
        self.assertEqual(envelope.best_ask_no, "0.53")


if __name__ == "__main__":
    unittest.main()
