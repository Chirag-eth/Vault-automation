import unittest

from pred_polymarket_sync.cli import _format_pred_payload
from pred_polymarket_sync.models import MappingRecord


class CliFormattingTests(unittest.TestCase):
    def test_formats_match_response_in_requested_shape(self):
        payload = {
            "market_name": "BRE_BRE_vs_ARS_EPL_2026",
            "market": {
                "question": "BRE_BRE_vs_ARS_EPL_2026",
                "status": "active",
                "outcomes": {
                    "YES": {"token_id": ""},
                    "NO": {"token_id": ""},
                },
                "pred_mapping": {
                    "market_id": "pred_market_123",
                },
            },
        }
        mapping = MappingRecord(
            pred_market_id="pred_market_123",
            pred_parent_market_id="parent_123",
            pred_fixture_id="fixture_123",
            pred_league_id="league_123",
            pred_home_team_id="home_123",
            pred_away_team_id="away_123",
            polymarket_market_id="poly_market_123",
            yes_token_id="yes_poly_token",
            no_token_id="no_poly_token",
            home_team_id="home_sd",
            home_team_name="Brentford",
            away_team_id="away_sd",
            away_team_name="Arsenal",
            league_id="epl",
            league_name="EPL",
            game_id="game_123",
            outcome_label="Brentford",
            match_score=100,
            match_reason="Exact match",
            tracking_status="upcoming",
        )
        formatted = _format_pred_payload(payload, mapping)
        self.assertEqual(
            formatted,
            {
                "market_name": "BRE_BRE_vs_ARS_EPL_2026",
                "market": {
                    "question": "BRE_BRE_vs_ARS_EPL_2026",
                    "status": "active",
                    "outcomes": {
                        "YES": {"token_id": "yes_poly_token"},
                        "NO": {"token_id": "no_poly_token"},
                    },
                    "pred_mapping": {
                        "market_id": "pred_market_123",
                    },
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
