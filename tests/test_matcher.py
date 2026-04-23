from datetime import datetime, timezone
import unittest

from pred_polymarket_sync.matcher import MarketMatcher, build_pred_market_bundles
from pred_polymarket_sync.models import (
    PolymarketMarket,
    PredFixture,
    PredMarket,
    PredMarketBundle,
    PredParentMarket,
    SportsDataFixtureMapping,
)
from pred_polymarket_sync.sources import DataSnapshot


class MarketMatcherTests(unittest.TestCase):
    def setUp(self):
        self.bundle = PredMarketBundle(
            market=PredMarket(
                market_id="pred_market_1",
                parent_market_id="parent_1",
                team_id="team_away",
                name="Liverpool",
                market_canonical_name="liv-psg-win-ucl-2026-04-08",
                market_code="LIV",
                rules="Liverpool to win",
                status="active",
                yes_position_id="yes",
                no_position_id="no",
            ),
            parent_market=PredParentMarket(
                parent_market_id="parent_1",
                league_id="league_1",
                type_reference_id="type_1",
                title="PSG vs Liverpool",
                description="PSG vs Liverpool",
                status="active",
                markets_open_time=None,
                markets_close_time=None,
                payout_time=None,
            ),
            fixture=PredFixture(
                fixture_id="fixture_1",
                name="PSG vs Liverpool",
                league_id="league_1",
                home_team_id="team_home",
                away_team_id="team_away",
            ),
            sportsdata_fixture=SportsDataFixtureMapping(
                cms_fixture_id="fixture_1",
                cms_league_id="league_1",
                sportsdata_game_id="109256",
                match_date=datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc),
                status="ACTIVE",
            ),
            sportsdata_home_team_id="514",
            sportsdata_away_team_id="513",
            sportsdata_competition_id="1",
            outcome_key="away",
            outcome_label="Liverpool",
            tracking_status="upcoming",
        )
        self.matcher = MarketMatcher(start_time_tolerance_minutes=30)

    def test_matches_clear_home_candidate(self):
        winner = PolymarketMarket(
            market_id="poly_1",
            question="Will Liverpool win against PSG?",
            slug="liverpool-win-vs-psg",
            game_id="109256",
            team_a_id="514",
            team_b_id="513",
            game_start_time=datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc),
            outcomes=["Yes", "No"],
            short_outcomes=["Yes", "No"],
            clob_token_ids=["yes_token", "no_token"],
            sports_market_type="moneyline",
            active=True,
            closed=False,
            raw={},
        )
        loser = PolymarketMarket(
            market_id="poly_2",
            question="Will PSG win against Liverpool?",
            slug="psg-win-vs-liverpool",
            game_id="109256",
            team_a_id="514",
            team_b_id="513",
            game_start_time=datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc),
            outcomes=["Yes", "No"],
            short_outcomes=["Yes", "No"],
            clob_token_ids=["yes_token_2", "no_token_2"],
            sports_market_type="moneyline",
            active=True,
            closed=False,
            raw={},
        )
        result = self.matcher.match(self.bundle, [winner, loser])
        self.assertEqual(result.status, "matched")
        self.assertEqual(result.mapping.polymarket_market_id, "poly_1")
        self.assertEqual(result.mapping.yes_token_id, "yes_token")

    def test_flags_ambiguous_when_candidates_too_close(self):
        candidate_a = PolymarketMarket(
            market_id="poly_a",
            question="Will Liverpool win?",
            slug="liverpool-win",
            game_id="109256",
            team_a_id="514",
            team_b_id="513",
            game_start_time=datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc),
            outcomes=["Yes", "No"],
            short_outcomes=["Yes", "No"],
            clob_token_ids=["a_yes", "a_no"],
            sports_market_type="moneyline",
            active=True,
            closed=False,
            raw={},
        )
        candidate_b = PolymarketMarket(
            market_id="poly_b",
            question="Will Liverpool beat PSG?",
            slug="liverpool-beat-psg",
            game_id="109256",
            team_a_id="514",
            team_b_id="513",
            game_start_time=datetime(2026, 4, 8, 19, 5, tzinfo=timezone.utc),
            outcomes=["Yes", "No"],
            short_outcomes=["Yes", "No"],
            clob_token_ids=["b_yes", "b_no"],
            sports_market_type="moneyline",
            active=True,
            closed=False,
            raw={},
        )
        matcher = MarketMatcher(
            start_time_tolerance_minutes=30,
            score_threshold=10,
            score_gap_threshold=15,
        )
        result = matcher.match(self.bundle, [candidate_a, candidate_b])
        self.assertEqual(result.status, "ambiguous")
        self.assertIsNotNone(result.review)

    def test_filters_out_completed_bundles(self):
        snapshot = DataSnapshot(
            markets=[
                PredMarket(
                    market_id="pred_market_2",
                    parent_market_id="parent_2",
                    team_id="team_away",
                    name="Liverpool",
                    market_canonical_name="liv-psg-win-ucl-2026-04-08",
                    market_code="LIV",
                    rules="Liverpool to win",
                    status="completed",
                    yes_position_id="yes",
                    no_position_id="no",
                )
            ],
            parent_markets=[
                PredParentMarket(
                    parent_market_id="parent_2",
                    league_id="league_1",
                    type_reference_id="type_1",
                    title="PSG vs Liverpool",
                    description="PSG vs Liverpool",
                    status="completed",
                    markets_open_time=None,
                    markets_close_time=None,
                    payout_time=None,
                )
            ],
            fixtures=[
                PredFixture(
                    fixture_id="fixture_1",
                    name="PSG vs Liverpool",
                    league_id="league_1",
                    home_team_id="team_home",
                    away_team_id="team_away",
                )
            ],
            fixture_mappings=[
                SportsDataFixtureMapping(
                    cms_fixture_id="fixture_1",
                    cms_league_id="league_1",
                    sportsdata_game_id="109256",
                    match_date=datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc),
                    status="COMPLETED",
                )
            ],
            team_mappings=[],
            league_mappings=[],
        )
        bundles = build_pred_market_bundles(snapshot)
        self.assertEqual(bundles, [])


if __name__ == "__main__":
    unittest.main()
