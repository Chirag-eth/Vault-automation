import argparse
import unittest

from pred_polymarket_sync.http_api import (
    _build_sport_rows,
    _has_csv_source_args,
    _has_database_source_args,
    _maybe_build_source_from_args,
    _search_exported_league_rows,
    _search_exported_team_rows,
)
from pred_polymarket_sync.models import PolymarketLeague


class HttpApiTests(unittest.TestCase):
    def test_reference_only_mode_without_source_args(self):
        args = argparse.Namespace(
            markets_csv="",
            parent_markets_csv="",
            fixtures_csv="",
            fixture_mappings_csv="",
            team_mappings_csv="",
            league_mappings_csv="",
            markets_table="",
            parent_markets_table="",
            fixtures_table="",
            fixture_mappings_table="",
            team_mappings_table="",
            league_mappings_table="",
        )
        self.assertFalse(_has_csv_source_args(args))
        self.assertFalse(_has_database_source_args(args))
        self.assertIsNone(_maybe_build_source_from_args(args, database_url=""))

    def test_search_exported_team_rows_filters_by_league_code(self):
        results = _search_exported_team_rows(
            team_rows=[
                {"team_id": "100005", "league_id": "2", "name": "Arsenal FC", "alternate_name": "Arsenal"},
                {"team_id": "200001", "league_id": "8", "name": "New York Yankees", "alternate_name": "Yankees"},
            ],
            league_rows=[
                {"league_id": "2", "name": "Premier League", "alternate_name": "epl", "sport": "soccer", "association": "premierleague.com"},
                {"league_id": "8", "name": "Major League Baseball", "alternate_name": "mlb", "sport": "baseball", "association": "mlb.com"},
            ],
            query="arsenal",
            league_id="",
            league_code="epl",
            limit=10,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["team_id"], "100005")

    def test_search_exported_league_rows_matches_name(self):
        results = _search_exported_league_rows(
            league_rows=[
                {"league_id": "2", "name": "Premier League", "alternate_name": "epl", "sport": "soccer", "association": "premierleague.com"},
                {"league_id": "8", "name": "Major League Baseball", "alternate_name": "mlb", "sport": "baseball", "association": "mlb.com"},
            ],
            query="premier",
            limit=10,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["league_id"], "2")

    def test_search_exported_league_rows_can_filter_by_sport(self):
        results = _search_exported_league_rows(
            league_rows=[
                {"league_id": "2", "name": "Premier League", "alternate_name": "epl", "sport": "soccer", "association": "premierleague.com"},
                {"league_id": "8", "name": "Major League Baseball", "alternate_name": "mlb", "sport": "baseball", "association": "mlb.com"},
            ],
            query="",
            sport="soccer",
            limit=10,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["league_id"], "2")

    def test_build_sport_rows_uses_shared_tag_when_available(self):
        rows = _build_sport_rows(
            [
                PolymarketLeague(
                    id="2",
                    league_code="epl",
                    series_id="10188",
                    tags="1,82,306,100639,100350",
                    ordering="home",
                    resolution="https://www.premierleague.com/",
                    image="",
                    created_at="",
                ),
                PolymarketLeague(
                    id="13",
                    league_code="ucl",
                    series_id="10204",
                    tags="1,100977,100639,1234,100350",
                    ordering="home",
                    resolution="https://www.uefa.com/uefachampionsleague/",
                    image="",
                    created_at="",
                ),
            ]
        )
        self.assertEqual(rows[0]["slug"], "soccer")
        self.assertEqual(rows[0]["polymarket_tag_id"], "100350")
        self.assertEqual(rows[0]["id_source"], "shared_tag")


if __name__ == "__main__":
    unittest.main()
