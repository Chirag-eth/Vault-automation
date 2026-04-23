import tempfile
import unittest
from pathlib import Path

from pred_polymarket_sync.exporters import (
    export_polymarket_reference_data,
    search_league_rows,
    search_team_rows,
)
from pred_polymarket_sync.models import PolymarketLeague, PolymarketTeam


class ExporterTests(unittest.TestCase):
    def _sample_teams(self):
        return [
            PolymarketTeam(
                id="1",
                name="Arsenal",
                league_code="epl",
                alias="Gunners",
                record="",
                logo="https://example.com/arsenal.png",
                abbreviation="ars",
                provider_id="9001",
                color="#FF0000",
                created_at="2026-01-01T00:00:00Z",
                updated_at="2026-01-02T00:00:00Z",
            ),
            PolymarketTeam(
                id="2",
                name="Brentford",
                league_code="epl",
                alias="Bees",
                record="",
                logo="https://example.com/brentford.png",
                abbreviation="bre",
                provider_id="9002",
                color="#990000",
                created_at="2026-01-01T00:00:00Z",
                updated_at="2026-01-02T00:00:00Z",
            ),
            PolymarketTeam(
                id="3",
                name="Yankees",
                league_code="mlb",
                alias="",
                record="",
                logo="https://example.com/yankees.png",
                abbreviation="nyy",
                provider_id="9003",
                color="#0000FF",
                created_at="2026-01-01T00:00:00Z",
                updated_at="2026-01-02T00:00:00Z",
            ),
        ]

    def _sample_leagues(self):
        return [
            PolymarketLeague(
                id="2",
                league_code="epl",
                series_id="10188",
                tags="1,82",
                ordering="home",
                resolution="https://www.premierleague.com/",
                image="https://example.com/epl.png",
                created_at="2026-01-01T00:00:00Z",
            ),
            PolymarketLeague(
                id="8",
                league_code="mlb",
                series_id="3",
                tags="1,100639,100381",
                ordering="away",
                resolution="https://www.mlb.com/",
                image="https://example.com/mlb.png",
                created_at="2026-01-01T00:00:00Z",
            ),
        ]

    def test_exports_csv_and_sql_files(self):
        teams = [self._sample_teams()[0]]
        leagues = [self._sample_leagues()[0]]
        with tempfile.TemporaryDirectory() as temp_dir:
            result = export_polymarket_reference_data(
                teams=teams,
                leagues=leagues,
                output_dir=Path(temp_dir),
            )
            self.assertEqual(result["team_count"], 1)
            self.assertEqual(result["league_count"], 1)
            teams_csv = Path(result["teams_csv"]).read_text(encoding="utf-8")
            leagues_sql = Path(result["leagues_sql"]).read_text(encoding="utf-8")
            self.assertIn("Arsenal", teams_csv)
            self.assertIn("team_id,league_id,name,alternate_name,team_location,logo_url,theme_color", teams_csv)
            self.assertIn("polymarket_leagues_reference", leagues_sql)
            self.assertIn("'Premier League'", leagues_sql)

    def test_filters_football_only_exports(self):
        teams = self._sample_teams()
        leagues = self._sample_leagues()
        with tempfile.TemporaryDirectory() as temp_dir:
            result = export_polymarket_reference_data(
                teams=teams,
                leagues=leagues,
                output_dir=Path(temp_dir),
                base_name="polymarket_football",
                football_only=True,
            )
            teams_csv = Path(result["teams_csv"]).read_text(encoding="utf-8")
            leagues_csv = Path(result["leagues_csv"]).read_text(encoding="utf-8")
            self.assertIn("Arsenal", teams_csv)
            self.assertNotIn("Yankees", teams_csv)
            self.assertIn("Premier League", leagues_csv)
            self.assertNotIn("Major League Baseball", leagues_csv)

    def test_search_team_rows_returns_ranked_matches(self):
        results = search_team_rows(
            teams=self._sample_teams(),
            leagues=self._sample_leagues(),
            query="ars",
            football_only=True,
            limit=5,
        )
        self.assertEqual(results[0]["name"], "Arsenal")
        self.assertEqual(results[0]["league_id"], "2")
        self.assertTrue(all(row["name"] != "Yankees" for row in results))

    def test_search_team_rows_can_filter_by_league_id(self):
        results = search_team_rows(
            teams=self._sample_teams(),
            leagues=self._sample_leagues(),
            query="",
            league_id="8",
            football_only=False,
            limit=5,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "Yankees")

    def test_search_league_rows_returns_humanized_matches(self):
        results = search_league_rows(
            leagues=self._sample_leagues(),
            query="premier",
            football_only=False,
            limit=5,
        )
        self.assertEqual(results[0]["league_id"], "2")
        self.assertEqual(results[0]["name"], "Premier League")


if __name__ == "__main__":
    unittest.main()
