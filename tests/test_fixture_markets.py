import unittest

from pred_polymarket_sync.fixture_markets import (
    build_fixture_orderbooks_payload,
    fetch_league_fixture_orderbooks,
    list_league_fixture_events,
    resolve_related_fixture_events,
    resolve_fixture_event,
    select_fixture_markets,
)


class FixtureMarketTests(unittest.TestCase):
    def _sample_event(self):
        return {
            "id": "event_1",
            "slug": "ucl-psg1-liv1-2026-04-08",
            "title": "Paris Saint-Germain FC vs. Liverpool FC",
            "startTime": "2026-04-08T19:00:00Z",
            "teams": [
                {"id": "148391", "name": "Paris Saint-Germain FC", "league": "ucl", "abbreviation": "psg1"},
                {"id": "148236", "name": "Liverpool FC", "league": "ucl", "abbreviation": "liv1"},
            ],
            "markets": [
                {
                    "id": "home_ml",
                    "question": "Will Paris Saint-Germain FC win on 2026-04-08?",
                    "slug": "ucl-psg1-liv1-2026-04-08-psg1",
                    "sportsMarketType": "moneyline",
                    "clobTokenIds": '["yes_home","no_home"]',
                    "active": True,
                    "closed": False,
                },
                {
                    "id": "draw_ml",
                    "question": "Will Paris Saint-Germain FC vs. Liverpool FC end in a draw?",
                    "slug": "ucl-psg1-liv1-2026-04-08-draw",
                    "sportsMarketType": "moneyline",
                    "clobTokenIds": '["yes_draw","no_draw"]',
                    "active": True,
                    "closed": False,
                },
                {
                    "id": "away_ml",
                    "question": "Will Liverpool FC win on 2026-04-08?",
                    "slug": "ucl-psg1-liv1-2026-04-08-liv1",
                    "sportsMarketType": "moneyline",
                    "clobTokenIds": '["yes_away","no_away"]',
                    "active": True,
                    "closed": False,
                },
                {
                    "id": "total_25",
                    "question": "Will there be over 2.5 goals?",
                    "slug": "ucl-psg1-liv1-2026-04-08-over-2-5",
                    "sportsMarketType": "total",
                    "line": "2.5",
                    "clobTokenIds": '["yes_total","no_total"]',
                    "active": True,
                    "closed": False,
                },
                {
                    "id": "total_55",
                    "question": "Will there be over 5.5 goals?",
                    "slug": "ucl-psg1-liv1-2026-04-08-over-5-5",
                    "sportsMarketType": "total",
                    "line": "5.5",
                    "clobTokenIds": '["yes_total_55","no_total_55"]',
                    "active": True,
                    "closed": False,
                },
                {
                    "id": "spread_home",
                    "question": "Will Paris Saint-Germain FC cover -0.5?",
                    "slug": "ucl-psg1-liv1-2026-04-08-psg1--0-5",
                    "sportsMarketType": "spread",
                    "line": "-0.5",
                    "groupItemTitle": "Paris Saint-Germain FC",
                    "clobTokenIds": '["yes_spread","no_spread"]',
                    "active": False,
                    "closed": True,
                },
                {
                    "id": "btts",
                    "question": "Will both teams to score on 2026-04-08?",
                    "slug": "ucl-psg1-liv1-2026-04-08-btts",
                    "sportsMarketType": "both teams to score",
                    "clobTokenIds": '["yes_btts","no_btts"]',
                    "active": True,
                    "closed": False,
                },
            ],
        }

    def _sample_snapshots(self):
        return [
            {
                "asset_id": "yes_home",
                "market": "home_ml",
                "bids": [{"price": "0.77", "size": "100"}],
                "asks": [{"price": "0.78", "size": "80"}],
            },
            {
                "asset_id": "no_home",
                "market": "home_ml",
                "bids": [{"price": "0.22", "size": "50"}],
                "asks": [{"price": "0.23", "size": "60"}],
            },
            {
                "asset_id": "yes_away",
                "market": "away_ml",
                "bids": [{"price": "0.05", "size": "25"}],
                "asks": [{"price": "0.06", "size": "30"}],
            },
            {
                "asset_id": "no_away",
                "market": "away_ml",
                "bids": [{"price": "0.94", "size": "25"}],
                "asks": [{"price": "0.95", "size": "30"}],
            },
            {
                "asset_id": "yes_total",
                "market": "total_25",
                "bids": [{"price": "0.55", "size": "22"}],
                "asks": [{"price": "0.56", "size": "25"}],
            },
            {
                "asset_id": "no_total",
                "market": "total_25",
                "bids": [{"price": "0.44", "size": "18"}],
                "asks": [{"price": "0.45", "size": "20"}],
            },
            {
                "asset_id": "yes_spread",
                "market": "spread_home",
                "bids": [{"price": "0.61", "size": "14"}],
                "asks": [{"price": "0.62", "size": "15"}],
            },
            {
                "asset_id": "no_spread",
                "market": "spread_home",
                "bids": [{"price": "0.38", "size": "13"}],
                "asks": [{"price": "0.39", "size": "16"}],
            },
            {
                "asset_id": "yes_btts",
                "market": "btts",
                "bids": [{"price": "0.71", "size": "10"}],
                "asks": [{"price": "0.72", "size": "12"}],
            },
            {
                "asset_id": "no_btts",
                "market": "btts",
                "bids": [{"price": "0.28", "size": "10"}],
                "asks": [{"price": "0.29", "size": "12"}],
            },
        ]

    def _sample_related_events(self):
        base = {
            "id": "event_1",
            "slug": "epl-ars-bou-2026-04-11",
            "title": "Arsenal FC vs. AFC Bournemouth",
            "startTime": "2026-04-11T11:30:00Z",
            "teams": [
                {"id": "100005", "name": "Arsenal FC", "league": "epl", "abbreviation": "ars"},
                {"id": "100010", "name": "AFC Bournemouth", "league": "epl", "abbreviation": "bou"},
            ],
            "markets": [
                {
                    "id": "ars_ml",
                    "question": "Will Arsenal FC win on 2026-04-11?",
                    "slug": "epl-ars-bou-2026-04-11-ars",
                    "sportsMarketType": "moneyline",
                    "clobTokenIds": '["yes_ars","no_ars"]',
                    "active": True,
                    "closed": False,
                },
                {
                    "id": "bou_ml",
                    "question": "Will AFC Bournemouth win on 2026-04-11?",
                    "slug": "epl-ars-bou-2026-04-11-bou",
                    "sportsMarketType": "moneyline",
                    "clobTokenIds": '["yes_bou","no_bou"]',
                    "active": True,
                    "closed": False,
                },
            ],
        }
        more = {
            "id": "event_1_more",
            "slug": "epl-ars-bou-2026-04-11-more-markets",
            "title": "Arsenal FC vs. AFC Bournemouth - More Markets",
            "startTime": "2026-04-11T11:30:00Z",
            "teams": base["teams"],
            "markets": [
                {
                    "id": "spread_ars_15",
                    "question": "Spread: Arsenal FC (-1.5)",
                    "slug": "epl-ars-bou-2026-04-11-spread-home-1pt5",
                    "sportsMarketType": "spreads",
                    "line": -1.5,
                    "clobTokenIds": '["yes_spread_ars","no_spread_ars"]',
                    "active": True,
                    "closed": False,
                },
                {
                    "id": "total_15",
                    "question": "Arsenal FC vs. AFC Bournemouth: O/U 1.5",
                    "slug": "epl-ars-bou-2026-04-11-total-1pt5",
                    "sportsMarketType": "totals",
                    "line": 1.5,
                    "clobTokenIds": '["yes_total_15","no_total_15"]',
                    "active": True,
                    "closed": False,
                },
                {
                    "id": "btts",
                    "question": "Arsenal FC vs. AFC Bournemouth: Both Teams to Score",
                    "slug": "epl-ars-bou-2026-04-11-btts",
                    "sportsMarketType": "both_teams_to_score",
                    "clobTokenIds": '["yes_btts","no_btts"]',
                    "active": True,
                    "closed": False,
                },
            ],
        }
        return base, more

    def test_select_fixture_markets_groups_expected_families(self):
        selected = select_fixture_markets(self._sample_event())
        self.assertEqual([item["market_id"] for item in selected["moneyline"]], ["home_ml", "away_ml"])
        self.assertEqual(selected["totals"][0]["line"], "over 2.5")
        self.assertEqual(selected["spreads"][0]["line"], "Paris Saint-Germain FC -0.5")
        self.assertEqual(selected["both_teams_to_score"][0]["market_id"], "btts")

    def test_build_fixture_orderbooks_payload_includes_empty_arrays_and_books(self):
        selected = select_fixture_markets(self._sample_event())
        payload = build_fixture_orderbooks_payload(
            event=self._sample_event(),
            selected_markets=selected,
            snapshots=self._sample_snapshots(),
        )
        self.assertEqual(payload["fixture_slug"], "ucl-psg1-liv1-2026-04-08")
        self.assertEqual(payload["league"], "ucl")
        self.assertEqual(payload["home_team_id"], "148391")
        self.assertEqual(payload["away_team_id"], "148236")
        self.assertEqual(payload["markets"]["moneyline"][0]["best_bid_yes"], "0.77")
        self.assertEqual(payload["markets"]["moneyline"][0]["bids"]["yes"][0]["price"], "0.77")
        self.assertEqual(payload["markets"]["spreads"][0]["closed"], True)

    def test_select_fixture_markets_merges_related_event_groups(self):
        base, more = self._sample_related_events()
        selected = select_fixture_markets([base, more])
        self.assertEqual([item["market_id"] for item in selected["moneyline"]], ["ars_ml", "bou_ml"])
        self.assertEqual(selected["totals"][0]["line"], "over 1.5")
        self.assertEqual(selected["spreads"][0]["line"], "Arsenal FC -1.5")
        self.assertEqual(selected["both_teams_to_score"][0]["market_id"], "btts")

    def test_resolve_related_fixture_events_collects_slug_variants(self):
        base, more = self._sample_related_events()

        class StubClient:
            def list_events(self, tag_slug="", limit=500, offset=0, closed=None):
                self.last_call = {"tag_slug": tag_slug, "closed": closed}
                return [base, more] if offset == 0 else []

        related = resolve_related_fixture_events(StubClient(), base, include_closed=False)
        self.assertEqual([event["slug"] for event in related], [
            "epl-ars-bou-2026-04-11",
            "epl-ars-bou-2026-04-11-more-markets",
        ])

    def test_list_league_fixture_events_filters_to_base_fixture_events(self):
        base, more = self._sample_related_events()
        season = {
            "id": "season_market",
            "slug": "english-premier-league-winner",
            "title": "English Premier League Winner",
            "markets": [],
        }

        class StubClient:
            def list_events(self, tag_slug="", limit=500, offset=0, closed=None):
                return [season, base, more] if offset == 0 else []

        items = list_league_fixture_events(
            polymarket_client=StubClient(),
            league_code="epl",
            include_closed=False,
            limit=25,
        )
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["base_slug"], "epl-ars-bou-2026-04-11")
        self.assertEqual(len(items[0]["events"]), 2)

    def test_fetch_league_fixture_orderbooks_returns_fixture_payloads(self):
        base, more = self._sample_related_events()
        snapshots = [
            {"asset_id": "yes_ars", "market": "ars_ml", "bids": [{"price": "0.68", "size": "10"}], "asks": [{"price": "0.69", "size": "10"}]},
            {"asset_id": "no_ars", "market": "ars_ml", "bids": [{"price": "0.31", "size": "10"}], "asks": [{"price": "0.32", "size": "10"}]},
            {"asset_id": "yes_bou", "market": "bou_ml", "bids": [{"price": "0.12", "size": "10"}], "asks": [{"price": "0.13", "size": "10"}]},
            {"asset_id": "no_bou", "market": "bou_ml", "bids": [{"price": "0.87", "size": "10"}], "asks": [{"price": "0.88", "size": "10"}]},
            {"asset_id": "yes_total_15", "market": "total_15", "bids": [{"price": "0.8", "size": "10"}], "asks": [{"price": "0.82", "size": "10"}]},
            {"asset_id": "no_total_15", "market": "total_15", "bids": [{"price": "0.18", "size": "10"}], "asks": [{"price": "0.2", "size": "10"}]},
            {"asset_id": "yes_spread_ars", "market": "spread_ars_15", "bids": [{"price": "0.95", "size": "10"}], "asks": [{"price": "0.97", "size": "10"}]},
            {"asset_id": "no_spread_ars", "market": "spread_ars_15", "bids": [{"price": "0.03", "size": "10"}], "asks": [{"price": "0.05", "size": "10"}]},
            {"asset_id": "yes_btts", "market": "btts", "bids": [{"price": "0.51", "size": "10"}], "asks": [{"price": "0.54", "size": "10"}]},
            {"asset_id": "no_btts", "market": "btts", "bids": [{"price": "0.46", "size": "10"}], "asks": [{"price": "0.49", "size": "10"}]},
        ]

        class StubClient:
            def list_events(self, tag_slug="", limit=500, offset=0, closed=None):
                return [base, more] if offset == 0 else []

            def get_order_books(self, token_ids):
                requested = set(token_ids)
                return [snapshot for snapshot in snapshots if snapshot["asset_id"] in requested]

        payload = fetch_league_fixture_orderbooks(
            polymarket_client=StubClient(),
            league_code="epl",
            include_closed=False,
            limit=10,
        )
        self.assertEqual(payload["league"], "epl")
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["markets"]["totals"][0]["line"], "over 1.5")
        self.assertEqual(payload["items"][0]["markets"]["spreads"][0]["line"], "Arsenal FC -1.5")
        self.assertEqual(payload["items"][0]["markets"]["both_teams_to_score"][0]["market_id"], "btts")

    def test_resolve_fixture_event_by_details_matches_abbreviation_and_kickoff(self):
        event = self._sample_event()

        class StubClient:
            def list_events(self, tag_slug="", limit=500, offset=0, closed=None):
                self.last_call = {"tag_slug": tag_slug, "closed": closed}
                return [event] if offset == 0 else []

        client = StubClient()
        resolved = resolve_fixture_event(
            polymarket_client=client,
            home_team="psg",
            away_team="liverpool",
            league_code="ucl",
            kickoff="2026-04-08T19:00:00Z",
            include_closed=True,
            start_time_tolerance_minutes=30,
        )
        self.assertEqual(resolved["slug"], "ucl-psg1-liv1-2026-04-08")


if __name__ == "__main__":
    unittest.main()
