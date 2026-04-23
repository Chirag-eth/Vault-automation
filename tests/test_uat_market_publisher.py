import unittest

from pred_polymarket_sync.uat_market_publisher import UatMarketPublisher


class StubHttpClient:
    def __init__(self, active_markets, vault_active_markets=None, internal_active_markets=None):
        self.active_markets = active_markets
        self.vault_active_markets = vault_active_markets if vault_active_markets is not None else {"data": active_markets}
        self.internal_active_markets = internal_active_markets if internal_active_markets is not None else {"data": active_markets}
        self.posts = []
        self.deletes = []

    def get_json(self, url, params=None, headers=None):
        if url.endswith("/api/v1/config/active-markets"):
            return self.vault_active_markets
        if "market-discovery/internal/active-markets" in url:
            return self.internal_active_markets
        return {"data": self.active_markets}

    def post_json(self, url, payload, headers=None):
        self.posts.append((url, payload, headers))
        return {"ok": True}

    def delete_json(self, url, headers=None):
        self.deletes.append((url, headers))
        return {"ok": True}


class StubPolymarketClient:
    def __init__(self, snapshots, event=None):
        self.snapshots = snapshots
        self.event = event or {
            "id": "event_1",
            "slug": "ars-bou",
            "title": "Arsenal FC vs. Bournemouth",
            "teams": [
                {"id": "1", "name": "Arsenal FC", "league": "epl", "abbreviation": "ars"},
                {"id": "2", "name": "Bournemouth", "league": "epl", "abbreviation": "bou"},
            ],
            "markets": [
                {
                    "id": "home_ml",
                    "question": "Will Arsenal FC win?",
                    "slug": "ars-bou-home",
                    "sportsMarketType": "moneyline",
                    "clobTokenIds": '["yes_shared","no_shared"]',
                    "active": True,
                    "closed": False,
                }
            ],
        }

    def get_order_books(self, token_ids):
        return [snapshot for snapshot in self.snapshots if snapshot.get("asset_id") in set(token_ids)]

    def get_event_by_slug(self, slug):
        return self.event


class UatMarketPublisherTests(unittest.TestCase):
    def test_rejects_non_uat_host(self):
        with self.assertRaises(RuntimeError):
            UatMarketPublisher(
                polymarket_client=StubPolymarketClient([]),
                base_url="https://prod.example",
            )

    def test_publish_to_active_markets_overrides_same_token_ids(self):
        active_markets = [
            {
                "status": "active",
                "market_name": "Market A",
                "market": {
                    "question": "Market A",
                    "status": "active",
                    "outcomes": {
                        "YES": {"token_id": "old_yes"},
                        "NO": {"token_id": "old_no"},
                    },
                    "pred_mapping": {"market_id": "pm_1"},
                },
            },
            {
                "status": "active",
                "market_name": "Market B",
                "market": {
                    "question": "Market B",
                    "status": "active",
                    "outcomes": {
                        "YES": {"token_id": "old_yes_2"},
                        "NO": {"token_id": "old_no_2"},
                    },
                    "pred_mapping": {"market_id": "pm_2"},
                },
            },
        ]
        publisher = UatMarketPublisher(
            polymarket_client=StubPolymarketClient([]),
            base_url="https://uat.example",
        )
        publisher._http = StubHttpClient(active_markets)

        result = publisher.publish_to_active_markets(
            yes_token_id="yes_shared",
            no_token_id="no_shared",
            dry_run=False,
            monitor_for_delete=False,
        )

        self.assertEqual(result["active_market_count"], 2)
        self.assertEqual(result["published_count"], 2)
        self.assertEqual(len(publisher._http.posts), 2)
        first_payload = publisher._http.posts[0][1]
        self.assertEqual(first_payload["market"]["outcomes"]["YES"]["token_id"], "yes_shared")
        self.assertEqual(first_payload["market"]["outcomes"]["NO"]["token_id"], "no_shared")
        self.assertEqual(first_payload["market"]["pred_mapping"]["market_id"], "pm_1")

    def test_auto_delete_condition_requires_both_books_to_be_001_000(self):
        publisher = UatMarketPublisher(
            polymarket_client=StubPolymarketClient([]),
            base_url="https://uat.example",
        )

        ready = publisher._auto_delete_condition(
            snapshots=[
                {
                    "asset_id": "yes_shared",
                    "bids": [{"price": "0.00"}],
                    "asks": [{"price": "0.01"}],
                },
                {
                    "asset_id": "no_shared",
                    "bids": [{"price": "0.00"}],
                    "asks": [{"price": "0.01"}],
                },
            ],
            yes_token_id="yes_shared",
            no_token_id="no_shared",
        )
        not_ready = publisher._auto_delete_condition(
            snapshots=[
                {
                    "asset_id": "yes_shared",
                    "bids": [{"price": "0.01"}],
                    "asks": [{"price": "0.02"}],
                },
                {
                    "asset_id": "no_shared",
                    "bids": [{"price": "0.00"}],
                    "asks": [{"price": "0.01"}],
                },
            ],
            yes_token_id="yes_shared",
            no_token_id="no_shared",
        )

        self.assertTrue(ready["ready"])
        self.assertFalse(not_ready["ready"])

    def test_delete_markets_for_job_uses_delete_template(self):
        publisher = UatMarketPublisher(
            polymarket_client=StubPolymarketClient([]),
            base_url="https://uat.example",
        )
        http = StubHttpClient([])
        publisher._http = http
        job = publisher._start_auto_delete_job(
            yes_token_id="yes_shared",
            no_token_id="no_shared",
            market_ids=["pm_1", "pm_2"],
            market_names=["Market A", "Market B"],
        )

        publisher._delete_markets_for_job(job.job_id)
        job_payload = publisher.get_job(job.job_id)

        self.assertEqual(len(http.deletes), 2)
        self.assertEqual(http.deletes[0][0], "https://uat.example/api/v1/config/markets/pm_1")
        self.assertEqual(job_payload["status"], "deleted")
        self.assertEqual(job_payload["deleted_count"], 2)

    def test_extract_market_items_supports_nested_data_markets_shape(self):
        publisher = UatMarketPublisher(
            polymarket_client=StubPolymarketClient([]),
            base_url="https://uat.example",
        )

        items = publisher._extract_market_items(
            {
                "data": {
                    "markets": [
                        {"market_id": "pm_1", "poly_token_id": "yes_1", "poly_no_token_id": "no_1"}
                    ]
                },
                "success": True,
            }
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["market_id"], "pm_1")

    def test_extract_market_items_supports_data_array_of_market_ids(self):
        publisher = UatMarketPublisher(
            polymarket_client=StubPolymarketClient([]),
            base_url="https://uat.example",
        )

        items = publisher._extract_market_items(
            {
                "data": [
                    "0x111",
                    "0x222",
                ]
            }
        )

        self.assertEqual(items, [{"market_id": "0x111"}, {"market_id": "0x222"}])

    def test_publish_remaining_active_markets_from_polymarket_url_attempts_all_ids(self):
        publisher = UatMarketPublisher(
            polymarket_client=StubPolymarketClient([]),
            base_url="https://uat.example",
            internal_base_url="https://api-internal.uat.example",
        )
        publisher._http = StubHttpClient(
            [],
            vault_active_markets={
                "data": {
                    "market_ids": ["pm_1"],
                    "markets": [{"MarketID": "pm_1"}],
                }
            },
            internal_active_markets={
                "data": {
                    "markets": [
                        {"market_id": "pm_1", "home_team": "West Ham", "away_team": "Man City"},
                        {"market_id": "pm_2", "home_team": "Arsenal", "away_team": "Bournemouth"},
                    ]
                }
            },
        )

        result = publisher.publish_remaining_active_markets_from_polymarket_url(
            polymarket_url="https://polymarket.com/event/ars-bou",
            dry_run=False,
            monitor_for_delete=False,
        )

        self.assertEqual(result["source_active_market_count"], 2)
        self.assertEqual(result["existing_vault_market_count"], 1)
        self.assertEqual(result["published_count"], 2)
        self.assertEqual(result["already_exists_count"], 0)
        self.assertEqual(len(publisher._http.posts), 2)
        posted_payload = publisher._http.posts[1][1]
        self.assertEqual(posted_payload["market"]["outcomes"]["YES"]["token_id"], "yes_shared")
        self.assertEqual(posted_payload["market"]["outcomes"]["NO"]["token_id"], "no_shared")
        self.assertEqual(posted_payload["market"]["pred_mapping"]["market_id"], "pm_2")


if __name__ == "__main__":
    unittest.main()
