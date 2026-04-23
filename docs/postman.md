# Postman Flow

This repo includes a local bridge that now also serves a dashboard, so you can use either a visual UI or Postman against the same local server.

## Start the bridge

```bash
pred-polymarket-http \
  --markets-csv /Users/chirag/Downloads/markets.csv \
  --parent-markets-csv /Users/chirag/Downloads/parent_markets.csv \
  --fixtures-csv /Users/chirag/Downloads/fixtures.csv \
  --fixture-mappings-csv /Users/chirag/Downloads/sports_data_fixture_mappings.csv \
  --team-mappings-csv /Users/chirag/Downloads/sports_data_team_mappings.csv \
  --league-mappings-csv /Users/chirag/Downloads/sports_data_league_mappings.csv
```

## Import the collection

Import [examples/postman_collection.json](/Users/chirag/Desktop/Vault-Automation /examples/postman_collection.json) into Postman.

## Open the dashboard

Open `http://127.0.0.1:8080/` for the visual workflow.

## Call the matcher

POST `http://127.0.0.1:8080/api/match` with the same payload shape you currently build by hand.

The response is returned in your compact storage format, with Polymarket token IDs written directly into `market.outcomes.YES/NO.token_id` and your Pred market ID preserved under `market.pred_mapping.market_id`.

## Alternatives to Postman

- terminal CLI for one-off matching
- CSV backfill jobs for existing markets
- direct DB polling for newly created markets
- HTTP sink into your internal service once that endpoint is ready
