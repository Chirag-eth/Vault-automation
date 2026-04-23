# Pred x Polymarket Sync

Automation scripts for:

- backfilling Polymarket mappings for existing Pred sports markets
- matching newly created Pred markets in near real time
- maintaining full-depth Yes/No orderbooks with an initial REST snapshot and live WebSocket updates
- flagging ambiguous or missing matches for manual review instead of guessing

## What This Repo Covers

The repo uses your existing Pred-side sports data as the source of truth:

- `markets`
- `parent_markets`
- `fixtures`
- `sports_data_fixture_mappings`
- `sports_data_team_mappings`
- `sports_data_league_mappings`

It then matches each Pred child market to exactly one Polymarket market using:

- exact `sportsdata_game_id` when available
- exact home/away sports data team IDs
- kickoff time proximity
- strict text matching against the Pred outcome name and Polymarket question / slug

If the best candidate is weak or tied, the market is flagged for manual review.
The automation only tracks fixtures that are still `upcoming` or `postponed`; ended / completed fixtures are skipped automatically.

## Repo Layout

- `src/pred_polymarket_sync/cli.py`: CLI entrypoint
- `src/pred_polymarket_sync/http_api.py`: local HTTP bridge and dashboard server
- `src/pred_polymarket_sync/matcher.py`: strict market matching rules
- `src/pred_polymarket_sync/orderbook.py`: snapshot + WebSocket full-depth book maintenance
- `src/pred_polymarket_sync/sources.py`: CSV and Postgres/MySQL readers
- `src/pred_polymarket_sync/sinks.py`: JSONL and HTTP sinks
- `src/pred_polymarket_sync/static/`: minimalist dashboard UI
- `sql/`: starter DDL for the new mapping and review tables
- `examples/`: env file, sample Postman payload, and Postman collection

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## Quickstart

### 1. Backfill existing markets from CSV exports

```bash
pred-polymarket-sync backfill \
  --markets-csv /Users/chirag/Downloads/markets.csv \
  --parent-markets-csv /Users/chirag/Downloads/parent_markets.csv \
  --fixtures-csv /Users/chirag/Downloads/fixtures.csv \
  --fixture-mappings-csv /Users/chirag/Downloads/sports_data_fixture_mappings.csv \
  --team-mappings-csv /Users/chirag/Downloads/sports_data_team_mappings.csv \
  --league-mappings-csv /Users/chirag/Downloads/sports_data_league_mappings.csv \
  --state-dir ./state \
  --sink jsonl
```

### 2. Start the dashboard and local API

Run the local bridge:

```bash
pred-polymarket-http \
  --markets-csv /Users/chirag/Downloads/markets.csv \
  --parent-markets-csv /Users/chirag/Downloads/parent_markets.csv \
  --fixtures-csv /Users/chirag/Downloads/fixtures.csv \
  --fixture-mappings-csv /Users/chirag/Downloads/sports_data_fixture_mappings.csv \
  --team-mappings-csv /Users/chirag/Downloads/sports_data_team_mappings.csv \
  --league-mappings-csv /Users/chirag/Downloads/sports_data_league_mappings.csv
```

Then open `http://127.0.0.1:8080/`.

The dashboard gives you:

- a manual matcher form
- the compact JSON response you want to store
- the latest mappings
- the current review queue
- the latest orderbook summaries

You can still POST your payload directly to `http://127.0.0.1:8080/api/match` or `http://127.0.0.1:8080/match`.

On a successful match, the response is returned in the same compact payload shape you want to store:

```json
{
  "market_name": "BRE_BRE_vs_ARS_EPL_2026",
  "market": {
    "question": "BRE_BRE_vs_ARS_EPL_2026",
    "status": "active",
    "outcomes": {
      "YES": {"token_id": "POLYMARKET_YES_TOKEN_ID"},
      "NO": {"token_id": "POLYMARKET_NO_TOKEN_ID"}
    },
    "pred_mapping": {
      "market_id": "YOUR_PRED_MARKET_ID"
    }
  }
}
```

### 3. Stream full-depth orderbooks for mapped markets

```bash
pred-polymarket-sync listen-orderbooks --state-dir ./state --sink jsonl
```

### 4. Retry unresolved / ambiguous markets

```bash
pred-polymarket-sync reconcile --state-dir ./state --sink jsonl
```

### 5. Export Polymarket team and league reference data

```bash
pred-polymarket-sync export-reference-data --output-dir ./exports
```

This writes:

- `exports/polymarket_teams.csv`
- `exports/polymarket_leagues.csv`
- `exports/polymarket_teams.sql`
- `exports/polymarket_leagues.sql`

Football-only refresh:

```bash
pred-polymarket-sync export-reference-data --football-only --output-dir ./exports
```

This writes:

- `exports/polymarket_football_teams.csv`
- `exports/polymarket_football_leagues.csv`
- `exports/polymarket_football_teams.sql`
- `exports/polymarket_football_leagues.sql`

### 6. Search the live Polymarket reference catalogs

Search teams:

```bash
pred-polymarket-sync search-teams --query Arsenal --football-only --limit 10
```

Search leagues:

```bash
pred-polymarket-sync search-leagues --query premier --football-only --limit 10
```

These return JSON with the same canonical team and league IDs used in the CSV and SQL exports.

### 7. Fetch grouped markets and orderbooks for one fixture

By fixture slug:

```bash
pred-polymarket-sync fixture-orderbooks --fixture-slug ucl-psg1-liv1-2026-04-08
```

By teams, league, and kickoff:

```bash
pred-polymarket-sync fixture-orderbooks \
  --home-team psg \
  --away-team liverpool \
  --league-code ucl \
  --kickoff 2026-04-08T19:00:00Z
```

This returns a grouped payload with:

- `moneyline` using home win and away win only
- `totals` for supported over lines
- `spreads`
- `both_teams_to_score`

Each market includes `yes_token_id`, `no_token_id`, `bids`, `asks`, `best_bid_yes`, `best_ask_yes`, `best_bid_no`, `best_ask_no`, `line`, `question`, and `slug`.

## Supported Input Modes

### CSV

Best for initial backfills and testing. Uses the semicolon-delimited exports you shared.

### Database

Set `DATABASE_URL` and pass table names instead of CSV paths. Supported schemes:

- `postgresql://...`
- `postgres://...`
- `mysql://...`

Example:

```bash
export DATABASE_URL='postgresql://user:pass@host:5432/dbname'
pred-polymarket-sync backfill \
  --markets-table public.markets \
  --parent-markets-table public.parent_markets \
  --fixtures-table public.fixtures \
  --fixture-mappings-table public.sports_data_fixture_mappings \
  --team-mappings-table public.sports_data_team_mappings \
  --league-mappings-table public.sports_data_league_mappings
```

## Output Sinks

### JSONL

Writes to files under `state/`:

- `mappings.jsonl`
- `reviews.jsonl`
- `orderbooks.jsonl`

### HTTP

Pushes mapping and orderbook payloads to your internal service endpoint.

## Local HTTP API

When you start the local server, it also exposes live reference catalog lookups:

- `GET /api/reference/sports`
- `GET /api/reference/leagues?sport=soccer&limit=1000`
- `GET /api/reference/participants?league_code=epl&limit=10000`
- `GET /api/reference/teams?q=arsenal&football_only=true&limit=10`
- `GET /api/reference/teams?q=ars&league_code=epl&limit=10`
- `GET /api/reference/leagues?q=premier&football_only=true&limit=10`
- `GET /api/fixture-orderbooks?fixture_slug=ucl-psg1-liv1-2026-04-08`
- `GET /api/fixture-orderbooks?home_team=psg&away_team=liverpool&league_code=ucl&kickoff=2026-04-08T19:00:00Z`
- `GET /api/fixture-markets?fixture_slug=ucl-psg1-liv1-2026-04-08&families=moneyline,totals,spreads,both_teams_to_score`

The reference routes return JSON in the same shape as the exported reference rows. The fixture route returns grouped market families with token IDs and full yes/no orderbooks.

Notes:

- `/api/reference/sports` returns a stable local `sport_id` slug like `soccer` or `american_football`.
- When Polymarket exposes a shared family tag across leagues, the same payload also includes `polymarket_tag_id`, for example `soccer -> 100350`, `cricket -> 517`, `tennis -> 864`.
- `/api/reference/leagues?sport=soccer` lists league/competition rows under that sport family, for example `epl`, `lal`, `bun`, `ucl`.
- `/api/reference/participants?league_code=epl` lists all teams in that league with their Polymarket `team_id`s.
- `/api/fixture-markets` is the more versatile alias for `/api/fixture-orderbooks`; it also supports `families=...`, `include_draw=true`, and either `fixture_slug` or `home_team + away_team + league_code + kickoff`.

Set:

- `SYNC_HTTP_BASE_URL`
- `SYNC_HTTP_MAPPING_PATH`
- `SYNC_HTTP_ORDERBOOK_PATH`
- `SYNC_HTTP_REVIEW_PATH`
- optional `SYNC_HTTP_AUTH_HEADER`
- optional `SYNC_HTTP_AUTH_TOKEN`

## Matching Behavior

The matcher is intentionally conservative:

- exact game ID match is heavily weighted
- exact home/away sports-data team IDs are heavily weighted
- kickoff tolerance is configurable, default `30` minutes
- exact outcome text matching is required for a confident result
- ties or weak matches are flagged for review

This is the safest way to protect against false positives when multiple Polymarket markets exist for the same fixture.

## Notes

- Polymarket discovery uses the official Gamma API `GET /markets`.
- Orderbook snapshots use the official CLOB `POST /books`.
- Real-time depth uses the official market WebSocket at `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
- Market/user channels require client `PING` every 10 seconds. The listener handles that.
- The dashboard state endpoint is `GET /api/state`.

## Tests

```bash
python3 -m unittest discover -s tests
```
