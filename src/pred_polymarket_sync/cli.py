from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List

from pred_polymarket_sync.config import Settings
from pred_polymarket_sync.exporters import (
    export_polymarket_reference_data,
    search_league_rows,
    search_team_rows,
)
from pred_polymarket_sync.fixture_markets import fetch_fixture_orderbooks
from pred_polymarket_sync.fixture_markets import fetch_league_fixture_orderbooks
from pred_polymarket_sync.matcher import MarketMatcher, build_pred_market_bundles
from pred_polymarket_sync.orderbook import OrderBookListener
from pred_polymarket_sync.polymarket import PolymarketClient
from pred_polymarket_sync.sinks import build_sink
from pred_polymarket_sync.sources import build_source_from_args
from pred_polymarket_sync.state import StateStore


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 1
    settings = Settings()
    polymarket = PolymarketClient(
        gamma_base_url=settings.gamma_base_url,
        clob_base_url=settings.clob_base_url,
        timeout_seconds=settings.http_timeout_seconds,
    )

    if args.command == "export-reference-data":
        football_only = bool(args.football_only)
        base_name = args.base_name or (
            "polymarket_football" if football_only else "polymarket"
        )
        export = export_polymarket_reference_data(
            teams=polymarket.list_teams(limit=args.team_batch_size),
            leagues=polymarket.list_leagues(),
            output_dir=Path(args.output_dir).resolve(),
            base_name=base_name,
            football_only=football_only,
        )
        print(json.dumps(export, ensure_ascii=True, indent=2))
        return 0
    if args.command == "search-teams":
        teams = polymarket.list_teams(limit=args.team_batch_size)
        leagues = polymarket.list_leagues()
        results = search_team_rows(
            teams=teams,
            leagues=leagues,
            query=args.query,
            league_id=args.league_id,
            league_code=args.league_code,
            football_only=bool(args.football_only),
            limit=args.limit,
        )
        print(json.dumps({"count": len(results), "items": results}, ensure_ascii=True, indent=2))
        return 0
    if args.command == "search-leagues":
        results = search_league_rows(
            leagues=polymarket.list_leagues(),
            query=args.query,
            sport=args.sport,
            football_only=bool(args.football_only),
            limit=args.limit,
        )
        print(json.dumps({"count": len(results), "items": results}, ensure_ascii=True, indent=2))
        return 0
    if args.command == "fixture-orderbooks":
        payload = fetch_fixture_orderbooks(
            polymarket_client=polymarket,
            fixture_slug=args.fixture_slug,
            home_team=args.home_team,
            away_team=args.away_team,
            league_code=args.league_code,
            kickoff=args.kickoff,
            include_closed=not bool(args.open_only),
            include_draw=bool(args.include_draw),
            requested_families=_parse_families(args.families),
            start_time_tolerance_minutes=settings.start_time_tolerance_minutes,
        )
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    if args.command == "league-fixture-orderbooks":
        payload = fetch_league_fixture_orderbooks(
            polymarket_client=polymarket,
            league_code=args.league_code,
            include_closed=not bool(args.open_only),
            include_draw=bool(args.include_draw),
            requested_families=_parse_families(args.families),
            limit=args.limit,
            date_from=args.date_from,
            date_to=args.date_to,
        )
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    settings.state_dir = Path(args.state_dir or settings.state_dir).resolve()
    sink = build_sink(
        sink_name=args.sink,
        state_dir=settings.state_dir,
        timeout_seconds=settings.http_timeout_seconds,
        base_url=settings.http_base_url,
        mapping_path=settings.http_mapping_path,
        review_path=settings.http_review_path,
        orderbook_path=settings.http_orderbook_path,
        auth_header=settings.http_auth_header,
        auth_token=settings.http_auth_token,
    )
    source = build_source_from_args(args, database_url=settings.database_url)
    matcher = MarketMatcher(
        start_time_tolerance_minutes=settings.start_time_tolerance_minutes,
        score_threshold=settings.score_threshold,
        score_gap_threshold=settings.score_gap_threshold,
    )
    state = StateStore(settings.state_dir)

    if args.command == "backfill":
        mappings, reviews = run_backfill(source, matcher, polymarket, sink, state)
        print(
            json.dumps(
                {"matched": len(mappings), "reviews": len(reviews)},
                ensure_ascii=True,
            )
        )
        return 0
    if args.command == "reconcile":
        mappings, reviews = run_backfill(source, matcher, polymarket, sink, state)
        print(
            json.dumps(
                {"reconciled_matched": len(mappings), "reconciled_reviews": len(reviews)},
                ensure_ascii=True,
            )
        )
        return 0
    if args.command == "match-realtime":
        return run_match_realtime(source, matcher, polymarket, sink, state, settings)
    if args.command == "listen-orderbooks":
        mappings = [
            item
            for item in state.load_mappings()
            if item.get("tracking_status", "") in {"", "upcoming", "postponed"}
        ]
        if not mappings:
            raise RuntimeError("No mappings found in state. Run backfill or match-realtime first.")
        listener = OrderBookListener(
            polymarket_client=polymarket,
            sink=sink,
            market_ws_url=settings.market_ws_url,
            reconnect_delay_seconds=settings.reconnect_delay_seconds,
        )
        asyncio.run(listener.run([_mapping_from_dict(item) for item in mappings]))
        return 0
    if args.command == "match-payload":
        payload = _load_payload(args.input_json)
        mapping = run_match_payload(payload, source, matcher, polymarket, sink, state)
        print(json.dumps(mapping, ensure_ascii=True, indent=2))
        return 0
    raise RuntimeError(f"Unknown command {args.command}")


def run_backfill(source, matcher, polymarket, sink, state):
    snapshot = source.load_snapshot()
    bundles = build_pred_market_bundles(snapshot)
    matches = []
    reviews = []
    candidate_cache: Dict[str, List] = {}
    for bundle in bundles:
        game_id = bundle.sportsdata_fixture.sportsdata_game_id if bundle.sportsdata_fixture else ""
        if not game_id:
            result = matcher.match(bundle, [])
        else:
            if game_id not in candidate_cache:
                candidate_cache[game_id] = polymarket.list_markets_for_game(game_id)
            result = matcher.match(bundle, candidate_cache[game_id])
        if result.mapping:
            matches.append(result.mapping)
            sink.publish_mapping(result.mapping)
        if result.review:
            reviews.append(result.review)
            sink.publish_review(result.review)
    state.save_mappings(matches)
    state.save_reviews(reviews)
    return matches, reviews


def run_match_payload(payload, source, matcher, polymarket, sink, state):
    market_id = (
        payload.get("market", {})
        .get("pred_mapping", {})
        .get("market_id")
        or payload.get("market_id")
        or ""
    )
    if not market_id:
        raise RuntimeError("Payload does not include market.pred_mapping.market_id")
    snapshot = source.load_snapshot()
    bundles = build_pred_market_bundles(snapshot)
    bundle = next((item for item in bundles if item.market.market_id == market_id), None)
    if not bundle:
        raise RuntimeError(f"Pred market_id {market_id} was not found in the current source data")
    game_id = bundle.sportsdata_fixture.sportsdata_game_id if bundle.sportsdata_fixture else ""
    candidates = polymarket.list_markets_for_game(game_id) if game_id else []
    result = matcher.match(bundle, candidates)
    if result.mapping:
        sink.publish_mapping(result.mapping)
        mappings = [_mapping_from_dict(item) for item in state.load_mappings()]
        mappings.append(result.mapping)
        state.save_mappings(mappings)
        return _format_pred_payload(payload, result.mapping)
    if result.review:
        sink.publish_review(result.review)
        reviews = state.load_reviews()
        reviews.append(asdict(result.review))
        state.save_reviews([_review_from_dict(item) for item in reviews])
        return {"status": result.status, "review": asdict(result.review)}
    return {"status": "not_found"}


def run_match_realtime(source, matcher, polymarket, sink, state, settings):
    seen_market_ids = {item["pred_market_id"] for item in state.load_mappings()}
    while True:
        snapshot = source.load_snapshot()
        bundles = build_pred_market_bundles(snapshot)
        new_bundles = [bundle for bundle in bundles if bundle.market.market_id not in seen_market_ids]
        if new_bundles:
            candidate_cache: Dict[str, List] = {}
            mappings = [_mapping_from_dict(item) for item in state.load_mappings()]
            reviews = [_review_from_dict(item) for item in state.load_reviews()]
            for bundle in new_bundles:
                game_id = bundle.sportsdata_fixture.sportsdata_game_id if bundle.sportsdata_fixture else ""
                if game_id not in candidate_cache:
                    candidate_cache[game_id] = polymarket.list_markets_for_game(game_id) if game_id else []
                result = matcher.match(bundle, candidate_cache[game_id])
                if result.mapping:
                    seen_market_ids.add(bundle.market.market_id)
                    mappings.append(result.mapping)
                    sink.publish_mapping(result.mapping)
                if result.review:
                    reviews.append(result.review)
                    sink.publish_review(result.review)
            state.save_mappings(mappings)
            state.save_reviews(reviews)
        time.sleep(settings.realtime_poll_seconds)


def _load_payload(path: str):
    if path == "-":
        return json.load(sys.stdin)
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _format_pred_payload(payload, mapping):
    market = payload.get("market", {}) if isinstance(payload, dict) else {}
    pred_mapping = market.get("pred_mapping", {}) if isinstance(market, dict) else {}
    pred_market_id = pred_mapping.get("market_id") or payload.get("market_id") or ""
    question = market.get("question") or payload.get("market_name") or ""
    status = market.get("status") or "active"
    market_name = payload.get("market_name") or question
    return {
        "market_name": market_name,
        "market": {
            "question": question,
            "status": status,
            "outcomes": {
                "YES": {"token_id": mapping.yes_token_id},
                "NO": {"token_id": mapping.no_token_id},
            },
            "pred_mapping": {
                "market_id": pred_market_id,
            },
        },
    }


def _mapping_from_dict(item):
    from pred_polymarket_sync.models import MappingRecord

    return MappingRecord(**item)


def _review_from_dict(item):
    from pred_polymarket_sync.models import ReviewRecord

    return ReviewRecord(**item)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pred x Polymarket automation")
    subparsers = parser.add_subparsers(dest="command")

    add_source_args(_base_command(subparsers.add_parser("backfill", help="Backfill mappings")))
    add_source_args(_base_command(subparsers.add_parser("reconcile", help="Retry/refresh mappings")))
    add_source_args(_base_command(subparsers.add_parser("match-realtime", help="Poll for newly created markets and match them")))

    _base_command(
        subparsers.add_parser(
            "listen-orderbooks",
            help="Stream full-depth orderbooks for the latest mappings",
        )
    )

    payload = _base_command(subparsers.add_parser("match-payload", help="Enrich one Postman payload with Polymarket IDs"))
    add_source_args(payload)
    payload.add_argument("--input-json", required=True, help="Path to a JSON payload file or '-' for stdin")

    export = subparsers.add_parser(
        "export-reference-data",
        help="Export Polymarket team and league reference data to CSV and SQL",
    )
    export.add_argument("--output-dir", default="./exports")
    export.add_argument("--team-batch-size", type=int, default=500)
    export.add_argument("--base-name", default="")
    export.add_argument("--football-only", action="store_true")

    search_teams = subparsers.add_parser(
        "search-teams",
        help="Search the live Polymarket global team catalog",
    )
    search_teams.add_argument("--query", default="")
    search_teams.add_argument("--league-id", default="")
    search_teams.add_argument("--league-code", default="")
    search_teams.add_argument("--football-only", action="store_true")
    search_teams.add_argument("--limit", type=int, default=25)
    search_teams.add_argument("--team-batch-size", type=int, default=500)

    search_leagues = subparsers.add_parser(
        "search-leagues",
        help="Search the live Polymarket global league catalog",
    )
    search_leagues.add_argument("--query", default="")
    search_leagues.add_argument("--sport", default="")
    search_leagues.add_argument("--football-only", action="store_true")
    search_leagues.add_argument("--limit", type=int, default=25)

    fixture_orderbooks = subparsers.add_parser(
        "fixture-orderbooks",
        help="Fetch grouped Polymarket markets and orderbooks for one fixture",
    )
    fixture_orderbooks.add_argument("--fixture-slug", default="")
    fixture_orderbooks.add_argument("--home-team", default="")
    fixture_orderbooks.add_argument("--away-team", default="")
    fixture_orderbooks.add_argument("--league-code", default="")
    fixture_orderbooks.add_argument("--kickoff", default="")
    fixture_orderbooks.add_argument("--open-only", action="store_true")
    fixture_orderbooks.add_argument("--include-draw", action="store_true")
    fixture_orderbooks.add_argument("--families", default="")

    league_orderbooks = subparsers.add_parser(
        "league-fixture-orderbooks",
        help="Fetch grouped Polymarket markets and orderbooks for all fixtures in one league",
    )
    league_orderbooks.add_argument("--league-code", required=True)
    league_orderbooks.add_argument("--open-only", action="store_true")
    league_orderbooks.add_argument("--include-draw", action="store_true")
    league_orderbooks.add_argument("--families", default="")
    league_orderbooks.add_argument("--limit", type=int, default=25)
    league_orderbooks.add_argument("--date-from", default="")
    league_orderbooks.add_argument("--date-to", default="")

    return parser


def _base_command(parser):
    parser.add_argument("--state-dir", default="./state")
    parser.add_argument("--sink", choices=["jsonl", "http", "both"], default="jsonl")
    return parser


def add_source_args(parser):
    parser.add_argument("--markets-csv")
    parser.add_argument("--parent-markets-csv")
    parser.add_argument("--fixtures-csv")
    parser.add_argument("--fixture-mappings-csv")
    parser.add_argument("--team-mappings-csv")
    parser.add_argument("--league-mappings-csv")
    parser.add_argument("--markets-table")
    parser.add_argument("--parent-markets-table")
    parser.add_argument("--fixtures-table")
    parser.add_argument("--fixture-mappings-table")
    parser.add_argument("--team-mappings-table")
    parser.add_argument("--league-mappings-table")
    return parser


def _parse_families(value: str) -> list[str] | None:
    if not value:
        return None
    items = [part.strip() for part in value.split(",") if part.strip()]
    return items or None


if __name__ == "__main__":
    raise SystemExit(main())
