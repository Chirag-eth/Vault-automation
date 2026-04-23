from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from pred_polymarket_sync.cli import run_match_payload
from pred_polymarket_sync.cms_client import CmsClient
from pred_polymarket_sync.config import Settings
from pred_polymarket_sync.exporters import search_league_rows, search_team_rows
from pred_polymarket_sync.fixture_markets import (
    fetch_fixture_orderbooks,
    fetch_league_fixture_orderbooks,
)
from pred_polymarket_sync.mapping_loader import MappingStore
from pred_polymarket_sync.matcher import MarketMatcher
from pred_polymarket_sync.models import PolymarketLeague
from pred_polymarket_sync.polymarket import PolymarketClient
from pred_polymarket_sync.sinks import build_sink
from pred_polymarket_sync.sources import build_source_from_args
from pred_polymarket_sync.state import StateStore
from pred_polymarket_sync.token_mapper import TokenMapper
from pred_polymarket_sync.uat_market_publisher import UatMarketPublisher
from pred_polymarket_sync.utils import normalize_text


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Local HTTP bridge for Postman")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--state-dir", default="./state")
    parser.add_argument("--sink", choices=["jsonl", "http", "both"], default="jsonl")
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
    # Polymarket CSV mapping files for token-mapping endpoints
    parser.add_argument("--polymarket-league-mappings", default="")
    parser.add_argument("--polymarket-team-mappings", default="")
    # CMS base URL for fixture-token-map endpoint
    parser.add_argument("--cms-base-url", default="")
    # Market-making service host (matched markets auto-POSTed after sync)
    parser.add_argument("--market-making-host", default="")
    args = parser.parse_args(argv)

    settings = Settings()
    settings.state_dir = Path(args.state_dir).resolve()
    source = _maybe_build_source_from_args(args, settings.database_url)
    matcher = MarketMatcher(
        start_time_tolerance_minutes=settings.start_time_tolerance_minutes,
        score_threshold=settings.score_threshold,
        score_gap_threshold=settings.score_gap_threshold,
    )
    polymarket = PolymarketClient(
        gamma_base_url=settings.gamma_base_url,
        clob_base_url=settings.clob_base_url,
        timeout_seconds=settings.http_timeout_seconds,
    )
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
    state = StateStore(settings.state_dir)
    # ------------------------------------------------------------------
    # Token-mapping infrastructure (read-only, v1 scope)
    # ------------------------------------------------------------------
    _league_mappings_path = (
        args.polymarket_league_mappings
        or settings.league_mappings_csv
    )
    _team_mappings_path = (
        args.polymarket_team_mappings
        or settings.team_mappings_csv
    )
    # Load CSV as base (frozen — never changes at runtime).
    # JSON overlay is re-read on every sync-fixture request so edits take effect immediately.
    _csv_mapping_store = MappingStore.from_csv(
        league_csv_path=_league_mappings_path,
        team_csv_path=_team_mappings_path,
    )
    _json_path = settings.mappings_json

    def _fresh_mapping_store() -> MappingStore:
        """Clone the CSV base and overlay the latest JSON (hot-reload on every request)."""
        store = MappingStore.clone(_csv_mapping_store)
        if _json_path:
            store.merge(MappingStore.from_json(_json_path))
        return store

    mapping_store = _fresh_mapping_store()
    print(f"Mappings loaded: {mapping_store.league_count} leagues, {mapping_store.team_count} teams")

    _cms_base = args.cms_base_url or settings.cms_base_url
    cms_client: CmsClient | None = None
    if _cms_base:
        cms_client = CmsClient(
            base_url=_cms_base,
            auth_header=settings.cms_auth_header,
            auth_token=settings.cms_auth_token,
            timeout=settings.http_timeout_seconds,
        )

    _mm_host = args.market_making_host or settings.market_making_host
    token_mapper = TokenMapper(
        polymarket_client=polymarket,
        cms_client=cms_client,
        mapping_store=mapping_store,
        start_time_tolerance_minutes=settings.start_time_tolerance_minutes,
        market_making_host=_mm_host,
        market_making_auth_header=settings.market_making_auth_header,
        market_making_auth_token=settings.market_making_auth_token,
        http_timeout_seconds=settings.http_timeout_seconds,
    )
    _is_uat_host = "uat" in _mm_host.lower()
    uat_market_publisher: UatMarketPublisher | None = (
        UatMarketPublisher(
            polymarket_client=polymarket,
            base_url=_mm_host,
            markets_path=settings.market_making_markets_path,
            delete_path_template=settings.market_making_delete_path_template,
            active_markets_path=settings.market_making_active_markets_path,
            internal_base_url=_cms_base,
            internal_active_markets_path=settings.uat_internal_active_markets_path,
            poll_seconds=settings.token_autodelete_poll_seconds,
            http_timeout_seconds=settings.http_timeout_seconds,
            actor_header=settings.uat_market_actor_header,
            actor_value=settings.uat_market_actor_value,
        )
        if _is_uat_host
        else None
    )
    # ------------------------------------------------------------------

    static_dir = Path(__file__).with_name("static")
    exports_dir = Path.cwd() / "exports" / "reference"
    reference_cache = {"teams": None, "leagues": None}
    export_row_cache = {}

    def load_leagues():
        if reference_cache["leagues"] is None:
            reference_cache["leagues"] = polymarket.list_leagues()
        return reference_cache["leagues"]

    def load_teams():
        if reference_cache["teams"] is None:
            reference_cache["teams"] = polymarket.list_teams()
        return reference_cache["teams"]

    def load_export_rows(football_only: bool):
        cache_key = "football" if football_only else "all"
        if cache_key not in export_row_cache:
            export_row_cache[cache_key] = _load_export_rows(exports_dir, football_only)
        return export_row_cache[cache_key]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self._req_start = time.monotonic()
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            if path in {"/", "/dashboard"}:
                self._file_response(static_dir / "dashboard.html", "text/html; charset=utf-8")
                return
            if path == "/assets/dashboard.css":
                self._file_response(static_dir / "dashboard.css", "text/css; charset=utf-8")
                return
            if path == "/assets/dashboard.js":
                self._file_response(
                    static_dir / "dashboard.js",
                    "application/javascript; charset=utf-8",
                )
                return
            if path in {"/health", "/api/health"}:
                self._json_response(
                    200,
                    {
                        "ok": True,
                        "server_time": datetime.now(timezone.utc).isoformat(),
                        "matching_source_configured": source is not None,
                    },
                )
                return
            if path == "/api/state":
                self._json_response(200, state.load_dashboard_state())
                return
            if path == "/api/reference/teams":
                football_only = _query_bool(query, "football_only")
                export_teams, export_leagues = load_export_rows(football_only)
                if export_teams is not None and export_leagues is not None:
                    items = _search_exported_team_rows(
                        team_rows=export_teams,
                        league_rows=export_leagues,
                        query=_query_value(query, "q"),
                        league_id=_query_value(query, "league_id"),
                        league_code=_query_value(query, "league_code"),
                        limit=_query_int(query, "limit", 25),
                    )
                else:
                    teams = load_teams()
                    leagues = load_leagues()
                    items = search_team_rows(
                        teams=teams,
                        leagues=leagues,
                        query=_query_value(query, "q"),
                        league_id=_query_value(query, "league_id"),
                        league_code=_query_value(query, "league_code"),
                        football_only=football_only,
                        limit=_query_int(query, "limit", 25),
                    )
                self._json_response(200, {"count": len(items), "items": items})
                return
            if path == "/api/reference/participants":
                football_only = _query_bool(query, "football_only")
                export_teams, export_leagues = load_export_rows(football_only)
                if export_teams is not None and export_leagues is not None:
                    items = _search_exported_team_rows(
                        team_rows=export_teams,
                        league_rows=export_leagues,
                        query=_query_value(query, "q"),
                        league_id=_query_value(query, "league_id"),
                        league_code=_query_value(query, "league_code"),
                        limit=_query_int(query, "limit", 10000),
                    )
                else:
                    teams = load_teams()
                    leagues = load_leagues()
                    items = search_team_rows(
                        teams=teams,
                        leagues=leagues,
                        query=_query_value(query, "q"),
                        league_id=_query_value(query, "league_id"),
                        league_code=_query_value(query, "league_code"),
                        football_only=football_only,
                        limit=_query_int(query, "limit", 10000),
                    )
                self._json_response(
                    200,
                    {
                        "count": len(items),
                        "league_id": _query_value(query, "league_id"),
                        "league_code": _query_value(query, "league_code"),
                        "items": items,
                    },
                )
                return
            if path == "/api/reference/leagues":
                football_only = _query_bool(query, "football_only")
                sport = _query_value(query, "sport")
                _, export_leagues = load_export_rows(football_only)
                if export_leagues is not None and not sport:
                    items = _search_exported_league_rows(
                        league_rows=export_leagues,
                        query=_query_value(query, "q"),
                        sport=sport,
                        limit=_query_int(query, "limit", 25),
                    )
                else:
                    leagues = load_leagues()
                    items = search_league_rows(
                        leagues=leagues,
                        query=_query_value(query, "q"),
                        sport=sport,
                        football_only=football_only,
                        limit=_query_int(query, "limit", 25),
                    )
                self._json_response(200, {"count": len(items), "items": items})
                return
            if path == "/api/reference/league-with-teams":
                league_code = _query_value(query, "league_code")
                league_id = _query_value(query, "league_id")
                if not league_code and not league_id:
                    self._json_response(400, {
                        "error": "missing_param",
                        "detail": "Provide league_code (e.g. lal) or league_id (e.g. 3)",
                    })
                    return
                export_teams, export_leagues = load_export_rows(football_only=False)
                if export_teams is None or export_leagues is None:
                    self._json_response(503, {"error": "exports_not_available"})
                    return
                # Resolve league_id from code if needed
                resolved_league_id = league_id
                if not resolved_league_id and league_code:
                    lc_norm = normalize_text(league_code)
                    for lr in export_leagues:
                        if normalize_text(lr.get("alternate_name", "")) == lc_norm:
                            resolved_league_id = str(lr.get("league_id", ""))
                            break
                if not resolved_league_id:
                    self._json_response(404, {
                        "error": "league_not_found",
                        "detail": f"No league matched league_code={league_code!r} league_id={league_id!r}",
                    })
                    return
                # Find the league row
                league_row = next(
                    (lr for lr in export_leagues if str(lr.get("league_id", "")) == resolved_league_id),
                    None,
                )
                # Collect all teams for this league
                team_rows = [
                    tr for tr in export_teams
                    if str(tr.get("league_id", "")) == resolved_league_id
                ]
                team_rows.sort(key=lambda r: normalize_text(r.get("name", "")))
                self._json_response(200, {
                    "league_id": resolved_league_id,
                    "league": league_row,
                    "team_count": len(team_rows),
                    "teams": team_rows,
                })
                return
            if path == "/api/reference/sports":
                items = _build_sport_rows(load_leagues())
                sport = normalize_text(_query_value(query, "sport"))
                if sport:
                    items = [item for item in items if normalize_text(str(item.get("sport_id", ""))) == sport or normalize_text(str(item.get("slug", ""))) == sport]
                self._json_response(200, {"count": len(items), "items": items})
                return
            if path in {"/api/fixture-orderbooks", "/api/fixture-markets"}:
                try:
                    payload = fetch_fixture_orderbooks(
                        polymarket_client=polymarket,
                        fixture_slug=_query_value(query, "fixture_slug"),
                        home_team=_query_value(query, "home_team"),
                        away_team=_query_value(query, "away_team"),
                        league_code=_query_value(query, "league_code"),
                        kickoff=_query_value(query, "kickoff"),
                        include_closed=not _query_bool(query, "open_only"),
                        include_draw=_query_bool(query, "include_draw"),
                        requested_families=_parse_csv_list(_query_value(query, "families")),
                        start_time_tolerance_minutes=settings.start_time_tolerance_minutes,
                    )
                except Exception as exc:
                    self._json_response(400, {"error": str(exc)})
                    return
                self._json_response(200, payload)
                return
            if path in {"/api/league-fixture-orderbooks", "/api/league-fixture-markets"}:
                try:
                    payload = fetch_league_fixture_orderbooks(
                        polymarket_client=polymarket,
                        league_code=_query_value(query, "league_code"),
                        include_closed=not _query_bool(query, "open_only"),
                        include_draw=_query_bool(query, "include_draw"),
                        requested_families=_parse_csv_list(_query_value(query, "families")),
                        limit=_query_int(query, "limit", 25),
                        date_from=_query_value(query, "date_from"),
                        date_to=_query_value(query, "date_to"),
                    )
                except Exception as exc:
                    self._json_response(400, {"error": str(exc)})
                    return
                self._json_response(200, payload)
                return

            # ----------------------------------------------------------
            # v1 token-mapping endpoints
            # ----------------------------------------------------------
            if path == "/api/v1/polymarket/resolve-fixture-slug":
                polymarket_url = _query_value(query, "polymarket_url")
                fixture_text = _query_value(query, "fixture_text")
                league_code = _query_value(query, "league_code")
                if not polymarket_url and not fixture_text:
                    self._json_response(400, {
                        "error": "missing_param",
                        "detail": "Provide polymarket_url or fixture_text",
                    })
                    return
                try:
                    if polymarket_url:
                        result = token_mapper.resolve_slug_from_url(polymarket_url)
                    else:
                        result = token_mapper.resolve_slug_from_text(fixture_text, league_code)
                except Exception as exc:
                    self._json_response(500, {"error": "internal_error", "detail": str(exc)})
                    return
                http_status = 200 if result.get("status") == "matched" else 404
                self._json_response(http_status, result)
                return

            if path == "/api/v1/polymarket/fixture-token-map":
                cms_fixture_id = _query_value(query, "cms_fixture_id")
                cms_url = _query_value(query, "cms_url")
                if not cms_fixture_id and cms_url:
                    from pred_polymarket_sync.token_mapper import extract_cms_fixture_id_from_url
                    cms_fixture_id = extract_cms_fixture_id_from_url(cms_url)
                if not cms_fixture_id:
                    self._json_response(400, {
                        "error": "missing_param",
                        "detail": "Provide cms_fixture_id or cms_url",
                    })
                    return
                pm_slug = _query_value(query, "polymarket_slug")
                include_closed = _query_bool(query, "include_closed")
                try:
                    result = token_mapper.map_fixture(
                        cms_fixture_id=cms_fixture_id,
                        polymarket_slug=pm_slug,
                        include_closed=include_closed,
                    )
                except Exception as exc:
                    self._json_response(500, {"error": "internal_error", "detail": str(exc)})
                    return
                self._json_response(200, result)
                return

            if path == "/api/v1/polymarket/sync-fixture":
                # Combined endpoint: polymarket_url + cms_fixture_id
                # Resolves slug, maps markets, and POSTs matched markets to market-making.
                polymarket_url = _query_value(query, "polymarket_url")
                cms_fixture_id = _query_value(query, "cms_fixture_id")
                cms_url = _query_value(query, "cms_url")
                dry_run = _query_bool(query, "dry_run")

                # Support CMS trade URL as fallback for cms_fixture_id
                if not cms_fixture_id and cms_url:
                    from pred_polymarket_sync.token_mapper import extract_cms_fixture_id_from_url
                    cms_fixture_id = extract_cms_fixture_id_from_url(cms_url)

                if not polymarket_url:
                    self._json_response(400, {
                        "error": "missing_param",
                        "detail": "polymarket_url is required",
                    })
                    return
                if not cms_fixture_id:
                    self._json_response(400, {
                        "error": "missing_param",
                        "detail": "cms_fixture_id (or cms_url) is required",
                    })
                    return
                token_mapper.reload_mappings(_fresh_mapping_store())
                try:
                    result = token_mapper.map_and_sync_fixture(
                        polymarket_url=polymarket_url,
                        cms_fixture_id=cms_fixture_id,
                        dry_run=dry_run,
                    )
                except Exception as exc:
                    self._json_response(500, {"error": "internal_error", "detail": str(exc)})
                    return
                self._json_response(200, result)
                return

            if path == "/api/v1/uat/publish-active-market-tokens":
                yes_token_id = _query_value(query, "yes_token_id")
                no_token_id = _query_value(query, "no_token_id")
                dry_run = _query_bool(query, "dry_run")
                monitor_for_delete = not _query_value(query, "monitor_for_delete").lower() == "false"

                if not yes_token_id:
                    self._json_response(400, {
                        "error": "missing_param",
                        "detail": "yes_token_id is required",
                    })
                    return
                if not no_token_id:
                    self._json_response(400, {
                        "error": "missing_param",
                        "detail": "no_token_id is required",
                    })
                    return
                if uat_market_publisher is None:
                    self._json_response(503, {"error": "uat_not_configured", "detail": "Server is running against a mainnet host; UAT publish is unavailable."})
                    return
                try:
                    result = uat_market_publisher.publish_to_active_markets(
                        yes_token_id=yes_token_id,
                        no_token_id=no_token_id,
                        dry_run=dry_run,
                        monitor_for_delete=monitor_for_delete,
                    )
                except Exception as exc:
                    self._json_response(500, {"error": "internal_error", "detail": str(exc)})
                    return
                self._json_response(200, result)
                return

            if path == "/api/v1/uat/publish-polymarket-url-to-rest-active-markets":
                polymarket_url = _query_value(query, "polymarket_url")
                dry_run = _query_bool(query, "dry_run")
                monitor_for_delete = not _query_value(query, "monitor_for_delete").lower() == "false"
                source_market_id = _query_value(query, "source_market_id")
                source_family = _query_value(query, "source_family")
                source_line = _query_value(query, "source_line")

                if not polymarket_url:
                    self._json_response(400, {
                        "error": "missing_param",
                        "detail": "polymarket_url is required",
                    })
                    return
                if uat_market_publisher is None:
                    self._json_response(503, {"error": "uat_not_configured", "detail": "Server is running against a mainnet host; UAT publish is unavailable."})
                    return
                try:
                    result = uat_market_publisher.publish_remaining_active_markets_from_polymarket_url(
                        polymarket_url=polymarket_url,
                        dry_run=dry_run,
                        monitor_for_delete=monitor_for_delete,
                        source_market_id=source_market_id,
                        source_family=source_family,
                        source_line=source_line,
                    )
                except Exception as exc:
                    self._json_response(500, {"error": "internal_error", "detail": str(exc)})
                    return
                self._json_response(200, result)
                return
            # ----------------------------------------------------------

            self._json_response(404, {"error": "not_found"})

        def do_POST(self):
            self._req_start = time.monotonic()
            path = urlparse(self.path).path

            if path == "/api/v1/polymarket/sync-fixture":
                content_length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(content_length)
                try:
                    body = json.loads(raw_body.decode("utf-8"))
                except Exception:
                    self._json_response(400, {"error": "invalid_json"})
                    return
                polymarket_url = str(body.get("polymarket_url") or "").strip()
                cms_fixture_id = str(body.get("cms_fixture_id") or "").strip()
                cms_url = str(body.get("cms_url") or "").strip()
                dry_run = bool(body.get("dry_run", False))

                if not cms_fixture_id and cms_url:
                    from pred_polymarket_sync.token_mapper import extract_cms_fixture_id_from_url
                    cms_fixture_id = extract_cms_fixture_id_from_url(cms_url)

                if not polymarket_url:
                    self._json_response(400, {"error": "missing_param", "detail": "polymarket_url is required"})
                    return
                if not cms_fixture_id:
                    self._json_response(400, {"error": "missing_param", "detail": "cms_fixture_id (or cms_url) is required"})
                    return
                token_mapper.reload_mappings(_fresh_mapping_store())
                try:
                    result = token_mapper.map_and_sync_fixture(
                        polymarket_url=polymarket_url,
                        cms_fixture_id=cms_fixture_id,
                        dry_run=dry_run,
                    )
                except Exception as exc:
                    self._json_response(500, {"error": "internal_error", "detail": str(exc)})
                    return
                self._json_response(200, result)
                return

            if path == "/api/v1/uat/publish-active-market-tokens":
                content_length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(content_length)
                try:
                    body = json.loads(raw_body.decode("utf-8"))
                except Exception:
                    self._json_response(400, {"error": "invalid_json"})
                    return

                yes_token_id = str(body.get("yes_token_id") or "").strip()
                no_token_id = str(body.get("no_token_id") or "").strip()
                dry_run = bool(body.get("dry_run", False))
                monitor_for_delete = bool(body.get("monitor_for_delete", True))

                if not yes_token_id:
                    self._json_response(400, {"error": "missing_param", "detail": "yes_token_id is required"})
                    return
                if not no_token_id:
                    self._json_response(400, {"error": "missing_param", "detail": "no_token_id is required"})
                    return

                if uat_market_publisher is None:
                    self._json_response(503, {"error": "uat_not_configured", "detail": "Server is running against a mainnet host; UAT publish is unavailable."})
                    return
                try:
                    result = uat_market_publisher.publish_to_active_markets(
                        yes_token_id=yes_token_id,
                        no_token_id=no_token_id,
                        dry_run=dry_run,
                        monitor_for_delete=monitor_for_delete,
                    )
                except Exception as exc:
                    self._json_response(500, {"error": "internal_error", "detail": str(exc)})
                    return
                self._json_response(200, result)
                return

            if path == "/api/v1/uat/publish-polymarket-url-to-rest-active-markets":
                content_length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(content_length)
                try:
                    body = json.loads(raw_body.decode("utf-8"))
                except Exception:
                    self._json_response(400, {"error": "invalid_json"})
                    return

                polymarket_url = str(body.get("polymarket_url") or "").strip()
                dry_run = bool(body.get("dry_run", False))
                monitor_for_delete = bool(body.get("monitor_for_delete", True))
                source_market_id = str(body.get("source_market_id") or "").strip()
                source_family = str(body.get("source_family") or "").strip()
                source_line = str(body.get("source_line") or "").strip()

                if not polymarket_url:
                    self._json_response(400, {"error": "missing_param", "detail": "polymarket_url is required"})
                    return
                if uat_market_publisher is None:
                    self._json_response(503, {"error": "uat_not_configured", "detail": "Server is running against a mainnet host; UAT publish is unavailable."})
                    return
                try:
                    result = uat_market_publisher.publish_remaining_active_markets_from_polymarket_url(
                        polymarket_url=polymarket_url,
                        dry_run=dry_run,
                        monitor_for_delete=monitor_for_delete,
                        source_market_id=source_market_id,
                        source_family=source_family,
                        source_line=source_line,
                    )
                except Exception as exc:
                    self._json_response(500, {"error": "internal_error", "detail": str(exc)})
                    return
                self._json_response(200, result)
                return

            if path not in {"/match", "/api/match"}:
                self._json_response(404, {"error": "not_found"})
                return
            content_length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(content_length)
            payload = json.loads(raw_body.decode("utf-8"))
            try:
                if source is None:
                    raise RuntimeError(
                        "Matching source is not configured. Start the server with CSV paths or DB table settings."
                    )
                response = run_match_payload(
                    payload=payload,
                    source=source,
                    matcher=matcher,
                    polymarket=polymarket,
                    sink=sink,
                    state=state,
                )
                self._json_response(200, response)
            except Exception as exc:
                self._json_response(400, {"error": str(exc)})

        def log_message(self, fmt, *args):
            return  # suppress default BaseHTTPRequestHandler noise

        # Paths that poll frequently — only log errors, skip 200s
        _SILENT_PATHS = {"/api/state", "/health", "/api/health"}

        def _log(self, status_code: int, payload: dict, duration_ms: float) -> None:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            method = getattr(self, "command", "?")
            path = urlparse(getattr(self, "path", "")).path

            # Suppress noisy polling endpoints unless they error
            if path in self._SILENT_PATHS and status_code < 400:
                return

            # Status colour: green=2xx, yellow=4xx, red=5xx
            if status_code < 300:
                status_str = f"\033[32m{status_code}\033[0m"
            elif status_code < 500:
                status_str = f"\033[33m{status_code}\033[0m"
            else:
                status_str = f"\033[31m{status_code}\033[0m"

            print(f"\n[{ts}] {method} {path}  {status_str}  {duration_ms:.0f}ms", flush=True)

            # For sync-fixture: print a compact summary instead of the full blob
            if path == "/api/v1/polymarket/sync-fixture":
                fixture = payload.get("fixture_name") or payload.get("cms_fixture_id", "")
                top_status = payload.get("status", "")
                slug = payload.get("polymarket_slug", "")
                error = payload.get("error", "")
                posted = payload.get("sync_posted", 0)
                already_exists = payload.get("sync_already_exists", 0)
                failed = payload.get("sync_failed", 0)

                if error:
                    print(f"  \033[31m✗ {error}\033[0m  {payload.get('detail','')}", flush=True)
                else:
                    status_icon = "\033[32m✓\033[0m" if top_status == "matched" else "\033[33m~\033[0m"
                    print(f"  {status_icon} {fixture}  [{slug}]  status={top_status}", flush=True)

                    for fam, result in payload.get("families", {}).items():
                        matched_n = len(result.get("matched", []))
                        unmatched_n = len(result.get("unmatched_cms_markets", []))
                        unsupported_n = len(result.get("unsupported_polymarket_markets", []))
                        fam_status = result.get("status", "")
                        icon = "\033[32m✓\033[0m" if fam_status == "matched" else (
                            "\033[33m~\033[0m" if fam_status == "partially_matched" else "\033[90m-\033[0m"
                        )
                        print(
                            f"  {icon} {fam:<12} matched={matched_n}"
                            + (f"  unmatched={unmatched_n}" if unmatched_n else "")
                            + (f"  polymarket_only={unsupported_n}" if unsupported_n else ""),
                            flush=True,
                        )

                    mm_line = f"  \033[36mmarket-making →\033[0m posted={posted}"
                    if already_exists:
                        mm_line += f"  already_exists={already_exists}"
                    if failed:
                        mm_line += f"  failed={failed}"
                    print(mm_line, flush=True)

                    # Log each sync result
                    for sr in payload.get("sync_results", []):
                        sr_status = sr.get("status", "")
                        mid = (sr.get("cms_market_id") or "")[-8:]
                        name = sr.get("market_name", "")[:60]
                        if sr_status == "posted":
                            print(f"    \033[32m↑ posted\033[0m  ...{mid}  {name}", flush=True)
                        elif sr_status == "already_exists":
                            print(f"    \033[90m= exists\033[0m  ...{mid}  {name}", flush=True)
                        elif sr_status == "failed":
                            print(f"    \033[31m✗ failed\033[0m  ...{mid}  {sr.get('error','')}", flush=True)
                        else:
                            print(f"    \033[90m· {sr_status}\033[0m  ...{mid}  {sr.get('reason','')}", flush=True)
            elif path == "/api/v1/uat/publish-active-market-tokens":
                print(
                    "  \033[36mactive-market publish →\033[0m"
                    f" active={payload.get('active_market_count', 0)}"
                    f"  published={payload.get('published_count', 0)}"
                    f"  failed={payload.get('failed_count', 0)}",
                    flush=True,
                )
                job = payload.get("auto_delete_job") or {}
                if job:
                    print(
                        f"  \033[35mauto-delete watcher →\033[0m {job.get('status','')}  {job.get('job_id','')}",
                        flush=True,
                    )
            elif path == "/api/v1/uat/publish-polymarket-url-to-rest-active-markets":
                print(
                    "  \033[36mrest-active publish →\033[0m"
                    f" source_active={payload.get('source_active_market_count', 0)}"
                    f"  existing={payload.get('existing_vault_market_count', 0)}"
                    f"  published={payload.get('published_count', 0)}"
                    f"  already_exists={payload.get('already_exists_count', 0)}"
                    f"  failed={payload.get('failed_count', 0)}",
                    flush=True,
                )
                source_market = payload.get("source_market") or {}
                if source_market:
                    print(
                        f"  \033[35msource market →\033[0m {source_market.get('family','')}  {source_market.get('line','')}  {source_market.get('market_id','')}",
                        flush=True,
                    )
            else:
                # All other endpoints: print compact response
                out = json.dumps(payload, ensure_ascii=False)
                if len(out) > 400:
                    out = out[:400] + " ..."
                print(f"  {out}", flush=True)

        def _json_response(self, status_code, payload):
            _t0 = getattr(self, "_req_start", time.monotonic())
            duration_ms = (time.monotonic() - _t0) * 1000
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            self._log(status_code, payload, duration_ms)

        def _file_response(self, path: Path, content_type: str):
            if not path.exists():
                self._json_response(404, {"error": "not_found"})
                return
            body = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(
        json.dumps(
            {
                "host": args.host,
                "port": args.port,
                "health": f"http://{args.host}:{args.port}/health",
                "dashboard": f"http://{args.host}:{args.port}/",
            },
            ensure_ascii=True,
        )
    )
    server.serve_forever()
    return 0


def _maybe_build_source_from_args(args, database_url: str):
    if _has_csv_source_args(args):
        return build_source_from_args(args, database_url=database_url)
    if _has_database_source_args(args) and database_url:
        return build_source_from_args(args, database_url=database_url)
    return None


def _has_csv_source_args(args) -> bool:
    required = (
        "markets_csv",
        "parent_markets_csv",
        "fixtures_csv",
        "fixture_mappings_csv",
        "team_mappings_csv",
        "league_mappings_csv",
    )
    return all(bool(getattr(args, field, "")) for field in required)


def _has_database_source_args(args) -> bool:
    required = (
        "markets_table",
        "parent_markets_table",
        "fixtures_table",
        "fixture_mappings_table",
        "team_mappings_table",
        "league_mappings_table",
    )
    return all(bool(getattr(args, field, "")) for field in required)


def _query_value(query: dict, key: str, default: str = "") -> str:
    values = query.get(key, [])
    if not values:
        return default
    return values[0]


def _query_bool(query: dict, key: str, default: bool = False) -> bool:
    value = _query_value(query, key, "")
    if not value:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _query_int(query: dict, key: str, default: int) -> int:
    value = _query_value(query, key, "")
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _load_export_rows(exports_dir: Path, football_only: bool):
    base_name = "polymarket_football" if football_only else "polymarket"
    teams_path = exports_dir / f"{base_name}_teams.csv"
    leagues_path = exports_dir / f"{base_name}_leagues.csv"
    if not teams_path.exists() or not leagues_path.exists():
        return None, None
    return _read_csv_rows(teams_path), _read_csv_rows(leagues_path)


def _read_csv_rows(path: Path):
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _search_exported_team_rows(
    team_rows,
    league_rows,
    query: str,
    league_id: str,
    league_code: str,
    limit: int,
):
    query_text = normalize_text(query)
    league_code_text = normalize_text(league_code)
    league_id_from_code = {
        normalize_text(row.get("alternate_name", "")): str(row.get("league_id", ""))
        for row in league_rows
    }
    if league_code_text and not league_id:
        league_id = league_id_from_code.get(league_code_text, "")
    ranked = []
    for row in team_rows:
        if league_id and str(row.get("league_id", "")) != str(league_id):
            continue
        name = normalize_text(row.get("name", ""))
        alternate_name = normalize_text(row.get("alternate_name", ""))
        score = 1 if not query_text else 0
        if query_text:
            if name == query_text or alternate_name == query_text:
                score = 100
            elif name.startswith(query_text) or alternate_name.startswith(query_text):
                score = 80
            elif query_text in name or query_text in alternate_name:
                score = 60
        if score <= 0:
            continue
        ranked.append((score, row))
    ranked.sort(key=lambda item: (-item[0], normalize_text(item[1].get("name", "")), item[1].get("team_id", "")))
    return [row for _, row in ranked[: max(limit, 1)]]


def _search_exported_league_rows_with_sport(league_rows, query: str, sport: str, limit: int):
    query_text = normalize_text(query)
    sport_text = normalize_text(sport)
    ranked = []
    for row in league_rows:
        if sport_text and normalize_text(row.get("sport", "")) != sport_text:
            continue
        name = normalize_text(row.get("name", ""))
        alternate_name = normalize_text(row.get("alternate_name", ""))
        association = normalize_text(row.get("association", ""))
        score = 1 if not query_text else 0
        if query_text:
            if name == query_text or alternate_name == query_text:
                score = 100
            elif name.startswith(query_text) or alternate_name.startswith(query_text):
                score = 80
            elif query_text in name or query_text in alternate_name:
                score = 60
            elif query_text in association:
                score = 40
        if score <= 0:
            continue
        ranked.append((score, row))
    ranked.sort(key=lambda item: (-item[0], normalize_text(item[1].get("name", "")), item[1].get("league_id", "")))
    return [row for _, row in ranked[: max(limit, 1)]]


def _search_exported_league_rows(league_rows, query: str, sport: str = "", limit: int = 25):
    return _search_exported_league_rows_with_sport(
        league_rows=league_rows,
        query=query,
        sport=sport,
        limit=limit,
    )


def _build_sport_rows(leagues: list[PolymarketLeague]):
    grouped: dict[str, dict] = {}
    for league in leagues:
        sport_slug = (_sport_slug(league) or "").strip().lower()
        if not sport_slug:
            continue
        group = grouped.setdefault(
            sport_slug,
            {
                "sport_id": sport_slug,
                "polymarket_tag_id": "",
                "name": _humanize_sport_name(sport_slug),
                "slug": sport_slug,
                "league_count": 0,
                "id_source": "family_slug",
                "_tag_counts": {},
            },
        )
        group["league_count"] += 1
        for tag_id in _non_generic_tag_ids(league.tags):
            group["_tag_counts"][tag_id] = group["_tag_counts"].get(tag_id, 0) + 1
    items = []
    for sport_slug, group in grouped.items():
        tag_id = _pick_family_tag_id(group["_tag_counts"])
        if tag_id:
            group["polymarket_tag_id"] = tag_id
            group["id_source"] = "shared_tag"
        del group["_tag_counts"]
        items.append(group)
    items.sort(key=lambda item: item["name"])
    return items


def _sport_slug(league: PolymarketLeague) -> str:
    from pred_polymarket_sync.exporters import derive_sport_family

    return derive_sport_family(league.league_code, league.resolution)


def _humanize_sport_name(sport_slug: str) -> str:
    words = (sport_slug or "").replace("_", " ").split()
    return " ".join(word.capitalize() for word in words)


def _non_generic_tag_ids(tags: str) -> list[str]:
    generic = {"1", "100639"}
    values = []
    for tag in str(tags or "").split(","):
        cleaned = tag.strip()
        if cleaned and cleaned not in generic:
            values.append(cleaned)
    return values


def _pick_family_tag_id(tag_counts: dict[str, int]) -> str:
    if not tag_counts:
        return ""
    ordered = sorted(tag_counts.items(), key=lambda item: (-item[1], int(item[0])))
    tag_id, count = ordered[0]
    if count >= 2:
        return tag_id
    return ""


def _parse_csv_list(value: str):
    if not value:
        return None
    items = [part.strip() for part in value.split(",") if part.strip()]
    return items or None


if __name__ == "__main__":
    raise SystemExit(main())
