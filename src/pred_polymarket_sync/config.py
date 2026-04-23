from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    gamma_base_url: str = os.getenv(
        "POLYMARKET_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"
    )
    clob_base_url: str = os.getenv(
        "POLYMARKET_CLOB_BASE_URL", "https://clob.polymarket.com"
    )
    market_ws_url: str = os.getenv(
        "POLYMARKET_MARKET_WS_URL",
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    )
    http_timeout_seconds: int = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
    start_time_tolerance_minutes: int = int(
        os.getenv("START_TIME_TOLERANCE_MINUTES", "30")
    )
    score_threshold: int = int(os.getenv("MATCH_SCORE_THRESHOLD", "85"))
    score_gap_threshold: int = int(os.getenv("MATCH_SCORE_GAP_THRESHOLD", "15"))
    realtime_poll_seconds: int = int(os.getenv("REALTIME_POLL_SECONDS", "15"))
    reconnect_delay_seconds: int = int(
        os.getenv("ORDERBOOK_RECONNECT_DELAY_SECONDS", "5")
    )
    state_dir: Path = Path(os.getenv("STATE_DIR", "./state")).resolve()
    database_url: str = os.getenv("DATABASE_URL", "")
    http_base_url: str = os.getenv("SYNC_HTTP_BASE_URL", "")
    http_mapping_path: str = os.getenv("SYNC_HTTP_MAPPING_PATH", "/mappings")
    http_orderbook_path: str = os.getenv("SYNC_HTTP_ORDERBOOK_PATH", "/orderbooks")
    http_review_path: str = os.getenv("SYNC_HTTP_REVIEW_PATH", "/reviews")
    http_auth_header: str = os.getenv("SYNC_HTTP_AUTH_HEADER", "")
    http_auth_token: str = os.getenv("SYNC_HTTP_AUTH_TOKEN", "")
    # CMS internal endpoint for fixture-token-map
    cms_base_url: str = os.getenv("CMS_BASE_URL", "")
    cms_auth_header: str = os.getenv("CMS_AUTH_HEADER", "")
    cms_auth_token: str = os.getenv("CMS_AUTH_TOKEN", "")
    # Polymarket CSV mapping files loaded at startup
    league_mappings_csv: str = os.getenv(
        "POLYMARKET_LEAGUE_MAPPINGS_CSV", "./exports/mappings/polymarket_league_mappings.csv"
    )
    team_mappings_csv: str = os.getenv(
        "POLYMARKET_TEAM_MAPPINGS_CSV", "./exports/mappings/polymarket_team_mappings.csv"
    )
    # Local JSON file for manually confirmed CMS↔Polymarket mappings
    mappings_json: str = os.getenv("POLYMARKET_MAPPINGS_JSON", "./exports/mappings/mappings.json")
    # Market-making service: matched markets are auto-POSTed here after sync
    market_making_host: str = os.getenv("MARKET_MAKING_HOST", "")
    market_making_auth_header: str = os.getenv("MARKET_MAKING_AUTH_HEADER", "")
    market_making_auth_token: str = os.getenv("MARKET_MAKING_AUTH_TOKEN", "")
    market_making_markets_path: str = os.getenv(
        "MARKET_MAKING_MARKETS_PATH", "/api/v1/config/markets"
    )
    market_making_delete_path_template: str = os.getenv(
        "MARKET_MAKING_DELETE_PATH_TEMPLATE", "/api/v1/config/markets/{market_id}"
    )
    market_making_active_markets_path: str = os.getenv(
        "MARKET_MAKING_ACTIVE_MARKETS_PATH", "/api/v1/config/active-markets"
    )
    token_autodelete_poll_seconds: int = int(
        os.getenv("TOKEN_AUTODELETE_POLL_SECONDS", "15")
    )
    uat_market_actor_header: str = os.getenv("UAT_MARKET_ACTOR_HEADER", "X-Actor")
    uat_market_actor_value: str = os.getenv("UAT_MARKET_ACTOR_VALUE", "piyush")
    uat_internal_active_markets_path: str = os.getenv(
        "UAT_INTERNAL_ACTIVE_MARKETS_PATH", "/api/v1/market-discovery/internal/active-markets"
    )
