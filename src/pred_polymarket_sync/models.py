from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class PredFixture:
    fixture_id: str
    name: str
    league_id: str
    home_team_id: str
    away_team_id: str


@dataclass
class PredParentMarket:
    parent_market_id: str
    league_id: str
    type_reference_id: str
    title: str
    description: str
    status: str
    markets_open_time: Optional[datetime]
    markets_close_time: Optional[datetime]
    payout_time: Optional[datetime]


@dataclass
class PredMarket:
    market_id: str
    parent_market_id: str
    team_id: str
    name: str
    market_canonical_name: str
    market_code: str
    rules: str
    status: str
    yes_position_id: str
    no_position_id: str


@dataclass
class SportsDataFixtureMapping:
    cms_fixture_id: str
    cms_league_id: str
    sportsdata_game_id: str
    match_date: Optional[datetime]
    status: str


@dataclass
class SportsDataTeamMapping:
    cms_team_id: str
    cms_league_id: str
    sportsdata_team_id: str
    status: str


@dataclass
class SportsDataLeagueMapping:
    cms_league_id: str
    sportsdata_competition_id: str
    sportsdata_season: str
    status: str


@dataclass
class PredMarketBundle:
    market: PredMarket
    parent_market: PredParentMarket
    fixture: PredFixture
    sportsdata_fixture: Optional[SportsDataFixtureMapping]
    sportsdata_home_team_id: str
    sportsdata_away_team_id: str
    sportsdata_competition_id: str
    outcome_key: str
    outcome_label: str
    tracking_status: str


@dataclass
class PolymarketMarket:
    market_id: str
    question: str
    slug: str
    game_id: str
    team_a_id: str
    team_b_id: str
    game_start_time: Optional[datetime]
    outcomes: List[str]
    short_outcomes: List[str]
    clob_token_ids: List[str]
    sports_market_type: str
    active: bool
    closed: bool
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PolymarketTeam:
    id: str
    name: str
    league_code: str
    alias: str
    record: str
    logo: str
    abbreviation: str
    provider_id: str
    color: str
    created_at: str
    updated_at: str


@dataclass
class PolymarketLeague:
    id: str
    league_code: str
    series_id: str
    tags: str
    ordering: str
    resolution: str
    image: str
    created_at: str


@dataclass
class MappingRecord:
    pred_market_id: str
    pred_parent_market_id: str
    pred_fixture_id: str
    pred_league_id: str
    pred_home_team_id: str
    pred_away_team_id: str
    polymarket_market_id: str
    yes_token_id: str
    no_token_id: str
    home_team_id: str
    home_team_name: str
    away_team_id: str
    away_team_name: str
    league_id: str
    league_name: str
    game_id: str
    outcome_label: str
    match_score: int
    match_reason: str
    tracking_status: str = ""
    source: str = "polymarket"


@dataclass
class ReviewRecord:
    pred_market_id: str
    pred_parent_market_id: str
    pred_fixture_id: str
    reason: str
    top_score: int
    candidate_market_ids: List[str]
    candidate_reasons: List[str]


@dataclass
class MatchResult:
    status: str
    mapping: Optional[MappingRecord]
    review: Optional[ReviewRecord]


@dataclass
class BookLevel:
    price: str
    size: str


@dataclass
class OrderBookState:
    asset_id: str
    market: str
    bids: Dict[str, str]
    asks: Dict[str, str]
    hash: str = ""
    timestamp: str = ""
    last_trade_price: str = ""
    tick_size: str = ""

    def best_bid(self) -> str:
        if not self.bids:
            return "0"
        return max(self.bids, key=lambda value: float(value))

    def best_ask(self) -> str:
        if not self.asks:
            return "0"
        return min(self.asks, key=lambda value: float(value))


@dataclass
class OrderBookEnvelope:
    polymarket_market_id: str
    yes_token_id: str
    no_token_id: str
    orderbook_snapshot: Dict[str, Any]
    best_bid_yes: str
    best_ask_yes: str
    best_bid_no: str
    best_ask_no: str
