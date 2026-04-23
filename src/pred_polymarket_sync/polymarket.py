from __future__ import annotations

from typing import Any, Dict, Iterable, List

from pred_polymarket_sync.http import HttpClient
from pred_polymarket_sync.models import (
    PolymarketLeague,
    PolymarketMarket,
    PolymarketTeam,
)
from pred_polymarket_sync.utils import parse_datetime, parse_jsonish_list


class PolymarketClient:
    def __init__(self, gamma_base_url: str, clob_base_url: str, timeout_seconds: int = 20):
        self.gamma_base_url = gamma_base_url.rstrip("/")
        self.clob_base_url = clob_base_url.rstrip("/")
        self.http = HttpClient(timeout_seconds=timeout_seconds)

    def list_markets_for_game(self, game_id: str) -> List[PolymarketMarket]:
        payload = self.http.get_json(
            f"{self.gamma_base_url}/markets",
            params={"game_id": game_id, "limit": 500, "closed": "false"},
        )
        if not isinstance(payload, list):
            return []
        return [self._to_market(item) for item in payload]

    def get_market(self, market_id: str) -> PolymarketMarket:
        payload = self.http.get_json(f"{self.gamma_base_url}/markets/{market_id}")
        return self._to_market(payload)

    def get_event_by_slug(self, slug: str) -> Dict[str, Any]:
        return self.http.get_json(f"{self.gamma_base_url}/events/slug/{slug}")

    def list_events(
        self,
        tag_slug: str = "",
        limit: int = 500,
        offset: int = 0,
        closed: bool | None = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if tag_slug:
            params["tag_slug"] = tag_slug
        if closed is not None:
            params["closed"] = str(closed).lower()
        payload = self.http.get_json(f"{self.gamma_base_url}/events", params=params)
        return payload if isinstance(payload, list) else []

    def list_teams(self, limit: int = 500) -> List[PolymarketTeam]:
        offset = 0
        teams: List[PolymarketTeam] = []
        while True:
            payload = self.http.get_json(
                f"{self.gamma_base_url}/teams",
                params={"limit": limit, "offset": offset},
            )
            if not isinstance(payload, list) or not payload:
                break
            teams.extend(self._to_team(item) for item in payload if isinstance(item, dict))
            if len(payload) < limit:
                break
            offset += limit
        return teams

    def list_leagues(self) -> List[PolymarketLeague]:
        payload = self.http.get_json(f"{self.gamma_base_url}/sports")
        if not isinstance(payload, list):
            return []
        return [self._to_league(item) for item in payload if isinstance(item, dict)]

    def get_order_books(self, token_ids: Iterable[str]) -> List[Dict[str, Any]]:
        request_body = [{"token_id": token_id} for token_id in token_ids if token_id]
        if not request_body:
            return []
        payload = self.http.post_json(f"{self.clob_base_url}/books", request_body)
        return payload if isinstance(payload, list) else []

    def _to_team(self, raw: Dict[str, Any]) -> PolymarketTeam:
        return PolymarketTeam(
            id=str(raw.get("id") or ""),
            name=str(raw.get("name") or ""),
            league_code=str(raw.get("league") or ""),
            alias=str(raw.get("alias") or ""),
            record=str(raw.get("record") or ""),
            logo=str(raw.get("logo") or ""),
            abbreviation=str(raw.get("abbreviation") or ""),
            provider_id=str(raw.get("providerId") or raw.get("provider_id") or ""),
            color=str(raw.get("color") or ""),
            created_at=str(raw.get("createdAt") or ""),
            updated_at=str(raw.get("updatedAt") or ""),
        )

    def _to_league(self, raw: Dict[str, Any]) -> PolymarketLeague:
        return PolymarketLeague(
            id=str(raw.get("id") or ""),
            league_code=str(raw.get("sport") or raw.get("league") or ""),
            series_id=str(raw.get("series") or ""),
            tags=str(raw.get("tags") or ""),
            ordering=str(raw.get("ordering") or ""),
            resolution=str(raw.get("resolution") or ""),
            image=str(raw.get("image") or ""),
            created_at=str(raw.get("createdAt") or ""),
        )

    def _to_market(self, raw: Dict[str, Any]) -> PolymarketMarket:
        game_id = str(
            raw.get("gameId")
            or raw.get("game_id")
            or raw.get("gameid")
            or ""
        )
        return PolymarketMarket(
            market_id=str(raw.get("conditionId") or raw.get("market") or raw.get("id") or ""),
            question=str(raw.get("question") or raw.get("title") or ""),
            slug=str(raw.get("slug") or ""),
            game_id=game_id,
            team_a_id=str(raw.get("teamAID") or raw.get("teamAId") or raw.get("team_a_id") or ""),
            team_b_id=str(raw.get("teamBID") or raw.get("teamBId") or raw.get("team_b_id") or ""),
            game_start_time=parse_datetime(
                raw.get("gameStartTime")
                or raw.get("startDate")
                or raw.get("startDateIso")
                or raw.get("endDate")
            ),
            outcomes=parse_jsonish_list(raw.get("outcomes")),
            short_outcomes=parse_jsonish_list(raw.get("shortOutcomes")),
            clob_token_ids=parse_jsonish_list(raw.get("clobTokenIds")),
            sports_market_type=str(
                raw.get("sportsMarketType")
                or raw.get("sports_market_type")
                or raw.get("marketType")
                or ""
            ),
            active=bool(raw.get("active", not raw.get("closed", False))),
            closed=bool(raw.get("closed", False)),
            raw=raw,
        )
