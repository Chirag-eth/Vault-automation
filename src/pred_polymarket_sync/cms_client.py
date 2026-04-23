from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from pred_polymarket_sync.http import HttpClient

_NUMERIC_RE = re.compile(r"(\d+(?:\.\d+)?)")


@dataclass
class CmsFixtureRecord:
    fixture_id: str
    name: str
    alternate_name: str      # "ROM vs ATA"
    canonical_name: str      # "roma-vs-atalanta-serie-a-enilive-2026-04-18"
    home_team_id: str
    away_team_id: str
    league_id: str           # often empty string in current CMS data
    kickoff: str             # ISO datetime string from game_start_time


@dataclass
class CmsMarketRecord:
    market_id: str           # child market_id (0x hex)
    market_name: str         # child market name e.g. "Roma"
    alternate_name: str      # short code e.g. "ROM"
    family: str              # normalised: moneyline | totals | spreads | btts
    line: str                # numeric string from parent market_line e.g. "2.5"
    team_id: str             # child team_id (empty for draw)
    outcome_role: str        # "home" | "away" | "draw" | ""
    status: str
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)


class CmsClient:
    """
    Read-only HTTP client for the CMS internal market-discovery endpoint.

    Actual endpoint:
      GET {base}/api/v1/market-discovery/internal/fixtures/market-data-by-fixture-id
          ?fixture_id={uuid}

    Response shape:
      {
        "data": {
          "fixture_data": {
            "fixture_id": "...",
            "name": "Roma vs Atalanta",
            "alternate_name": "ROM vs ATA",
            "game_start_time": "2026-04-14T15:25:00Z",
            "home_team_id": "7eda7e2a-...",
            "away_team_id": "012d571b-...",
            "canonical_name": "roma-vs-atalanta-serie-a-enilive-2026-04-18"
          },
          "parent_markets_list": [
            {
              "parent_market_data": {
                "parent_market_id": "0x...",
                "parent_market_family": "moneyline",  // already normalised
                "market_line": "0",
                "title": "...",
                "status": "active"
              },
              "markets": [
                { "market_id": "0x...", "team_id": "...", "name": "Roma",
                  "alternate_name": "ROM", "status": "active" },
                ...
              ],
              "sports_info": {
                "home_team": { "market_id": "0x...", "team_id": "...", "name": "Roma",
                               "market_code": "ROM" },
                "away_team": { "market_id": "0x...", ... },
                "draw":      { "market_id": "0x...", ... }  // moneyline only
              }
            },
            ...
          ]
        }
      }
    """

    def __init__(
        self,
        base_url: str,
        auth_header: str = "",
        auth_token: str = "",
        timeout: int = 20,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._auth_header = auth_header
        self._auth_token = auth_token
        self._http = HttpClient(timeout_seconds=timeout)

    def get_fixture_markets(
        self, cms_fixture_id: str
    ) -> Tuple[CmsFixtureRecord, List[CmsMarketRecord]]:
        """
        Call GET .../market-data-by-fixture-id?fixture_id={uuid}.
        Returns (CmsFixtureRecord, list[CmsMarketRecord]).
        Raises RuntimeError on HTTP error or unexpected response shape.
        """
        url = (
            f"{self._base_url}/api/v1/market-discovery/internal/fixtures"
            f"/market-data-by-fixture-id"
        )
        headers: Dict[str, str] = {}
        if self._auth_header and self._auth_token:
            headers[self._auth_header] = self._auth_token

        raw = self._http.get_json(
            url, params={"fixture_id": cms_fixture_id}, headers=headers
        )
        if not isinstance(raw, dict):
            raise RuntimeError(
                f"CMS returned unexpected response shape for fixture {cms_fixture_id!r}: "
                f"expected object, got {type(raw).__name__}"
            )

        data = raw.get("data")
        if not isinstance(data, dict):
            raise RuntimeError(
                f"CMS response missing 'data' object for fixture {cms_fixture_id!r}. "
                f"Top-level keys: {list(raw.keys())}"
            )

        fixture = self._parse_fixture(data, cms_fixture_id)
        markets = self._parse_markets(data, fixture)
        return fixture, markets

    # ------------------------------------------------------------------
    # Internal parsers
    # ------------------------------------------------------------------

    def _parse_fixture(self, data: Dict[str, Any], cms_fixture_id: str) -> CmsFixtureRecord:
        f: Dict[str, Any] = data.get("fixture_data") or {}
        league_data: Dict[str, Any] = f.get("league_data") or {}
        return CmsFixtureRecord(
            fixture_id=str(f.get("fixture_id") or cms_fixture_id),
            name=str(f.get("name") or ""),
            alternate_name=str(f.get("alternate_name") or ""),
            canonical_name=str(f.get("canonical_name") or ""),
            home_team_id=str(f.get("home_team_id") or ""),
            away_team_id=str(f.get("away_team_id") or ""),
            league_id=str(league_data.get("league_id") or ""),
            kickoff=str(
                f.get("game_start_time")
                or f.get("kickoff")
                or f.get("start_time")
                or ""
            ),
        )

    def _parse_markets(
        self, data: Dict[str, Any], fixture: CmsFixtureRecord
    ) -> List[CmsMarketRecord]:
        parent_list = data.get("parent_markets_list") or []
        if not isinstance(parent_list, list):
            return []

        result: List[CmsMarketRecord] = []
        for parent_item in parent_list:
            if not isinstance(parent_item, dict):
                continue
            records = self._parse_parent_market(parent_item, fixture)
            result.extend(records)
        return result

    def _parse_parent_market(
        self, parent_item: Dict[str, Any], fixture: CmsFixtureRecord
    ) -> List[CmsMarketRecord]:
        """
        Flatten one parent_markets_list entry into individual CmsMarketRecord objects.
        Each child in markets[] becomes one CmsMarketRecord.
        Role assignment for moneyline uses sports_info.home_team / away_team / draw.
        """
        pmd: Dict[str, Any] = parent_item.get("parent_market_data") or {}
        family_raw = str(pmd.get("parent_market_family") or "").lower().strip()
        family = _normalise_family(family_raw)
        if not family:
            return []

        line = str(pmd.get("market_line") or "").strip()
        parent_status = str(pmd.get("status") or "active")

        children: List[Dict[str, Any]] = parent_item.get("markets") or []
        sports_info: Dict[str, Any] = parent_item.get("sports_info") or {}

        # Build market_id → role map from sports_info (definitive for moneyline)
        role_by_mid: Dict[str, str] = {}
        if family == "moneyline":
            for si_key, role in (("home_team", "home"), ("away_team", "away"), ("draw", "draw")):
                entry = sports_info.get(si_key)
                if isinstance(entry, dict):
                    mid = str(entry.get("market_id") or "").strip()
                    if mid:
                        role_by_mid[mid] = role

        parent_market_id = str(pmd.get("parent_market_id") or "").strip()

        records: List[CmsMarketRecord] = []
        for idx, child in enumerate(children):
            if not isinstance(child, dict):
                continue

            child_name = str(child.get("name") or "").strip()
            alt_name = str(child.get("alternate_name") or "").strip()

            market_id = str(child.get("market_id") or "").strip()
            # Fallback: use parent_market_id + index when child has no market_id
            if not market_id:
                if not parent_market_id:
                    continue
                market_id = f"{parent_market_id}:{idx}"

            team_id = str(child.get("team_id") or "").strip()

            # Re-classify family from market name when CMS mislabels non-moneyline markets
            effective_family, effective_line = _infer_family_and_line(
                labeled_family=family,
                name=child_name,
                alt_name=alt_name,
                parent_line=line,
                team_id=team_id,
            )

            # Role: from sports_info for moneyline; from team_id comparison otherwise
            outcome_role = role_by_mid.get(market_id, "")
            if not outcome_role and effective_family != "moneyline":
                if team_id and team_id == fixture.home_team_id:
                    outcome_role = "home"
                elif team_id and team_id == fixture.away_team_id:
                    outcome_role = "away"

            records.append(CmsMarketRecord(
                market_id=market_id,
                market_name=child_name,
                alternate_name=alt_name,
                family=effective_family,
                line=effective_line,
                team_id=team_id,
                outcome_role=outcome_role,
                status=str(child.get("status") or parent_status),
                raw=child,
            ))

        return records


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _infer_family_and_line(
    labeled_family: str,
    name: str,
    alt_name: str,
    parent_line: str,
    team_id: str,
) -> tuple[str, str]:
    """
    Re-classify the market family and derive the correct line from the market
    name / alternate_name when CMS mislabels non-moneyline markets as 'spreads'.

    Priority (highest → lowest):
      1. BTTS keyword → btts
      2. 'Totals Over/Under' with no team → totals, line from alt_name
      3. Team-specific 'Over/Under Goals' (has team_id) → totals, line from alt_name
      4. Labeled family stays as-is
    """
    name_up = name.upper()
    alt_up = alt_name.upper()

    # BTTS detection
    if "BOTH TEAMS TO SCORE" in name_up or name_up in {"BTTS", "BTS"}:
        return "btts", ""

    # Team-specific non-moneyline markets (has team_id) = spreads.
    # Do NOT reclassify these even if the name contains "Over/Goals".
    # But moneyline markets also carry team_id for home/away — don't touch those.
    if team_id and labeled_family != "moneyline":
        return "spreads", parent_line

    # Generic totals: 'Totals Over X Goals' or 'Over/Under X Goals' with no team
    is_over_under = (
        "OVER" in name_up or "UNDER" in name_up or "O/U" in name_up
    ) and ("GOAL" in name_up or "TOTAL" in name_up or re.search(r"\d+\.\d+", name_up))

    if is_over_under:
        # Extract numeric line from alt_name first, then name
        line = _extract_line(alt_name) or _extract_line(name) or parent_line
        return "totals", line

    return labeled_family, parent_line


def _extract_line(text: str) -> str:
    """Extract first decimal/integer number from text as a line string."""
    m = _NUMERIC_RE.search(text)
    return m.group(1) if m else ""


def _normalise_family(raw: str) -> str:
    if raw in {"moneyline", "money_line", "ml"}:
        return "moneyline"
    if raw in {"totals", "total", "over_under", "ou", "over/under"}:
        return "totals"
    if raw in {"spreads", "spread", "handicap", "handicaps"}:
        return "spreads"
    if raw in {"btts", "both_teams_to_score", "both teams to score", "bts"}:
        return "btts"
    return ""
