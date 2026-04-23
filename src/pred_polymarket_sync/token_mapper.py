from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from pred_polymarket_sync.cms_client import CmsClient, CmsFixtureRecord, CmsMarketRecord
from pred_polymarket_sync.http import HttpClient
from pred_polymarket_sync.fixture_markets import (
    FAMILY_KEYS,
    _event_start_time,
    _event_team_names,
    _is_fixture_event,
    collect_game_lines_events,
    select_fixture_markets,
)
from pred_polymarket_sync.mapping_loader import MappingStore
from pred_polymarket_sync.polymarket import PolymarketClient
from pred_polymarket_sync.utils import normalize_text, parse_datetime

# V1 scope: leagues the token mapper is permitted to resolve
V1_LEAGUE_SLUGS = ("epl", "lal")

# CMS family name → Polymarket family key (FAMILY_KEYS)
CMS_TO_PM_FAMILY: Dict[str, str] = {
    "moneyline": "moneyline",
    "totals": "totals",
    "spreads": "spreads",
    "btts": "both_teams_to_score",
}

_SIGNED_NUMBER_RE = re.compile(r"([+-]?\d+(?:\.\d+)?)")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

class TokenMapper:
    """
    Read-only service that maps CMS fixture markets to Polymarket Yes/No token IDs.

    Steps (matching algorithm):
      1. Fetch CMS fixture + markets via CmsClient
      2. Resolve Polymarket league + team identities via MappingStore
      3. Resolve Polymarket fixture (by slug or by search)
      4. Collect Game Lines events (base + -more-markets only)
      5. Classify Polymarket markets into families
      6. Moneyline gate: home + away must match; if not, return unmatched
      7. Match totals (exact numeric line)
      8. Match spreads (team context + abs line value)
      9. Match btts (family presence)
    """

    def __init__(
        self,
        polymarket_client: PolymarketClient,
        cms_client: Optional[CmsClient],
        mapping_store: MappingStore,
        start_time_tolerance_minutes: int = 30,
        market_making_host: str = "",
        market_making_auth_header: str = "",
        market_making_auth_token: str = "",
        http_timeout_seconds: int = 20,
    ) -> None:
        self._poly = polymarket_client
        self._cms = cms_client
        self._mappings = mapping_store
        self._tolerance = start_time_tolerance_minutes
        self._market_making_host = market_making_host.rstrip("/")
        self._market_making_auth_header = market_making_auth_header
        self._market_making_auth_token = market_making_auth_token
        self._http = HttpClient(timeout_seconds=http_timeout_seconds)

    def reload_mappings(self, store: "MappingStore") -> None:
        """Replace the active mapping store (used for hot-reload without restart)."""
        self._mappings = store

    # ------------------------------------------------------------------
    # Endpoint 2 — fixture token map
    # ------------------------------------------------------------------

    def map_fixture(
        self,
        cms_fixture_id: str,
        polymarket_slug: str = "",
        include_closed: bool = False,
    ) -> Dict[str, Any]:
        """
        Full matching pipeline.  Returns a JSON-serialisable dict.
        All failures are encoded in the returned dict (status + error),
        never raised as exceptions.
        """
        if self._cms is None:
            return _error_result(
                cms_fixture_id=cms_fixture_id,
                reason="cms_not_configured",
                detail="CMS_BASE_URL is not set; cannot fetch fixture markets.",
            )

        # Step 1 — load CMS data
        try:
            cms_fixture, cms_markets = self._cms.get_fixture_markets(cms_fixture_id)
        except RuntimeError as exc:
            return _error_result(
                cms_fixture_id=cms_fixture_id,
                reason="cms_fetch_failed",
                detail=str(exc),
            )

        # Step 2 — resolve Polymarket identities
        # League mapping is only required for fixture text-search (no slug provided).
        # When polymarket_slug is given the fixture is already identified, so we skip
        # the league check and only require team mappings for market matching.
        league_map = self._mappings.get_league(cms_fixture.league_id)
        if not league_map and not polymarket_slug:
            return _error_result(
                cms_fixture_id=cms_fixture_id,
                reason="league_not_mapped",
                detail=(
                    f"No mapped Polymarket league for cms_league_id={cms_fixture.league_id!r}. "
                    f"Provide polymarket_slug to bypass league lookup."
                ),
            )

        home_map = self._mappings.get_team(cms_fixture.home_team_id)
        if not home_map:
            return _error_result(
                cms_fixture_id=cms_fixture_id,
                reason="home_team_not_mapped",
                detail=f"No mapped Polymarket team for cms_team_id={cms_fixture.home_team_id!r}",
            )

        away_map = self._mappings.get_team(cms_fixture.away_team_id)
        if not away_map:
            return _error_result(
                cms_fixture_id=cms_fixture_id,
                reason="away_team_not_mapped",
                detail=f"No mapped Polymarket team for cms_team_id={cms_fixture.away_team_id!r}",
            )

        # Step 3 — resolve Polymarket fixture
        if polymarket_slug:
            try:
                base_event = self._poly.get_event_by_slug(polymarket_slug)
                if not isinstance(base_event, dict):
                    raise RuntimeError(f"Unexpected response for slug {polymarket_slug!r}")
            except RuntimeError as exc:
                return _error_result(
                    cms_fixture_id=cms_fixture_id,
                    reason="polymarket_slug_not_found",
                    detail=str(exc),
                )
            fixture_candidates: List[Tuple[float, Dict[str, Any]]] = [(1.0, base_event)]
        else:
            fixture_candidates = _find_fixture_candidates(
                polymarket_client=self._poly,
                league_slug=league_map.polymarket_league_slug,
                home_team_name=home_map.polymarket_team_name,
                away_team_name=away_map.polymarket_team_name,
                kickoff_str=cms_fixture.kickoff,
                tolerance_minutes=self._tolerance,
            )

        if not fixture_candidates:
            return _error_result(
                cms_fixture_id=cms_fixture_id,
                reason="no_polymarket_fixture",
                detail=(
                    f"No Polymarket fixture found for "
                    f"home={home_map.polymarket_team_name!r} "
                    f"away={away_map.polymarket_team_name!r} "
                    f"league={league_map.polymarket_league_slug!r} "
                    f"kickoff={cms_fixture.kickoff!r}"
                ),
            )

        # Multiple candidates → ambiguous, do not auto-pick
        if len(fixture_candidates) > 1:
            return {
                "status": "unmatched",
                "cms_fixture_id": cms_fixture_id,
                "fixture_name": cms_fixture.name,
                "polymarket_slug": "",
                "polymarket_event_id": "",
                "error": "fixture_ambiguous",
                "detail": "Multiple Polymarket fixture candidates found; provide polymarket_slug to resolve.",
                "fixture_candidates": [
                    {
                        "slug": str(ev.get("slug") or ""),
                        "event_id": str(ev.get("id") or ""),
                        "title": str(ev.get("title") or ""),
                        "start_time": str(ev.get("startTime") or ""),
                        "confidence": round(score, 3),
                    }
                    for score, ev in fixture_candidates
                ],
            }

        best_score, base_event = fixture_candidates[0]
        polymarket_slug = str(base_event.get("slug") or "")
        polymarket_event_id = str(base_event.get("id") or "")

        # Step 4 — collect Game Lines only
        game_lines_events = collect_game_lines_events(
            polymarket_client=self._poly,
            base_event=base_event,
        )

        # Step 5 — classify Polymarket markets (all total lines)
        pm_markets = select_fixture_markets(
            game_lines_events,
            include_draw=True,
            all_total_lines=True,
        )

        # Group CMS markets by family
        cms_by_family: Dict[str, List[CmsMarketRecord]] = {
            "moneyline": [],
            "totals": [],
            "spreads": [],
            "btts": [],
        }
        for m in cms_markets:
            if m.family in cms_by_family:
                cms_by_family[m.family].append(m)

        # Step 6 — moneyline gate
        ml_result = _match_moneyline(
            cms_markets=cms_by_family["moneyline"],
            pm_markets=pm_markets["moneyline"],
            home_team_mapping=home_map,
            away_team_mapping=away_map,
            cms_fixture=cms_fixture,
            polymarket_slug=polymarket_slug,
        )

        gate_passed = ml_result["gate_passed"]
        families: Dict[str, Any] = {
            "moneyline": ml_result["family_result"],
            "totals": _empty_family(),
            "spreads": _empty_family(),
            "btts": _empty_family(),
        }

        if not gate_passed:
            families["totals"] = _gate_failed_family(
                cms_markets=cms_by_family["totals"],
                pm_markets=pm_markets["totals"],
                polymarket_slug=polymarket_slug,
            )
            families["spreads"] = _gate_failed_family(
                cms_markets=cms_by_family["spreads"],
                pm_markets=pm_markets["spreads"],
                polymarket_slug=polymarket_slug,
            )
            families["btts"] = _gate_failed_family(
                cms_markets=cms_by_family["btts"],
                pm_markets=pm_markets["both_teams_to_score"],
                polymarket_slug=polymarket_slug,
            )
        else:
            # Steps 7-9 — match remaining families
            families["totals"] = _match_totals(
                cms_markets=cms_by_family["totals"],
                pm_markets=pm_markets["totals"],
                polymarket_slug=polymarket_slug,
            )
            families["spreads"] = _match_spreads(
                cms_markets=cms_by_family["spreads"],
                pm_markets=pm_markets["spreads"],
                home_team_mapping=home_map,
                away_team_mapping=away_map,
                polymarket_slug=polymarket_slug,
            )
            families["btts"] = _match_btts(
                cms_markets=cms_by_family["btts"],
                pm_markets=pm_markets["both_teams_to_score"],
                polymarket_slug=polymarket_slug,
            )

        # Top-level unmatched list (all families combined)
        all_unmatched = []
        for fam_result in families.values():
            all_unmatched.extend(fam_result.get("unmatched_cms_markets", []))

        total_cms = sum(
            len(v) for v in cms_by_family.values()
            if isinstance(v, list)
        )
        total_matched = sum(
            len(fam_result.get("matched", []))
            for fam_result in families.values()
        )

        if total_cms == 0:
            top_status = "unmatched"
        elif total_matched == total_cms:
            top_status = "matched"
        elif total_matched == 0:
            top_status = "unmatched"
        else:
            top_status = "partially_matched"

        return {
            "status": top_status,
            "cms_fixture_id": cms_fixture_id,
            "fixture_name": cms_fixture.name,
            "polymarket_slug": polymarket_slug,
            "polymarket_event_id": polymarket_event_id,
            "fixture_confidence": round(best_score, 3),
            "families": families,
            "unmatched_cms_markets": all_unmatched,
        }

    # ------------------------------------------------------------------
    # Combined endpoint — sync fixture (resolve + map + push)
    # ------------------------------------------------------------------

    def map_and_sync_fixture(
        self,
        polymarket_url: str,
        cms_fixture_id: str,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Single combined pipeline:
          1. Extract Polymarket slug from polymarket_url
          2. Run map_fixture (CMS fetch → Polymarket match)
          3. POST each matched market to {market_making_host}/api/v1/config/markets

        Set dry_run=True to skip the POST step and preview results only.
        """
        slug = _extract_slug_from_url(polymarket_url)
        if not slug:
            return _error_result(
                cms_fixture_id=cms_fixture_id,
                reason="slug_extraction_failed",
                detail=f"Could not extract slug from Polymarket URL: {polymarket_url!r}",
            )

        result = self.map_fixture(
            cms_fixture_id=cms_fixture_id,
            polymarket_slug=slug,
            include_closed=False,
        )

        # Collect matched markets eligible for market-making sync.
        # Only markets with a 0x-prefixed cms_market_id are on-chain pred markets;
        # UUID-style IDs (e.g. "d7901645-...") are internal CMS records and must be skipped.
        all_matched: List[Dict[str, Any]] = []
        for fam_result in result.get("families", {}).values():
            for m in fam_result.get("matched", []):
                if (m.get("cms_market_id") or "").startswith("0x"):
                    all_matched.append(m)

        sync_results: List[Dict[str, Any]] = []
        if not all_matched:
            pass  # nothing to post
        elif dry_run:
            sync_results = [
                {"status": "skipped", "reason": "dry_run", "cms_market_id": m.get("cms_market_id")}
                for m in all_matched
            ]
        elif not self._market_making_host:
            sync_results = [
                {
                    "status": "skipped",
                    "reason": "market_making_not_configured",
                    "cms_market_id": m.get("cms_market_id"),
                }
                for m in all_matched
            ]
        else:
            for matched in all_matched:
                sync_results.append(self._post_to_market_making(matched))

        result["sync_results"] = sync_results
        result["sync_posted"] = sum(1 for r in sync_results if r.get("status") == "posted")
        result["sync_already_exists"] = sum(1 for r in sync_results if r.get("status") == "already_exists")
        result["sync_failed"] = sum(1 for r in sync_results if r.get("status") == "failed")
        result["dry_run"] = dry_run
        return result

    def _post_to_market_making(self, matched: Dict[str, Any]) -> Dict[str, Any]:
        """POST one matched market to the market-making config endpoint."""
        market_payload = matched.get("market", {})
        # Use the Polymarket question as the canonical market_name
        market_name = str(
            matched.get("question")
            or (market_payload.get("question") if isinstance(market_payload, dict) else "")
            or matched.get("cms_market_id")
            or ""
        )

        body = {
            "market_name": market_name,
            "market": market_payload,
        }

        url = f"{self._market_making_host}/api/v1/config/markets"
        headers: Dict[str, str] = {}
        if self._market_making_auth_header and self._market_making_auth_token:
            headers[self._market_making_auth_header] = self._market_making_auth_token

        try:
            response = self._http.post_json(url, body, headers=headers)
            return {
                "status": "posted",
                "cms_market_id": matched.get("cms_market_id"),
                "market_name": market_name,
                "response": response,
            }
        except RuntimeError as exc:
            err = str(exc)
            # Treat duplicate-key constraint as "already_exists" — market is already configured
            if "duplicate key" in err.lower() or "uniq_pred_market_id" in err:
                return {
                    "status": "already_exists",
                    "cms_market_id": matched.get("cms_market_id"),
                    "market_name": market_name,
                }
            return {
                "status": "failed",
                "cms_market_id": matched.get("cms_market_id"),
                "market_name": market_name,
                "error": err,
            }

    # ------------------------------------------------------------------
    # Endpoint 1 — resolve fixture slug
    # ------------------------------------------------------------------

    def resolve_slug_from_url(self, polymarket_url: str) -> Dict[str, Any]:
        slug = _extract_slug_from_url(polymarket_url)
        if not slug:
            return {
                "status": "unmatched",
                "source": "url",
                "best_match": None,
                "candidates": [],
                "error": "slug_extraction_failed",
                "detail": f"Could not extract slug from URL: {polymarket_url!r}",
            }
        try:
            event = self._poly.get_event_by_slug(slug)
        except RuntimeError as exc:
            return {
                "status": "unmatched",
                "source": "url",
                "best_match": None,
                "candidates": [],
                "error": "event_not_found",
                "detail": str(exc),
            }
        if not isinstance(event, dict):
            return {
                "status": "unmatched",
                "source": "url",
                "best_match": None,
                "candidates": [],
                "error": "event_not_found",
                "detail": f"No event found for slug {slug!r}",
            }
        return {
            "status": "matched",
            "source": "url",
            "best_match": _event_to_candidate(event, score=1.0),
            "candidates": [],
            **self._fetch_game_lines_markets(event),
        }

    def resolve_slug_from_text(
        self,
        fixture_text: str,
        league_code: str = "",
    ) -> Dict[str, Any]:
        home_text, away_text = _split_fixture_text(fixture_text)

        league_slugs = [league_code.strip().lower()] if league_code.strip() else list(V1_LEAGUE_SLUGS)

        scored: List[Tuple[float, Dict[str, Any]]] = []
        seen_ids: set = set()

        for slug in league_slugs:
            for closed in (False, True):
                offset = 0
                while True:
                    events = self._poly.list_events(
                        tag_slug=slug, limit=500, offset=offset, closed=closed
                    )
                    if not events:
                        break
                    for ev in events:
                        if not isinstance(ev, dict):
                            continue
                        if not _is_fixture_event(ev, slug):
                            continue
                        eid = str(ev.get("id") or "")
                        if eid in seen_ids:
                            continue
                        seen_ids.add(eid)
                        score = _score_fixture_vs_text(ev, home_text, away_text)
                        if score >= 0.5:
                            scored.append((score, ev))
                    if len(events) < 500:
                        break
                    offset += 500

        scored.sort(key=lambda x: -x[0])

        if not scored:
            return {
                "status": "unmatched",
                "source": "text_search",
                "best_match": None,
                "candidates": [],
                "error": "no_match",
                "detail": f"No Polymarket event found for fixture_text={fixture_text!r}",
            }

        best_score, best_event = scored[0]
        candidates = [_event_to_candidate(ev, score=s) for s, ev in scored]

        return {
            "status": "matched",
            "source": "text_search",
            "best_match": _event_to_candidate(best_event, score=best_score),
            "candidates": candidates,
            **self._fetch_game_lines_markets(best_event),
        }

    def _fetch_game_lines_markets(self, base_event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch Game Lines (base + -more-markets), classify into families,
        and return a markets dict keyed by family with full token IDs per market.
        """
        game_lines = collect_game_lines_events(
            polymarket_client=self._poly,
            base_event=base_event,
        )
        classified = select_fixture_markets(
            game_lines,
            include_draw=True,
            all_total_lines=True,
        )

        markets: Dict[str, Any] = {}
        for family in ("moneyline", "totals", "spreads", "both_teams_to_score"):
            display_key = "btts" if family == "both_teams_to_score" else family
            markets[display_key] = [
                {
                    "market_id": m["market_id"],
                    "question": m["question"],
                    "line": m["line"],
                    "yes_token_id": m["yes_token_id"],
                    "no_token_id": m["no_token_id"],
                    "active": m["active"],
                    "closed": m["closed"],
                }
                for m in classified.get(family, [])
            ]

        total_markets = sum(len(v) for v in markets.values())
        return {
            "markets": markets,
            "total_markets": total_markets,
            "game_lines_events": [str(ev.get("slug") or "") for ev in game_lines],
        }


# ---------------------------------------------------------------------------
# Moneyline matching
# ---------------------------------------------------------------------------

# Set to True to re-enable draw market matching.
_DRAW_MATCHING_ENABLED = False


def _match_moneyline(
    cms_markets: List[CmsMarketRecord],
    pm_markets: List[Dict[str, Any]],
    home_team_mapping: Any,
    away_team_mapping: Any,
    cms_fixture: CmsFixtureRecord,
    polymarket_slug: str,
) -> Dict[str, Any]:
    """
    Match CMS moneyline markets to Polymarket moneyline markets.
    Returns {"gate_passed": bool, "family_result": dict}.

    Gate passes when both home AND away moneylines are matched.
    Draw is not required for the gate.
    """
    pm_by_role: Dict[str, Dict[str, Any]] = {}
    for pm in pm_markets:
        line = normalize_text(str(pm.get("line") or ""))
        if line == "draw":
            pm_by_role["draw"] = pm
        elif normalize_text(home_team_mapping.polymarket_team_name) in line or line in normalize_text(home_team_mapping.polymarket_team_name):
            pm_by_role["home"] = pm
        elif normalize_text(away_team_mapping.polymarket_team_name) in line or line in normalize_text(away_team_mapping.polymarket_team_name):
            pm_by_role["away"] = pm

    matched = []
    unmatched = []
    candidates_log = []

    for cms in cms_markets:
        role = _resolve_moneyline_role(cms, cms_fixture, home_team_mapping, away_team_mapping)

        # Draw matching temporarily disabled — appear as unmatched with explicit reason.
        if role == "draw" and not _DRAW_MATCHING_ENABLED:
            unmatched.append(_unmatched_market(cms=cms, reason="draw_matching_disabled", candidates=[]))
            continue

        pm = pm_by_role.get(role) if role else None

        if pm and role:
            confidence = _team_name_confidence(
                query=home_team_mapping.polymarket_team_name if role == "home" else (
                    away_team_mapping.polymarket_team_name if role == "away" else "draw"
                ),
                pm_line=str(pm.get("line") or ""),
            )
            matched.append(_matched_market(
                cms=cms,
                pm=pm,
                polymarket_slug=polymarket_slug,
                confidence=confidence,
                notes=f"moneyline {role}",
            ))
        else:
            reason = "no_polymarket_moneyline_draw" if role == "draw" else (
                f"no_polymarket_moneyline_{role}" if role else "outcome_role_unknown"
            )
            # Offer all PM moneylines as candidates
            cands = [
                {
                    "polymarket_market_id": str(pm_c.get("market_id") or ""),
                    "question": str(pm_c.get("question") or ""),
                    "event_slug": polymarket_slug,
                    "confidence": 0.3,
                    "reason": f"pm_moneyline_candidate_line={pm_c.get('line')!r}",
                }
                for pm_c in pm_markets
            ]
            unmatched.append(_unmatched_market(cms=cms, reason=reason, candidates=cands))

    home_matched = any(
        m["notes"] == "moneyline home" for m in matched
    )
    away_matched = any(
        m["notes"] == "moneyline away" for m in matched
    )
    gate_passed = home_matched and away_matched

    # Polymarket moneylines that were not consumed by any CMS market
    matched_pm_ids = {m["polymarket_market_id"] for m in matched}
    unsupported = [
        _unsupported_pm(pm, polymarket_slug)
        for pm in pm_markets
        if str(pm.get("market_id") or "") not in matched_pm_ids
    ]

    family_result = _family_result(
        matched=matched,
        unmatched=unmatched,
        candidate_markets=candidates_log,
        unsupported_polymarket_markets=unsupported,
    )
    return {"gate_passed": gate_passed, "family_result": family_result}


def _resolve_moneyline_role(
    cms: CmsMarketRecord,
    cms_fixture: CmsFixtureRecord,
    home_map: Any,
    away_map: Any,
) -> str:
    """
    Determine outcome role for a CMS moneyline market.
    Priority: explicit outcome_role → team_id comparison → fallback name checks.
    """
    role = cms.outcome_role.lower().strip()
    if role in {"home", "away", "draw"}:
        return role
    # Infer from team_id
    if cms.team_id:
        if cms.team_id == cms_fixture.home_team_id:
            return "home"
        if cms.team_id == cms_fixture.away_team_id:
            return "away"
    # Fallback: name-based draw detection (already attempted in CmsClient but re-check)
    name_upper = cms.market_name.upper()
    if "DRAW" in name_upper:
        return "draw"
    return ""


# ---------------------------------------------------------------------------
# Totals matching
# ---------------------------------------------------------------------------

def _match_totals(
    cms_markets: List[CmsMarketRecord],
    pm_markets: List[Dict[str, Any]],
    polymarket_slug: str,
) -> Dict[str, Any]:
    """
    CMS totals = Over market.  Match by exact numeric line.
    Polymarket YES = Over, NO = Under.
    """
    # Index PM totals by numeric line value
    pm_by_line: Dict[float, Dict[str, Any]] = {}
    for pm in pm_markets:
        val = _extract_numeric(str(pm.get("line") or ""))
        if val is not None:
            pm_by_line[val] = pm

    matched = []
    unmatched = []

    for cms in cms_markets:
        cms_val = _extract_numeric(cms.line)
        if cms_val is None:
            unmatched.append(_unmatched_market(
                cms=cms,
                reason="cms_line_not_numeric",
                candidates=[],
            ))
            continue

        pm = pm_by_line.get(cms_val)
        if pm:
            matched.append(_matched_market(
                cms=cms,
                pm=pm,
                polymarket_slug=polymarket_slug,
                confidence=1.0,
                notes="totals: YES=Over, NO=Under",
            ))
        else:
            # Suggest nearest PM totals as candidates
            cands = sorted(
                [
                    {
                        "polymarket_market_id": str(pm_c.get("market_id") or ""),
                        "question": str(pm_c.get("question") or ""),
                        "event_slug": polymarket_slug,
                        "confidence": round(1.0 - abs((_extract_numeric(str(pm_c.get("line") or "")) or 0) - cms_val) * 0.1, 2),
                        "reason": f"pm_total_line={pm_c.get('line')!r}",
                    }
                    for pm_c in pm_markets
                ],
                key=lambda c: -c["confidence"],
            )
            unmatched.append(_unmatched_market(
                cms=cms,
                reason="no_polymarket_total_line",
                candidates=cands[:3],
            ))

    matched_pm_ids = {m["polymarket_market_id"] for m in matched}
    unsupported = [
        _unsupported_pm(pm, polymarket_slug)
        for pm in pm_markets
        if str(pm.get("market_id") or "") not in matched_pm_ids
    ]

    return _family_result(
        matched=matched,
        unmatched=unmatched,
        candidate_markets=[],
        unsupported_polymarket_markets=unsupported,
    )


# ---------------------------------------------------------------------------
# Spreads matching
# ---------------------------------------------------------------------------

def _match_spreads(
    cms_markets: List[CmsMarketRecord],
    pm_markets: List[Dict[str, Any]],
    home_team_mapping: Any,
    away_team_mapping: Any,
    polymarket_slug: str,
) -> Dict[str, Any]:
    """
    Match by team context + absolute line value.
    CMS lines are always presented as negative for both sides.
    Polymarket lines carry a sign; we compare abs values.
    Requires explicit CMS team_id.
    """
    matched = []
    unmatched = []

    for cms in cms_markets:
        if not cms.team_id:
            unmatched.append(_unmatched_market(
                cms=cms,
                reason="missing_cms_team_id",
                candidates=[],
            ))
            continue

        # Resolve team context
        if cms.team_id == home_team_mapping.cms_team_id:
            pm_team_name = home_team_mapping.polymarket_team_name
        elif cms.team_id == away_team_mapping.cms_team_id:
            pm_team_name = away_team_mapping.polymarket_team_name
        else:
            unmatched.append(_unmatched_market(
                cms=cms,
                reason="cms_team_id_not_home_or_away",
                candidates=[],
            ))
            continue

        cms_abs = abs(_extract_numeric(cms.line) or 0)
        if cms_abs == 0:
            unmatched.append(_unmatched_market(
                cms=cms,
                reason="cms_spread_line_not_numeric",
                candidates=[],
            ))
            continue

        pm_team_norm = normalize_text(pm_team_name)
        pm_match = None
        for pm in pm_markets:
            pm_line = str(pm.get("line") or "")
            pm_line_norm = normalize_text(pm_line)
            # PM line format: "{team_name} {spread_value}"
            if not pm_line_norm.startswith(pm_team_norm):
                continue
            # Spread value is always the last number in the PM line
            # (format: "{team_name} {signed_value}"). Using last avoids
            # false positives when team names start with digits (e.g. "1. FSV Mainz 05").
            nums = _SIGNED_NUMBER_RE.findall(pm_line)
            pm_abs = abs(float(nums[-1])) if nums else 0
            if pm_abs == cms_abs:
                pm_match = pm
                break

        if pm_match:
            matched.append(_matched_market(
                cms=cms,
                pm=pm_match,
                polymarket_slug=polymarket_slug,
                confidence=1.0,
                notes=f"spread team={pm_team_name} abs_line={cms_abs}",
            ))
        else:
            cands = [
                {
                    "polymarket_market_id": str(pm_c.get("market_id") or ""),
                    "question": str(pm_c.get("question") or ""),
                    "event_slug": polymarket_slug,
                    "confidence": 0.3,
                    "reason": f"pm_spread_line={pm_c.get('line')!r}",
                }
                for pm_c in pm_markets
                if normalize_text(str(pm_c.get("line") or "")).startswith(pm_team_norm)
            ]
            unmatched.append(_unmatched_market(
                cms=cms,
                reason="no_polymarket_spread_line",
                candidates=cands[:3],
            ))

    matched_pm_ids = {m["polymarket_market_id"] for m in matched}
    unsupported = [
        _unsupported_pm(pm, polymarket_slug)
        for pm in pm_markets
        if str(pm.get("market_id") or "") not in matched_pm_ids
    ]

    return _family_result(
        matched=matched,
        unmatched=unmatched,
        candidate_markets=[],
        unsupported_polymarket_markets=unsupported,
    )


# ---------------------------------------------------------------------------
# BTTS matching
# ---------------------------------------------------------------------------

def _match_btts(
    cms_markets: List[CmsMarketRecord],
    pm_markets: List[Dict[str, Any]],
    polymarket_slug: str,
) -> Dict[str, Any]:
    """At most one BTTS market per fixture. YES=both score, NO=not both."""
    pm = pm_markets[0] if pm_markets else None

    matched = []
    unmatched = []

    for cms in cms_markets:
        if pm:
            matched.append(_matched_market(
                cms=cms,
                pm=pm,
                polymarket_slug=polymarket_slug,
                confidence=1.0,
                notes="btts: YES=both score, NO=not both",
            ))
        else:
            unmatched.append(_unmatched_market(
                cms=cms,
                reason="no_polymarket_btts",
                candidates=[],
            ))

    matched_pm_ids = {m["polymarket_market_id"] for m in matched}
    unsupported = [
        _unsupported_pm(p, polymarket_slug)
        for p in pm_markets
        if str(p.get("market_id") or "") not in matched_pm_ids
    ]

    return _family_result(
        matched=matched,
        unmatched=unmatched,
        candidate_markets=[],
        unsupported_polymarket_markets=unsupported,
    )


# ---------------------------------------------------------------------------
# Fixture candidate search
# ---------------------------------------------------------------------------

def _find_fixture_candidates(
    polymarket_client: PolymarketClient,
    league_slug: str,
    home_team_name: str,
    away_team_name: str,
    kickoff_str: str,
    tolerance_minutes: int,
) -> List[Tuple[float, Dict[str, Any]]]:
    """
    Scan Polymarket events for the league and score each base fixture event
    against the provided criteria.  Returns scored pairs sorted descending.
    """
    target_kickoff = parse_datetime(kickoff_str)
    scored: List[Tuple[float, Dict[str, Any]]] = []
    seen_ids: set = set()

    for closed in (False, True):
        offset = 0
        while True:
            events = polymarket_client.list_events(
                tag_slug=league_slug, limit=500, offset=offset, closed=closed
            )
            if not events:
                break
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                if not _is_fixture_event(ev, league_slug):
                    continue
                eid = str(ev.get("id") or "")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                score = _score_fixture(
                    event=ev,
                    home_name=home_team_name,
                    away_name=away_team_name,
                    target_kickoff=target_kickoff,
                    tolerance_minutes=tolerance_minutes,
                )
                if score >= 0.5:
                    scored.append((score, ev))
            if len(events) < 500:
                break
            offset += 500

    scored.sort(key=lambda x: -x[0])
    return scored


def _score_fixture(
    event: Dict[str, Any],
    home_name: str,
    away_name: str,
    target_kickoff: Any,
    tolerance_minutes: int,
) -> float:
    home_event, away_event = _event_team_names(event)
    home_score = _team_name_score(home_name, home_event)
    away_score = _team_name_score(away_name, away_event)

    if home_score == 0.0 or away_score == 0.0:
        return 0.0

    team_score = (home_score + away_score) / 2.0

    if target_kickoff:
        event_start = _event_start_time(event)
        if event_start:
            delta_min = abs((event_start - target_kickoff).total_seconds() / 60)
            if delta_min > tolerance_minutes:
                return 0.0
            kickoff_factor = 1.0 - (delta_min / tolerance_minutes) * 0.15
        else:
            kickoff_factor = 0.85  # no kickoff info → slight penalty
    else:
        kickoff_factor = 0.85

    return round(team_score * kickoff_factor, 3)


def _score_fixture_vs_text(
    event: Dict[str, Any], home_text: str, away_text: str
) -> float:
    home_event, away_event = _event_team_names(event)
    home_score = _team_name_score(home_text, home_event) if home_text else 0.5
    away_score = _team_name_score(away_text, away_event) if away_text else 0.5
    if home_score == 0.0 and away_score == 0.0:
        return 0.0
    return round((home_score + away_score) / 2.0, 3)


def _team_name_score(query: str, candidate: str) -> float:
    q = normalize_text(query)
    c = normalize_text(candidate)
    if not q or not c:
        return 0.0
    if q == c:
        return 1.0
    if q in c or c in q:
        return 0.8
    return 0.0


def _team_name_confidence(query: str, pm_line: str) -> float:
    q = normalize_text(query)
    p = normalize_text(pm_line)
    if q == p:
        return 1.0
    if q in p or p in q:
        return 0.9
    return 0.7


# ---------------------------------------------------------------------------
# URL slug extraction
# ---------------------------------------------------------------------------

def _extract_slug_from_url(url: str) -> str:
    """Extract the event slug from a Polymarket event URL."""
    from urllib.parse import urlparse
    parsed = urlparse(url.strip())
    path = parsed.path.rstrip("/")
    for prefix in ("/event/", "/events/"):
        idx = path.find(prefix)
        if idx >= 0:
            return path[idx + len(prefix):].split("/")[0]
    # Fallback: last non-empty path segment
    parts = [p for p in path.split("/") if p]
    return parts[-1] if parts else ""


def extract_cms_fixture_id_from_url(url: str) -> str:
    """
    Extract the fixture identifier from a CMS fixture URL.

    Handles formats like:
      https://uat-frankfurt.pred.app/trade/inter-vs-cagliari-serie-a-2026-04-17
      https://pred.app/trade/inter-vs-cagliari-serie-a-2026-04-17
      https://any-host/any/path/FIXTURE-ID

    Returns the last non-empty path segment, which is the fixture slug/id.
    """
    from urllib.parse import urlparse
    parsed = urlparse(url.strip())
    parts = [p for p in parsed.path.split("/") if p]
    return parts[-1] if parts else ""


def _split_fixture_text(text: str) -> Tuple[str, str]:
    """Split 'Arsenal vs Bournemouth' → ('Arsenal', 'Bournemouth')."""
    for sep in (" vs. ", " vs ", " VS. ", " VS "):
        if sep in text:
            left, right = text.split(sep, 1)
            return left.strip(), right.strip()
    return text.strip(), ""


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------

def _matched_market(
    cms: CmsMarketRecord,
    pm: Dict[str, Any],
    polymarket_slug: str,
    confidence: float,
    notes: str,
) -> Dict[str, Any]:
    return {
        "cms_market_id": cms.market_id,
        "market_name": cms.market_name,
        "market_family": cms.family,
        "market_line": cms.line,
        "question": str(pm.get("question") or ""),
        "status": str(pm.get("active") and not pm.get("closed") and "active" or "closed"),
        "polymarket_market_id": str(pm.get("market_id") or ""),
        "polymarket_event_slug": polymarket_slug,
        "confidence": round(confidence, 3),
        "notes": notes,
        "market": {
            "question": str(pm.get("question") or ""),
            "status": "active" if pm.get("active") and not pm.get("closed") else "closed",
            "outcomes": {
                "YES": {"token_id": str(pm.get("yes_token_id") or "")},
                "NO": {"token_id": str(pm.get("no_token_id") or "")},
            },
            "pred_mapping": {
                "market_id": cms.market_id,
            },
        },
    }


def _unmatched_market(
    cms: CmsMarketRecord,
    reason: str,
    candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "cms_market_id": cms.market_id,
        "market_name": cms.market_name,
        "market_family": cms.family,
        "market_line": cms.line,
        "cms_team_id": cms.team_id,
        "reason": reason,
        "candidates": candidates,
    }


def _unsupported_pm(pm: Dict[str, Any], polymarket_slug: str) -> Dict[str, Any]:
    return {
        "polymarket_market_id": str(pm.get("market_id") or ""),
        "question": str(pm.get("question") or ""),
        "family": str(pm.get("family") or ""),
        "line": str(pm.get("line") or ""),
        "event_slug": polymarket_slug,
    }


def _family_result(
    matched: List[Dict[str, Any]],
    unmatched: List[Dict[str, Any]],
    candidate_markets: List[Dict[str, Any]],
    unsupported_polymarket_markets: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if matched and not unmatched:
        status = "matched"
    elif matched:
        status = "partially_matched"
    else:
        status = "unmatched"
    return {
        "status": status,
        "matched": matched,
        "unmatched_cms_markets": unmatched,
        "candidate_markets": candidate_markets,
        "unsupported_polymarket_markets": unsupported_polymarket_markets,
    }


def _empty_family() -> Dict[str, Any]:
    return _family_result(
        matched=[],
        unmatched=[],
        candidate_markets=[],
        unsupported_polymarket_markets=[],
    )


def _gate_failed_family(
    cms_markets: List[CmsMarketRecord],
    pm_markets: List[Dict[str, Any]],
    polymarket_slug: str,
) -> Dict[str, Any]:
    """Produce an unmatched family result where all CMS markets failed due to the gate."""
    unmatched = [
        _unmatched_market(cms=m, reason="moneyline_gate_failed", candidates=[])
        for m in cms_markets
    ]
    unsupported = [_unsupported_pm(pm, polymarket_slug) for pm in pm_markets]
    return _family_result(
        matched=[],
        unmatched=unmatched,
        candidate_markets=[],
        unsupported_polymarket_markets=unsupported,
    )


def _event_to_candidate(event: Dict[str, Any], score: float) -> Dict[str, Any]:
    return {
        "slug": str(event.get("slug") or ""),
        "event_id": str(event.get("id") or ""),
        "title": str(event.get("title") or ""),
        "start_time": str(event.get("startTime") or ""),
        "confidence": round(score, 3),
    }


def _error_result(
    cms_fixture_id: str, reason: str, detail: str
) -> Dict[str, Any]:
    return {
        "status": "unmatched",
        "cms_fixture_id": cms_fixture_id,
        "fixture_name": "",
        "polymarket_slug": "",
        "polymarket_event_id": "",
        "error": reason,
        "detail": detail,
        "families": {
            "moneyline": _empty_family(),
            "totals": _empty_family(),
            "spreads": _empty_family(),
            "btts": _empty_family(),
        },
        "unmatched_cms_markets": [],
    }


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

def _extract_numeric(text: str) -> Optional[float]:
    """Extract the first numeric value from a string, ignoring sign for safety."""
    if not text:
        return None
    match = _SIGNED_NUMBER_RE.search(text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None
