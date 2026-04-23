from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

from pred_polymarket_sync.models import (
    MappingRecord,
    MatchResult,
    PolymarketMarket,
    PredMarketBundle,
    ReviewRecord,
)
from pred_polymarket_sync.sources import DataSnapshot
from pred_polymarket_sync.utils import normalize_text


@dataclass
class CandidateScore:
    market: PolymarketMarket
    score: int
    reasons: List[str]


def build_pred_market_bundles(snapshot: DataSnapshot) -> List[PredMarketBundle]:
    parent_by_id = {item.parent_market_id: item for item in snapshot.parent_markets}
    fixture_by_parent_id = {}
    for fixture in snapshot.fixtures:
        fixture_by_parent_id[fixture.fixture_id] = fixture

    fixture_mapping_by_fixture_id = {
        item.cms_fixture_id: item for item in snapshot.fixture_mappings if item.cms_fixture_id
    }
    team_mapping_by_team_id = {
        item.cms_team_id: item for item in snapshot.team_mappings if item.cms_team_id
    }
    league_mapping_by_league_id = {
        item.cms_league_id: item for item in snapshot.league_mappings if item.cms_league_id
    }
    fixture_by_id = {item.fixture_id: item for item in snapshot.fixtures}

    bundles: List[PredMarketBundle] = []
    for market in snapshot.markets:
        parent_market = parent_by_id.get(market.parent_market_id)
        if not parent_market:
            continue
        fixture = fixture_by_id.get(parent_market.parent_market_id) or _find_fixture_for_parent(
            parent_market, snapshot.fixtures
        )
        if not fixture:
            continue
        sportsdata_fixture = fixture_mapping_by_fixture_id.get(fixture.fixture_id)
        home_mapping = team_mapping_by_team_id.get(fixture.home_team_id)
        away_mapping = team_mapping_by_team_id.get(fixture.away_team_id)
        league_mapping = league_mapping_by_league_id.get(fixture.league_id)
        outcome_key, outcome_label = _derive_outcome(market, fixture)
        tracking_status = _derive_tracking_status(
            market=market,
            parent_market=parent_market,
            sportsdata_fixture=sportsdata_fixture,
        )
        if tracking_status not in {"upcoming", "postponed"}:
            continue
        bundles.append(
            PredMarketBundle(
                market=market,
                parent_market=parent_market,
                fixture=fixture,
                sportsdata_fixture=sportsdata_fixture,
                sportsdata_home_team_id=home_mapping.sportsdata_team_id if home_mapping else "",
                sportsdata_away_team_id=away_mapping.sportsdata_team_id if away_mapping else "",
                sportsdata_competition_id=league_mapping.sportsdata_competition_id
                if league_mapping
                else "",
                outcome_key=outcome_key,
                outcome_label=outcome_label,
                tracking_status=tracking_status,
            )
        )
    return bundles


def _find_fixture_for_parent(parent_market, fixtures):
    normalized_parent = normalize_text(parent_market.title)
    for fixture in fixtures:
        if normalize_text(fixture.name) == normalized_parent:
            return fixture
    return None


def _derive_outcome(market, fixture):
    if market.team_id and market.team_id == fixture.home_team_id:
        return "home", market.name or "home"
    if market.team_id and market.team_id == fixture.away_team_id:
        return "away", market.name or "away"
    if normalize_text(market.name) == "draw" or normalize_text(market.market_code) == "draw":
        return "draw", market.name or "draw"
    return "other", market.name or market.market_code or market.market_id


def _derive_tracking_status(
    market,
    parent_market,
    sportsdata_fixture,
    now: Optional[datetime] = None,
) -> str:
    now = now or datetime.now(timezone.utc)
    status_text = " ".join(
        [
            normalize_text(market.status),
            normalize_text(parent_market.status),
            normalize_text(sportsdata_fixture.status if sportsdata_fixture else ""),
        ]
    )
    if "postponed" in status_text or "rescheduled" in status_text:
        return "postponed"
    if any(
        term in status_text
        for term in (
            "ended",
            "completed",
            "complete",
            "finished",
            "settled",
            "resolved",
            "closed",
            "cancelled",
            "canceled",
        )
    ):
        return "ignored"
    event_time = None
    if sportsdata_fixture and sportsdata_fixture.match_date:
        event_time = sportsdata_fixture.match_date
    elif parent_market.markets_close_time:
        event_time = parent_market.markets_close_time
    elif parent_market.markets_open_time:
        event_time = parent_market.markets_open_time
    if event_time and event_time >= now:
        return "upcoming"
    return "ignored"


class MarketMatcher:
    def __init__(self, start_time_tolerance_minutes: int = 30, score_threshold: int = 85, score_gap_threshold: int = 15):
        self.start_time_tolerance_minutes = start_time_tolerance_minutes
        self.score_threshold = score_threshold
        self.score_gap_threshold = score_gap_threshold

    def match(self, bundle: PredMarketBundle, candidates: Sequence[PolymarketMarket]) -> MatchResult:
        scored = sorted(
            [self._score_candidate(bundle, candidate) for candidate in candidates],
            key=lambda item: item.score,
            reverse=True,
        )
        if not scored:
            return MatchResult(
                status="not_found",
                mapping=None,
                review=ReviewRecord(
                    pred_market_id=bundle.market.market_id,
                    pred_parent_market_id=bundle.parent_market.parent_market_id,
                    pred_fixture_id=bundle.fixture.fixture_id,
                    reason="No Polymarket candidates returned for this game.",
                    top_score=0,
                    candidate_market_ids=[],
                    candidate_reasons=[],
                ),
            )

        best = scored[0]
        next_best = scored[1] if len(scored) > 1 else None
        if best.score < self.score_threshold:
            return self._review(
                bundle,
                scored,
                f"Top candidate score {best.score} is below threshold {self.score_threshold}.",
            )
        if next_best and (best.score - next_best.score) < self.score_gap_threshold:
            return self._review(
                bundle,
                scored,
                f"Top two candidates are too close ({best.score} vs {next_best.score}).",
            )
        mapping = self._to_mapping(bundle, best)
        return MatchResult(status="matched", mapping=mapping, review=None)

    def _review(self, bundle: PredMarketBundle, scored: Sequence[CandidateScore], reason: str) -> MatchResult:
        return MatchResult(
            status="ambiguous",
            mapping=None,
            review=ReviewRecord(
                pred_market_id=bundle.market.market_id,
                pred_parent_market_id=bundle.parent_market.parent_market_id,
                pred_fixture_id=bundle.fixture.fixture_id,
                reason=reason,
                top_score=scored[0].score if scored else 0,
                candidate_market_ids=[item.market.market_id for item in scored[:5]],
                candidate_reasons=[
                    f"{item.market.market_id}: {', '.join(item.reasons)}"
                    for item in scored[:5]
                ],
            ),
        )

    def _to_mapping(self, bundle: PredMarketBundle, candidate: CandidateScore) -> MappingRecord:
        token_ids = candidate.market.clob_token_ids
        yes_token_id = token_ids[0] if len(token_ids) > 0 else ""
        no_token_id = token_ids[1] if len(token_ids) > 1 else ""
        return MappingRecord(
            pred_market_id=bundle.market.market_id,
            pred_parent_market_id=bundle.parent_market.parent_market_id,
            pred_fixture_id=bundle.fixture.fixture_id,
            pred_league_id=bundle.fixture.league_id,
            pred_home_team_id=bundle.fixture.home_team_id,
            pred_away_team_id=bundle.fixture.away_team_id,
            polymarket_market_id=candidate.market.market_id,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            home_team_id=candidate.market.team_a_id or bundle.sportsdata_home_team_id,
            home_team_name=bundle.fixture.name.split(" vs ")[0] if " vs " in bundle.fixture.name else bundle.fixture.name,
            away_team_id=candidate.market.team_b_id or bundle.sportsdata_away_team_id,
            away_team_name=bundle.fixture.name.split(" vs ")[1] if " vs " in bundle.fixture.name else bundle.market.name,
            league_id=bundle.sportsdata_competition_id or bundle.fixture.league_id,
            league_name=bundle.parent_market.title,
            game_id=candidate.market.game_id or (bundle.sportsdata_fixture.sportsdata_game_id if bundle.sportsdata_fixture else ""),
            outcome_label=bundle.outcome_label,
            tracking_status=bundle.tracking_status,
            match_score=candidate.score,
            match_reason="; ".join(candidate.reasons),
        )

    def _score_candidate(self, bundle: PredMarketBundle, candidate: PolymarketMarket) -> CandidateScore:
        score = 0
        reasons: List[str] = []
        if not candidate.market_id or len(candidate.clob_token_ids) < 2:
            return CandidateScore(candidate, 0, ["Missing condition ID or Yes/No token IDs"])

        if bundle.sportsdata_fixture and candidate.game_id == bundle.sportsdata_fixture.sportsdata_game_id:
            score += 50
            reasons.append("Exact game ID match")

        if bundle.sportsdata_home_team_id and candidate.team_a_id == bundle.sportsdata_home_team_id:
            score += 10
            reasons.append("Exact home team ID match")
        if bundle.sportsdata_away_team_id and candidate.team_b_id == bundle.sportsdata_away_team_id:
            score += 10
            reasons.append("Exact away team ID match")
        if {
            candidate.team_a_id,
            candidate.team_b_id,
        } == {bundle.sportsdata_home_team_id, bundle.sportsdata_away_team_id}:
            score += 10
            reasons.append("Exact home/away team pair match")

        if bundle.sportsdata_fixture and bundle.sportsdata_fixture.match_date and candidate.game_start_time:
            delta_minutes = abs(
                int(
                    (
                        candidate.game_start_time - bundle.sportsdata_fixture.match_date
                    ).total_seconds()
                    / 60
                )
            )
            if delta_minutes == 0:
                score += 20
                reasons.append("Exact kickoff match")
            elif delta_minutes <= self.start_time_tolerance_minutes:
                score += max(5, 20 - delta_minutes // 3)
                reasons.append(f"Kickoff within {delta_minutes} minutes")

        candidate_text = normalize_text(
            " ".join(
                [
                    candidate.question,
                    candidate.slug,
                    " ".join(candidate.outcomes),
                    " ".join(candidate.short_outcomes),
                    str(candidate.raw.get("title", "")),
                    str(candidate.raw.get("groupItemTitle", "")),
                ]
            )
        )
        outcome_text = normalize_text(bundle.outcome_label)
        if outcome_text and outcome_text in candidate_text:
            score += 25
            reasons.append("Outcome label found in Polymarket text")
        subject_phrases = [
            f"will {outcome_text}",
            f"{outcome_text} win",
            f"{outcome_text} beat",
        ]
        if outcome_text and any(phrase in candidate_text for phrase in subject_phrases):
            score += 20
            reasons.append("Outcome label is the subject of the market question")
        if bundle.outcome_key == "draw" and "draw" in candidate_text:
            score += 15
            reasons.append("Draw market text match")
        if bundle.outcome_key in {"home", "away"}:
            team_name = normalize_text(bundle.outcome_label)
            if team_name and team_name in candidate_text:
                score += 10
                reasons.append("Team name found in question/slug")
            opponent_name = _opponent_label(bundle)
            if opponent_name:
                opponent_phrases = [
                    f"will {opponent_name}",
                    f"{opponent_name} win",
                    f"{opponent_name} beat",
                ]
                if any(phrase in candidate_text for phrase in opponent_phrases):
                    score -= 20
                    reasons.append("Question subject appears to be the opponent outcome")

        parent_title = normalize_text(bundle.parent_market.title)
        if parent_title and parent_title in candidate_text:
            score += 5
            reasons.append("Parent market title found in Polymarket text")

        if bundle.market.market_canonical_name:
            canonical_hint = normalize_text(bundle.market.market_canonical_name)
            if canonical_hint and canonical_hint in candidate_text:
                score += 5
                reasons.append("Canonical market name found in Polymarket text")

        market_type = normalize_text(candidate.sports_market_type)
        if market_type and market_type in candidate_text:
            score += 2
            reasons.append("Market type echoed in candidate text")

        return CandidateScore(candidate, score, reasons or ["No strong signals"])


def _opponent_label(bundle: PredMarketBundle) -> str:
    fixture_parts = [part.strip() for part in bundle.fixture.name.split(" vs ") if part.strip()]
    normalized_outcome = normalize_text(bundle.outcome_label)
    for part in fixture_parts:
        normalized_part = normalize_text(part)
        if normalized_part and normalized_part != normalized_outcome:
            return normalized_part
    return ""
