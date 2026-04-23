from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pred_polymarket_sync.orderbook import book_from_snapshot
from pred_polymarket_sync.polymarket import PolymarketClient
from pred_polymarket_sync.utils import normalize_text, parse_datetime, parse_jsonish_list

TOTAL_LINES = {"1.5", "2.5", "3.5", "4.5"}
FAMILY_KEYS = ("moneyline", "totals", "spreads", "both_teams_to_score")
RELATED_EVENT_SUFFIXES = (
    "-more-markets",
    "-exact-score",
    "-halftime-result",
    "-player-props",
)
TOTAL_RE = re.compile(r"\bover\s+([0-9]+(?:\.[0-9]+)?)\b", re.IGNORECASE)
SIGNED_NUMBER_RE = re.compile(r"([+-]\d+(?:\.\d+)?)")


def collect_game_lines_events(
    polymarket_client: PolymarketClient,
    base_event: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Return the Game Lines event set for one fixture:
      - the base event
      - the ``{base_slug}-more-markets`` event, if it exists

    This is intentionally narrower than ``resolve_related_fixture_events``,
    which collects all known suffixes including exact-score, halftime-result,
    and player-props.  For token mapping we only want Game Lines.
    """
    base_slug = str(base_event.get("slug") or "")
    events: List[Dict[str, Any]] = [base_event]
    if not base_slug:
        return events
    more_slug = f"{base_slug}-more-markets"
    try:
        more = polymarket_client.get_event_by_slug(more_slug)
        if isinstance(more, dict) and more.get("id"):
            events.append(more)
    except Exception:
        pass
    return events


def fetch_fixture_orderbooks(
    polymarket_client: PolymarketClient,
    fixture_slug: str = "",
    home_team: str = "",
    away_team: str = "",
    league_code: str = "",
    kickoff: str = "",
    include_closed: bool = True,
    include_draw: bool = False,
    requested_families: Optional[Sequence[str]] = None,
    start_time_tolerance_minutes: int = 60,
) -> Dict[str, Any]:
    base_event = resolve_fixture_event(
        polymarket_client=polymarket_client,
        fixture_slug=fixture_slug,
        home_team=home_team,
        away_team=away_team,
        league_code=league_code,
        kickoff=kickoff,
        include_closed=include_closed,
        start_time_tolerance_minutes=start_time_tolerance_minutes,
    )
    related_events = resolve_related_fixture_events(
        polymarket_client=polymarket_client,
        base_event=base_event,
        include_closed=include_closed,
    )
    selected_markets = select_fixture_markets(
        related_events,
        include_draw=include_draw,
        requested_families=requested_families,
    )
    token_ids = _token_ids_for_markets(selected_markets)
    snapshots = polymarket_client.get_order_books(token_ids)
    return build_fixture_orderbooks_payload(base_event, selected_markets, snapshots)


def fetch_league_fixture_orderbooks(
    polymarket_client: PolymarketClient,
    league_code: str,
    include_closed: bool = True,
    include_draw: bool = False,
    requested_families: Optional[Sequence[str]] = None,
    limit: int = 100,
    date_from: str = "",
    date_to: str = "",
) -> Dict[str, Any]:
    if not league_code:
        raise RuntimeError("league_code is required")

    league_events = list_league_fixture_events(
        polymarket_client=polymarket_client,
        league_code=league_code,
        include_closed=include_closed,
        limit=limit,
        date_from=date_from,
        date_to=date_to,
    )
    payloads = []
    for item in league_events:
        selected_markets = select_fixture_markets(
            item["events"],
            include_draw=include_draw,
            requested_families=requested_families,
        )
        token_ids = _token_ids_for_markets(selected_markets)
        snapshots = polymarket_client.get_order_books(token_ids)
        payloads.append(
            build_fixture_orderbooks_payload(
                event=item["base_event"],
                selected_markets=selected_markets,
                snapshots=snapshots,
            )
        )

    return {
        "league": league_code,
        "count": len(payloads),
        "items": payloads,
    }


def resolve_fixture_event(
    polymarket_client: PolymarketClient,
    fixture_slug: str = "",
    home_team: str = "",
    away_team: str = "",
    league_code: str = "",
    kickoff: str = "",
    include_closed: bool = True,
    start_time_tolerance_minutes: int = 60,
) -> Dict[str, Any]:
    if fixture_slug:
        payload = polymarket_client.get_event_by_slug(fixture_slug)
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected response for fixture slug {fixture_slug}")
        return payload

    if not (home_team and away_team and league_code and kickoff):
        raise RuntimeError(
            "Provide fixture_slug or all of home_team, away_team, league_code, and kickoff."
        )

    target_kickoff = parse_datetime(kickoff)
    if not target_kickoff:
        raise RuntimeError("kickoff must be an ISO-like datetime, for example 2026-04-08T19:00:00Z")

    seen_event_ids = set()
    closed_options = [False, True] if include_closed else [False]
    for closed in closed_options:
        offset = 0
        while True:
            events = polymarket_client.list_events(
                tag_slug=league_code,
                limit=500,
                offset=offset,
                closed=closed,
            )
            if not events:
                break
            for event in events:
                event_id = str(event.get("id") or "")
                if event_id in seen_event_ids:
                    continue
                seen_event_ids.add(event_id)
                if _event_matches(
                    event=event,
                    home_team=home_team,
                    away_team=away_team,
                    league_code=league_code,
                    kickoff=target_kickoff,
                    tolerance_minutes=start_time_tolerance_minutes,
                ):
                    return event
            if len(events) < 500:
                break
            offset += 500

    raise RuntimeError(
        f"No Polymarket fixture matched home_team={home_team}, away_team={away_team}, "
        f"league_code={league_code}, kickoff={kickoff}"
    )


def resolve_related_fixture_events(
    polymarket_client: PolymarketClient,
    base_event: Dict[str, Any],
    include_closed: bool = True,
) -> List[Dict[str, Any]]:
    base_slug = str(base_event.get("slug") or "")
    league_code = _event_league_code(base_event)
    if not base_slug or not league_code:
        return [base_event]

    events_by_slug = {base_slug: base_event}
    closed_options = [False, True] if include_closed else [False]
    for closed in closed_options:
        offset = 0
        while True:
            events = polymarket_client.list_events(
                tag_slug=league_code,
                limit=500,
                offset=offset,
                closed=closed,
            )
            if not events:
                break
            for event in events:
                if not isinstance(event, dict):
                    continue
                slug = str(event.get("slug") or "")
                if slug == base_slug or slug.startswith(f"{base_slug}-"):
                    events_by_slug[slug] = event
            if len(events) < 500:
                break
            offset += 500

    return [events_by_slug[slug] for slug in sorted(events_by_slug)]


def list_league_fixture_events(
    polymarket_client: PolymarketClient,
    league_code: str,
    include_closed: bool = True,
    limit: int = 100,
    date_from: str = "",
    date_to: str = "",
) -> List[Dict[str, Any]]:
    events_by_slug: Dict[str, Dict[str, Any]] = {}
    closed_options = [False, True] if include_closed else [False]
    for closed in closed_options:
        offset = 0
        while True:
            events = polymarket_client.list_events(
                tag_slug=league_code,
                limit=500,
                offset=offset,
                closed=closed,
            )
            if not events:
                break
            for event in events:
                if not isinstance(event, dict):
                    continue
                slug = str(event.get("slug") or "")
                if slug:
                    events_by_slug[slug] = event
            if len(events) < 500:
                break
            offset += 500

    start_after = parse_datetime(date_from) if date_from else None
    end_before = parse_datetime(date_to) if date_to else None
    grouped_events = _group_events_by_base_slug(events_by_slug.values())
    ordered = []
    for base_slug, grouped in grouped_events.items():
        base_event = grouped["base_event"]
        if not _is_fixture_event(base_event, league_code):
            continue
        start_time = _event_start_time(base_event)
        if start_after and (not start_time or start_time < start_after):
            continue
        if end_before and (not start_time or start_time > end_before):
            continue
        ordered.append(
            {
                "base_slug": base_slug,
                "base_event": base_event,
                "events": grouped["events"],
                "start_time": start_time,
            }
        )

    ordered.sort(
        key=lambda item: (
            item["start_time"] or datetime.max.replace(tzinfo=timezone.utc),
            str(item["base_event"].get("slug") or ""),
        )
    )
    return ordered[: max(limit, 1)]


def select_fixture_markets(
    events: Dict[str, Any] | Sequence[Dict[str, Any]],
    include_draw: bool = False,
    requested_families: Optional[Sequence[str]] = None,
    all_total_lines: bool = False,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Classify markets from one or more related events into family buckets.

    ``all_total_lines=True`` bypasses the hard-coded ``TOTAL_LINES`` whitelist
    so that any numeric total line is accepted.  Use this when the caller
    (e.g. the token mapper) needs to match whatever lines Polymarket exposes,
    not just the pre-approved set.
    """
    event_list = [events] if isinstance(events, dict) else list(events)
    if not event_list:
        return {family: [] for family in FAMILY_KEYS}

    home_team, away_team = _event_team_names(event_list[0])
    allowed_families = {
        normalize_text(item).replace(" ", "_")
        for item in (requested_families or FAMILY_KEYS)
        if normalize_text(item)
    }
    selections = {family: [] for family in FAMILY_KEYS}
    seen_market_ids = set()
    for event in event_list:
        for market in event.get("markets", []):
            if not isinstance(market, dict):
                continue
            selection = _classify_market(
                market=market,
                home_team=home_team,
                away_team=away_team,
                include_draw=include_draw,
                all_total_lines=all_total_lines,
            )
            if not selection:
                continue
            if selection["family"] not in allowed_families:
                continue
            market_id = selection["market_id"]
            if market_id in seen_market_ids:
                continue
            seen_market_ids.add(market_id)
            selections[selection["family"]].append(selection)

    selections["moneyline"].sort(key=lambda item: 0 if item["line"] == home_team else 1)
    selections["totals"].sort(key=lambda item: _extract_numeric_value(item["line"]))
    selections["spreads"].sort(key=lambda item: (normalize_text(item["line"]), item["slug"]))
    selections["both_teams_to_score"].sort(key=lambda item: item["slug"])
    return selections


def build_fixture_orderbooks_payload(
    event: Dict[str, Any],
    selected_markets: Dict[str, List[Dict[str, Any]]],
    snapshots: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    home_team, away_team = _event_team_names(event)
    home_team_id, away_team_id = _event_team_ids(event)
    books_by_token = {
        book.asset_id: book
        for book in (
            book_from_snapshot(snapshot)
            for snapshot in snapshots
            if isinstance(snapshot, dict)
        )
    }
    payload = {
        "fixture_slug": str(event.get("slug") or ""),
        "home_team": home_team,
        "home_team_id": home_team_id,
        "away_team": away_team,
        "away_team_id": away_team_id,
        "league": _event_league_code(event),
        "markets": {family: [] for family in FAMILY_KEYS},
    }
    for family in FAMILY_KEYS:
        for market in selected_markets.get(family, []):
            yes_book = books_by_token.get(market["yes_token_id"])
            no_book = books_by_token.get(market["no_token_id"])
            payload["markets"][family].append(
                {
                    "market_id": market["market_id"],
                    "market_type": family,
                    "line": market["line"],
                    "question": market["question"],
                    "slug": market["slug"],
                    "yes_token_id": market["yes_token_id"],
                    "no_token_id": market["no_token_id"],
                    "bids": {
                        "yes": _sorted_levels(yes_book.bids, reverse=True) if yes_book else [],
                        "no": _sorted_levels(no_book.bids, reverse=True) if no_book else [],
                    },
                    "asks": {
                        "yes": _sorted_levels(yes_book.asks, reverse=False) if yes_book else [],
                        "no": _sorted_levels(no_book.asks, reverse=False) if no_book else [],
                    },
                    "best_bid_yes": yes_book.best_bid() if yes_book else "0",
                    "best_ask_yes": yes_book.best_ask() if yes_book else "0",
                    "best_bid_no": no_book.best_bid() if no_book else "0",
                    "best_ask_no": no_book.best_ask() if no_book else "0",
                    "active": bool(market["active"]),
                    "closed": bool(market["closed"]),
                }
            )
    return payload


def _group_events_by_base_slug(events: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for event in events:
        slug = str(event.get("slug") or "")
        if not slug:
            continue
        base_slug = _base_fixture_slug(slug)
        bucket = grouped.setdefault(base_slug, {"base_event": None, "events": []})
        bucket["events"].append(event)
        if slug == base_slug:
            bucket["base_event"] = event

    return {
        base_slug: {
            "base_event": bucket["base_event"] or bucket["events"][0],
            "events": sorted(bucket["events"], key=lambda item: str(item.get("slug") or "")),
        }
        for base_slug, bucket in grouped.items()
        if bucket["events"]
    }


def _base_fixture_slug(slug: str) -> str:
    for suffix in RELATED_EVENT_SUFFIXES:
        if slug.endswith(suffix):
            return slug[: -len(suffix)]
    return slug


def _is_fixture_event(event: Dict[str, Any], league_code: str = "") -> bool:
    slug = str(event.get("slug") or "")
    title = str(event.get("title") or "")
    teams = event.get("teams") or []
    if any(slug.endswith(suffix) for suffix in RELATED_EVENT_SUFFIXES):
        return False
    if not isinstance(teams, list) or len(teams) < 2:
        return False
    if " vs. " not in title and " vs " not in title:
        return False
    event_league = normalize_text(_event_league_code(event))
    if league_code and event_league and event_league != normalize_text(league_code):
        return False
    return True


def _event_start_time(event: Dict[str, Any]) -> Optional[datetime]:
    return parse_datetime(
        str(
            event.get("startTime")
            or event.get("endDate")
            or event.get("eventDate")
            or event.get("startDate")
            or ""
        )
    )


def _classify_market(
    market: Dict[str, Any],
    home_team: str,
    away_team: str,
    include_draw: bool = False,
    all_total_lines: bool = False,
) -> Optional[Dict[str, Any]]:
    market_id = str(market.get("id") or market.get("conditionId") or "")
    question = str(market.get("question") or "")
    slug = str(market.get("slug") or "")
    token_ids = parse_jsonish_list(market.get("clobTokenIds"))
    if not market_id or len(token_ids) < 2:
        return None

    question_text = normalize_text(question)
    group_title = str(market.get("groupItemTitle") or "")
    market_type_hint = normalize_text(str(market.get("sportsMarketType") or market.get("marketType") or ""))

    if "both teams to score" in question_text or market_type_hint in {
        "both teams to score",
        "both teams to score market",
        "btts",
    }:
        return _selection(
            market=market,
            family="both_teams_to_score",
            line="both teams to score",
            token_ids=token_ids,
        )

    if "moneyline" in market_type_hint:
        if "draw" in question_text:
            if not include_draw:
                return None
            return _selection(
                market=market,
                family="moneyline",
                line="draw",
                token_ids=token_ids,
            )
        if _team_matches_text(home_team, question, group_title):
            return _selection(
                market=market,
                family="moneyline",
                line=home_team,
                token_ids=token_ids,
            )
        if _team_matches_text(away_team, question, group_title):
            return _selection(
                market=market,
                family="moneyline",
                line=away_team,
                token_ids=token_ids,
            )
        return None

    if "spread" in market_type_hint or "handicap" in question_text:
        spread_value = _extract_spread_value(market)
        if not spread_value:
            return None
        if _team_matches_text(home_team, question, group_title):
            line = f"{home_team} {spread_value}"
        elif _team_matches_text(away_team, question, group_title):
            line = f"{away_team} {spread_value}"
        elif _team_matches_text(home_team, group_title):
            line = f"{home_team} {spread_value}"
        elif _team_matches_text(away_team, group_title):
            line = f"{away_team} {spread_value}"
        else:
            return None
        return _selection(
            market=market,
            family="spreads",
            line=line,
            token_ids=token_ids,
        )

    if "total" in market_type_hint or "over" in question_text:
        total_line = _extract_total_line(market)
        if total_line and (all_total_lines or total_line in TOTAL_LINES):
            return _selection(
                market=market,
                family="totals",
                line=f"over {total_line}",
                token_ids=token_ids,
            )
    return None


def _selection(market: Dict[str, Any], family: str, line: str, token_ids: Sequence[str]) -> Dict[str, Any]:
    return {
        "market_id": str(market.get("id") or market.get("conditionId") or ""),
        "question": str(market.get("question") or ""),
        "slug": str(market.get("slug") or ""),
        "family": family,
        "line": line,
        "yes_token_id": token_ids[0] if len(token_ids) > 0 else "",
        "no_token_id": token_ids[1] if len(token_ids) > 1 else "",
        "active": bool(market.get("active", False)),
        "closed": bool(market.get("closed", False)),
    }


def _event_matches(
    event: Dict[str, Any],
    home_team: str,
    away_team: str,
    league_code: str,
    kickoff: datetime,
    tolerance_minutes: int,
) -> bool:
    home_event_team, away_event_team = _event_team_names(event)
    if not _team_matches_query(home_team, home_event_team, _event_team_abbreviation(event, 0)):
        return False
    if not _team_matches_query(away_team, away_event_team, _event_team_abbreviation(event, 1)):
        return False
    if league_code and normalize_text(_event_league_code(event)) != normalize_text(league_code):
        return False

    event_start = parse_datetime(
        str(event.get("startTime") or event.get("endDate") or event.get("startDate") or "")
    )
    if not event_start:
        return False
    delta_minutes = abs(int((event_start - kickoff).total_seconds() / 60))
    return delta_minutes <= tolerance_minutes


def _event_team_names(event: Dict[str, Any]) -> tuple[str, str]:
    teams = event.get("teams") or []
    if isinstance(teams, list) and len(teams) >= 2:
        home_team = str((teams[0] or {}).get("name") or "")
        away_team = str((teams[1] or {}).get("name") or "")
        return home_team, away_team
    title = str(event.get("title") or "")
    if " vs. " in title:
        left, right = title.split(" vs. ", 1)
        return left.strip(), right.strip()
    if " vs " in title:
        left, right = title.split(" vs ", 1)
        return left.strip(), right.strip()
    return "", ""


def _event_team_ids(event: Dict[str, Any]) -> tuple[str, str]:
    teams = event.get("teams") or []
    if isinstance(teams, list) and len(teams) >= 2:
        home_team_id = str((teams[0] or {}).get("id") or "")
        away_team_id = str((teams[1] or {}).get("id") or "")
        return home_team_id, away_team_id
    return "", ""


def _event_team_abbreviation(event: Dict[str, Any], index: int) -> str:
    teams = event.get("teams") or []
    if isinstance(teams, list) and len(teams) > index and isinstance(teams[index], dict):
        return str(teams[index].get("abbreviation") or "")
    return ""


def _event_league_code(event: Dict[str, Any]) -> str:
    teams = event.get("teams") or []
    if isinstance(teams, list) and teams and isinstance(teams[0], dict):
        league_code = str(teams[0].get("league") or "")
        if league_code:
            return league_code
    for tag in event.get("tags", []):
        if not isinstance(tag, dict):
            continue
        slug = str(tag.get("slug") or "")
        if slug not in {"sports", "games", "soccer"}:
            return slug
    return ""


def _team_matches_query(query: str, team_name: str, abbreviation: str) -> bool:
    query_text = normalize_text(query)
    team_text = normalize_text(team_name)
    abbreviation_text = normalize_text(abbreviation)
    if not query_text or not team_text:
        return False
    if query_text == team_text or query_text == abbreviation_text:
        return True
    if query_text in team_text or team_text in query_text:
        return True
    return abbreviation_text.startswith(query_text) if abbreviation_text else False


def _team_matches_text(team_name: str, *haystacks: str) -> bool:
    team_text = normalize_text(team_name)
    if not team_text:
        return False
    for haystack in haystacks:
        haystack_text = normalize_text(haystack)
        if team_text and team_text in haystack_text:
            return True
    return False


def _extract_total_line(market: Dict[str, Any]) -> str:
    line_value = market.get("line")
    if line_value not in (None, ""):
        return _format_numeric_line(line_value).lstrip("+")
    question = str(market.get("question") or "")
    match = TOTAL_RE.search(question)
    if match:
        return match.group(1)
    return ""


def _extract_spread_value(market: Dict[str, Any]) -> str:
    line_value = market.get("line")
    if line_value not in (None, ""):
        return _format_numeric_line(line_value)
    for value in (
        str(market.get("groupItemTitle") or ""),
        str(market.get("question") or ""),
        str(market.get("slug") or ""),
    ):
        match = SIGNED_NUMBER_RE.search(value)
        if match:
            return match.group(1)
    return ""


def _format_numeric_line(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return ""
    try:
        number = float(text)
    except ValueError:
        match = SIGNED_NUMBER_RE.search(text)
        return match.group(1) if match else text
    if number > 0:
        return f"+{number:.1f}"
    return f"{number:.1f}"


def _extract_numeric_value(text: str) -> float:
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    if not match:
        return 999.0
    return float(match.group(1))


def _token_ids_for_markets(selected_markets: Dict[str, Iterable[Dict[str, Any]]]) -> List[str]:
    token_ids: List[str] = []
    seen = set()
    for family in FAMILY_KEYS:
        for market in selected_markets.get(family, []):
            for token_id in (market["yes_token_id"], market["no_token_id"]):
                if token_id and token_id not in seen:
                    seen.add(token_id)
                    token_ids.append(token_id)
    return token_ids


def _sorted_levels(levels: Dict[str, str], reverse: bool) -> List[Dict[str, str]]:
    return [
        {"price": price, "size": levels[price]}
        for price in sorted(levels, key=lambda item: float(item), reverse=reverse)
    ]
