from __future__ import annotations

import csv
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlparse

from pred_polymarket_sync.models import PolymarketLeague, PolymarketTeam
from pred_polymarket_sync.utils import ensure_dir, normalize_text


def export_polymarket_reference_data(
    teams: Sequence[PolymarketTeam],
    leagues: Sequence[PolymarketLeague],
    output_dir: Path,
    base_name: str = "polymarket",
    football_only: bool = False,
) -> dict:
    output_dir = ensure_dir(output_dir)
    teams, leagues = filter_reference_data(
        teams=teams,
        leagues=leagues,
        football_only=football_only,
    )

    teams_csv = output_dir / f"{base_name}_teams.csv"
    leagues_csv = output_dir / f"{base_name}_leagues.csv"
    teams_sql = output_dir / f"{base_name}_teams.sql"
    leagues_sql = output_dir / f"{base_name}_leagues.sql"

    teams_rows = build_team_rows(teams, leagues)
    leagues_rows = build_league_rows(leagues)

    write_csv(teams_csv, teams_rows)
    write_csv(leagues_csv, leagues_rows)
    write_teams_sql(teams_sql, teams_rows)
    write_leagues_sql(leagues_sql, leagues_rows)

    return {
        "teams_csv": str(teams_csv),
        "leagues_csv": str(leagues_csv),
        "teams_sql": str(teams_sql),
        "leagues_sql": str(leagues_sql),
        "team_count": len(teams),
        "league_count": len(leagues),
    }


def write_csv(path: Path, rows: Sequence[dict]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        if not rows:
            handle.write("")
            return
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def build_team_rows(
    teams: Sequence[PolymarketTeam],
    leagues: Sequence[PolymarketLeague],
) -> Sequence[dict]:
    league_id_by_code = {league.league_code: league.id for league in leagues}
    return [
        {
            "team_id": team.id,
            "league_id": league_id_by_code.get(team.league_code, ""),
            "name": team.name,
            "alternate_name": team.alias or team.abbreviation or "",
            "team_location": "",
            "logo_url": team.logo,
            "theme_color": team.color,
        }
        for team in teams
    ]


def build_league_rows(leagues: Sequence[PolymarketLeague]) -> Sequence[dict]:
    return [
        {
            "league_id": league.id,
            "name": friendly_league_name(league.league_code),
            "alternate_name": league.league_code,
            "sport": derive_sport_family(league.league_code, league.resolution),
            "association": derive_association(league.resolution),
        }
        for league in leagues
    ]


def filter_reference_data(
    teams: Sequence[PolymarketTeam],
    leagues: Sequence[PolymarketLeague],
    football_only: bool = False,
) -> tuple[list[PolymarketTeam], list[PolymarketLeague]]:
    filtered_teams = list(teams)
    filtered_leagues = list(leagues)
    if football_only:
        filtered_leagues = [league for league in filtered_leagues if is_football_league(league)]
        league_codes = {league.league_code for league in filtered_leagues}
        filtered_teams = [team for team in filtered_teams if team.league_code in league_codes]
    return filtered_teams, filtered_leagues


def search_team_rows(
    teams: Sequence[PolymarketTeam],
    leagues: Sequence[PolymarketLeague],
    query: str = "",
    league_id: str = "",
    league_code: str = "",
    football_only: bool = False,
    limit: int = 25,
) -> list[dict]:
    filtered_teams, filtered_leagues = filter_reference_data(
        teams=teams,
        leagues=leagues,
        football_only=football_only,
    )
    league_id_by_code = {league.league_code: league.id for league in filtered_leagues}
    query_text = normalize_text(query)
    league_code_text = normalize_text(league_code)
    ranked: list[tuple[int, dict]] = []
    for team in filtered_teams:
        row = {
            "team_id": team.id,
            "league_id": league_id_by_code.get(team.league_code, ""),
            "name": team.name,
            "alternate_name": team.alias or team.abbreviation or "",
            "team_location": "",
            "logo_url": team.logo,
            "theme_color": team.color,
        }
        if league_id and row["league_id"] != str(league_id):
            continue
        if league_code_text and normalize_text(team.league_code) != league_code_text:
            continue
        score = _team_search_score(team, query_text)
        if query_text and score <= 0:
            continue
        ranked.append((score, row))
    ranked.sort(key=lambda item: (-item[0], normalize_text(item[1]["name"]), item[1]["team_id"]))
    return [row for _, row in ranked[: max(limit, 1)]]


def search_league_rows(
    leagues: Sequence[PolymarketLeague],
    query: str = "",
    sport: str = "",
    football_only: bool = False,
    limit: int = 25,
) -> list[dict]:
    _, filtered_leagues = filter_reference_data(
        teams=[],
        leagues=leagues,
        football_only=football_only,
    )
    sport_text = normalize_text(sport)
    query_text = normalize_text(query)
    ranked: list[tuple[int, dict]] = []
    for league in filtered_leagues:
        row = {
            "league_id": league.id,
            "name": friendly_league_name(league.league_code),
            "alternate_name": league.league_code,
            "sport": derive_sport_family(league.league_code, league.resolution),
            "association": derive_association(league.resolution),
        }
        if sport_text and normalize_text(str(row.get("sport", ""))) != sport_text:
            continue
        score = _league_search_score(row, query_text)
        if query_text and score <= 0:
            continue
        ranked.append((score, row))
    ranked.sort(key=lambda item: (-item[0], normalize_text(item[1]["name"]), item[1]["league_id"]))
    return [row for _, row in ranked[: max(limit, 1)]]


def _team_search_score(team: PolymarketTeam, query_text: str) -> int:
    if not query_text:
        return 1
    haystacks = [
        normalize_text(team.name),
        normalize_text(team.alias),
        normalize_text(team.abbreviation),
        normalize_text(team.league_code),
    ]
    if haystacks[0] == query_text:
        return 100
    if haystacks[1] == query_text or haystacks[2] == query_text:
        return 90
    if haystacks[0].startswith(query_text):
        return 80
    if any(haystack.startswith(query_text) for haystack in haystacks[1:3] if haystack):
        return 70
    if query_text in haystacks[0]:
        return 60
    if any(query_text in haystack for haystack in haystacks[1:] if haystack):
        return 50
    return 0


def _league_search_score(row: dict, query_text: str) -> int:
    if not query_text:
        return 1
    name = normalize_text(str(row.get("name", "")))
    alternate_name = normalize_text(str(row.get("alternate_name", "")))
    association = normalize_text(str(row.get("association", "")))
    if name == query_text or alternate_name == query_text:
        return 100
    if name.startswith(query_text) or alternate_name.startswith(query_text):
        return 80
    if query_text in name or query_text in alternate_name:
        return 60
    if query_text in association:
        return 40
    return 0


def write_teams_sql(path: Path, rows: Sequence[dict]) -> None:
    lines = [
        "-- Polymarket teams reference export",
        "CREATE TABLE IF NOT EXISTS polymarket_teams_reference (",
        "    team_id INTEGER PRIMARY KEY,",
        "    league_id INTEGER NOT NULL,",
        "    name TEXT NOT NULL,",
        "    alternate_name TEXT NOT NULL,",
        "    team_location TEXT NOT NULL,",
        "    logo_url TEXT NOT NULL,",
        "    theme_color TEXT NOT NULL",
        ");",
        "",
        "DELETE FROM polymarket_teams_reference;",
    ]
    for row in rows:
        lines.append(
            "INSERT INTO polymarket_teams_reference "
            "(team_id, league_id, name, alternate_name, team_location, logo_url, theme_color) "
            f"VALUES ({sql_int(row['team_id'])}, {sql_int(row['league_id'])}, {sql_text(row['name'])}, "
            f"{sql_text(row['alternate_name'])}, {sql_text(row['team_location'])}, "
            f"{sql_text(row['logo_url'])}, {sql_text(row['theme_color'])});"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_leagues_sql(path: Path, rows: Sequence[dict]) -> None:
    lines = [
        "-- Polymarket leagues reference export",
        "CREATE TABLE IF NOT EXISTS polymarket_leagues_reference (",
        "    league_id INTEGER PRIMARY KEY,",
        "    name TEXT NOT NULL,",
        "    alternate_name TEXT NOT NULL,",
        "    sport TEXT NOT NULL,",
        "    association TEXT NOT NULL",
        ");",
        "",
        "DELETE FROM polymarket_leagues_reference;",
    ]
    for row in rows:
        lines.append(
            "INSERT INTO polymarket_leagues_reference "
            "(league_id, name, alternate_name, sport, association) "
            f"VALUES ({sql_int(row['league_id'])}, {sql_text(row['name'])}, {sql_text(row['alternate_name'])}, "
            f"{sql_text(row['sport'])}, {sql_text(row['association'])});"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


LEAGUE_NAME_OVERRIDES = {
    "epl": "Premier League",
    "lal": "LaLiga",
    "ucl": "UEFA Champions League",
    "bun": "Bundesliga",
    "sea": "Serie A",
    "fl1": "Ligue 1",
    "mlb": "Major League Baseball",
    "nfl": "National Football League",
    "nba": "National Basketball Association",
    "wnba": "Women's National Basketball Association",
    "atp": "ATP Tour",
    "wta": "WTA Tour",
    "ipl": "Indian Premier League",
}


SPORT_FAMILY_OVERRIDES = {
    "epl": "soccer",
    "lal": "soccer",
    "ucl": "soccer",
    "bun": "soccer",
    "sea": "soccer",
    "fl1": "soccer",
    "mlb": "baseball",
    "nfl": "american_football",
    "cfb": "american_football",
    "ncaab": "basketball",
    "nba": "basketball",
    "wnba": "basketball",
    "cwbb": "basketball",
    "bkligend": "basketball",
    "bknbl": "basketball",
    "atp": "tennis",
    "wta": "tennis",
    "ipl": "cricket",
    "odi": "cricket",
    "t20": "cricket",
    "abb": "cricket",
    "csa": "cricket",
    "crban": "cricket",
    "wbc": "baseball",
    "lol": "esports",
    "fifa": "esports",
    "val": "esports",
    "cs2": "esports",
    "csgo": "esports",
    "dota2": "esports",
    "mlbb": "esports",
    "ow": "esports",
    "codmw": "esports",
    "pubg": "esports",
    "r6siege": "esports",
    "rl": "esports",
    "rutopft": "rugby",
}


def friendly_league_name(league_code: str) -> str:
    code = (league_code or "").strip().lower()
    if not code:
        return ""
    if code in LEAGUE_NAME_OVERRIDES:
        return LEAGUE_NAME_OVERRIDES[code]
    return code.upper()


def derive_sport_family(league_code: str, resolution_url: str) -> str:
    code = (league_code or "").strip().lower()
    if code in SPORT_FAMILY_OVERRIDES:
        return SPORT_FAMILY_OVERRIDES[code]
    host = urlparse(resolution_url or "").netloc.lower()
    if any(
        token in host
        for token in (
            "uefa",
            "fifa",
            "premierleague",
            "laliga",
            "bundesliga",
            "seriea",
            "ligue1",
            "cafonline",
            "the-afc",
            "oceaniafootball",
            "eredivisie",
            "afa.com.ar",
            "ligamx",
            "conmebol",
            "ligaportugal",
            "mlssoccer",
            "efl.com",
            "spfl.co.uk",
            "rfs.ru",
            "thefa.com",
            "tff.org",
            "jleague",
            "kleague",
            "ussoccer",
        )
    ):
        return "soccer"
    if "mlb" in host:
        return "baseball"
    if any(token in host for token in ("nba", "wnba", "ncaa")):
        return "basketball"
    if any(token in host for token in ("nfl", "cfb")):
        return "american_football"
    if any(token in host for token in ("atptour", "wtatennis")):
        return "tennis"
    if any(token in host for token in ("icc-cricket", "iplt20", "bigbash", "cricket")):
        return "cricket"
    if any(token in host for token in ("liquipedia", "hltv")):
        return "esports"
    if any(token in host for token in ("lnr.fr", "top-14", "world.rugby")):
        return "rugby"
    return ""


def derive_association(resolution_url: str) -> str:
    host = urlparse(resolution_url or "").netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


FOOTBALL_LEAGUE_CODES = {
    "epl",
    "lal",
    "ucl",
    "bun",
    "sea",
    "fl1",
    "acn",
    "afc",
    "ofc",
    "fif",
    "ere",
    "arg",
    "itc",
    "mex",
    "sud",
    "tur",
    "cof",
    "caf",
    "rus",
    "efa",
    "efl",
    "mls",
    "por",
    "sco",
    "ned",
    "bel",
    "ukr",
    "jpl",
    "ksa",
    "brz",
    "ger",
    "fra",
    "esp",
    "ita",
    "ned",
    "lib",
    "con",
    "ueu",
    "wcq",
    "eqw",
    "eqm",
    "uclw",
}


def is_football_league(league: PolymarketLeague) -> bool:
    code = (league.league_code or "").strip().lower()
    if code in FOOTBALL_LEAGUE_CODES:
        return True
    sport = derive_sport_family(league.league_code, league.resolution)
    if sport == "soccer":
        return True
    association = derive_association(league.resolution)
    return any(
        token in association
        for token in (
            "football",
            "premierleague",
            "laliga",
            "bundesliga",
            "ligue1",
            "seriea",
            "fifa",
            "uefa",
            "eredivisie",
            "conmebol",
            "ligamx",
            "the-afc",
            "cafonline",
            "mlssoccer",
            "ligaportugal",
        )
    )


def sql_text(value: str) -> str:
    return "'" + (value or "").replace("'", "''") + "'"


def sql_int(value: str) -> str:
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return "0"
