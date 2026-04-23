from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class LeagueMapping:
    cms_league_id: str
    cms_league_name: str
    polymarket_sport_slug: str
    polymarket_sport_id: str
    polymarket_league_slug: str   # tag_slug used in Gamma API calls (e.g. "epl", "lal")
    polymarket_league_name: str
    status: str
    notes: str


@dataclass
class TeamMapping:
    cms_team_id: str
    cms_league_id: str
    cms_team_name: str
    polymarket_team_id: str       # numeric string from teams CSV (team_id column)
    polymarket_league_id: str
    polymarket_team_name: str     # full name (name column)
    polymarket_team_slug: str     # short slug (alternate_name column)
    status: str
    notes: str


class MappingStore:
    """
    In-memory index of curated CMS↔Polymarket league and team mappings.

    Only rows with status == "mapped" are loaded.  The store is read-only
    after construction and safe for concurrent reads from the HTTP server.
    """

    def __init__(self) -> None:
        self._leagues: Dict[str, LeagueMapping] = {}
        self._teams: Dict[str, TeamMapping] = {}

    @classmethod
    def from_csv(cls, league_csv_path: str, team_csv_path: str) -> "MappingStore":
        store = cls()
        store._load_leagues(league_csv_path)
        store._load_teams(team_csv_path)
        return store

    @classmethod
    def from_json(cls, json_path: str) -> "MappingStore":
        """Load mappings from a JSON file (exports/mappings.json)."""
        store = cls()
        try:
            with open(json_path, encoding="utf-8") as fh:
                data = json.load(fh)
            for entry in data.get("leagues", []):
                cms_id = (entry.get("cms_league_id") or "").strip()
                if not cms_id:
                    continue
                store._leagues[cms_id] = LeagueMapping(
                    cms_league_id=cms_id,
                    cms_league_name=(entry.get("cms_league_name") or "").strip(),
                    polymarket_sport_slug=(entry.get("polymarket_sport_slug") or "").strip(),
                    polymarket_sport_id=(entry.get("polymarket_sport_id") or "").strip(),
                    polymarket_league_slug=(entry.get("polymarket_league_slug") or "").strip(),
                    polymarket_league_name=(entry.get("polymarket_league_name") or "").strip(),
                    status="mapped",
                    notes=(entry.get("notes") or "").strip(),
                )
            for entry in data.get("teams", []):
                cms_id = (entry.get("cms_team_id") or "").strip()
                if not cms_id:
                    continue
                store._teams[cms_id] = TeamMapping(
                    cms_team_id=cms_id,
                    cms_league_id=(entry.get("cms_league_id") or "").strip(),
                    cms_team_name=(entry.get("cms_team_name") or "").strip(),
                    polymarket_team_id=(entry.get("polymarket_team_id") or "").strip(),
                    polymarket_league_id=(entry.get("polymarket_league_id") or "").strip(),
                    polymarket_team_name=(entry.get("polymarket_team_name") or "").strip(),
                    polymarket_team_slug=(entry.get("polymarket_team_slug") or "").strip(),
                    status="mapped",
                    notes=(entry.get("notes") or "").strip(),
                )
        except FileNotFoundError:
            pass
        return store

    def merge(self, other: "MappingStore") -> None:
        """Merge entries from another store into this one (other takes priority)."""
        self._leagues.update(other._leagues)
        self._teams.update(other._teams)

    @classmethod
    def clone(cls, source: "MappingStore") -> "MappingStore":
        """Shallow-copy a store (values are immutable dataclasses, so shallow is fine)."""
        store = cls()
        store._leagues = dict(source._leagues)
        store._teams = dict(source._teams)
        return store

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    def get_league(self, cms_league_id: str) -> Optional[LeagueMapping]:
        return self._leagues.get(cms_league_id.strip())

    def get_team(self, cms_team_id: str) -> Optional[TeamMapping]:
        return self._teams.get(cms_team_id.strip())

    @property
    def league_count(self) -> int:
        return len(self._leagues)

    @property
    def team_count(self) -> int:
        return len(self._teams)

    # ------------------------------------------------------------------
    # Loaders
    # ------------------------------------------------------------------

    def _load_leagues(self, path: str) -> None:
        try:
            with open(path, newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    if (row.get("status") or "").strip() != "mapped":
                        continue
                    cms_id = (row.get("cms_league_id") or "").strip()
                    if not cms_id:
                        continue
                    self._leagues[cms_id] = LeagueMapping(
                        cms_league_id=cms_id,
                        cms_league_name=(row.get("cms_league_name") or "").strip(),
                        polymarket_sport_slug=(row.get("polymarket_sport_slug") or "").strip(),
                        polymarket_sport_id=(row.get("polymarket_sport_id") or "").strip(),
                        polymarket_league_slug=(row.get("polymarket_league_slug") or "").strip(),
                        polymarket_league_name=(row.get("polymarket_league_name") or "").strip(),
                        status="mapped",
                        notes=(row.get("notes") or "").strip(),
                    )
        except FileNotFoundError:
            pass

    def _load_teams(self, path: str) -> None:
        try:
            with open(path, newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    if (row.get("status") or "").strip() != "mapped":
                        continue
                    cms_id = (row.get("cms_team_id") or "").strip()
                    if not cms_id:
                        continue
                    self._teams[cms_id] = TeamMapping(
                        cms_team_id=cms_id,
                        cms_league_id=(row.get("cms_league_id") or "").strip(),
                        cms_team_name=(row.get("cms_team_name") or "").strip(),
                        polymarket_team_id=(row.get("polymarket_team_id") or "").strip(),
                        polymarket_league_id=(row.get("polymarket_league_id") or "").strip(),
                        polymarket_team_name=(row.get("polymarket_team_name") or "").strip(),
                        polymarket_team_slug=(row.get("polymarket_team_slug") or "").strip(),
                        status="mapped",
                        notes=(row.get("notes") or "").strip(),
                    )
        except FileNotFoundError:
            pass
