from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urlparse

from pred_polymarket_sync.models import (
    PredFixture,
    PredMarket,
    PredParentMarket,
    SportsDataFixtureMapping,
    SportsDataLeagueMapping,
    SportsDataTeamMapping,
)
from pred_polymarket_sync.utils import (
    load_semicolon_csv,
    parse_datetime,
    validate_table_name,
)


@dataclass
class DataSnapshot:
    markets: List[PredMarket]
    parent_markets: List[PredParentMarket]
    fixtures: List[PredFixture]
    fixture_mappings: List[SportsDataFixtureMapping]
    team_mappings: List[SportsDataTeamMapping]
    league_mappings: List[SportsDataLeagueMapping]


class PredDataSource(ABC):
    @abstractmethod
    def load_snapshot(self) -> DataSnapshot:
        raise NotImplementedError


class CsvPredDataSource(PredDataSource):
    def __init__(
        self,
        markets_csv: str,
        parent_markets_csv: str,
        fixtures_csv: str,
        fixture_mappings_csv: str,
        team_mappings_csv: str,
        league_mappings_csv: str,
    ):
        self.markets_csv = markets_csv
        self.parent_markets_csv = parent_markets_csv
        self.fixtures_csv = fixtures_csv
        self.fixture_mappings_csv = fixture_mappings_csv
        self.team_mappings_csv = team_mappings_csv
        self.league_mappings_csv = league_mappings_csv

    def load_snapshot(self) -> DataSnapshot:
        return DataSnapshot(
            markets=[self._market(row) for row in load_semicolon_csv(self.markets_csv)],
            parent_markets=[
                self._parent_market(row)
                for row in load_semicolon_csv(self.parent_markets_csv)
            ],
            fixtures=[self._fixture(row) for row in load_semicolon_csv(self.fixtures_csv)],
            fixture_mappings=[
                self._fixture_mapping(row)
                for row in load_semicolon_csv(self.fixture_mappings_csv)
            ],
            team_mappings=[
                self._team_mapping(row)
                for row in load_semicolon_csv(self.team_mappings_csv)
            ],
            league_mappings=[
                self._league_mapping(row)
                for row in load_semicolon_csv(self.league_mappings_csv)
            ],
        )

    def _market(self, row: Dict[str, str]) -> PredMarket:
        return PredMarket(
            market_id=row.get("market_id", ""),
            parent_market_id=row.get("parent_market_id", ""),
            team_id=row.get("team_id", ""),
            name=row.get("name", ""),
            market_canonical_name=row.get("market_canonical_name", ""),
            market_code=row.get("market_code", ""),
            rules=row.get("rules", ""),
            status=row.get("status", ""),
            yes_position_id=row.get("yes_position_id", ""),
            no_position_id=row.get("no_position_id", ""),
        )

    def _parent_market(self, row: Dict[str, str]) -> PredParentMarket:
        return PredParentMarket(
            parent_market_id=row.get("parent_market_id", ""),
            league_id=row.get("league_id", ""),
            type_reference_id=row.get("type_reference_id", ""),
            title=row.get("title", ""),
            description=row.get("description", ""),
            status=row.get("status", ""),
            markets_open_time=parse_datetime(row.get("markets_open_time")),
            markets_close_time=parse_datetime(row.get("markets_close_time")),
            payout_time=parse_datetime(row.get("payout_time")),
        )

    def _fixture(self, row: Dict[str, str]) -> PredFixture:
        return PredFixture(
            fixture_id=row.get("fixture_id", ""),
            name=row.get("name", ""),
            league_id=row.get("league_id", ""),
            home_team_id=row.get("home_team_id", ""),
            away_team_id=row.get("away_team_id", ""),
        )

    def _fixture_mapping(self, row: Dict[str, str]) -> SportsDataFixtureMapping:
        return SportsDataFixtureMapping(
            cms_fixture_id=row.get("cms_fixture_id", ""),
            cms_league_id=row.get("cms_league_id", ""),
            sportsdata_game_id=row.get("sportsdata_game_id", ""),
            match_date=parse_datetime(row.get("match_date")),
            status=row.get("status", ""),
        )

    def _team_mapping(self, row: Dict[str, str]) -> SportsDataTeamMapping:
        return SportsDataTeamMapping(
            cms_team_id=row.get("cms_team_id", ""),
            cms_league_id=row.get("cms_league_id", ""),
            sportsdata_team_id=row.get("sportsdata_team_id", ""),
            status=row.get("status", ""),
        )

    def _league_mapping(self, row: Dict[str, str]) -> SportsDataLeagueMapping:
        return SportsDataLeagueMapping(
            cms_league_id=row.get("cms_league_id", ""),
            sportsdata_competition_id=row.get("sportsdata_competition_id", ""),
            sportsdata_season=row.get("sportsdata_season", ""),
            status=row.get("status", ""),
        )


class DatabasePredDataSource(PredDataSource):
    def __init__(
        self,
        database_url: str,
        markets_table: str,
        parent_markets_table: str,
        fixtures_table: str,
        fixture_mappings_table: str,
        team_mappings_table: str,
        league_mappings_table: str,
    ):
        if not database_url:
            raise ValueError("DATABASE_URL is required for database mode")
        self.database_url = database_url
        self.markets_table = validate_table_name(markets_table)
        self.parent_markets_table = validate_table_name(parent_markets_table)
        self.fixtures_table = validate_table_name(fixtures_table)
        self.fixture_mappings_table = validate_table_name(fixture_mappings_table)
        self.team_mappings_table = validate_table_name(team_mappings_table)
        self.league_mappings_table = validate_table_name(league_mappings_table)

    def load_snapshot(self) -> DataSnapshot:
        with self._connect() as connection:
            markets = [self._market(row) for row in self._query(connection, self.markets_table)]
            parent_markets = [
                self._parent_market(row)
                for row in self._query(connection, self.parent_markets_table)
            ]
            fixtures = [self._fixture(row) for row in self._query(connection, self.fixtures_table)]
            fixture_mappings = [
                self._fixture_mapping(row)
                for row in self._query(connection, self.fixture_mappings_table)
            ]
            team_mappings = [
                self._team_mapping(row)
                for row in self._query(connection, self.team_mappings_table)
            ]
            league_mappings = [
                self._league_mapping(row)
                for row in self._query(connection, self.league_mappings_table)
            ]
        return DataSnapshot(
            markets=markets,
            parent_markets=parent_markets,
            fixtures=fixtures,
            fixture_mappings=fixture_mappings,
            team_mappings=team_mappings,
            league_mappings=league_mappings,
        )

    def _connect(self):
        parsed = urlparse(self.database_url)
        scheme = parsed.scheme.lower()
        if scheme in {"postgresql", "postgres"}:
            try:
                import psycopg
            except ImportError as exc:
                raise RuntimeError(
                    "psycopg is not installed. Run: pip install -r requirements.txt"
                ) from exc
            return psycopg.connect(self.database_url)
        if scheme == "mysql":
            try:
                import pymysql
            except ImportError as exc:
                raise RuntimeError(
                    "PyMySQL is not installed. Run: pip install -r requirements.txt"
                ) from exc
            return pymysql.connect(
                host=parsed.hostname,
                port=parsed.port or 3306,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.lstrip("/"),
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
        raise RuntimeError(
            f"Unsupported DATABASE_URL scheme '{parsed.scheme}'. Use postgres/postgresql/mysql."
        )

    def _query(self, connection, table_name: str) -> List[Dict[str, str]]:
        query = f"SELECT * FROM {table_name}"
        if hasattr(connection, "cursor") and "pymysql" in connection.__class__.__module__:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
        else:
            with connection.cursor() as cursor:
                cursor.execute(query)
                columns = [column.name if hasattr(column, "name") else column[0] for column in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return [{str(key): "" if value is None else str(value) for key, value in row.items()} for row in rows]

    def _market(self, row: Dict[str, str]) -> PredMarket:
        return CsvPredDataSource._market(self, row)

    def _parent_market(self, row: Dict[str, str]) -> PredParentMarket:
        return CsvPredDataSource._parent_market(self, row)

    def _fixture(self, row: Dict[str, str]) -> PredFixture:
        return CsvPredDataSource._fixture(self, row)

    def _fixture_mapping(self, row: Dict[str, str]) -> SportsDataFixtureMapping:
        return CsvPredDataSource._fixture_mapping(self, row)

    def _team_mapping(self, row: Dict[str, str]) -> SportsDataTeamMapping:
        return CsvPredDataSource._team_mapping(self, row)

    def _league_mapping(self, row: Dict[str, str]) -> SportsDataLeagueMapping:
        return CsvPredDataSource._league_mapping(self, row)


def build_source_from_args(args, database_url: Optional[str] = None) -> PredDataSource:
    if getattr(args, "markets_csv", None):
        return CsvPredDataSource(
            markets_csv=args.markets_csv,
            parent_markets_csv=args.parent_markets_csv,
            fixtures_csv=args.fixtures_csv,
            fixture_mappings_csv=args.fixture_mappings_csv,
            team_mappings_csv=args.team_mappings_csv,
            league_mappings_csv=args.league_mappings_csv,
        )
    db_url = database_url or os.getenv("DATABASE_URL", "")
    return DatabasePredDataSource(
        database_url=db_url,
        markets_table=args.markets_table,
        parent_markets_table=args.parent_markets_table,
        fixtures_table=args.fixtures_table,
        fixture_mappings_table=args.fixture_mappings_table,
        team_mappings_table=args.team_mappings_table,
        league_mappings_table=args.league_mappings_table,
    )
