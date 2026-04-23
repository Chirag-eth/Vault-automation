from __future__ import annotations

import copy
import concurrent.futures
import threading
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urlparse

from pred_polymarket_sync.fixture_markets import collect_game_lines_events, select_fixture_markets
from pred_polymarket_sync.http import HttpClient
from pred_polymarket_sync.polymarket import PolymarketClient


@dataclass
class AutoDeleteJob:
    job_id: str
    yes_token_id: str
    no_token_id: str
    market_ids: List[str]
    market_names: List[str]
    created_at: float
    status: str = "watching"
    deleted_count: int = 0
    last_condition: Dict[str, Any] | None = None
    last_error: str = ""
    finished_at: float = 0.0


class UatMarketPublisher:
    def __init__(
        self,
        polymarket_client: PolymarketClient,
        base_url: str,
        markets_path: str = "/api/v1/config/markets",
        delete_path_template: str = "/api/v1/config/markets/{market_id}",
        active_markets_path: str = "/api/v1/config/active-markets",
        internal_base_url: str = "",
        internal_active_markets_path: str = "/api/v1/market-discovery/internal/active-markets",
        poll_seconds: int = 15,
        http_timeout_seconds: int = 20,
        publish_workers: int = 8,
        actor_header: str = "X-Actor",
        actor_value: str = "piyush",
    ) -> None:
        self._poly = polymarket_client
        self._base_url = base_url.rstrip("/")
        self._ensure_uat_base_url()
        self._markets_path = markets_path
        self._delete_path_template = delete_path_template
        self._active_markets_path = active_markets_path
        self._internal_base_url = internal_base_url.rstrip("/")
        self._internal_active_markets_path = internal_active_markets_path
        self._poll_seconds = max(poll_seconds, 1)
        self._publish_workers = max(publish_workers, 1)
        self._http = HttpClient(timeout_seconds=http_timeout_seconds)
        self._actor_header = actor_header
        self._actor_value = actor_value
        self._jobs: Dict[str, AutoDeleteJob] = {}
        self._jobs_lock = threading.Lock()

    def publish_to_active_markets(
        self,
        yes_token_id: str,
        no_token_id: str,
        dry_run: bool = False,
        monitor_for_delete: bool = True,
    ) -> Dict[str, Any]:
        if not self._base_url:
            raise RuntimeError("MARKET_MAKING_HOST is not configured")
        if not yes_token_id:
            raise RuntimeError("yes_token_id is required")
        if not no_token_id:
            raise RuntimeError("no_token_id is required")

        active_markets = self._load_active_markets()
        publish_results: List[Dict[str, Any]] = []
        published_market_ids: List[str] = []
        published_market_names: List[str] = []

        for item in active_markets:
            prepared = self._prepare_market_payload(item, yes_token_id=yes_token_id, no_token_id=no_token_id)
            market_id = prepared["market_id"]
            market_name = prepared["market_name"]
            if dry_run:
                publish_results.append(
                    {
                        "status": "skipped",
                        "reason": "dry_run",
                        "market_id": market_id,
                        "market_name": market_name,
                    }
                )
                continue
            try:
                response = self._http.post_json(
                    self._url(self._markets_path),
                    {
                        "market_name": market_name,
                        "market": prepared["market"],
                    },
                    headers=self._post_headers(market_id),
                )
                publish_results.append(
                    {
                        "status": "published",
                        "market_id": market_id,
                        "market_name": market_name,
                        "response": response,
                    }
                )
                published_market_ids.append(market_id)
                published_market_names.append(market_name)
            except RuntimeError as exc:
                publish_results.append(
                    {
                        "status": "failed",
                        "market_id": market_id,
                        "market_name": market_name,
                        "error": str(exc),
                    }
                )

        watcher_job: Optional[AutoDeleteJob] = None
        if monitor_for_delete and not dry_run and published_market_ids:
            watcher_job = self._start_auto_delete_job(
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                market_ids=published_market_ids,
                market_names=published_market_names,
            )

        return {
            "yes_token_id": yes_token_id,
            "no_token_id": no_token_id,
            "active_market_count": len(active_markets),
            "published_count": sum(1 for item in publish_results if item["status"] == "published"),
            "failed_count": sum(1 for item in publish_results if item["status"] == "failed"),
            "results": publish_results,
            "dry_run": dry_run,
            "auto_delete_job": self._job_payload(watcher_job),
        }

    def publish_remaining_active_markets_from_polymarket_url(
        self,
        polymarket_url: str,
        dry_run: bool = False,
        monitor_for_delete: bool = True,
        source_market_id: str = "",
        source_family: str = "",
        source_line: str = "",
    ) -> Dict[str, Any]:
        if not self._base_url:
            raise RuntimeError("MARKET_MAKING_HOST is not configured")
        source_market = self._resolve_source_market(
            polymarket_url=polymarket_url,
            source_market_id=source_market_id,
            source_family=source_family,
            source_line=source_line,
        )
        source_active_markets = self._load_internal_active_markets()
        existing_market_ids = self._load_existing_vault_market_ids()

        prepared_markets: List[Dict[str, Any]] = []
        publish_results: List[Dict[str, Any]] = []
        published_market_ids: List[str] = []
        published_market_names: List[str] = []

        for item in source_active_markets:
            prepared = self._prepare_market_payload(
                item,
                yes_token_id=source_market["yes_token_id"],
                no_token_id=source_market["no_token_id"],
            )
            prepared_markets.append(prepared)

        if dry_run:
            for prepared in prepared_markets:
                market_id = prepared["market_id"]
                market_name = prepared["market_name"]
                publish_results.append(
                    {
                        "status": "skipped",
                        "reason": "dry_run",
                        "market_id": market_id,
                        "market_name": market_name,
                    }
                )
        else:
            max_workers = min(self._publish_workers, max(len(prepared_markets), 1))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(
                        self._post_market_payload,
                        market_id=prepared["market_id"],
                        market_name=prepared["market_name"],
                        market=prepared["market"],
                    ): prepared
                    for prepared in prepared_markets
                }
                for future in concurrent.futures.as_completed(future_map):
                    prepared = future_map[future]
                    market_id = prepared["market_id"]
                    market_name = prepared["market_name"]
                    try:
                        result = future.result()
                    except Exception as exc:
                        result = {
                            "status": "failed",
                            "market_id": market_id,
                            "market_name": market_name,
                            "error": str(exc),
                        }
                    publish_results.append(result)
                    if result["status"] == "published":
                        published_market_ids.append(market_id)
                        published_market_names.append(market_name)

        publish_results.sort(key=lambda item: str(item.get("market_id") or ""))

        watcher_job: Optional[AutoDeleteJob] = None
        if monitor_for_delete and not dry_run and published_market_ids:
            watcher_job = self._start_auto_delete_job(
                yes_token_id=source_market["yes_token_id"],
                no_token_id=source_market["no_token_id"],
                market_ids=published_market_ids,
                market_names=published_market_names,
            )

        return {
            "polymarket_url": polymarket_url,
            "source_market": source_market,
            "source_active_market_count": len(source_active_markets),
            "existing_vault_market_count": len(existing_market_ids),
            "published_count": sum(1 for item in publish_results if item["status"] == "published"),
            "already_exists_count": sum(1 for item in publish_results if item["status"] == "already_exists"),
            "failed_count": sum(1 for item in publish_results if item["status"] == "failed"),
            "results": publish_results,
            "dry_run": dry_run,
            "auto_delete_job": self._job_payload(watcher_job),
        }

    def _ensure_uat_base_url(self) -> None:
        if not self._base_url:
            return
        parsed = urlparse(self._base_url)
        host = (parsed.hostname or "").lower()
        if "uat" not in host:
            raise RuntimeError(
                f"UAT market publisher requires a UAT host, got {self._base_url!r}"
            )

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            return self._job_payload(job)

    def _load_active_markets(self) -> List[Dict[str, Any]]:
        response = self._http.get_json(
            self._url(self._markets_path),
            params={"status": "active"},
            headers=self._base_headers(),
        )
        items = self._extract_market_items(response)
        return [item for item in items if self._is_active_market(item)]

    def _load_internal_active_markets(self) -> List[Dict[str, Any]]:
        if not self._internal_base_url:
            raise RuntimeError("CMS_BASE_URL is not configured for internal active markets")
        response = self._http.get_json(
            self._absolute_url(self._internal_base_url, self._internal_active_markets_path)
        )
        items = self._extract_market_items(response)
        return [item for item in items if self._is_active_market(item)]

    def _load_existing_vault_market_ids(self) -> set[str]:
        response = self._http.get_json(
            self._url(self._active_markets_path),
            headers=self._base_headers(),
        )
        market_ids = set()
        if isinstance(response, dict):
            data = response.get("data")
            if isinstance(data, dict):
                raw_ids = data.get("market_ids")
                if isinstance(raw_ids, list):
                    market_ids.update(str(item) for item in raw_ids if item)
        for item in self._extract_market_items(response):
            market_id = self._extract_market_id(item, self._extract_market_dict(item))
            if market_id:
                market_ids.add(market_id)
        return market_ids

    def _prepare_market_payload(
        self,
        item: Dict[str, Any],
        yes_token_id: str,
        no_token_id: str,
    ) -> Dict[str, Any]:
        market = self._extract_market_dict(item)
        market_id = self._extract_market_id(item, market)
        if not market_id:
            raise RuntimeError(f"Could not determine active market id from payload: {item}")
        market_name = self._extract_market_name(item, market, market_id)
        question = self._extract_market_question(item, market, market_name)

        market_copy = {
            "question": question,
            "status": "active",
            "outcomes": {
                "YES": {"token_id": yes_token_id},
                "NO": {"token_id": no_token_id},
            },
            "pred_mapping": {
                "market_id": market_id,
            },
        }

        return {
            "market_id": market_id,
            "market_name": market_name,
            "market": market_copy,
        }

    def _resolve_source_market(
        self,
        polymarket_url: str,
        source_market_id: str = "",
        source_family: str = "",
        source_line: str = "",
    ) -> Dict[str, Any]:
        slug = self._extract_slug_from_url(polymarket_url)
        if not slug:
            raise RuntimeError(f"Could not extract slug from Polymarket URL: {polymarket_url!r}")
        event = self._poly.get_event_by_slug(slug)
        if not isinstance(event, dict):
            raise RuntimeError(f"No Polymarket event found for slug {slug!r}")

        game_lines = collect_game_lines_events(self._poly, event)
        classified = select_fixture_markets(game_lines, include_draw=True, all_total_lines=True)

        candidates: List[Dict[str, Any]] = []
        family_order = [
            ("moneyline", "moneyline"),
            ("totals", "totals"),
            ("spreads", "spreads"),
            ("both_teams_to_score", "btts"),
        ]
        for family_key, display_family in family_order:
            for item in classified.get(family_key, []):
                candidates.append(
                    {
                        "market_id": str(item.get("market_id") or ""),
                        "family": display_family,
                        "line": str(item.get("line") or ""),
                        "question": str(item.get("question") or ""),
                        "yes_token_id": str(item.get("yes_token_id") or ""),
                        "no_token_id": str(item.get("no_token_id") or ""),
                        "active": bool(item.get("active")),
                        "closed": bool(item.get("closed")),
                    }
                )

        active_candidates = [item for item in candidates if item["active"] and not item["closed"]]
        if not active_candidates:
            raise RuntimeError(f"No active Polymarket markets found for {polymarket_url!r}")

        if source_market_id:
            for item in active_candidates:
                if item["market_id"] == source_market_id:
                    return item
            raise RuntimeError(f"Requested source_market_id {source_market_id!r} was not found in active Polymarket markets")

        normalized_family = source_family.strip().lower()
        normalized_line = source_line.strip().lower()
        if normalized_family or normalized_line:
            for item in active_candidates:
                family_match = not normalized_family or item["family"] == normalized_family
                line_match = not normalized_line or item["line"].strip().lower() == normalized_line
                if family_match and line_match:
                    return item
            raise RuntimeError("No active Polymarket market matched source_family/source_line")

        return active_candidates[0]

    def _post_market_payload(
        self,
        market_id: str,
        market_name: str,
        market: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            response = self._http.post_json(
                self._url(self._markets_path),
                {"market_name": market_name, "market": market},
                headers=self._post_headers(market_id),
            )
            return {
                "status": "published",
                "market_id": market_id,
                "market_name": market_name,
                "response": response,
            }
        except RuntimeError as exc:
            err = str(exc)
            if "duplicate key" in err.lower() or "uniq_pred_market_id" in err:
                return {
                    "status": "already_exists",
                    "market_id": market_id,
                    "market_name": market_name,
                }
            return {
                "status": "failed",
                "market_id": market_id,
                "market_name": market_name,
                "error": err,
            }

    def _start_auto_delete_job(
        self,
        yes_token_id: str,
        no_token_id: str,
        market_ids: Sequence[str],
        market_names: Sequence[str],
    ) -> AutoDeleteJob:
        job = AutoDeleteJob(
            job_id=str(uuid.uuid4()),
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            market_ids=list(market_ids),
            market_names=list(market_names),
            created_at=time.time(),
        )
        with self._jobs_lock:
            self._jobs[job.job_id] = job

        thread = threading.Thread(
            target=self._watch_auto_delete_job,
            args=(job.job_id,),
            daemon=True,
            name=f"uat-auto-delete-{job.job_id[:8]}",
        )
        thread.start()
        return job

    def _watch_auto_delete_job(self, job_id: str) -> None:
        while True:
            with self._jobs_lock:
                job = self._jobs.get(job_id)
            if job is None:
                return
            if job.status not in {"watching", "deleting"}:
                return

            try:
                snapshots = self._poly.get_order_books([job.yes_token_id, job.no_token_id])
                condition = self._auto_delete_condition(snapshots, job.yes_token_id, job.no_token_id)
                with self._jobs_lock:
                    current = self._jobs.get(job_id)
                    if current is None:
                        return
                    current.last_condition = condition

                if condition["ready"]:
                    self._delete_markets_for_job(job_id)
                    return
            except Exception as exc:
                with self._jobs_lock:
                    current = self._jobs.get(job_id)
                    if current is None:
                        return
                    current.last_error = str(exc)

            time.sleep(self._poll_seconds)

    def _delete_markets_for_job(self, job_id: str) -> None:
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job.status = "deleting"

        deleted_count = 0
        errors: List[str] = []
        for market_id in job.market_ids:
            try:
                self._http.delete_json(
                    self._url(self._delete_path_template.format(market_id=market_id)),
                    headers=self._base_headers(),
                )
                deleted_count += 1
            except RuntimeError as exc:
                errors.append(f"{market_id}: {exc}")

        with self._jobs_lock:
            current = self._jobs.get(job_id)
            if current is None:
                return
            current.deleted_count = deleted_count
            current.finished_at = time.time()
            current.status = "deleted" if not errors else "delete_failed"
            current.last_error = " | ".join(errors)

    def _auto_delete_condition(
        self,
        snapshots: Sequence[Dict[str, Any]],
        yes_token_id: str,
        no_token_id: str,
    ) -> Dict[str, Any]:
        by_id = {
            str(item.get("asset_id") or ""): item
            for item in snapshots
            if isinstance(item, dict)
        }
        yes_snapshot = by_id.get(yes_token_id, {})
        no_snapshot = by_id.get(no_token_id, {})

        yes_best_bid = self._best_price(yes_snapshot.get("bids", []), highest=True)
        yes_best_ask = self._best_price(yes_snapshot.get("asks", []), highest=False)
        no_best_bid = self._best_price(no_snapshot.get("bids", []), highest=True)
        no_best_ask = self._best_price(no_snapshot.get("asks", []), highest=False)

        yes_ready = yes_best_ask == Decimal("0.01") and yes_best_bid == Decimal("0.00")
        no_ready = no_best_ask == Decimal("0.01") and no_best_bid == Decimal("0.00")

        return {
            "ready": yes_ready and no_ready,
            "yes": {
                "best_bid": self._decimal_to_str(yes_best_bid),
                "best_ask": self._decimal_to_str(yes_best_ask),
            },
            "no": {
                "best_bid": self._decimal_to_str(no_best_bid),
                "best_ask": self._decimal_to_str(no_best_ask),
            },
        }

    def _extract_market_items(self, response: Any) -> List[Dict[str, Any]]:
        if isinstance(response, list):
            return [self._normalize_market_item(item) for item in response if self._normalize_market_item(item) is not None]
        if isinstance(response, dict):
            for key in ("data", "items", "markets", "results", "market_ids"):
                value = response.get(key)
                if isinstance(value, list):
                    return [self._normalize_market_item(item) for item in value if self._normalize_market_item(item) is not None]
                if isinstance(value, dict):
                    for nested_key in ("markets", "items", "results", "data", "market_ids"):
                        nested_value = value.get(nested_key)
                        if isinstance(nested_value, list):
                            return [self._normalize_market_item(item) for item in nested_value if self._normalize_market_item(item) is not None]
        raise RuntimeError(f"Unexpected active markets response shape: {type(response).__name__}")

    @staticmethod
    def _normalize_market_item(item: Any) -> Optional[Dict[str, Any]]:
        if isinstance(item, dict):
            return item
        if isinstance(item, str) and item.strip():
            return {"market_id": item.strip()}
        return None

    @staticmethod
    def _is_active_market(item: Dict[str, Any]) -> bool:
        status = str(item.get("status") or item.get("state") or "").lower()
        if status:
            return status == "active"
        if item.get("is_active") is not None:
            return bool(item.get("is_active"))
        market = item.get("market")
        if isinstance(market, dict):
            nested_status = str(market.get("status") or market.get("state") or "").lower()
            if nested_status:
                return nested_status == "active"
        return True

    @staticmethod
    def _extract_market_dict(item: Dict[str, Any]) -> Dict[str, Any]:
        market = item.get("market")
        if isinstance(market, dict):
            return market
        return item

    @staticmethod
    def _extract_market_id(item: Dict[str, Any], market: Dict[str, Any]) -> str:
        pred_mapping = market.get("pred_mapping")
        if isinstance(pred_mapping, dict):
            value = pred_mapping.get("market_id")
            if value:
                return str(value)
        for source in (item, market):
            for key in ("market_id", "pred_market_id", "id", "MarketID", "ParentMarketID", "parent_market_id"):
                value = source.get(key)
                if value:
                    return str(value)
        return ""

    @staticmethod
    def _extract_market_name(item: Dict[str, Any], market: Dict[str, Any], market_id: str) -> str:
        for source in (item, market):
            for key in ("market_name", "name", "question", "title"):
                value = source.get(key)
                if value:
                    return UatMarketPublisher._sanitize_name(str(value))
        for first_key, second_key in (
            ("home_team", "away_team"),
            ("homeTeam", "awayTeam"),
            ("home_team_name", "away_team_name"),
            ("homeTeamName", "awayTeamName"),
        ):
            left = item.get(first_key) or market.get(first_key)
            right = item.get(second_key) or market.get(second_key)
            if left and right:
                return UatMarketPublisher._sanitize_name(f"{left}_{right}")
        return UatMarketPublisher._fallback_name(market_id)

    @staticmethod
    def _extract_market_question(item: Dict[str, Any], market: Dict[str, Any], market_name: str) -> str:
        for source in (item, market):
            value = source.get("question")
            if value:
                return UatMarketPublisher._sanitize_name(str(value))
        for first_key, second_key in (
            ("home_team", "away_team"),
            ("homeTeam", "awayTeam"),
            ("home_team_name", "away_team_name"),
            ("homeTeamName", "awayTeamName"),
        ):
            left = item.get(first_key) or market.get(first_key)
            right = item.get(second_key) or market.get(second_key)
            if left and right:
                return UatMarketPublisher._sanitize_name(f"{left}_vs_{right}")
        return market_name

    @staticmethod
    def _sanitize_name(value: str) -> str:
        cleaned = "".join(ch if ch.isalnum() else "_" for ch in value.strip())
        while "__" in cleaned:
            cleaned = cleaned.replace("__", "_")
        return cleaned.strip("_") or "market"

    @staticmethod
    def _fallback_name(market_id: str) -> str:
        suffix = market_id[-12:] if len(market_id) > 12 else market_id
        return f"market_{suffix}"

    @staticmethod
    def _best_price(levels: Any, highest: bool) -> Optional[Decimal]:
        if not isinstance(levels, list) or not levels:
            return None
        prices: List[Decimal] = []
        for level in levels:
            if not isinstance(level, dict):
                continue
            try:
                prices.append(Decimal(str(level.get("price"))))
            except (InvalidOperation, TypeError):
                continue
        if not prices:
            return None
        return max(prices) if highest else min(prices)

    @staticmethod
    def _decimal_to_str(value: Optional[Decimal]) -> str:
        return "" if value is None else format(value, "f")

    def _base_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self._actor_header and self._actor_value:
            headers[self._actor_header] = self._actor_value
        return headers

    def _post_headers(self, market_id: str) -> Dict[str, str]:
        headers = self._base_headers()
        headers["X-Idempotency-Key"] = f"upsert-market-{market_id}-{int(time.time())}"
        return headers

    def _url(self, path: str) -> str:
        return f"{self._base_url}{path}"

    @staticmethod
    def _absolute_url(base_url: str, path: str) -> str:
        return f"{base_url.rstrip('/')}{path}"

    @staticmethod
    def _extract_slug_from_url(url: str) -> str:
        parsed = urlparse(url.strip())
        path = parsed.path.rstrip("/")
        for prefix in ("/event/", "/events/"):
            idx = path.find(prefix)
            if idx >= 0:
                return path[idx + len(prefix):].split("/")[0]
        parts = [part for part in path.split("/") if part]
        return parts[-1] if parts else ""

    @staticmethod
    def _job_payload(job: Optional[AutoDeleteJob]) -> Optional[Dict[str, Any]]:
        if job is None:
            return None
        return {
            "job_id": job.job_id,
            "yes_token_id": job.yes_token_id,
            "no_token_id": job.no_token_id,
            "market_ids": list(job.market_ids),
            "market_names": list(job.market_names),
            "created_at": job.created_at,
            "status": job.status,
            "deleted_count": job.deleted_count,
            "last_condition": copy.deepcopy(job.last_condition),
            "last_error": job.last_error,
            "finished_at": job.finished_at,
        }
