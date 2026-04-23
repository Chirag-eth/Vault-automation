from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from pred_polymarket_sync.models import MappingRecord, ReviewRecord
from pred_polymarket_sync.utils import ensure_dir, to_jsonable


class StateStore:
    def __init__(self, state_dir: Path):
        self.state_dir = ensure_dir(state_dir)
        self.mappings_path = self.state_dir / "latest_mappings.json"
        self.reviews_path = self.state_dir / "latest_reviews.json"
        self.orderbooks_path = self.state_dir / "orderbooks.jsonl"

    def save_mappings(self, mappings: List[MappingRecord]) -> None:
        self._write_json(self.mappings_path, [to_jsonable(item) for item in mappings])

    def load_mappings(self) -> List[Dict[str, str]]:
        return self._read_json(self.mappings_path, [])

    def save_reviews(self, reviews: List[ReviewRecord]) -> None:
        self._write_json(self.reviews_path, [to_jsonable(item) for item in reviews])

    def load_reviews(self) -> List[Dict[str, str]]:
        return self._read_json(self.reviews_path, [])

    def load_latest_orderbooks(self, limit: int = 24) -> List[Dict]:
        if not self.orderbooks_path.exists():
            return []
        latest_by_market: Dict[str, Dict] = {}
        with open(self.orderbooks_path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                market_id = str(payload.get("polymarket_market_id") or "")
                if not market_id:
                    continue
                latest_by_market[market_id] = payload
        items = list(latest_by_market.values())
        items.sort(
            key=lambda item: (
                item.get("orderbook_snapshot", {})
                .get("yes", {})
                .get("timestamp", "")
            ),
            reverse=True,
        )
        return items[:limit]

    def load_dashboard_state(self, limit: int = 24) -> Dict:
        mappings = self.load_mappings()
        reviews = self.load_reviews()
        orderbooks = self.load_latest_orderbooks(limit=limit)
        return {
            "summary": {
                "mappings": len(mappings),
                "reviews": len(reviews),
                "orderbooks": len(orderbooks),
            },
            "mappings": list(reversed(mappings[-limit:])),
            "reviews": list(reversed(reviews[-limit:])),
            "orderbooks": orderbooks,
        }

    def _write_json(self, path: Path, payload) -> None:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)

    def _read_json(self, path: Path, default):
        if not path.exists():
            return default
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
