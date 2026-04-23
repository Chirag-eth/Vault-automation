from __future__ import annotations

import asyncio
import json
from typing import Dict, Iterable, List

from pred_polymarket_sync.models import MappingRecord, OrderBookEnvelope, OrderBookState
from pred_polymarket_sync.polymarket import PolymarketClient
from pred_polymarket_sync.sinks import Sink


def book_from_snapshot(snapshot: Dict) -> OrderBookState:
    return OrderBookState(
        asset_id=str(snapshot.get("asset_id") or ""),
        market=str(snapshot.get("market") or ""),
        bids={item["price"]: item["size"] for item in snapshot.get("bids", [])},
        asks={item["price"]: item["size"] for item in snapshot.get("asks", [])},
        hash=str(snapshot.get("hash") or ""),
        timestamp=str(snapshot.get("timestamp") or ""),
        last_trade_price=str(snapshot.get("last_trade_price") or ""),
        tick_size=str(snapshot.get("tick_size") or ""),
    )


def apply_price_change(book: OrderBookState, change: Dict) -> None:
    side = str(change.get("side") or "").upper()
    price = str(change.get("price") or "")
    size = str(change.get("size") or "")
    if not price:
        return
    levels = book.bids if side == "BUY" else book.asks
    if size == "0":
        levels.pop(price, None)
    else:
        levels[price] = size
    book.hash = str(change.get("hash") or book.hash)


def build_envelope(mapping: MappingRecord, books_by_token: Dict[str, OrderBookState]) -> OrderBookEnvelope:
    yes_book = books_by_token.get(mapping.yes_token_id)
    no_book = books_by_token.get(mapping.no_token_id)
    return OrderBookEnvelope(
        polymarket_market_id=mapping.polymarket_market_id,
        yes_token_id=mapping.yes_token_id,
        no_token_id=mapping.no_token_id,
        orderbook_snapshot={
            "yes": {
                "bids": _sorted_levels(yes_book.bids, reverse=True) if yes_book else [],
                "asks": _sorted_levels(yes_book.asks, reverse=False) if yes_book else [],
                "hash": yes_book.hash if yes_book else "",
                "timestamp": yes_book.timestamp if yes_book else "",
            },
            "no": {
                "bids": _sorted_levels(no_book.bids, reverse=True) if no_book else [],
                "asks": _sorted_levels(no_book.asks, reverse=False) if no_book else [],
                "hash": no_book.hash if no_book else "",
                "timestamp": no_book.timestamp if no_book else "",
            },
        },
        best_bid_yes=yes_book.best_bid() if yes_book else "0",
        best_ask_yes=yes_book.best_ask() if yes_book else "0",
        best_bid_no=no_book.best_bid() if no_book else "0",
        best_ask_no=no_book.best_ask() if no_book else "0",
    )


def _sorted_levels(levels: Dict[str, str], reverse: bool) -> List[Dict[str, str]]:
    return [
        {"price": price, "size": levels[price]}
        for price in sorted(levels, key=lambda item: float(item), reverse=reverse)
    ]


class OrderBookListener:
    def __init__(
        self,
        polymarket_client: PolymarketClient,
        sink: Sink,
        market_ws_url: str,
        reconnect_delay_seconds: int = 5,
    ):
        self.polymarket_client = polymarket_client
        self.sink = sink
        self.market_ws_url = market_ws_url
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.books_by_token: Dict[str, OrderBookState] = {}

    async def run(self, mappings: Iterable[MappingRecord]) -> None:
        mappings = list(mappings)
        token_ids = _unique_token_ids(mappings)
        if not token_ids:
            raise RuntimeError("No mapped token IDs found for orderbook listener")
        snapshots = self.polymarket_client.get_order_books(token_ids)
        for snapshot in snapshots:
            book = book_from_snapshot(snapshot)
            self.books_by_token[book.asset_id] = book
        for mapping in mappings:
            self.sink.publish_orderbook(build_envelope(mapping, self.books_by_token))
        token_to_mappings = _token_mapping_index(mappings)
        while True:
            try:
                await self._listen_loop(token_ids, token_to_mappings)
            except Exception:
                await asyncio.sleep(self.reconnect_delay_seconds)

    async def _listen_loop(self, token_ids, token_to_mappings):
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError(
                "websockets is not installed. Run: pip install -r requirements.txt"
            ) from exc

        async with websockets.connect(self.market_ws_url) as websocket:
            await websocket.send(
                json.dumps(
                    {
                        "assets_ids": token_ids,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                )
            )
            ping_task = asyncio.create_task(self._ping_loop(websocket))
            try:
                async for raw_message in websocket:
                    if raw_message == "PONG":
                        continue
                    message = json.loads(raw_message)
                    event_type = message.get("event_type")
                    changed_tokens: List[str] = []
                    if event_type == "book":
                        book = book_from_snapshot(message)
                        self.books_by_token[book.asset_id] = book
                        changed_tokens.append(book.asset_id)
                    elif event_type == "price_change":
                        for change in message.get("price_changes", []):
                            token_id = str(change.get("asset_id") or "")
                            book = self.books_by_token.get(token_id)
                            if not book:
                                continue
                            apply_price_change(book, change)
                            book.timestamp = str(message.get("timestamp") or book.timestamp)
                            changed_tokens.append(token_id)
                    elif event_type == "tick_size_change":
                        token_id = str(message.get("asset_id") or "")
                        book = self.books_by_token.get(token_id)
                        if book:
                            book.tick_size = str(message.get("new_tick_size") or book.tick_size)
                            changed_tokens.append(token_id)
                    for token_id in changed_tokens:
                        for mapping in token_to_mappings.get(token_id, []):
                            self.sink.publish_orderbook(
                                build_envelope(mapping, self.books_by_token)
                            )
            finally:
                ping_task.cancel()

    async def _ping_loop(self, websocket) -> None:
        while True:
            await asyncio.sleep(10)
            await websocket.send("PING")


def _unique_token_ids(mappings: Iterable[MappingRecord]) -> List[str]:
    token_ids: List[str] = []
    seen = set()
    for mapping in mappings:
        for token_id in (mapping.yes_token_id, mapping.no_token_id):
            if token_id and token_id not in seen:
                seen.add(token_id)
                token_ids.append(token_id)
    return token_ids


def _token_mapping_index(mappings: Iterable[MappingRecord]):
    index: Dict[str, List[MappingRecord]] = {}
    for mapping in mappings:
        index.setdefault(mapping.yes_token_id, []).append(mapping)
        index.setdefault(mapping.no_token_id, []).append(mapping)
    return index
