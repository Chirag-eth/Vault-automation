from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from pred_polymarket_sync.http import HttpClient
from pred_polymarket_sync.models import MappingRecord, OrderBookEnvelope, ReviewRecord
from pred_polymarket_sync.utils import append_jsonl, ensure_dir, to_jsonable


class Sink(ABC):
    @abstractmethod
    def publish_mapping(self, payload: MappingRecord) -> None:
        raise NotImplementedError

    @abstractmethod
    def publish_review(self, payload: ReviewRecord) -> None:
        raise NotImplementedError

    @abstractmethod
    def publish_orderbook(self, payload: OrderBookEnvelope) -> None:
        raise NotImplementedError


class JsonlSink(Sink):
    def __init__(self, state_dir: Path):
        self.state_dir = ensure_dir(state_dir)

    def publish_mapping(self, payload: MappingRecord) -> None:
        append_jsonl(self.state_dir / "mappings.jsonl", payload)

    def publish_review(self, payload: ReviewRecord) -> None:
        append_jsonl(self.state_dir / "reviews.jsonl", payload)

    def publish_orderbook(self, payload: OrderBookEnvelope) -> None:
        append_jsonl(self.state_dir / "orderbooks.jsonl", payload)


class HttpSink(Sink):
    def __init__(
        self,
        base_url: str,
        mapping_path: str,
        review_path: str,
        orderbook_path: str,
        timeout_seconds: int = 20,
        auth_header: str = "",
        auth_token: str = "",
    ):
        self.base_url = base_url.rstrip("/")
        self.mapping_path = mapping_path
        self.review_path = review_path
        self.orderbook_path = orderbook_path
        self.http = HttpClient(timeout_seconds=timeout_seconds)
        self.headers: Dict[str, str] = {}
        if auth_header and auth_token:
            self.headers[auth_header] = auth_token

    def publish_mapping(self, payload: MappingRecord) -> None:
        self.http.post_json(
            f"{self.base_url}{self.mapping_path}",
            to_jsonable(payload),
            headers=self.headers,
        )

    def publish_review(self, payload: ReviewRecord) -> None:
        self.http.post_json(
            f"{self.base_url}{self.review_path}",
            to_jsonable(payload),
            headers=self.headers,
        )

    def publish_orderbook(self, payload: OrderBookEnvelope) -> None:
        self.http.post_json(
            f"{self.base_url}{self.orderbook_path}",
            to_jsonable(payload),
            headers=self.headers,
        )


class CompositeSink(Sink):
    def __init__(self, sinks: Iterable[Sink]):
        self.sinks = list(sinks)

    def publish_mapping(self, payload: MappingRecord) -> None:
        for sink in self.sinks:
            sink.publish_mapping(payload)

    def publish_review(self, payload: ReviewRecord) -> None:
        for sink in self.sinks:
            sink.publish_review(payload)

    def publish_orderbook(self, payload: OrderBookEnvelope) -> None:
        for sink in self.sinks:
            sink.publish_orderbook(payload)


def build_sink(
    sink_name: str,
    state_dir: Path,
    timeout_seconds: int = 20,
    base_url: str = "",
    mapping_path: str = "/mappings",
    review_path: str = "/reviews",
    orderbook_path: str = "/orderbooks",
    auth_header: str = "",
    auth_token: str = "",
) -> Sink:
    if sink_name == "http":
        if not base_url:
            raise ValueError("SYNC_HTTP_BASE_URL is required when sink=http")
        return HttpSink(
            base_url=base_url,
            mapping_path=mapping_path,
            review_path=review_path,
            orderbook_path=orderbook_path,
            timeout_seconds=timeout_seconds,
            auth_header=auth_header,
            auth_token=auth_token,
        )
    if sink_name == "both":
        if not base_url:
            raise ValueError("SYNC_HTTP_BASE_URL is required when sink=both")
        return CompositeSink(
            [
                JsonlSink(state_dir),
                HttpSink(
                    base_url=base_url,
                    mapping_path=mapping_path,
                    review_path=review_path,
                    orderbook_path=orderbook_path,
                    timeout_seconds=timeout_seconds,
                    auth_header=auth_header,
                    auth_token=auth_token,
                ),
            ]
        )
    return JsonlSink(state_dir)
