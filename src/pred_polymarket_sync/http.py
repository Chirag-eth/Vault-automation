from __future__ import annotations

import json
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class HttpClient:
    def __init__(self, timeout_seconds: int = 20):
        self.timeout_seconds = timeout_seconds
        self.default_headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        }

    def get_json(
        self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None
    ) -> Any:
        return self.request_json("GET", url, params=params, headers=headers)

    def request_json(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        payload: Any = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        if params:
            query = urlencode(
                {key: value for key, value in params.items() if value not in (None, "")},
                doseq=True,
            )
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{query}"
        body = None
        method_upper = method.upper()
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
        merged_headers = dict(self.default_headers)
        if body is not None:
            merged_headers["Content-Type"] = "application/json"
        if headers:
            merged_headers.update(headers)
        request = Request(url, data=body, headers=merged_headers, method=method_upper)
        return self._read_json(request)

    def post_json(
        self,
        url: str,
        payload: Any,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        return self.request_json("POST", url, payload=payload, headers=headers)

    def delete_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        return self.request_json("DELETE", url, headers=headers)

    def _read_json(self, request: Request) -> Any:
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8")
                if not body:
                    return None
                return json.loads(body)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"HTTP {exc.code} for {request.full_url}: {body}"
            ) from exc
        except URLError as exc:
            raise RuntimeError(f"Network error for {request.full_url}: {exc}") from exc
