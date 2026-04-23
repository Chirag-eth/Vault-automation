from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.]+$")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def load_semicolon_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        rows = []
        for row in reader:
            cleaned = {}
            for key, value in row.items():
                normalized_key = key.strip() if key else key
                cleaned[normalized_key] = clean_csv_value(value)
            rows.append(cleaned)
        return rows


def clean_csv_value(value: Optional[str]) -> str:
    if value is None:
        return ""
    value = value.strip()
    if value == '""':
        return ""
    if len(value) >= 2 and value[0] == value[-1] == '"':
        return value[1:-1]
    return value


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    value = re.sub(r"([+-]\d{2})$", r"\1:00", value)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def parse_jsonish_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    text = str(value).strip()
    if not text:
        return []
    try:
        loaded = json.loads(text)
        if isinstance(loaded, list):
            return [str(item) for item in loaded]
    except json.JSONDecodeError:
        pass
    if "," in text:
        return [part.strip() for part in text.split(",") if part.strip()]
    return [text]


def json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, sort_keys=True)


def to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return to_jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [to_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def append_jsonl(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json_dumps(to_jsonable(payload)))
        handle.write("\n")


def validate_table_name(name: str) -> str:
    if not IDENTIFIER_RE.match(name):
        raise ValueError(
            "Unsafe table name. Use only letters, numbers, underscores, and dots."
        )
    return name


def first(items: Iterable[Any]) -> Any:
    for item in items:
        return item
    return None
