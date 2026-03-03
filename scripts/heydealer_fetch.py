#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_DB_PATH = os.path.join("data", "heydealer.db")


def load_json_file(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    if path.lstrip().startswith(("{", "[")) and not os.path.exists(path):
        return json.loads(path)
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_params(args: argparse.Namespace) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if args.brand:
        params["brand"] = args.brand
    if args.model_group:
        params["model_group"] = args.model_group
    if args.model:
        params["model"] = args.model
    params.update(load_json_file(args.params_json))
    return params


def request_json(
    api_url: str,
    params: Dict[str, Any],
    headers: Dict[str, str],
    method: str,
    timeout: int,
) -> Any:
    method = method.upper()
    if method not in {"GET", "POST"}:
        raise ValueError(f"Unsupported method: {method}")

    if method == "GET":
        query = urllib.parse.urlencode(params)
        url = f"{api_url}?{query}" if query else api_url
        data = None
    else:
        url = api_url
        data = json.dumps(params).encode("utf-8")
        headers = {**headers, "Content-Type": "application/json"}

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = resp.read()
    return json.loads(payload)


def find_list_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "items", "results", "list", "rows"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        for value in payload.values():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return [item for item in value if isinstance(item, dict)]
    return []


def pick_value(item: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
    for key in keys:
        if key in item:
            return item.get(key)
    return None


def parse_price(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def parse_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000.0
        try:
            return datetime.utcfromtimestamp(timestamp).date().isoformat()
        except ValueError:
            return None
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value[:19], fmt).date().isoformat()
            except ValueError:
                continue
    return None


def normalize_record(item: Dict[str, Any]) -> Dict[str, Any]:
    id_keys = ["id", "vehicle_id", "car_id", "stockId", "stock_id", "listing_id", "auction_id"]
    price_keys = [
        "auction_price",
        "auctionPrice",
        "final_price",
        "finalPrice",
        "bid_price",
        "bidPrice",
        "winning_bid",
        "winningBid",
        "price",
        "amount",
        "bidAmount",
        "finalBid",
        "auction_amount",
        "hammer_price",
        "hammerPrice",
    ]
    date_keys = [
        "auction_date",
        "auctionDate",
        "sold_at",
        "soldAt",
        "auction_at",
        "auctionAt",
        "bid_at",
        "bidAt",
        "date",
        "created_at",
        "createdAt",
        "updated_at",
        "updatedAt",
    ]
    brand_keys = ["brand", "make", "maker", "manufacturer"]
    model_group_keys = ["model_group", "modelGroup", "series", "model_family", "modelFamily", "group"]
    model_keys = ["model", "model_name", "modelName", "name"]
    trim_keys = ["trim", "grade", "variant", "trim_name", "trimName"]

    vehicle_id = pick_value(item, id_keys)
    price_value = pick_value(item, price_keys)
    date_value = pick_value(item, date_keys)

    record = {
        "vehicle_id": str(vehicle_id) if vehicle_id is not None else None,
        "brand": pick_value(item, brand_keys),
        "model_group": pick_value(item, model_group_keys),
        "model": pick_value(item, model_keys),
        "trim": pick_value(item, trim_keys),
        "auction_date": parse_date(date_value),
        "auction_price": parse_price(price_value),
        "raw_json": json.dumps(item, ensure_ascii=False),
    }
    return record


def build_dedupe_key(record: Dict[str, Any]) -> str:
    if record.get("vehicle_id"):
        return f"id:{record['vehicle_id']}"
    parts = [
        record.get("brand") or "",
        record.get("model_group") or "",
        record.get("model") or "",
        record.get("trim") or "",
        record.get("auction_date") or "",
        str(record.get("auction_price") or ""),
    ]
    return "|".join(parts)


def ensure_database(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            dedupe_key TEXT PRIMARY KEY,
            vehicle_id TEXT,
            brand TEXT,
            model_group TEXT,
            model TEXT,
            trim TEXT,
            auction_date TEXT,
            auction_price REAL,
            raw_json TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def upsert_records(conn: sqlite3.Connection, records: List[Dict[str, Any]]) -> Tuple[int, int]:
    inserted = 0
    updated = 0
    for record in records:
        dedupe_key = build_dedupe_key(record)
        cursor = conn.execute(
            """
            INSERT INTO prices (
                dedupe_key,
                vehicle_id,
                brand,
                model_group,
                model,
                trim,
                auction_date,
                auction_price,
                raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dedupe_key) DO UPDATE SET
                vehicle_id=excluded.vehicle_id,
                brand=excluded.brand,
                model_group=excluded.model_group,
                model=excluded.model,
                trim=excluded.trim,
                auction_date=excluded.auction_date,
                auction_price=excluded.auction_price,
                raw_json=excluded.raw_json,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                dedupe_key,
                record.get("vehicle_id"),
                record.get("brand"),
                record.get("model_group"),
                record.get("model"),
                record.get("trim"),
                record.get("auction_date"),
                record.get("auction_price"),
                record.get("raw_json"),
            ),
        )
        if cursor.rowcount == 1:
            inserted += 1
        else:
            updated += 1
    conn.commit()
    return inserted, updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Heydealer auction prices and store them in SQLite.")
    parser.add_argument("--api-url", required=True, help="API URL captured from DevTools Network.")
    parser.add_argument("--brand")
    parser.add_argument("--model-group")
    parser.add_argument("--model")
    parser.add_argument("--method", default="GET", help="HTTP method to use (GET or POST).")
    parser.add_argument("--headers-json", help="Path to JSON file with request headers (cookies/token).")
    parser.add_argument("--params-json", help="Path to JSON file with extra query/body parameters.")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    args = parser.parse_args()

    headers = load_json_file(args.headers_json)
    params = build_params(args)

    payload = request_json(args.api_url, params, headers, args.method, args.timeout)
    items = find_list_payload(payload)

    if not items:
        print("No items found in response. Save raw payload for inspection.", file=sys.stderr)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1

    records = [normalize_record(item) for item in items]

    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    with sqlite3.connect(args.db_path) as conn:
        ensure_database(conn)
        inserted, updated = upsert_records(conn, records)

    print(f"Stored {len(records)} records (inserted={inserted}, updated={updated}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
