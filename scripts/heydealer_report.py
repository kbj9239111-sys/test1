#!/usr/bin/env python3
import argparse
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_DB_PATH = "data/heydealer.db"


def parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000.0
        try:
            return datetime.utcfromtimestamp(timestamp).date()
        except ValueError:
            return None
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value[:19], fmt).date()
            except ValueError:
                continue
    return None


def fetch_rows(
    conn: sqlite3.Connection,
    brand: Optional[str],
    model_group: Optional[str],
    model: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> List[Tuple]:
    filters = []
    params: List[Any] = []
    if brand:
        filters.append("brand = ?")
        params.append(brand)
    if model_group:
        filters.append("model_group = ?")
        params.append(model_group)
    if model:
        filters.append("model = ?")
        params.append(model)
    if start_date:
        filters.append("auction_date >= ?")
        params.append(start_date)
    if end_date:
        filters.append("auction_date <= ?")
        params.append(end_date)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
        SELECT auction_date, auction_price
        FROM prices
        {where_clause}
        ORDER BY auction_date ASC
    """
    return list(conn.execute(query, params))


def summarize(prices: Iterable[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    values = [price for price in prices if price is not None]
    if not values:
        return None, None, None
    return sum(values) / len(values), max(values), min(values)


def print_summary(rows: List[Tuple]) -> None:
    prices = [row[1] for row in rows]
    avg, max_price, min_price = summarize(prices)
    print("Summary")
    print("=======")
    if avg is None:
        print("No data available for the selected filters.")
        return
    print(f"Average: {avg:,.0f}")
    print(f"Max:     {max_price:,.0f}")
    print(f"Min:     {min_price:,.0f}")


def print_trend(rows: List[Tuple]) -> None:
    buckets: Dict[date, List[float]] = defaultdict(list)
    for auction_date, price in rows:
        parsed = parse_date(auction_date)
        if parsed and price is not None:
            buckets[parsed].append(price)

    if not buckets:
        return

    print("\nDaily trend")
    print("==========")
    for day in sorted(buckets):
        avg, max_price, min_price = summarize(buckets[day])
        if avg is None:
            continue
        print(f"{day.isoformat()} | avg {avg:,.0f} | max {max_price:,.0f} | min {min_price:,.0f}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Report Heydealer auction price statistics.")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--brand")
    parser.add_argument("--model-group")
    parser.add_argument("--model")
    parser.add_argument("--days", type=int, default=30, help="Look back N days from today.")
    parser.add_argument("--start-date", help="Override start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", help="Override end date (YYYY-MM-DD).")
    args = parser.parse_args()

    if args.start_date or args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        end_date = date.today().isoformat()
        start_date = (date.today() - timedelta(days=args.days)).isoformat()

    with sqlite3.connect(args.db_path) as conn:
        rows = fetch_rows(conn, args.brand, args.model_group, args.model, start_date, end_date)

    print_summary(rows)
    print_trend(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
