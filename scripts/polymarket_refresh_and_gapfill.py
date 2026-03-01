#!/usr/bin/env python3
"""Polymarket refresh + gap-fill (ClickHouse).

Purpose:
- Re-fetch a *lookback window* of updates for each entity (series/events/markets)
  and insert into ClickHouse (raw + normalized).
- This helps keep existing records up-to-date and backfills missed objects that
  were updated within the lookback period.

How it works:
1) For each entity, read the current max(updated_at_utc) from ClickHouse.
2) Compute refresh_until = max_updated_at - 

def entity_singular(entity: str) -> str:
    return entity[:-1] if entity.endswith('s') else entity
LOOKBACK_HOURS.
3) Pull from Polymarket API ordered by updatedAt desc, and stop once updatedAt <= refresh_until.
4) Insert all fetched objects (even duplicates). Your tables use ReplacingMergeTree,
   so use FINAL / argMax in analytics as needed.

Env:
- Same ClickHouse env vars as polymarket_crawl_to_clickhouse.py
- LOOKBACK_HOURS (default: 72)
- PAGE_LIMIT, MAX_PAGES, POLY_BASE, ORDER_PRIMARY, ORDER_FALLBACK (optional)

Outputs:
- Writes a JSON report to ./artifacts/polymarket_refresh_report.json
"""

import os, json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import clickhouse_connect
from scripts.clickhouse_optimize import optimize_random_partitions

# Reuse most helpers from the incremental crawler
import scripts.polymarket_crawl_to_clickhouse as pm

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "72"))

def _dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _ch_max_updated(entity: str, ch) -> Optional[datetime]:
    tbl = {
        "events": pm.EVENT_TABLE,
        "markets": pm.MARKET_TABLE,
        "series": pm.SERIES_TABLE,
    }[entity]
    # updated_at_utc is Nullable(DateTime64) in most designs; handle NULLs.
    q = f"""SELECT max(ifNull(updated_at_utc, collected_at_utc)) AS mx FROM {pm.CH_DB}.{tbl}"""
    try:
        r = ch.query(q)
        mx = r.result_rows[0][0] if r.result_rows else None
        return mx
    except Exception:
        return None

def fetch_refresh_window(entity: str, ch, refresh_until_iso: str) -> int:
    url = pm.ENDPOINTS[entity]
    order_used = pm.pick_order(entity)
    total_written = 0
    raw_rows: List[list] = []
    norm_rows: List[list] = []

    for page in range(pm.MAX_PAGES):
        offset = page * pm.PAGE_LIMIT
        params = {"limit": str(pm.PAGE_LIMIT), "offset": str(offset), "order": order_used, "ascending": "false"}
        data, meta = pm.safe_get_json(url, params)
        if data is None:
            print(f"[STOP] {entity} fetch failed at offset={offset}")
            break

        items = data
        if isinstance(data, dict):
            for k in ("data", entity, "results"):
                if k in data and isinstance(data[k], list):
                    items = data[k]
                    break

        if not isinstance(items, list) or not items:
            break

        collected_at = pm.utc_now_dt()
        stop = False

        for obj in items:
            if not isinstance(obj, dict):
                continue

            updated = obj.get("updatedAt") or obj.get("updated_at") or ""
            # Stop once we're past the lookback window (API is sorted desc).
            if updated and pm.iso_leq(updated, refresh_until_iso):
                stop = True
                break

            rk = pm.uuid7()
            raw_rows.append(pm.to_raw_row(entity, obj, meta, params, collected_at, rk))

            if entity == "events":
                row = pm.to_event_row(obj, collected_at, rk)
            elif entity == "markets":
                row = pm.to_market_row(obj, collected_at, rk)
            else:
                row = pm.to_series_row(obj, collected_at, rk)

            if row:
                norm_rows.append(row)

            total_written += 1

        # flush per page
        if raw_rows:
            ch.insert(
                pm.RAW_TABLE,
                raw_rows,
                column_names=[
                    "entity","object_id","collected_at_utc","raw_key","endpoint","request_params",
                    "http_status","response_ms","body_json"
                ],
            )
            raw_rows.clear()

        if norm_rows:
            if entity == "events":
                ch.insert(
                    pm.EVENT_TABLE,
                    norm_rows,
                    column_names=[
                        "event_id","raw_key","collected_at_utc","created_at_utc","updated_at_utc",
                        "title","ticker","slug","description",
                        "active","archived","closed","restricted",
                        "start_date_utc","end_date_utc","closed_time_utc","creation_date_utc",
                        "series_slug","series_ids","market_ids",
                        "icon_url","image_url","volume"
                    ],
                )
            elif entity == "markets":
                ch.insert(
                    pm.MARKET_TABLE,
                    norm_rows,
                    column_names=[
                        "market_id","raw_key","collected_at_utc","created_at_utc","updated_at_utc",
                        "condition_id","question_id","slug","question","description",
                        "resolution","end_date_utc","closed_time_utc",
                        "active","archived","closed","restricted",
                        "market_type","category","subcategory","condition",
                        "volume","liquidity","fee","spread",
                        "outcomes","outcome_prices","best_bid","best_ask","last_trade_price",
                        "one_day_volume","one_week_volume","one_month_volume",
                        "icon_url","image_url","event_id","series_id"
                    ],
                )
            else:
                ch.insert(
                    pm.SERIES_TABLE,
                    norm_rows,
                    column_names=[
                        "series_id","raw_key","collected_at_utc","created_at_utc","updated_at_utc",
                        "slug","title","ticker","description",
                        "active","archived","closed","restricted",
                        "start_date_utc","end_date_utc","volume",
                        "icon_url","image_url","event_ids"
                    ],
                )
            norm_rows.clear()

        if stop:
            break

    return total_written

def main():
    ch = pm.get_ch_client()
    os.makedirs("artifacts", exist_ok=True)

    report = {
        "run_at_utc": _dt_to_iso(datetime.now(timezone.utc)),
        "lookback_hours": LOOKBACK_HOURS,
        "entities": {},
    }

    wrote_total = 0
    for entity in ("events", "markets", "series"):
        mx = _ch_max_updated(entity, ch)
        if mx is None:
            # If table is empty, just run the incremental crawler for that entity using checkpoint.
            print(f"[INFO] {entity}: no max(updated_at_utc). Falling back to incremental ingestion.")
            checkpoint = pm.load_checkpoint()
            wrote, new_cp = pm.fetch_and_insert(entity, checkpoint, ch)
            wrote_total += wrote
            if new_cp:
                checkpoint[entity] = new_cp
                pm.save_checkpoint(checkpoint)
            report["entities"][entity] = {
                "mode": "fallback_incremental",
                "inserted_objects": wrote,
                "max_updated_at_utc": None,
            }
            continue

        refresh_until = mx - timedelta(hours=LOOKBACK_HOURS)
        refresh_until_iso = _dt_to_iso(refresh_until)

        print(f"[REFRESH] {entity}: max_updated_at_utc={mx} refresh_until={refresh_until_iso}")
        wrote = fetch_refresh_window(entity, ch, refresh_until_iso)
        wrote_total += wrote
        report["entities"][entity] = {
            "mode": "lookback_refresh",
            "inserted_objects": wrote,
            "max_updated_at_utc": _dt_to_iso(mx),
            "refresh_until_utc": refresh_until_iso,
        }

    report["inserted_objects_total"] = wrote_total
    out_path = os.path.join("artifacts", "polymarket_refresh_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nDONE. inserted_objects_total={wrote_total} report={out_path}")

if __name__ == "__main__":
    main()
