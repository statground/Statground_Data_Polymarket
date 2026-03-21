#!/usr/bin/env python3
"""Polymarket refresh + gap-fill (ClickHouse).

Purpose:
- Re-fetch a lookback window of updates for each entity (series/events/markets)
  and insert into ClickHouse normalized tables.
- Each normalized row also stores the original API payload in raw_json.
- This keeps existing records up-to-date and backfills missed objects that
  were updated within the lookback period.

How it works:
1) For each entity, read the current max(updated_at_utc) from ClickHouse.
2) Compute refresh_until = max_updated_at - LOOKBACK_HOURS.
3) Pull from Polymarket API ordered by updatedAt desc, and stop once updatedAt <= refresh_until.
4) Insert all fetched objects (even duplicates). Your tables use ReplacingMergeTree,
   so use argMax / max(collected_at_utc) patterns in analytics as needed.

Env:
- Same ClickHouse env vars as polymarket_crawl_to_clickhouse.py
- LOOKBACK_HOURS (default: 72)
- PAGE_LIMIT, MAX_PAGES, POLY_BASE, ORDER_PRIMARY, ORDER_FALLBACK (optional)

Outputs:
- Optionally writes a JSON report when REFRESH_REPORT_PATH is set.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

# Reuse most helpers from the incremental crawler
import scripts.polymarket_crawl_to_clickhouse as pm

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "72"))
REFRESH_REPORT_PATH = os.getenv("REFRESH_REPORT_PATH", "").strip()


def _dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _ch_max_updated(entity: str, ch) -> Optional[datetime]:
    tbl = pm.ENTITY_TABLES[entity]
    q = f"SELECT max(ifNull(updated_at_utc, collected_at_utc)) AS mx FROM {pm.CH_DB}.{tbl}"
    try:
        r = ch.query(q)
        mx = r.result_rows[0][0] if r.result_rows else None
        return mx
    except Exception:
        return None


def fetch_refresh_window(entity: str, ch, refresh_until_iso: str):
    url = pm.ENDPOINTS[entity]
    order_used = pm.pick_order(entity)
    total_written = 0
    norm_rows = []

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
            row = pm.build_entity_row(entity, obj, collected_at, rk, ch)
            if row:
                norm_rows.append(row)
                total_written += 1

        ch = pm.flush_entity_rows(entity, ch, norm_rows)

        print(f"[{entity}] page={page+1} offset={offset} inserted={total_written} http_status={meta.get('http_status')}")
        if stop:
            break
        if len(items) < pm.PAGE_LIMIT:
            break
        if pm.BASE_SLEEP > 0:
            time.sleep(pm.BASE_SLEEP)

    ch = pm.flush_entity_rows(entity, ch, norm_rows, force=True)
    return total_written, ch


def main():
    ch = pm.get_ch_client()
    print(
        f"[CONFIG] insert_batch_size_default={pm.INSERT_BATCH_SIZE} "
        f"insert_batch_size_events={pm.get_entity_insert_batch_size('events')} "
        f"insert_batch_size_markets={pm.get_entity_insert_batch_size('markets')} "
        f"insert_batch_size_series={pm.get_entity_insert_batch_size('series')} "
        f"insert_split_after_attempt={pm.INSERT_SPLIT_AFTER_ATTEMPT} "
        f"insert_min_split_batch_rows={pm.INSERT_MIN_SPLIT_BATCH_ROWS}",
        flush=True,
    )
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
            wrote, new_cp, ch = pm.fetch_and_insert(entity, checkpoint, ch)
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
        wrote, ch = fetch_refresh_window(entity, ch, refresh_until_iso)
        wrote_total += wrote
        report["entities"][entity] = {
            "mode": "lookback_refresh",
            "inserted_objects": wrote,
            "max_updated_at_utc": _dt_to_iso(mx),
            "refresh_until_utc": refresh_until_iso,
        }

    pm.optimize_after_batch(ch)
    report["inserted_objects_total"] = wrote_total

    if REFRESH_REPORT_PATH:
        out_dir = os.path.dirname(REFRESH_REPORT_PATH)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(REFRESH_REPORT_PATH, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"[REPORT] wrote refresh report -> {REFRESH_REPORT_PATH}")

    print(f"\nDONE. inserted_objects_total={wrote_total}")


if __name__ == "__main__":
    main()