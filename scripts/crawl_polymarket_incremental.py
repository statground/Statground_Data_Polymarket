#!/usr/bin/env python3
"""
Polymarket Gamma API incremental crawler (events / markets / series)

Design goals
- Safe for GitHub Actions: incremental, resumable, low-noise commits
- Keeps your existing archive format:
  by_created/<entity>/YYYY/MM/<entity>_<id>.json
  by_created/<entity>/YYYY/MM/<entity>_<id>.meta.json

How it works
- For each endpoint, requests pages ordered by updatedAt desc (fallback: createdAt desc)
- Stops paging once every item in a page is <= last checkpoint
- Updates checkpoint files under .state/

Env
- TARGET_YEAR: "2026" (only store items whose folder_dt year matches; others are skipped)
- MAX_PAGES: safety cap per entity per run (default 200)
- PAGE_LIMIT: page size (default 100)
- SLEEP_SEC: base sleep between pages (default 0.25)

Notes
- Polymarket docs confirm pagination and sortable fields for /events, /markets, /series. citeturn1view0turn1view1turn3search0
"""
import os
import json
import time
import random
import shutil
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List
import urllib.parse
import urllib.request
import urllib.error

BASE = "https://gamma-api.polymarket.com"
ENDPOINTS = {
    "events": f"{BASE}/events",
    "markets": f"{BASE}/markets",
    "series": f"{BASE}/series",
}

ROOT_DIR = os.getcwd()
OUT_ROOT = os.path.join(ROOT_DIR, "by_created")
STATE_DIR = os.path.join(ROOT_DIR, ".state")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "45"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "100"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "200"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.25"))
TARGET_YEAR = os.getenv("TARGET_YEAR", "").strip()
TARGET_MODE = os.getenv("TARGET_MODE", "year").strip().lower()  # year | null_only | all

USER_AGENT = os.getenv("USER_AGENT", "statground-polymarket-archive-bot/1.0")

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def parse_iso_to_utc_naive(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        s2 = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return None

def parse_published_at_to_utc_naive(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    try:
        if s.endswith("+00"):
            s = s + ":00"
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return parse_iso_to_utc_naive(s)

def dt_to_subdir(dt: Optional[datetime]) -> str:
    if dt is None:
        return "null"
    return os.path.join(f"{dt.year:04d}", f"{dt.month:02d}")

def dump_json_minified(path: str, obj: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

def dump_json_pretty(path: str, obj: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def http_get_json(url: str, params: Dict[str, Any]) -> Any:
    qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
    full = f"{url}?{qs}" if qs else url
    req = urllib.request.Request(full, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")
            data = resp.read()
            if not data:
                return None
            return json.loads(data.decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise
    except Exception as e:
        raise

def safe_get_json(url: str, params: Dict[str, Any], max_retries: int = 8) -> Any:
    for attempt in range(1, max_retries + 1):
        try:
            return http_get_json(url, params)
        except urllib.error.HTTPError as e:
            code = getattr(e, "code", None)
            # retry on 429 / 5xx
            if code in (429,) or (isinstance(code, int) and code >= 500):
                sleep_s = min(60, 2 ** attempt) + random.random()
                print(f"[HTTP {code}] retry in {sleep_s:.1f}s params={params}")
                time.sleep(sleep_s)
                continue
            print(f"[HTTP {code}] abort. url={url} params={params}")
            raise
        except Exception as e:
            sleep_s = min(60, 2 ** attempt) + random.random()
            print(f"[NETWORK/OTHER] {e} retry in {sleep_s:.1f}s params={params}")
            time.sleep(sleep_s)
            continue
    return None

# ---------- Folder date rules (same as your Colab version) ----------
def pick_event_folder_dt(ev: dict) -> Optional[datetime]:
    dt = parse_iso_to_utc_naive(ev.get("createdAt"))
    if dt is not None:
        return dt
    markets = ev.get("markets") or []
    pubs: List[datetime] = []
    for m in markets:
        if not isinstance(m, dict):
            continue
        pa = m.get("published_at") or m.get("publishedAt")
        dt_pa = parse_published_at_to_utc_naive(pa) if pa else None
        if dt_pa is not None:
            pubs.append(dt_pa)
    return max(pubs) if pubs else None

def pick_market_folder_dt(m: dict) -> Optional[datetime]:
    dt = parse_iso_to_utc_naive(m.get("createdAt"))
    if dt is not None:
        return dt
    pa = m.get("published_at") or m.get("publishedAt")
    dt_pa = parse_published_at_to_utc_naive(pa) if pa else None
    return dt_pa

def pick_series_folder_dt(s: dict) -> Optional[datetime]:
    dt = parse_iso_to_utc_naive(s.get("createdAt"))
    if dt is not None:
        return dt
    pa = s.get("publishedAt") or s.get("published_at")
    return parse_published_at_to_utc_naive(pa) if pa else None

def should_accept_dt(dt: Optional[datetime]) -> bool:
    """
    TARGET_MODE:
      - "year": keep only dt.year == TARGET_YEAR (dt must exist)
      - "null_only": keep only dt is None (for Statground_Data_Polymarket null bucket)
      - "all": keep everything
    """
    if TARGET_MODE == "all":
        return True
    if TARGET_MODE == "null_only":
        return dt is None
    # default: year mode
    if not TARGET_YEAR:
        return False
    if dt is None:
        return False
    return f"{dt.year:04d}" == TARGET_YEAR

# ---------- Writers ----------
def write_event(ev: dict) -> Tuple[bool, str, Optional[datetime]]:
    eid = ev.get("id")
    if eid is None:
        return False, "no_id", None

    folder_dt = pick_event_folder_dt(ev)
    if not should_accept_dt(folder_dt):
        return False, "skip_year", folder_dt

    subdir = dt_to_subdir(folder_dt)
    target_dir = os.path.join(OUT_ROOT, "events", subdir)
    ensure_dir(target_dir)

    event_path = os.path.join(target_dir, f"event_{eid}.json")
    meta_path  = os.path.join(target_dir, f"event_{eid}.meta.json")

    dump_json_minified(event_path, ev)

    markets = ev.get("markets") or []
    series  = ev.get("series") or []
    meta = {
        "entity": "event",
        "event_id": eid,
        "folder_rule": "event.createdAt (fallback: max(markets[].published_at))",
        "folder_dt_utc": (folder_dt.isoformat() + "Z") if folder_dt else None,
        "event_createdAt": ev.get("createdAt"),
        "event_creationDate": ev.get("creationDate"),
        "event_startDate": ev.get("startDate"),
        "event_endDate": ev.get("endDate") or ev.get("endDateIso"),
        "event_updatedAt": ev.get("updatedAt"),
        "markets_len": len(markets),
        "market_ids": [m.get("id") for m in markets if isinstance(m, dict) and m.get("id") is not None],
        "series_len": len(series),
        "series_ids": [s.get("id") for s in series if isinstance(s, dict) and s.get("id") is not None],
        "series_slug": ev.get("seriesSlug"),
    }
    dump_json_pretty(meta_path, meta)
    return True, subdir, folder_dt

def write_market(m: dict) -> Tuple[bool, str, Optional[datetime]]:
    mid = m.get("id")
    if mid is None:
        return False, "no_id", None

    folder_dt = pick_market_folder_dt(m)
    if not should_accept_dt(folder_dt):
        return False, "skip_year", folder_dt

    subdir = dt_to_subdir(folder_dt)
    target_dir = os.path.join(OUT_ROOT, "markets", subdir)
    ensure_dir(target_dir)

    market_path = os.path.join(target_dir, f"market_{mid}.json")
    meta_path   = os.path.join(target_dir, f"market_{mid}.meta.json")

    dump_json_minified(market_path, m)

    meta = {
        "entity": "market",
        "market_id": mid,
        "folder_rule": "market.createdAt (fallback: market.published_at)",
        "folder_dt_utc": (folder_dt.isoformat() + "Z") if folder_dt else None,
        "market_createdAt": m.get("createdAt"),
        "market_updatedAt": m.get("updatedAt"),
        "market_startDate": m.get("startDate"),
        "market_endDate": m.get("endDate") or m.get("endDateIso"),
        "market_closedTime": m.get("closedTime"),
        "conditionId": m.get("conditionId"),
        "slug": m.get("slug"),
        "question": m.get("question"),
    }
    dump_json_pretty(meta_path, meta)
    return True, subdir, folder_dt

def write_series(s: dict) -> Tuple[bool, str, Optional[datetime]]:
    sid = s.get("id")
    if sid is None:
        return False, "no_id", None

    folder_dt = pick_series_folder_dt(s)
    if not should_accept_dt(folder_dt):
        return False, "skip_year", folder_dt

    subdir = dt_to_subdir(folder_dt)
    target_dir = os.path.join(OUT_ROOT, "series", subdir)
    ensure_dir(target_dir)

    series_path = os.path.join(target_dir, f"series_{sid}.json")
    meta_path   = os.path.join(target_dir, f"series_{sid}.meta.json")

    dump_json_minified(series_path, s)

    meta = {
        "entity": "series",
        "series_id": sid,
        "folder_rule": "series.createdAt (fallback: series.publishedAt)",
        "folder_dt_utc": (folder_dt.isoformat() + "Z") if folder_dt else None,
        "series_createdAt": s.get("createdAt"),
        "series_publishedAt": s.get("publishedAt") or s.get("published_at"),
        "series_updatedAt": s.get("updatedAt"),
        "slug": s.get("slug"),
        "ticker": s.get("ticker"),
        "title": s.get("title"),
        "startDate": s.get("startDate"),
        "recurrence": s.get("recurrence"),
        "seriesType": s.get("seriesType"),
    }
    dump_json_pretty(meta_path, meta)
    return True, subdir, folder_dt

# ---------- Checkpointing ----------
def state_path(entity: str) -> str:
    return os.path.join(STATE_DIR, f"last_seen_{entity}.txt")

def load_checkpoint(entity: str) -> Optional[datetime]:
    p = state_path(entity)
    if not os.path.exists(p):
        return None
    s = open(p, "r", encoding="utf-8").read().strip()
    if not s:
        return None
    # store as ISO UTC naive
    try:
        dt = datetime.fromisoformat(s)
        return dt
    except Exception:
        return None

def save_checkpoint(entity: str, dt: Optional[datetime]):
    ensure_dir(STATE_DIR)
    p = state_path(entity)
    with open(p, "w", encoding="utf-8") as f:
        f.write(dt.isoformat() if dt else "")

def updated_dt(obj: dict) -> Optional[datetime]:
    # prefer updatedAt, fallback createdAt, fallback published_at
    for k in ("updatedAt", "createdAt"):
        dt = parse_iso_to_utc_naive(obj.get(k))
        if dt:
            return dt
    pa = obj.get("published_at") or obj.get("publishedAt")
    return parse_published_at_to_utc_naive(pa) if pa else None

def fetch_incremental(entity: str, url: str, writer):
    cp = load_checkpoint(entity)
    print(f"\n=== {entity} incremental ===")
    print(f"checkpoint: {cp.isoformat() if cp else '(none)'}")
    ensure_dir(os.path.join(OUT_ROOT, entity))

    newest_seen: Optional[datetime] = cp
    total = written = skipped = 0

    # try updatedAt ordering first (best for incremental), fallback to id
    order_fields = ["updatedAt", "id"]
    for order_field in order_fields:
        try:
            total = written = skipped = 0
            newest_seen = cp
            stop = False

            for page in range(MAX_PAGES):
                params = {
                    "limit": PAGE_LIMIT,
                    "offset": page * PAGE_LIMIT,
                    "order": order_field,
                    "ascending": "false",
                }
                data = safe_get_json(url, params)
                if data is None:
                    print("fetch failed -> stop")
                    break

                # unwrap common wrapper shapes
                if isinstance(data, dict):
                    for k in ("data", entity, "results"):
                        if k in data and isinstance(data[k], list):
                            data = data[k]
                            break

                if not isinstance(data, list) or not data:
                    print("no more data -> stop")
                    break

                page_has_newer = False
                all_older_or_equal = True

                for obj in data:
                    if not isinstance(obj, dict):
                        continue
                    total += 1
                    udt = updated_dt(obj)

                    if udt and (newest_seen is None or udt > newest_seen):
                        newest_seen = udt
                    if cp is None:
                        # first run is huge; we still run but you probably already seeded repos
                        pass
                    else:
                        if udt is None:
                            # keep going, but don't use for stop decision
                            all_older_or_equal = False
                        elif udt > cp:
                            page_has_newer = True
                            all_older_or_equal = False
                        else:
                            # udt <= cp
                            pass

                    ok, reason, _dt_folder = writer(obj)
                    if ok:
                        written += 1
                    else:
                        skipped += 1

                # stop condition: if we have a checkpoint and this whole page is older/equal
                if cp is not None and all_older_or_equal:
                    stop = True

                print(f"{entity}: page={page} total={total} written={written} skipped={skipped} newest_seen={newest_seen.isoformat() if newest_seen else None}")

                if stop:
                    print(f"{entity}: reached checkpoint boundary -> stop paging")
                    break

                time.sleep(SLEEP_SEC + random.random() * 0.2)

            # if succeeded, break out of order_field loop
            break
        except urllib.error.HTTPError as e:
            # if ordering by updatedAt isn't supported, retry with next field
            code = getattr(e, "code", None)
            print(f"{entity}: order={order_field} got HTTP {code}, trying next order field...")
            continue

    # save checkpoint if advanced
    if newest_seen and (cp is None or newest_seen > cp):
        save_checkpoint(entity, newest_seen)
    else:
        # ensure state dir exists, keep file
        save_checkpoint(entity, cp)

    # write manifest for debugging
    ensure_dir(os.path.join(OUT_ROOT, entity))
    manifest = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "entity": entity,
        "endpoint": url,
        "order_attempted": order_fields,
        "page_limit": PAGE_LIMIT,
        "max_pages": MAX_PAGES,
        "checkpoint_before": cp.isoformat() if cp else None,
        "checkpoint_after": newest_seen.isoformat() if newest_seen else (cp.isoformat() if cp else None),
        "total_seen": total,
        "total_written": written,
        "total_skipped": skipped,
        "target_year": TARGET_YEAR or None,
    }
    dump_json_pretty(os.path.join(OUT_ROOT, entity, "_manifest_incremental.json"), manifest)

def main():
    ensure_dir(OUT_ROOT)
    ensure_dir(STATE_DIR)

    # entity-specific writers
    fetch_incremental("events", ENDPOINTS["events"], write_event)
    fetch_incremental("markets", ENDPOINTS["markets"], write_market)
    fetch_incremental("series", ENDPOINTS["series"], write_series)

if __name__ == "__main__":
    main()
