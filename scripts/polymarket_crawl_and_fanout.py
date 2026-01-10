#!/usr/bin/env python3
"""
Polymarket daily incremental crawler + year repo fan-out (with auto-create repos)

- Fetches events/markets/series from Polymarket Gamma API
- Writes JSON + META JSON into by_created/<entity>/<YYYY>/<MM>/
- Splits into Statground_Data_Polymarket_<YYYY> repos automatically
- If year repo doesn't exist, creates it using GitHub REST API (requires PAT)

Designed to run inside GitHub Actions of Statground_Data_Polymarket (orchestrator repo).
"""
import os
import json
import time
import random
import shutil
import subprocess
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# -----------------------
# Config (env)
# -----------------------
ORG = os.getenv("ORG", "statground")
PREFIX = os.getenv("REPO_PREFIX", "Statground_Data_Polymarket")

GH_PAT = os.getenv("GH_PAT", "")  # required for push + auto-create
DEFAULT_BRANCH = os.getenv("DEFAULT_BRANCH", "main")

BASE = os.getenv("POLY_BASE", "https://gamma-api.polymarket.com")
ENDPOINTS = {
    "events": f"{BASE}/events",
    "markets": f"{BASE}/markets",
    "series": f"{BASE}/series",
}

PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "100"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "200"))  # safety cap per entity
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "8"))
BASE_SLEEP = float(os.getenv("BASE_SLEEP", "0.4"))

# incremental ordering preference
ORDER_PRIMARY = os.getenv("ORDER_PRIMARY", "updatedAt")  # try updatedAt first
ORDER_FALLBACK = os.getenv("ORDER_FALLBACK", "id")       # then id

# output
OUT_ROOT = os.getenv("OUT_ROOT", "by_created")
STATE_PATH = os.getenv("STATE_PATH", ".state/polymarket_checkpoint.json")

# -----------------------
# Utils
# -----------------------
def run(cmd: List[str], cwd: Optional[str] = None) -> str:
    res = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{res.stdout}")
    return res.stdout

def http_get_json(url: str, params: Dict[str, str], timeout: int = REQUEST_TIMEOUT) -> object:
    qs = "&".join([f"{urllib.parse.quote(str(k))}={urllib.parse.quote(str(v))}" for k, v in params.items()])
    full = f"{url}?{qs}" if qs else url
    req = urllib.request.Request(full)
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "statground-polymarket-crawler")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def safe_get_json(url: str, params: Dict[str, str]) -> Optional[object]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return http_get_json(url, params)
        except urllib.error.HTTPError as e:
            # 429/5xx retry, others fail fast
            if e.code == 429 or e.code >= 500:
                sleep_s = min(60, 2 ** attempt) + random.random()
                print(f"[HTTP {e.code}] retry in {sleep_s:.1f}s url={url} params={params}")
                time.sleep(sleep_s)
                continue
            body = (e.read().decode("utf-8", errors="ignore") if hasattr(e, "read") else "")[:200]
            print(f"[HTTP {e.code}] fail url={url} params={params} body={body}")
            return None
        except Exception as e:
            sleep_s = min(60, 2 ** attempt) + random.random()
            print(f"[NET] {e} retry in {sleep_s:.1f}s url={url} params={params}")
            time.sleep(sleep_s)
            continue
    return None

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def dump_json_minified(path: str, obj):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

def dump_json_pretty(path: str, obj):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def parse_iso_to_utc_naive(s: Optional[str]):
    if not s:
        return None
    try:
        s2 = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return None

def parse_published_at_to_utc_naive(s: Optional[str]):
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

def dt_to_subdir(dt):
    if dt is None:
        return "null"
    return os.path.join(f"{dt.year:04d}", f"{dt.month:02d}")

def pick_event_folder_dt(ev: dict):
    dt = parse_iso_to_utc_naive(ev.get("createdAt"))
    if dt is not None:
        return dt
    markets = ev.get("markets") or []
    pubs = []
    for m in markets:
        if not isinstance(m, dict):
            continue
        pa = m.get("published_at") or m.get("publishedAt")
        dt_pa = parse_published_at_to_utc_naive(pa) if pa else None
        if dt_pa is not None:
            pubs.append(dt_pa)
    return max(pubs) if pubs else None

def pick_market_folder_dt(m: dict):
    dt = parse_iso_to_utc_naive(m.get("createdAt"))
    if dt is not None:
        return dt
    pa = m.get("published_at") or m.get("publishedAt")
    return parse_published_at_to_utc_naive(pa) if pa else None

def pick_series_folder_dt(s: dict):
    dt = parse_iso_to_utc_naive(s.get("createdAt"))
    if dt is not None:
        return dt
    pa = s.get("publishedAt") or s.get("published_at")
    return parse_published_at_to_utc_naive(pa) if pa else None

def write_entity(entity: str, obj: dict, out_base: str) -> Tuple[bool, str, Optional[str]]:
    """
    Returns: ok, subdir, updatedAt
    """
    _id = obj.get("id")
    if _id is None:
        return False, "no_id", None

    if entity == "events":
        folder_dt = pick_event_folder_dt(obj)
    elif entity == "markets":
        folder_dt = pick_market_folder_dt(obj)
    else:
        folder_dt = pick_series_folder_dt(obj)

    subdir = dt_to_subdir(folder_dt)
    target_dir = os.path.join(out_base, entity, subdir)
    ensure_dir(target_dir)

    # filenames
    prefix = entity[:-1]  # events->event, markets->market, series->serie? but we want "series"
    if entity == "series":
        prefix = "series"

    data_path = os.path.join(target_dir, f"{prefix}_{_id}.json")
    meta_path = os.path.join(target_dir, f"{prefix}_{_id}.meta.json")

    dump_json_minified(data_path, obj)

    # meta (minimal but useful)
    if entity == "events":
        markets = obj.get("markets") or []
        series = obj.get("series") or []
        meta = {
            "entity": "event",
            "event_id": _id,
            "folder_rule": "event.createdAt (fallback: max(markets[].published_at))",
            "folder_dt_utc": (folder_dt.isoformat() + "Z") if folder_dt else None,
            "event_createdAt": obj.get("createdAt"),
            "event_creationDate": obj.get("creationDate"),
            "event_startDate": obj.get("startDate"),
            "event_endDate": obj.get("endDate") or obj.get("endDateIso"),
            "event_updatedAt": obj.get("updatedAt"),
            "markets_len": len(markets),
            "market_ids": [m.get("id") for m in markets if isinstance(m, dict) and m.get("id") is not None],
            "series_len": len(series),
            "series_ids": [s.get("id") for s in series if isinstance(s, dict) and s.get("id") is not None],
            "series_slug": obj.get("seriesSlug"),
        }
    elif entity == "markets":
        meta = {
            "entity": "market",
            "market_id": _id,
            "folder_rule": "market.createdAt (fallback: market.published_at)",
            "folder_dt_utc": (folder_dt.isoformat() + "Z") if folder_dt else None,
            "market_createdAt": obj.get("createdAt"),
            "market_updatedAt": obj.get("updatedAt"),
            "market_startDate": obj.get("startDate"),
            "market_endDate": obj.get("endDate") or obj.get("endDateIso"),
            "market_closedTime": obj.get("closedTime"),
            "conditionId": obj.get("conditionId"),
            "slug": obj.get("slug"),
            "question": obj.get("question"),
        }
    else:
        meta = {
            "entity": "series",
            "series_id": _id,
            "folder_rule": "series.createdAt (fallback: series.publishedAt)",
            "folder_dt_utc": (folder_dt.isoformat() + "Z") if folder_dt else None,
            "series_createdAt": obj.get("createdAt"),
            "series_publishedAt": obj.get("publishedAt") or obj.get("published_at"),
            "series_updatedAt": obj.get("updatedAt"),
            "slug": obj.get("slug"),
            "ticker": obj.get("ticker"),
            "title": obj.get("title"),
            "startDate": obj.get("startDate"),
            "recurrence": obj.get("recurrence"),
            "seriesType": obj.get("seriesType"),
        }
    dump_json_pretty(meta_path, meta)

    updated = obj.get("updatedAt") or obj.get("updated_at")
    return True, subdir, updated

# -----------------------
# Checkpoint
# -----------------------
def load_state() -> Dict[str, str]:
    if not os.path.exists(STATE_PATH):
        return {}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        try:
            return json.load(f) or {}
        except Exception:
            return {}

def save_state(state: Dict[str, str]):
    ensure_dir(os.path.dirname(STATE_PATH))
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)

def iso_leq(a: str, b: str) -> bool:
    """Return True if a <= b in time. Empty treated as False."""
    if not a or not b:
        return False
    da = parse_iso_to_utc_naive(a)
    db = parse_iso_to_utc_naive(b)
    if da is None or db is None:
        return False
    return da <= db

# -----------------------
# GitHub: ensure repo exists (auto-create)
# -----------------------
def gh_api_json(method: str, url: str, token: str, data: Optional[dict] = None) -> Tuple[int, object]:
    req = urllib.request.Request(url, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", "statground-polymarket-orchestrator")
    req.add_header("Authorization", f"Bearer {token}")
    body = None
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data=body, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8") or "null")
    except urllib.error.HTTPError as e:
        try:
            payload = e.read().decode("utf-8", errors="ignore")
            obj = json.loads(payload) if payload else None
        except Exception:
            obj = None
        return e.code, obj

def repo_name_for_year(year: Optional[int]) -> str:
    if year is None:
        return PREFIX
    return f"{PREFIX}_{year}"

def ensure_year_repo_exists(year: int):
    if not GH_PAT:
        raise RuntimeError("GH_PAT is required for auto-create repos.")
    repo = repo_name_for_year(year)
    # check
    code, _ = gh_api_json("GET", f"https://api.github.com/repos/{ORG}/{repo}", GH_PAT)
    if code == 200:
        return
    if code != 404:
        raise RuntimeError(f"Unexpected status checking repo {ORG}/{repo}: {code}")
    # create
    print(f"[CREATE] {ORG}/{repo}")
    payload = {
        "name": repo,
        "private": False,
        "auto_init": True,
        "description": f"Polymarket archive {year} (auto-created)",
    }
    code2, obj2 = gh_api_json("POST", f"https://api.github.com/orgs/{ORG}/repos", GH_PAT, payload)
    if code2 not in (201,):
        raise RuntimeError(f"Failed to create repo {ORG}/{repo}: status={code2} resp={obj2}")

# -----------------------
# Fan-out: copy staged files into target repos and push
# -----------------------
def git_url(repo: str) -> str:
    if not GH_PAT:
        raise RuntimeError("GH_PAT is required for push.")
    return f"https://x-access-token:{GH_PAT}@github.com/{ORG}/{repo}.git"

def fanout_and_push(staging_root: str) -> int:
    """
    staging_root contains:
      by_created/events/YYYY/MM/...
      by_created/markets/YYYY/MM/...
      by_created/series/YYYY/MM/...
      by_created/events/null/...
    We split by year folder (top-level YYYY), and push into each year repo.
    Returns number of repos updated.
    """
    # Build year -> list of file relative paths
    year_to_files: Dict[Optional[int], List[str]] = {}

    for entity in ("events", "markets", "series"):
        entity_dir = os.path.join(staging_root, entity)
        if not os.path.isdir(entity_dir):
            continue
        for y in os.listdir(entity_dir):
            y_path = os.path.join(entity_dir, y)
            if not os.path.isdir(y_path):
                continue
            year: Optional[int]
            if y == "null":
                year = None
            else:
                try:
                    year = int(y)
                except Exception:
                    year = None

            # collect all files under y_path
            for root, _, files in os.walk(y_path):
                for fn in files:
                    rel = os.path.relpath(os.path.join(root, fn), staging_root)
                    year_to_files.setdefault(year, []).append(rel)

    updated_repos = 0
    workdir = os.path.abspath(".tmp_fanout")
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir, exist_ok=True)

    for year, rel_files in year_to_files.items():
        repo = repo_name_for_year(year)
        if year is not None:
            ensure_year_repo_exists(year)

        dest = os.path.join(workdir, repo)
        if os.path.exists(dest):
            shutil.rmtree(dest)

        print(f"[CLONE] {repo}")
        run(["git", "clone", "--depth=1", git_url(repo), dest])

        changed = False
        for rel in rel_files:
            src = os.path.join(staging_root, rel)
            dst = os.path.join(dest, rel)  # keep same by_created/... structure
            ensure_dir(os.path.dirname(dst))
            # copy if new or different size/mtime quick check; then overwrite
            shutil.copy2(src, dst)
            changed = True

        if not changed:
            continue

        # commit if repo has changes
        run(["git", "add", "-A"], cwd=dest)
        # if no changes, skip
        status = run(["git", "status", "--porcelain"], cwd=dest).strip()
        if not status:
            print(f"[SKIP] no changes in {repo}")
            continue

        run(["git", "config", "user.name", "statground-bot"], cwd=dest)
        run(["git", "config", "user.email", "statground-bot@users.noreply.github.com"], cwd=dest)

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        msg = f"Polymarket daily update ({ts})"
        run(["git", "commit", "-m", msg], cwd=dest)
        run(["git", "push", "origin", DEFAULT_BRANCH], cwd=dest)
        updated_repos += 1

    shutil.rmtree(workdir)
    return updated_repos

# -----------------------
# Fetch incremental
# -----------------------
def fetch_incremental(entity: str, state: Dict[str, str]) -> Tuple[int, Optional[str]]:
    """
    Fetch pages ordered by updatedAt desc (or fallback), stop when we reach checkpoint.
    Writes files into OUT_ROOT/<entity>/...
    Returns (written_count, new_checkpoint)
    """
    url = ENDPOINTS[entity]
    last_cp = state.get(entity, "")
    best_seen = last_cp

    # Try primary order first; if API rejects, fallback
    orders_to_try = [ORDER_PRIMARY, ORDER_FALLBACK]
    order_used = None

    for order in orders_to_try:
        # Quick probe: request first page
        params = {"limit": str(PAGE_LIMIT), "offset": "0", "order": order, "ascending": "false"}
        data = safe_get_json(url, params)
        if isinstance(data, list):
            order_used = order
            break
        # some endpoints return dict with "data"
        if isinstance(data, dict):
            for k in ("data", entity, "results"):
                if k in data and isinstance(data[k], list):
                    order_used = order
                    break
        if order_used:
            break

    if not order_used:
        raise RuntimeError(f"Failed to fetch {entity}: API returned non-list for both orders.")

    print(f"[FETCH] {entity} order={order_used} checkpoint={last_cp or '(none)'}")

    total_written = 0
    out_base = OUT_ROOT

    for page in range(MAX_PAGES):
        offset = page * PAGE_LIMIT
        params = {"limit": str(PAGE_LIMIT), "offset": str(offset), "order": order_used, "ascending": "false"}
        data = safe_get_json(url, params)
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

        stop = False
        for obj in items:
            if not isinstance(obj, dict):
                continue

            updated = obj.get("updatedAt") or obj.get("updated_at") or ""
            # stop condition (only reliable when ordered by updatedAt desc)
            if last_cp and order_used.lower() == "updatedat" and iso_leq(updated, last_cp):
                stop = True
                break

            ok, _subdir, upd = write_entity(entity, obj, OUT_ROOT)
            if ok:
                total_written += 1
                if upd and (not best_seen or not iso_leq(upd, best_seen)):
                    best_seen = upd

        print(f"[{entity}] page={page+1} offset={offset} wrote={total_written}")
        if stop:
            print(f"[{entity}] reached checkpoint -> stop")
            break
        if len(items) < PAGE_LIMIT:
            break
        time.sleep(BASE_SLEEP)

    return total_written, (best_seen if best_seen else None)

# -----------------------
# MAIN
# -----------------------
def main():
    if not GH_PAT:
        raise RuntimeError("GH_PAT is required. Set secret and env GH_PAT.")

    # Clean staging output (only for this run)
    if os.path.exists(OUT_ROOT):
        shutil.rmtree(OUT_ROOT)
    ensure_dir(OUT_ROOT)

    state = load_state()
    new_state = dict(state)

    # 1) fetch incremental into staging (by_created/...)
    wrote_total = 0
    for entity in ("events", "markets", "series"):
        wrote, new_cp = fetch_incremental(entity, state)
        wrote_total += wrote
        if new_cp:
            new_state[entity] = new_cp

    # 2) fan-out into repos (auto-create yearly repos)
    updated_repos = fanout_and_push(OUT_ROOT)

    # 3) update checkpoint in orchestrator repo itself (so next run is incremental)
    save_state(new_state)
    # commit checkpoint file to orchestrator repo
    run(["git", "add", STATE_PATH])
    if run(["git", "status", "--porcelain"]).strip():
        run(["git", "config", "user.name", "statground-bot"])
        run(["git", "config", "user.email", "statground-bot@users.noreply.github.com"])
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        run(["git", "commit", "-m", f"Update crawl checkpoint ({ts})"])
        run(["git", "push", "origin", DEFAULT_BRANCH])
        print("[OK] checkpoint committed")
    else:
        print("[OK] checkpoint unchanged")

    print(f"\nDONE. staged_new_files={wrote_total} updated_repos={updated_repos}")


if __name__ == "__main__":
    main()
