#!/usr/bin/env python3

"""
Polymarket incremental crawler (events/markets/series) + fan-out by year repos.

Design goals:
- Incremental fetch by updatedAt (fallback order=id).
- Save JSON + meta JSON files into by_created/{entity}/{YYYY}/{MM}/.
- Fan out files into year repos: Statground_Data_Polymarket_{YYYY}
  (year <= 2022 goes into _2022 as per your rule? Actually year is direct; you can rename rule later.)
- Year repo auto-creation supported (requires PAT with Administration write).
- Avoid orchestrator git push conflicts by updating checkpoint via GitHub Contents API (SHA-safe).
- Write POLYMARKET_COUNTS.json into each year repo on every crawl (so hourly stats can read counts without cloning).

Env:
- GH_PAT, ORG, REPO_PREFIX, ORCHESTRATOR_REPO, DEFAULT_BRANCH
- POLY_BASE, PAGE_LIMIT, MAX_PAGES, ORDER_PRIMARY, ORDER_FALLBACK
- OUT_ROOT, CHECKPOINT_PATH
- COUNTS_FILE_NAME
"""
import os
import json
import time
import random
import shutil
import subprocess
import urllib.parse
import urllib.request
import urllib.error
import base64
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

ORG = os.getenv("ORG", "statground")
PREFIX = os.getenv("REPO_PREFIX", "Statground_Data_Polymarket")
ORCHESTRATOR_REPO = os.getenv("ORCHESTRATOR_REPO", PREFIX)
DEFAULT_BRANCH = os.getenv("DEFAULT_BRANCH", "main")
GH_PAT = os.getenv("GH_PAT", "")

BASE = os.getenv("POLY_BASE", "https://gamma-api.polymarket.com")
ENDPOINTS = {
    "events": f"{BASE}/events",
    "markets": f"{BASE}/markets",
    "series": f"{BASE}/series",
}

PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "100"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "200"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "8"))
BASE_SLEEP = float(os.getenv("BASE_SLEEP", "0.4"))

ORDER_PRIMARY = os.getenv("ORDER_PRIMARY", "updatedAt")
ORDER_FALLBACK = os.getenv("ORDER_FALLBACK", "id")

OUT_ROOT = os.getenv("OUT_ROOT", "by_created")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", ".state/polymarket_checkpoint.json")
COUNTS_FILE_NAME = os.getenv("COUNTS_FILE_NAME", "POLYMARKET_COUNTS.json")

def run(cmd: List[str], cwd: Optional[str] = None) -> str:
    res = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{res.stdout}")
    return res.stdout

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def dump_json_minified(path: str, obj):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

def dump_json_pretty(path: str, obj):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)

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
            if e.code == 429 or e.code >= 500:
                sleep_s = min(60, 2 ** attempt) + random.random()
                print(f"[HTTP {e.code}] retry in {sleep_s:.1f}s url={url} params={params}")
                time.sleep(sleep_s)
                continue
            body = ""
            try:
                body = (e.read().decode("utf-8", errors="ignore") or "")[:200]
            except Exception:
                pass
            print(f"[HTTP {e.code}] fail url={url} params={params} body={body}")
            return None
        except Exception as e:
            sleep_s = min(60, 2 ** attempt) + random.random()
            print(f"[NET] {e} retry in {sleep_s:.1f}s url={url} params={params}")
            time.sleep(sleep_s)
            continue
    return None

def write_entity(entity: str, obj: dict, out_root: str) -> Tuple[bool, str, Optional[str], Optional[int]]:
    _id = obj.get("id")
    if _id is None:
        return False, "no_id", None, None

    if entity == "events":
        folder_dt = pick_event_folder_dt(obj)
        prefix = "event"
    elif entity == "markets":
        folder_dt = pick_market_folder_dt(obj)
        prefix = "market"
    else:
        folder_dt = pick_series_folder_dt(obj)
        prefix = "series"

    subdir = dt_to_subdir(folder_dt)
    year = None
    if folder_dt is not None:
        year = folder_dt.year

    target_dir = os.path.join(out_root, entity, subdir)
    ensure_dir(target_dir)

    data_path = os.path.join(target_dir, f"{prefix}_{_id}.json")
    meta_path = os.path.join(target_dir, f"{prefix}_{_id}.meta.json")

    dump_json_minified(data_path, obj)

    # Meta
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
    return True, subdir, updated, year

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
            raw = resp.read().decode("utf-8") or "null"
            try:
                return resp.status, json.loads(raw)
            except Exception:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        payload = ""
        try:
            payload = e.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
        try:
            obj = json.loads(payload) if payload else None
        except Exception:
            obj = payload[:200] if payload else None
        return e.code, obj

def get_content(path: str) -> Tuple[Optional[str], Optional[bytes]]:
    url = f"https://api.github.com/repos/{ORG}/{ORCHESTRATOR_REPO}/contents/{urllib.parse.quote(path)}?ref={urllib.parse.quote(DEFAULT_BRANCH)}"
    code, obj = gh_api_json("GET", url, GH_PAT)
    if code == 200 and isinstance(obj, dict) and obj.get("content"):
        return obj.get("sha"), base64.b64decode(obj["content"].encode("utf-8"))
    if code == 404:
        return None, None
    raise RuntimeError(f"Failed to read {path}: status={code} resp={obj}")

def put_content(path: str, content_bytes: bytes, message: str):
    sha, _ = get_content(path)
    url = f"https://api.github.com/repos/{ORG}/{ORCHESTRATOR_REPO}/contents/{urllib.parse.quote(path)}"
    payload = {"message": message, "content": base64.b64encode(content_bytes).decode("utf-8"), "branch": DEFAULT_BRANCH}
    if sha:
        payload["sha"] = sha
    code, obj = gh_api_json("PUT", url, GH_PAT, payload)
    if code not in (200, 201):
        raise RuntimeError(f"Failed to PUT {path}: status={code} resp={obj}")

def load_checkpoint() -> Dict[str, str]:
    _, content = get_content(CHECKPOINT_PATH)
    if not content:
        return {}
    try:
        return json.loads(content.decode("utf-8")) or {}
    except Exception:
        return {}

def save_checkpoint(state: Dict[str, str]):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    put_content(CHECKPOINT_PATH, json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
                f"Update crawl checkpoint ({ts})")


def save_targets(repos: List[str]):
    """
    Save list of target repos for stats (avoid org listing permissions).
    Written into orchestrator repo via Contents API.
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    payload = {
        "updated_at_utc": ts,
        "org": ORG,
        "prefix": PREFIX,
        "repos": sorted(set(repos)),
    }
    put_content(
        ".state/polymarket_targets.json",
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        f"Update Polymarket targets ({ts})"
    )


def iso_leq(a: str, b: str) -> bool:
    da = parse_iso_to_utc_naive(a) if a else None
    db = parse_iso_to_utc_naive(b) if b else None
    if da is None or db is None:
        return False
    return da <= db

def pick_order(entity: str) -> str:
    url = ENDPOINTS[entity]
    for order in (ORDER_PRIMARY, ORDER_FALLBACK):
        params = {"limit": str(PAGE_LIMIT), "offset": "0", "order": order, "ascending": "false"}
        data = safe_get_json(url, params)
        items = data
        if isinstance(data, dict):
            for k in ("data", entity, "results"):
                if k in data and isinstance(data[k], list):
                    items = data[k]
                    break
        if isinstance(items, list):
            return order
    raise RuntimeError(f"Failed to fetch {entity}: API returned non-list for both orders.")

def fetch_incremental(entity: str, checkpoint: Dict[str, str]) -> Tuple[int, Optional[str]]:
    url = ENDPOINTS[entity]
    last_cp = checkpoint.get(entity, "")
    best_seen = last_cp

    order_used = pick_order(entity)
    print(f"[FETCH] {entity} order={order_used} checkpoint={(last_cp or '(none)')}")

    total_written = 0
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
            if last_cp and order_used.lower() == "updatedat" and iso_leq(updated, last_cp):
                stop = True
                break
            ok, _subdir, upd, _year = write_entity(entity, obj, OUT_ROOT)
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

def repo_name_for_year(year: Optional[int]) -> str:
    if year is None:
        return PREFIX
    return f"{PREFIX}_{year}"

def ensure_repo_exists(repo: str):
    code, _ = gh_api_json("GET", f"https://api.github.com/repos/{ORG}/{repo}", GH_PAT)
    if code == 200:
        return
    if code != 404:
        raise RuntimeError(f"Unexpected status checking repo {ORG}/{repo}: {code}")
    print(f"[CREATE] {ORG}/{repo}")
    payload = {
        "name": repo,
        "private": False,
        "auto_init": True,
        "description": f"Polymarket archive {repo.replace(PREFIX+'_','')} (auto-created)",
    }
    code2, obj2 = gh_api_json("POST", f"https://api.github.com/orgs/{ORG}/repos", GH_PAT, payload)
    if code2 != 201:
        raise RuntimeError(f"Failed to create repo {ORG}/{repo}: status={code2} resp={obj2}")

def git_url(repo: str) -> str:
    return f"https://x-access-token:{GH_PAT}@github.com/{ORG}/{repo}.git"

def compute_counts_via_git_ls_files(repo_dir: str) -> Dict[str, int]:
    """Count tracked files using `git ls-files`.

    IMPORTANT: Do NOT rely on directory names like `/events/` because the archive layout
    can be either `events/...` or `by_created/events/...`. Instead, classify by filename
    prefixes (`event_`, `market_`, `series_`) and `.meta.json` suffix.
    """
    out = run(["git", "ls-files"], cwd=repo_dir)
    files = [line.strip() for line in out.splitlines() if line.strip()]

    total = len(files)
    json_files = sum(1 for p in files if p.endswith(".json"))

    def base_name(path: str) -> str:
        return path.rsplit("/", 1)[-1]

    def is_entity_json(fn: str, prefix: str) -> bool:
        return fn.startswith(prefix) and fn.endswith(".json") and (not fn.endswith(".meta.json"))

    def is_entity_meta(fn: str, prefix: str) -> bool:
        return fn.startswith(prefix) and fn.endswith(".meta.json")

    event_json  = sum(1 for p in files if is_entity_json(base_name(p), "event_"))
    market_json = sum(1 for p in files if is_entity_json(base_name(p), "market_"))
    series_json = sum(1 for p in files if is_entity_json(base_name(p), "series_"))

    event_meta  = sum(1 for p in files if is_entity_meta(base_name(p), "event_"))
    market_meta = sum(1 for p in files if is_entity_meta(base_name(p), "market_"))
    series_meta = sum(1 for p in files if is_entity_meta(base_name(p), "series_"))

    return {
        "total_files": total,
        "json_files": json_files,
        "event_json": event_json,
        "market_json": market_json,
        "series_json": series_json,
        "event_meta": event_meta,
        "market_meta": market_meta,
        "series_meta": series_meta,
    }

def fanout_and_push(staging_root: str) -> int:
    year_to_files: Dict[Optional[int], List[str]] = {}
    touched_repos: List[str] = []

    for entity in ("events", "markets", "series"):
        entity_dir = os.path.join(staging_root, entity)
        if not os.path.isdir(entity_dir):
            continue
        for y in os.listdir(entity_dir):
            y_path = os.path.join(entity_dir, y)
            if not os.path.isdir(y_path):
                continue
            year = None
            if y != "null":
                try:
                    year = int(y)
                except Exception:
                    year = None

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
        ensure_repo_exists(repo)
        touched_repos.append(repo)

        dest = os.path.join(workdir, repo)
        if os.path.exists(dest):
            shutil.rmtree(dest)

        print(f"[CLONE] {repo}")
        run(["git", "clone", "--depth=1", git_url(repo), dest])

        # Copy new/updated files
        for rel in rel_files:
            src = os.path.join(staging_root, rel)
            dst = os.path.join(dest, rel)
            ensure_dir(os.path.dirname(dst))
            shutil.copy2(src, dst)

        # Update counts file in the target repo
        counts = compute_counts_via_git_ls_files(dest)
        counts_payload = {
            "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "repo": repo,
            "counts": counts,
        }
        with open(os.path.join(dest, COUNTS_FILE_NAME), "w", encoding="utf-8") as f:
            json.dump(counts_payload, f, ensure_ascii=False, indent=2, sort_keys=True)

        run(["git", "add", "-A"], cwd=dest)
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

    # Save targets list for hourly stats
    try:
        save_targets([ORCHESTRATOR_REPO] + touched_repos)
    except Exception as e:
        print(f"[WARN] failed to save targets: {e}")

    shutil.rmtree(workdir)
    return updated_repos

def main():
    if not GH_PAT:
        raise RuntimeError("GH_PAT is required. Map secret POLYMARKET_PAT to env GH_PAT.")

    checkpoint = load_checkpoint()
    new_checkpoint = dict(checkpoint)

    if os.path.exists(OUT_ROOT):
        shutil.rmtree(OUT_ROOT)
    ensure_dir(OUT_ROOT)

    wrote_total = 0
    for entity in ("events", "markets", "series"):
        wrote, new_cp = fetch_incremental(entity, checkpoint)
        wrote_total += wrote
        if new_cp:
            new_checkpoint[entity] = new_cp

    updated_repos = fanout_and_push(OUT_ROOT)
    save_checkpoint(new_checkpoint)

    print(f"\nDONE. staged_new_files={wrote_total} updated_repos={updated_repos}")

if __name__ == "__main__":
    main()
