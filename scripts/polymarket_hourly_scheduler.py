#!/usr/bin/env python3

"""
Hourly scheduler for a single workflow:
- Runs every hour.
- Crawls Polymarket only once per UTC day (when due).
- Always updates repo stats after crawl (or alone if crawl not due).

Key goals:
- One workflow YAML only.
- If crawl is due, do: crawl -> stats (in this order).
- Avoid orchestrator push conflicts: crawler version should update checkpoint via GitHub Contents API.
- The scheduler state is stored in the orchestrator repo using GitHub Contents API (SHA safe update).

Requirements:
- Secret: POLYMARKET_PAT (fine-grained PAT)
  - Contents: read/write
  - Actions: read/write (optional; not needed here because we run stats inline)
  - Administration: read/write (only if crawler auto-creates year repos)

Expected scripts in repo:
- scripts/polymarket_crawl_and_fanout.py  (the PAT v2 version that updates checkpoint via API)
- scripts/update_repo_stats.py            (your repo-stats generator)
"""
import os
import json
import base64
import urllib.parse
import urllib.request
import urllib.error
import subprocess
from datetime import datetime, timezone
from typing import Optional, Tuple

ORG = os.getenv("ORG", "statground")
ORCHESTRATOR_REPO = os.getenv("ORCHESTRATOR_REPO", "Statground_Data_Polymarket")
DEFAULT_BRANCH = os.getenv("DEFAULT_BRANCH", "main")
GH_PAT = os.getenv("GH_PAT", "")

SCHED_STATE_PATH = os.getenv("SCHED_STATE_PATH", ".state/polymarket_scheduler.json")
CRAWL_ONCE_PER_UTC_DAY = os.getenv("CRAWL_ONCE_PER_UTC_DAY", "true").lower() in ("1","true","yes")

def run(cmd, cwd=None):
    p = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(p.stdout)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return p.stdout

def gh_api_json(method: str, url: str, token: str, data: Optional[dict] = None) -> Tuple[int, object]:
    req = urllib.request.Request(url, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", "statground-polymarket-scheduler")
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
        obj = None
        try:
            obj = json.loads(payload) if payload else None
        except Exception:
            obj = payload[:200] if payload else None
        return e.code, obj

def get_content(path: str) -> Tuple[Optional[str], Optional[bytes]]:
    url = f"https://api.github.com/repos/{ORG}/{ORCHESTRATOR_REPO}/contents/{urllib.parse.quote(path)}?ref={urllib.parse.quote(DEFAULT_BRANCH)}"
    code, obj = gh_api_json("GET", url, GH_PAT)
    if code == 200 and isinstance(obj, dict) and obj.get("content"):
        sha = obj.get("sha")
        content = base64.b64decode(obj["content"].encode("utf-8"))
        return sha, content
    if code == 404:
        return None, None
    raise RuntimeError(f"Failed to read {path}: status={code} resp={obj}")

def put_content(path: str, content_bytes: bytes, message: str):
    sha, _ = get_content(path)
    url = f"https://api.github.com/repos/{ORG}/{ORCHESTRATOR_REPO}/contents/{urllib.parse.quote(path)}"
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch": DEFAULT_BRANCH,
    }
    if sha:
        payload["sha"] = sha
    code, obj = gh_api_json("PUT", url, GH_PAT, payload)
    if code not in (200, 201):
        raise RuntimeError(f"Failed to PUT {path}: status={code} resp={obj}")

def load_sched_state() -> dict:
    _, content = get_content(SCHED_STATE_PATH)
    if not content:
        return {}
    try:
        return json.loads(content.decode("utf-8")) or {}
    except Exception:
        return {}

def save_sched_state(state: dict):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    msg = f"Update scheduler state ({ts})"
    put_content(SCHED_STATE_PATH, json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"), msg)

def utc_day_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def should_crawl(state: dict) -> bool:
    if not CRAWL_ONCE_PER_UTC_DAY:
        return True
    last = state.get("last_crawl_utc_day")
    return last != utc_day_str()

def main():
    if not GH_PAT:
        raise RuntimeError("GH_PAT is required. Set secret POLYMARKET_PAT and env GH_PAT.")

    state = load_sched_state()
    do_crawl = should_crawl(state)

    if do_crawl:
        print(f"[SCHED] crawl due (last={state.get('last_crawl_utc_day')}) -> running crawl then stats")
        run(["python", "scripts/polymarket_crawl_and_fanout.py"])
        state["last_crawl_utc_day"] = utc_day_str()
        state["last_crawl_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
        save_sched_state(state)
    else:
        print(f"[SCHED] crawl not due today ({utc_day_str()}) -> stats only")

    # Always run stats after (or alone)
    run(["python", "scripts/update_repo_stats.py"])
    print("[DONE]")

if __name__ == "__main__":
    main()
