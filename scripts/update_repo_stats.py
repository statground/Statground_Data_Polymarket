#!/usr/bin/env python3

"""
Hourly repo stats generator.

Important:
- Does NOT clone big repos.
- Reads per-repo counts from a small file (default: POLYMARKET_COUNTS.json)
  that the daily crawler writes into each year repo.
- Writes POLYMARKET_REPO_STATS.md to orchestrator repo via GitHub Contents API (SHA-safe).
"""
import os
import json
import base64
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

ORG = os.getenv("ORG", "statground")
PREFIX = os.getenv("REPO_PREFIX", "Statground_Data_Polymarket")
ORCHESTRATOR_REPO = os.getenv("ORCHESTRATOR_REPO", "Statground_Data_Polymarket")
DEFAULT_BRANCH = os.getenv("DEFAULT_BRANCH", "main")
GH_PAT = os.getenv("GH_PAT", "")

COUNTS_FILE_NAME = os.getenv("COUNTS_FILE_NAME", "POLYMARKET_COUNTS.json")
STATS_MD_PATH = os.getenv("STATS_MD_PATH", "POLYMARKET_REPO_STATS.md")

def gh_api_json(method: str, url: str, token: str, data: Optional[dict] = None) -> Tuple[int, object]:
    req = urllib.request.Request(url, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", "statground-polymarket-stats")
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

def list_repos_in_org(org: str) -> List[str]:
    names = []
    page = 1
    per_page = 100
    while True:
        url = f"https://api.github.com/orgs/{org}/repos?per_page={per_page}&page={page}"
        code, obj = gh_api_json("GET", url, GH_PAT)
        if code != 200 or not isinstance(obj, list):
            raise RuntimeError(f"Failed to list repos: status={code} resp={obj}")
        if not obj:
            break
        for r in obj:
            n = (r or {}).get("name")
            if n:
                names.append(n)
        if len(obj) < per_page:
            break
        page += 1
    return names

def discover_target_repos() -> List[str]:
    names = list_repos_in_org(ORG)
    targets = [n for n in names if n == PREFIX or n.startswith(PREFIX + "_")]
    # Sort: base repo first, then numeric years ascending, then other
    def key(n: str):
        if n == PREFIX:
            return (0, 0, n)
        tail = n[len(PREFIX)+1:] if n.startswith(PREFIX + "_") else ""
        try:
            y = int(tail)
            return (1, y, n)
        except Exception:
            return (2, 0, n)
    return sorted(targets, key=key)

def get_file_from_repo(repo: str, path: str) -> Optional[bytes]:
    url = f"https://api.github.com/repos/{ORG}/{repo}/contents/{urllib.parse.quote(path)}?ref={urllib.parse.quote(DEFAULT_BRANCH)}"
    code, obj = gh_api_json("GET", url, GH_PAT)
    if code == 200 and isinstance(obj, dict) and obj.get("content"):
        return base64.b64decode(obj["content"].encode("utf-8"))
    if code == 404:
        return None
    raise RuntimeError(f"Failed to read {repo}/{path}: status={code} resp={obj}")

def get_stats_md_sha() -> Optional[str]:
    url = f"https://api.github.com/repos/{ORG}/{ORCHESTRATOR_REPO}/contents/{urllib.parse.quote(STATS_MD_PATH)}?ref={urllib.parse.quote(DEFAULT_BRANCH)}"
    code, obj = gh_api_json("GET", url, GH_PAT)
    if code == 200 and isinstance(obj, dict):
        return obj.get("sha")
    if code == 404:
        return None
    raise RuntimeError(f"Failed to read sha of {STATS_MD_PATH}: status={code} resp={obj}")

def put_stats_md(content: str):
    sha = get_stats_md_sha()
    url = f"https://api.github.com/repos/{ORG}/{ORCHESTRATOR_REPO}/contents/{urllib.parse.quote(STATS_MD_PATH)}"
    payload = {
        "message": f"Update Polymarket repo stats ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')})",
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "branch": DEFAULT_BRANCH,
    }
    if sha:
        payload["sha"] = sha
    code, obj = gh_api_json("PUT", url, GH_PAT, payload)
    if code not in (200, 201):
        raise RuntimeError(f"Failed to PUT {STATS_MD_PATH}: status={code} resp={obj}")

def main():
    if not GH_PAT:
        raise RuntimeError("Missing GH_PAT. Map secret POLYMARKET_PAT to env GH_PAT.")
    repos = discover_target_repos()

    rows = []
    totals = {
        "total_files": 0,
        "json_files": 0,
        "event_json": 0,
        "market_json": 0,
        "series_json": 0,
        "event_meta": 0,
        "market_meta": 0,
        "series_meta": 0,
    }

    for repo in repos:
        raw = get_file_from_repo(repo, COUNTS_FILE_NAME)
        if not raw:
            row = {
                "repo": repo,
                "total_files": 0, "json_files": 0,
                "event_json": 0, "market_json": 0, "series_json": 0,
                "event_meta": 0, "market_meta": 0, "series_meta": 0,
                "note": "counts file missing (will appear after next daily crawl)",
            }
        else:
            try:
                obj = json.loads(raw.decode("utf-8"))
                c = (obj or {}).get("counts") or {}
                row = {
                    "repo": repo,
                    "total_files": int(c.get("total_files", 0)),
                    "json_files": int(c.get("json_files", 0)),
                    "event_json": int(c.get("event_json", 0)),
                    "market_json": int(c.get("market_json", 0)),
                    "series_json": int(c.get("series_json", 0)),
                    "event_meta": int(c.get("event_meta", 0)),
                    "market_meta": int(c.get("market_meta", 0)),
                    "series_meta": int(c.get("series_meta", 0)),
                    "note": "",
                }
            except Exception:
                row = {
                    "repo": repo,
                    "total_files": 0, "json_files": 0,
                    "event_json": 0, "market_json": 0, "series_json": 0,
                    "event_meta": 0, "market_meta": 0, "series_meta": 0,
                    "note": "counts file invalid JSON",
                }

        rows.append(row)
        for k in totals.keys():
            totals[k] += row[k]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    md = []
    md.append("# Polymarket Repo Stats")
    md.append(f"Updated: {now}")
    md.append(f"Owner: {ORG}")
    md.append(f"Prefix: {PREFIX} (auto-detect main + year repos)")
    md.append("")
    md.append("## Summary (All Repos)")
    md.append(f"- Total files: {totals['total_files']:,}")
    md.append(f"- JSON files: {totals['json_files']:,}")
    md.append(f"- event JSON: {totals['event_json']:,} (excluding meta)")
    md.append(f"- market JSON: {totals['market_json']:,} (excluding meta)")
    md.append(f"- series JSON: {totals['series_json']:,} (excluding meta)")
    md.append(f"- event meta: {totals['event_meta']:,}")
    md.append(f"- market meta: {totals['market_meta']:,}")
    md.append(f"- series meta: {totals['series_meta']:,}")
    md.append("")
    md.append("## Per Repository")
    md.append("Repository | Total files | JSON files | event JSON | market JSON | series JSON | event meta | market meta | series meta | Note")
    md.append("---|---:|---:|---:|---:|---:|---:|---:|---:|---")
    for r in rows:
        md.append(
            f"{r['repo']} | {r['total_files']:,} | {r['json_files']:,} | "
            f"{r['event_json']:,} | {r['market_json']:,} | {r['series_json']:,} | "
            f"{r['event_meta']:,} | {r['market_meta']:,} | {r['series_meta']:,} | {r['note']}"
        )
    md.append("")
    md.append("### Notes")
    md.append("- Counts are read from `POLYMARKET_COUNTS.json` in each repo.")
    md.append("- That file is updated during the **daily crawl** (once per UTC day).")
    md.append("- This stats workflow runs hourly, so timestamps update hourly even if counts do not change.")
    md.append("")

    put_stats_md("\n".join(md))
    print("[OK] stats md updated via GitHub Contents API")

if __name__ == "__main__":
    main()
