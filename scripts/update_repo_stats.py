#!/usr/bin/env python3
import os
import re
import json
import shutil
import subprocess
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import urllib.request
import urllib.error

ORG = os.getenv("ORG", "statground")
PREFIX = os.getenv("REPO_PREFIX", "Statground_Data_Polymarket")
DEFAULT_BRANCH = os.getenv("DEFAULT_BRANCH", "main")
OUT_MD = os.getenv("OUT_MD", "POLYMARKET_REPO_STATS.md")

# ORG_MODE="org"|"user"|"auto"
ORG_MODE = os.getenv("ORG_MODE", "auto").lower()

YEAR_REPO_RE = re.compile(rf"^{re.escape(PREFIX)}_(\d{{4}})$")


def run(cmd: List[str], cwd: str = None) -> str:
    res = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{res.stdout}")
    return res.stdout


def http_get_json(url: str, token: str = "") -> object:
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", "statground-polymarket-stats-bot")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def list_repos(owner: str, token: str = "", mode: str = "auto") -> List[str]:
    """
    Robust repo listing for both organizations and users.

    GitHub can return 404 for /orgs/{org}/repos if:
      - the token lacks org permissions (404 masking),
      - the owner is not an org,
      - org settings restrict listing.
    In those cases, /users/{owner}/repos may still work for public repos.
    """
    def paged(endpoint: str, use_token: bool) -> List[str]:
        repos: List[str] = []
        page = 1
        while True:
            url = f"https://api.github.com{endpoint}?per_page=100&page={page}&type=public"
            tk = token if use_token else ""
            data = http_get_json(url, token=tk)
            if not isinstance(data, list) or not data:
                break
            for r in data:
                name = r.get("name") if isinstance(r, dict) else None
                if isinstance(name, str):
                    repos.append(name)
            if len(data) < 100:
                break
            page += 1
        return repos

    if mode == "org":
        candidates = [f"/orgs/{owner}/repos"]
    elif mode == "user":
        candidates = [f"/users/{owner}/repos"]
    else:
        candidates = [f"/orgs/{owner}/repos", f"/users/{owner}/repos"]

    last_err: Optional[Exception] = None
    for ep in candidates:
        try:
            return paged(ep, use_token=bool(token))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (401, 403, 404):
                try:
                    return paged(ep, use_token=False)
                except Exception as e2:
                    last_err = e2
                    continue
            continue
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"Failed to list repos for owner={owner}. last_err={last_err}")


def discover_target_repos(owner: str, token: str = "") -> List[str]:
    names = list_repos(owner, token=token, mode=ORG_MODE)

    targets: List[str] = []
    for n in names:
        if n == PREFIX:
            targets.append(n)
        else:
            if YEAR_REPO_RE.match(n):
                targets.append(n)

    def keyfn(x: str):
        if x == PREFIX:
            return (0, 0)
        m = YEAR_REPO_RE.match(x)
        y = int(m.group(1)) if m else 9999
        return (1, y)

    targets.sort(key=keyfn)
    return targets


def clone_no_checkout(owner: str, repo: str, token: str, workdir: str) -> str:
    dest = os.path.join(workdir, repo)
    if os.path.exists(dest):
        shutil.rmtree(dest)

    if token:
        url = f"https://x-access-token:{token}@github.com/{owner}/{repo}.git"
    else:
        url = f"https://github.com/{owner}/{repo}.git"

    run(["git", "clone", "--filter=blob:none", "--no-checkout", "--depth=1", url, dest])
    return dest


def list_paths(repo_dir: str) -> List[str]:
    try:
        run(["git", "rev-parse", "--verify", f"origin/{DEFAULT_BRANCH}"], cwd=repo_dir)
        ref = f"origin/{DEFAULT_BRANCH}"
    except Exception:
        ref = "HEAD"
    out = run(["git", "ls-tree", "-r", "--name-only", ref], cwd=repo_dir)
    return [line.strip() for line in out.splitlines() if line.strip()]


def count_entities(paths: List[str]) -> Dict[str, int]:
    total_files = len(paths)

    json_files = 0
    event_json = market_json = series_json = 0
    event_meta = market_meta = series_meta = 0

    for p in paths:
        base = os.path.basename(p)
        if not p.endswith(".json"):
            continue

        json_files += 1

        if base.endswith(".meta.json"):
            if base.startswith("event_"):
                event_meta += 1
            elif base.startswith("market_"):
                market_meta += 1
            elif base.startswith("series_"):
                series_meta += 1
            continue

        if base.startswith("event_"):
            event_json += 1
        elif base.startswith("market_"):
            market_json += 1
        elif base.startswith("series_"):
            series_json += 1

    return {
        "total_files": total_files,
        "json_files": json_files,
        "event_json": event_json,
        "market_json": market_json,
        "series_json": series_json,
        "event_meta": event_meta,
        "market_meta": market_meta,
        "series_meta": series_meta,
    }


def fmt_int(n: int) -> str:
    return f"{n:,}"


def main():
    token = os.getenv("GH_READ_TOKEN", "")
    workdir = os.path.abspath(".tmp_repo_scan")

    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir, exist_ok=True)

    targets = discover_target_repos(ORG, token=token)
    if not targets:
        raise RuntimeError(f"No target repos found for owner={ORG} prefix={PREFIX}")

    rows: List[Tuple[str, Dict[str, int]]] = []
    totals = {
        "total_files": 0, "json_files": 0,
        "event_json": 0, "market_json": 0, "series_json": 0,
        "event_meta": 0, "market_meta": 0, "series_meta": 0
    }

    for repo in targets:
        print(f"[SCAN] {repo}")
        repo_dir = clone_no_checkout(ORG, repo, token, workdir)
        paths = list_paths(repo_dir)
        counts = count_entities(paths)
        rows.append((repo, counts))
        for k in totals.keys():
            totals[k] += counts[k]

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    md: List[str] = []
    md.append("# Polymarket Repo Stats")
    md.append("")
    md.append(f"- Updated: **{now_utc}**")
    md.append(f"- Owner: `{ORG}` (mode: `{ORG_MODE}`)")
    md.append(f"- Prefix: `{PREFIX}` (auto-detect main + year repos)")
    md.append("")

    md.append("## Summary (All Repos)")
    md.append("")
    md.append(f"- Total files: **{fmt_int(totals['total_files'])}**")
    md.append(f"- JSON files: **{fmt_int(totals['json_files'])}**")
    md.append(f"- event JSON: **{fmt_int(totals['event_json'])}** (excluding meta)")
    md.append(f"- market JSON: **{fmt_int(totals['market_json'])}** (excluding meta)")
    md.append(f"- series JSON: **{fmt_int(totals['series_json'])}** (excluding meta)")
    md.append(f"- event meta: **{fmt_int(totals['event_meta'])}**")
    md.append(f"- market meta: **{fmt_int(totals['market_meta'])}**")
    md.append(f"- series meta: **{fmt_int(totals['series_meta'])}**")
    md.append("")

    md.append("## Per Repository")
    md.append("")
    md.append("| Repository | Total files | JSON files | event JSON | market JSON | series JSON | event meta | market meta | series meta |")
    md.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for repo, c in rows:
        md.append(
            f"| `{repo}` | {fmt_int(c['total_files'])} | {fmt_int(c['json_files'])} | "
            f"{fmt_int(c['event_json'])} | {fmt_int(c['market_json'])} | {fmt_int(c['series_json'])} | "
            f"{fmt_int(c['event_meta'])} | {fmt_int(c['market_meta'])} | {fmt_int(c['series_meta'])} |"
        )

    md.append("")
    md.append("### Notes")
    md.append("- Repo listing: tries `/orgs/{owner}/repos` then `/users/{owner}/repos`. If token causes 404 masking, retries without token.")
    md.append("- Entity counts are `event_/market_/series_` excluding `*.meta.json`. Meta counts shown separately.")
    md.append("- Uses `git clone --filter=blob:none --no-checkout --depth=1` + `git ls-tree -r` for scalable listing.")
    md.append("")

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md))

    shutil.rmtree(workdir)
    print(f"[OK] wrote {OUT_MD}")


if __name__ == "__main__":
    main()
