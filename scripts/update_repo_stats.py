#!/usr/bin/env python3
import os
import re
import json
import shutil
import subprocess
from datetime import datetime, timezone
from typing import Dict, List, Tuple
import urllib.request

ORG = os.getenv("ORG", "statground")
PREFIX = os.getenv("REPO_PREFIX", "Statground_Data_Polymarket")
DEFAULT_BRANCH = os.getenv("DEFAULT_BRANCH", "main")
OUT_MD = os.getenv("OUT_MD", "POLYMARKET_REPO_STATS.md")

YEAR_REPO_RE = re.compile(rf"^{re.escape(PREFIX)}_(\d{{4}})$")

def run(cmd: List[str], cwd: str = None) -> str:
    res = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{res.stdout}")
    return res.stdout

def http_get_json(url: str, token: str = "") -> dict:
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", "statground-polymarket-stats-bot")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))

def list_repos_in_org(org: str, token: str = "") -> List[str]:
    repos: List[str] = []
    page = 1
    while True:
        url = f"https://api.github.com/orgs/{org}/repos?per_page=100&page={page}&type=public"
        data = http_get_json(url, token=token)
        if not isinstance(data, list) or not data:
            break
        for r in data:
            name = r.get("name")
            if isinstance(name, str):
                repos.append(name)
        if len(data) < 100:
            break
        page += 1
    return repos

def discover_target_repos(org: str, token: str = "") -> List[str]:
    names = list_repos_in_org(org, token=token)
    targets = []
    for n in names:
        if n == PREFIX:
            targets.append(n)
        else:
            m = YEAR_REPO_RE.match(n)
            if m:
                targets.append(n)
    def keyfn(x: str):
        if x == PREFIX:
            return (0, 0)
        m = YEAR_REPO_RE.match(x)
        y = int(m.group(1)) if m else 9999
        return (1, y)
    targets.sort(key=keyfn)
    return targets

def clone_no_checkout(org: str, repo: str, token: str, workdir: str) -> str:
    dest = os.path.join(workdir, repo)
    if os.path.exists(dest):
        shutil.rmtree(dest)
    if token:
        url = f"https://x-access-token:{token}@github.com/{org}/{repo}.git"
    else:
        url = f"https://github.com/{org}/{repo}.git"
    run(["git","clone","--filter=blob:none","--no-checkout","--depth=1",url,dest])
    return dest

def list_paths(repo_dir: str) -> List[str]:
    try:
        run(["git","rev-parse","--verify",f"origin/{DEFAULT_BRANCH}"], cwd=repo_dir)
        ref = f"origin/{DEFAULT_BRANCH}"
    except Exception:
        ref = "HEAD"
    out = run(["git","ls-tree","-r","--name-only",ref], cwd=repo_dir)
    return [line.strip() for line in out.splitlines() if line.strip()]

def count_entities(paths: List[str]) -> Dict[str, int]:
    total_files = len(paths)
    json_files = 0
    event_json = 0
    market_json = 0
    series_json = 0
    for p in paths:
        if p.endswith(".json"):
            json_files += 1
            base = os.path.basename(p)
            if base.endswith(".meta.json"):
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
    rows = []
    totals = {"total_files":0,"json_files":0,"event_json":0,"market_json":0,"series_json":0}
    for repo in targets:
        repo_dir = clone_no_checkout(ORG, repo, token, workdir)
        paths = list_paths(repo_dir)
        counts = count_entities(paths)
        rows.append((repo, counts))
        for k in totals:
            totals[k] += counts[k]
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    md = []
    md.append("# Polymarket Repo Stats")
    md.append(f"- Updated: **{now_utc}**")
    md.append("")
    md.append("## Summary (All Repos)")
    md.append(f"- Total files: **{fmt_int(totals['total_files'])}**")
    md.append(f"- JSON files: **{fmt_int(totals['json_files'])}**")
    md.append(f"- event JSON: **{fmt_int(totals['event_json'])}**")
    md.append(f"- market JSON: **{fmt_int(totals['market_json'])}**")
    md.append(f"- series JSON: **{fmt_int(totals['series_json'])}**")
    md.append("")
    md.append("## Per Repository")
    md.append("| Repository | Total files | JSON files | event JSON | market JSON | series JSON |")
    md.append("|---|---:|---:|---:|---:|---:|")
    for repo, c in rows:
        md.append(f"| `{repo}` | {fmt_int(c['total_files'])} | {fmt_int(c['json_files'])} | {fmt_int(c['event_json'])} | {fmt_int(c['market_json'])} | {fmt_int(c['series_json'])} |")
    with open(OUT_MD,"w",encoding="utf-8") as f:
        f.write("\n".join(md))
    shutil.rmtree(workdir)

if __name__=="__main__":
    main()
