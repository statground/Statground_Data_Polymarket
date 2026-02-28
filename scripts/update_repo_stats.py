#!/usr/bin/env python3
"""
Repo stats generator (ClickHouse-backed).

Writes POLYMARKET_REPO_STATS.md to orchestrator repo via GitHub Contents API (SHA-safe).

Counts are derived from ClickHouse tables:
- polymarket_raw
- polymarket_event
- polymarket_market
- polymarket_series
"""
import os
import json
import base64
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Dict, Tuple

import clickhouse_connect

ORG = os.getenv("ORG", "statground")
ORCHESTRATOR_REPO = os.getenv("ORCHESTRATOR_REPO", "Statground_Data_Polymarket")
DEFAULT_BRANCH = os.getenv("DEFAULT_BRANCH", "main")
GH_TOKEN = os.getenv("GH_TOKEN", "") or os.getenv("GITHUB_TOKEN", "")

OUT_MD_PATH = os.getenv("OUT_MD_PATH", "POLYMARKET_REPO_STATS.md")

# ClickHouse
CH_HOST = os.getenv("CLICKHOUSE_HOST", "")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DATABASE", "statground_polymarket")
CH_IFACE = os.getenv("CLICKHOUSE_INTERFACE", "http").lower()

RAW_TABLE = os.getenv("RAW_TABLE", "polymarket_raw")
EVENT_TABLE = os.getenv("EVENT_TABLE", "polymarket_event")
MARKET_TABLE = os.getenv("MARKET_TABLE", "polymarket_market")
SERIES_TABLE = os.getenv("SERIES_TABLE", "polymarket_series")


def gh_api_json(method: str, url: str, token: str, payload=None) -> Tuple[int, object]:
    req = urllib.request.Request(url, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data=data, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (json.loads(body) if body else {})
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8")
            return e.code, (json.loads(body) if body else {})
        except Exception:
            return e.code, {}
    except Exception:
        return 0, {}

def get_content(path: str) -> Tuple[str, bytes]:
    url = f"https://api.github.com/repos/{ORG}/{ORCHESTRATOR_REPO}/contents/{urllib.parse.quote(path)}?ref={DEFAULT_BRANCH}"
    code, obj = gh_api_json("GET", url, GH_TOKEN)
    if code != 200 or not isinstance(obj, dict):
        return "", b""
    sha = obj.get("sha") or ""
    content_b64 = obj.get("content", "") or ""
    if content_b64:
        return sha, base64.b64decode(content_b64)
    return sha, b""

def put_content(path: str, content_bytes: bytes, message: str):
    url = f"https://api.github.com/repos/{ORG}/{ORCHESTRATOR_REPO}/contents/{urllib.parse.quote(path)}"
    sha, _ = get_content(path)
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch": DEFAULT_BRANCH,
    }
    if sha:
        payload["sha"] = sha
    code, obj = gh_api_json("PUT", url, GH_TOKEN, payload)
    if code not in (200, 201):
        raise RuntimeError(f"Failed to PUT {path}: status={code} resp={obj}")

def ch_client():
    if not CH_HOST:
        raise RuntimeError("CLICKHOUSE_HOST is required.")
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
        interface=CH_IFACE,
    )

def q1(ch, sql: str):
    # returns first row as dict
    res = ch.query(sql)
    if not res.result_rows:
        return {}
    cols = res.column_names
    row = res.result_rows[0]
    return {cols[i]: row[i] for i in range(len(cols))}

def main():
    if not GH_TOKEN:
        raise RuntimeError("Need GH_TOKEN or GITHUB_TOKEN (Actions provides GITHUB_TOKEN).")

    ch = ch_client()

    raw = q1(ch, f"""
        SELECT
          count() AS rows,
          uniqExact(tuple(entity, object_id)) AS uniq_keys,
          max(collected_at_utc) AS last_collected_utc
        FROM {RAW_TABLE}
    """)

    ev = q1(ch, f"""
        SELECT
          count() AS rows,
          uniqExact(event_id) AS uniq_ids,
          max(updated_at_utc) AS last_updated_utc
        FROM {EVENT_TABLE}
    """)

    mk = q1(ch, f"""
        SELECT
          count() AS rows,
          uniqExact(market_id) AS uniq_ids,
          max(updated_at_utc) AS last_updated_utc
        FROM {MARKET_TABLE}
    """)

    sr = q1(ch, f"""
        SELECT
          count() AS rows,
          uniqExact(series_id) AS uniq_ids,
          max(updated_at_utc) AS last_updated_utc
        FROM {SERIES_TABLE}
    """)

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    md = []
    md.append("# Polymarket repository stats (ClickHouse)")
    md.append("")
    md.append(f"- Updated: **{now_utc}**")
    md.append(f"- ClickHouse: **{CH_HOST}:{CH_PORT} / {CH_DB}** (interface={CH_IFACE})")
    md.append("")
    md.append("## Tables")
    md.append("")
    md.append("| Table | Rows | Unique IDs/Keys | Last updated (UTC) |")
    md.append("|---|---:|---:|---|")
    md.append(f"| `{RAW_TABLE}` | {raw.get('rows',0):,} | {raw.get('uniq_keys',0):,} | {str(raw.get('last_collected_utc',''))} |")
    md.append(f"| `{EVENT_TABLE}` | {ev.get('rows',0):,} | {ev.get('uniq_ids',0):,} | {str(ev.get('last_updated_utc',''))} |")
    md.append(f"| `{MARKET_TABLE}` | {mk.get('rows',0):,} | {mk.get('uniq_ids',0):,} | {str(mk.get('last_updated_utc',''))} |")
    md.append(f"| `{SERIES_TABLE}` | {sr.get('rows',0):,} | {sr.get('uniq_ids',0):,} | {str(sr.get('last_updated_utc',''))} |")
    md.append("")
    md.append("## Notes")
    md.append("- Tables use `ReplacingMergeTree`, so duplicates can exist physically until merges.")
    md.append("- For exact latest-row reads, use `FINAL` or `argMax(..., updated_at_ms)` patterns.")

    content = ("\n".join(md) + "\n").encode("utf-8")
    put_content(OUT_MD_PATH, content, f"Update Polymarket repo stats ({now_utc})")
    print("DONE. wrote", OUT_MD_PATH)

if __name__ == "__main__":
    main()
