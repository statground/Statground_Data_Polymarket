#!/usr/bin/env python3
"""
Polymarket incremental crawler -> ClickHouse (normalized only).

What changed vs previous version:
- No GitHub file fan-out / no year repos.
- No dedicated polymarket_raw table writes.
- Each fetched object is inserted directly into one normalized ClickHouse table,
  and the original API payload is stored in the table's raw_json column.
- Checkpoint is still stored in this repo under .state/ using GitHub Contents API (SHA-safe),
  so workflow concurrency won't corrupt the checkpoint.

Required env (GitHub Actions secrets recommended):
- CLICKHOUSE_HOST
- CLICKHOUSE_PORT
- CLICKHOUSE_USER
- CLICKHOUSE_PASSWORD
- CLICKHOUSE_DATABASE   (default: statground_polymarket)
- CLICKHOUSE_INTERFACE  (http|native, default: http)

Optional:
- GH_TOKEN (PAT) or GITHUB_TOKEN
- ORG, ORCHESTRATOR_REPO, DEFAULT_BRANCH
- POLY_BASE, PAGE_LIMIT, MAX_PAGES, ORDER_PRIMARY, ORDER_FALLBACK

Notes:
- ClickHouse tables are expected to exist (DDL in your polymarket_*.SQL).
- Engines are ReplacingMergeTree, so duplicate (same id) rows may exist physically until merges;
  for analytics, use argMax / max(collected_at_utc) patterns where needed.
"""
from __future__ import annotations

import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import clickhouse_connect

from scripts.clickhouse_optimize import optimize_random_partitions

# -------------------------
# Env
# -------------------------
POLY_BASE = os.getenv("POLY_BASE", "https://gamma-api.polymarket.com").rstrip("/")
ENDPOINTS = {
    "events":  f"{POLY_BASE}/events",
    "markets": f"{POLY_BASE}/markets",
    "series":  f"{POLY_BASE}/series",
}

PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "100"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "200"))
ORDER_PRIMARY = os.getenv("ORDER_PRIMARY", "updatedAt")
ORDER_FALLBACK = os.getenv("ORDER_FALLBACK", "id")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))
BASE_SLEEP = float(os.getenv("BASE_SLEEP", "0.2"))

CH_CONNECT_TIMEOUT = int(os.getenv("CH_CONNECT_TIMEOUT", "10"))
CH_SEND_RECEIVE_TIMEOUT = int(os.getenv("CH_SEND_RECEIVE_TIMEOUT", "900"))
CH_QUERY_RETRIES = int(os.getenv("CH_QUERY_RETRIES", "2"))
INSERT_BATCH_SIZE = int(os.getenv("INSERT_BATCH_SIZE", "1000"))
INSERT_MAX_RETRIES = int(os.getenv("INSERT_MAX_RETRIES", "4"))
INSERT_RETRY_BASE_SLEEP = float(os.getenv("INSERT_RETRY_BASE_SLEEP", "1.0"))

INSERT_MAX_PARTITIONS_PER_BLOCK = int(
    os.getenv("INSERT_MAX_PARTITIONS_PER_BLOCK", os.getenv("OPTIMIZE_PARTITIONS", "128"))
)
INSERT_THROW_ON_MAX_PARTITIONS_PER_BLOCK = os.getenv(
    "INSERT_THROW_ON_MAX_PARTITIONS_PER_BLOCK", "1"
).strip().lower() in ("1", "true", "yes", "y")

# GitHub Contents API for checkpoint/state
ORG = os.getenv("ORG", "statground")
ORCHESTRATOR_REPO = os.getenv("ORCHESTRATOR_REPO", "Statground_Data_Polymarket")
DEFAULT_BRANCH = os.getenv("DEFAULT_BRANCH", "main")
GH_TOKEN = os.getenv("GH_TOKEN", "") or os.getenv("GITHUB_TOKEN", "")

CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", ".state/polymarket_checkpoint.json")

# ClickHouse
CH_HOST = os.getenv("CLICKHOUSE_HOST", "")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DATABASE", "statground_polymarket")
CH_IFACE = os.getenv("CLICKHOUSE_INTERFACE", "http").lower()  # http|native

EVENT_TABLE = os.getenv("EVENT_TABLE", "polymarket_event")
MARKET_TABLE = os.getenv("MARKET_TABLE", "polymarket_market")
SERIES_TABLE = os.getenv("SERIES_TABLE", "polymarket_series")

ENTITY_TABLES = {
    "events": EVENT_TABLE,
    "markets": MARKET_TABLE,
    "series": SERIES_TABLE,
}

_BASE_INSERT_COLUMNS = {
    "events": [
        "event_id", "raw_key", "collected_at_utc", "created_at_utc", "updated_at_utc",
        "title", "ticker", "slug", "description",
        "active", "archived", "closed", "restricted",
        "start_date_utc", "end_date_utc", "closed_time_utc", "creation_date_utc",
        "series_slug", "series_ids", "market_ids",
        "icon_url", "image_url", "volume",
    ],
    "markets": [
        "market_id", "raw_key", "collected_at_utc", "created_at_utc", "updated_at_utc",
        "condition_id", "question_id", "slug", "question", "description",
        "resolution_source", "resolved_by",
        "active", "approved", "archived", "closed", "restricted", "neg_risk",
        "start_date_utc", "end_date_utc", "closed_time_utc",
        "best_ask", "best_bid", "last_trade_price", "spread", "volume",
        "outcomes", "outcome_prices", "clob_token_ids",
        "series_slug", "series_ids", "event_ids",
    ],
    "series": [
        "series_id", "raw_key", "collected_at_utc", "created_at_utc", "updated_at_utc",
        "slug", "ticker", "title",
        "active", "archived", "closed",
        "recurrence", "series_type",
        "liquidity", "volume", "volume_24h",
        "event_ids",
    ],
}

_TABLE_SCHEMA_CACHE: Dict[str, Dict[str, str]] = {}
_RAW_JSON_SKIP = object()


# -------------------------
# Small helpers
# -------------------------
def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_utc(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # Accept "Z" or "+00:00"
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def b01(v) -> int:
    return 1 if bool(v) else 0


def safe_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def safe_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def safe_u64(v) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        x = int(v)
        if x < 0:
            return None
        return x
    except Exception:
        return None


def uuid7() -> uuid.UUID:
    """
    Minimal UUIDv7 generator (time-ordered).
    - 48 bits unix epoch milliseconds
    - 12 bits random
    - 62 bits random
    """
    ms = int(time.time() * 1000)
    rand_a = random.getrandbits(12)
    rand_b = random.getrandbits(62)
    # Compose 128 bits
    value = (ms & ((1 << 48) - 1)) << 80
    value |= (0x7 << 76)  # version 7
    value |= (rand_a & ((1 << 12) - 1)) << 64
    value |= (0b10 << 62)  # variant RFC 4122
    value |= (rand_b & ((1 << 62) - 1))
    return uuid.UUID(int=value)


# -------------------------
# GitHub Contents API
# -------------------------
def gh_api_json(method: str, url: str, token: str, payload: Optional[dict] = None) -> Tuple[int, object]:
    req = urllib.request.Request(url, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req.add_header("Content-Type", "application/json")
    else:
        data = None
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


def get_content(path: str) -> Tuple[Optional[str], bytes]:
    url = f"https://api.github.com/repos/{ORG}/{ORCHESTRATOR_REPO}/contents/{urllib.parse.quote(path)}?ref={DEFAULT_BRANCH}"
    code, obj = gh_api_json("GET", url, GH_TOKEN)
    if code != 200 or not isinstance(obj, dict):
        return None, b""
    sha = obj.get("sha")
    content_b64 = obj.get("content", "") or ""
    if content_b64:
        import base64
        return sha, base64.b64decode(content_b64)
    return sha, b""


def put_content(path: str, content_bytes: bytes, message: str):
    import base64
    url = f"https://api.github.com/repos/{ORG}/{ORCHESTRATOR_REPO}/contents/{urllib.parse.quote(path)}"
    sha, _old = get_content(path)
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


def load_checkpoint() -> Dict[str, str]:
    _, content = get_content(CHECKPOINT_PATH)
    if not content:
        return {}
    try:
        return json.loads(content.decode("utf-8")) or {}
    except Exception:
        return {}


def save_checkpoint(checkpoint: Dict[str, str]):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    put_content(
        CHECKPOINT_PATH,
        json.dumps(checkpoint, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        f"Update Polymarket checkpoint ({ts})"
    )


# -------------------------
# HTTP fetch with meta
# -------------------------
def http_get_json_with_meta(url: str, params: Dict[str, str], timeout: int = REQUEST_TIMEOUT) -> Tuple[object, dict]:
    qs = "&".join([f"{urllib.parse.quote(str(k))}={urllib.parse.quote(str(v))}" for k, v in params.items()])
    full = f"{url}?{qs}" if qs else url
    req = urllib.request.Request(full)
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "statground-polymarket-crawler")
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        ms = int((time.time() - t0) * 1000)
        return json.loads(body), {"http_status": int(getattr(resp, "status", 200) or 200), "response_ms": ms, "full_url": full}


def safe_get_json(url: str, params: Dict[str, str]) -> Tuple[Optional[object], dict]:
    last_meta = {}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data, meta = http_get_json_with_meta(url, params)
            return data, meta
        except urllib.error.HTTPError as e:
            code = int(getattr(e, "code", 0) or 0)
            if code == 429 or code >= 500:
                sleep_s = min(60, 2 ** attempt) + random.random()
                print(f"[HTTP {code}] retry in {sleep_s:.1f}s url={url} params={params}")
                time.sleep(sleep_s)
                continue
            body = ""
            try:
                body = (e.read().decode("utf-8", errors="ignore") or "")[:200]
            except Exception:
                pass
            print(f"[HTTP {code}] fail url={url} params={params} body={body}")
            last_meta = {"http_status": code, "response_ms": 0, "full_url": url}
            return None, last_meta
        except Exception as e:
            sleep_s = min(60, 2 ** attempt) + random.random()
            print(f"[ERR] {e} retry in {sleep_s:.1f}s url={url} params={params}")
            time.sleep(sleep_s)
            continue
    return None, last_meta


# -------------------------
# ClickHouse insert mapping
# -------------------------
def get_ch_client():
    if not CH_HOST:
        raise RuntimeError("CLICKHOUSE_HOST is required.")
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
        interface=CH_IFACE,
        connect_timeout=CH_CONNECT_TIMEOUT,
        send_receive_timeout=CH_SEND_RECEIVE_TIMEOUT,
        query_retries=CH_QUERY_RETRIES,
        client_name="statground-polymarket-crawler",
    )


def get_table_schema(ch, table_name: str) -> Dict[str, str]:
    cache_key = f"{CH_DB}.{table_name}"
    cached = _TABLE_SCHEMA_CACHE.get(cache_key)
    if cached is not None:
        return cached

    q = f"DESCRIBE TABLE {CH_DB}.{table_name}"
    rows = ch.query(q).result_rows
    schema = {str(r[0]): str(r[1]) for r in rows}
    _TABLE_SCHEMA_CACHE[cache_key] = schema
    return schema


def get_insert_columns(entity: str, ch) -> List[str]:
    columns = list(_BASE_INSERT_COLUMNS[entity])
    table_name = ENTITY_TABLES[entity]
    schema = get_table_schema(ch, table_name)
    if "raw_json" in schema:
        columns.append("raw_json")
    return columns


def prepare_raw_json_value(entity: str, obj: dict, ch):
    table_name = ENTITY_TABLES[entity]
    schema = get_table_schema(ch, table_name)
    raw_type = schema.get("raw_json")
    if not raw_type:
        return _RAW_JSON_SKIP
    # ClickHouse JSON type should accept native Python dict/list via clickhouse-connect.
    # For String-compatible legacy schemas, fall back to JSON text.
    if str(raw_type).upper().startswith("JSON"):
        return obj
    return json.dumps(obj, ensure_ascii=False)


def extract_ids(arr, id_key="id") -> List[int]:
    out = []
    if not arr:
        return out
    if isinstance(arr, list):
        for x in arr:
            if isinstance(x, dict):
                v = safe_u64(x.get(id_key))
            else:
                v = safe_u64(x)
            if v is not None:
                out.append(v)
    return out


def to_event_row(obj: dict, collected_at: datetime, raw_key: uuid.UUID, raw_json=_RAW_JSON_SKIP) -> Optional[list]:
    eid = safe_u64(obj.get("id") or obj.get("eventId") or obj.get("event_id"))
    if eid is None:
        return None
    row = [
        int(eid),
        raw_key,
        collected_at,
        parse_iso_utc(obj.get("createdAt")),
        parse_iso_utc(obj.get("updatedAt")),
        safe_str(obj.get("title")),
        safe_str(obj.get("ticker")),
        safe_str(obj.get("slug")),
        safe_str(obj.get("description")),
        b01(obj.get("active")),
        b01(obj.get("archived")),
        b01(obj.get("closed")),
        b01(obj.get("restricted")),
        parse_iso_utc(obj.get("startDate")),
        parse_iso_utc(obj.get("endDate")),
        parse_iso_utc(obj.get("closedTime")),
        parse_iso_utc(obj.get("creationDate")),
        safe_str(obj.get("seriesSlug")),
        extract_ids(obj.get("series")) or extract_ids(obj.get("seriesIds")) or extract_ids(obj.get("series_id")) or extract_ids(obj.get("seriesID")),
        extract_ids(obj.get("markets")) or extract_ids(obj.get("marketIds")) or extract_ids(obj.get("market_ids")),
        safe_str(obj.get("icon")),
        safe_str(obj.get("image")),
        safe_float(obj.get("volume")),
    ]
    if raw_json is not _RAW_JSON_SKIP:
        row.append(raw_json)
    return row


def to_market_row(obj: dict, collected_at: datetime, raw_key: uuid.UUID, raw_json=_RAW_JSON_SKIP) -> Optional[list]:
    mid = safe_u64(obj.get("id") or obj.get("marketId") or obj.get("market_id"))
    if mid is None:
        return None
    outcomes = obj.get("outcomes")
    if not isinstance(outcomes, list):
        outcomes = []
    outcome_prices = obj.get("outcomePrices")
    if not isinstance(outcome_prices, list):
        outcome_prices = []
    clob_token_ids = obj.get("clobTokenIds")
    if not isinstance(clob_token_ids, list):
        clob_token_ids = []
    row = [
        int(mid),
        raw_key,
        collected_at,
        parse_iso_utc(obj.get("createdAt")),
        parse_iso_utc(obj.get("updatedAt")),
        safe_str(obj.get("conditionId")),
        safe_str(obj.get("questionID") or obj.get("questionId")),
        safe_str(obj.get("slug")),
        safe_str(obj.get("question")),
        safe_str(obj.get("description")),
        safe_str(obj.get("resolutionSource")),
        obj.get("resolvedBy") if obj.get("resolvedBy") is not None else None,
        b01(obj.get("active")),
        b01(obj.get("approved")),
        b01(obj.get("archived")),
        b01(obj.get("closed")),
        b01(obj.get("restricted")),
        b01(obj.get("negRisk")),
        parse_iso_utc(obj.get("startDate")),
        parse_iso_utc(obj.get("endDate")),
        parse_iso_utc(obj.get("closedTime")),
        safe_float(obj.get("bestAsk")),
        safe_float(obj.get("bestBid")),
        safe_float(obj.get("lastTradePrice")),
        safe_float(obj.get("spread")),
        safe_float(obj.get("volume")),
        [safe_str(x) for x in outcomes],
        [safe_str(x) for x in outcome_prices],
        [safe_str(x) for x in clob_token_ids],
        safe_str(obj.get("seriesSlug")),
        extract_ids(obj.get("series")),
        extract_ids(obj.get("events")) or extract_ids(obj.get("eventIds")),
    ]
    if raw_json is not _RAW_JSON_SKIP:
        row.append(raw_json)
    return row


def to_series_row(obj: dict, collected_at: datetime, raw_key: uuid.UUID, raw_json=_RAW_JSON_SKIP) -> Optional[list]:
    sid = safe_u64(obj.get("id") or obj.get("seriesId") or obj.get("series_id"))
    if sid is None:
        return None
    row = [
        int(sid),
        raw_key,
        collected_at,
        parse_iso_utc(obj.get("createdAt")),
        parse_iso_utc(obj.get("updatedAt")),
        safe_str(obj.get("slug")),
        safe_str(obj.get("ticker")),
        safe_str(obj.get("title")),
        b01(obj.get("active")),
        b01(obj.get("archived")),
        b01(obj.get("closed")),
        safe_str(obj.get("recurrence")),
        safe_str(obj.get("seriesType")),
        safe_float(obj.get("liquidity")),
        safe_float(obj.get("volume")),
        safe_float(obj.get("volume24hr") or obj.get("volume24h")),
        extract_ids(obj.get("events")) or extract_ids(obj.get("eventIds")),
    ]
    if raw_json is not _RAW_JSON_SKIP:
        row.append(raw_json)
    return row


def build_entity_row(entity: str, obj: dict, collected_at: datetime, raw_key: uuid.UUID, ch) -> Optional[list]:
    raw_json = prepare_raw_json_value(entity, obj, ch)
    if entity == "events":
        return to_event_row(obj, collected_at, raw_key, raw_json=raw_json)
    if entity == "markets":
        return to_market_row(obj, collected_at, raw_key, raw_json=raw_json)
    if entity == "series":
        return to_series_row(obj, collected_at, raw_key, raw_json=raw_json)
    raise ValueError(f"Unknown entity: {entity}")


def get_insert_settings() -> Dict[str, object]:
    settings: Dict[str, object] = {}

    if INSERT_MAX_PARTITIONS_PER_BLOCK > 0:
        settings["max_partitions_per_insert_block"] = INSERT_MAX_PARTITIONS_PER_BLOCK

    settings["throw_on_max_partitions_per_insert_block"] = (
        1 if INSERT_THROW_ON_MAX_PARTITIONS_PER_BLOCK else 0
    )
    return settings
    

def _is_retryable_insert_error(ex: Exception) -> bool:
    cls_name = ex.__class__.__name__.lower()
    msg = str(ex).lower()
    if isinstance(ex, TimeoutError):
        return True
    if "operationalerror" in cls_name or "protocolerror" in cls_name:
        return True
    retry_markers = (
        "timed out",
        "timeout",
        "connection aborted",
        "connection reset",
        "broken pipe",
        "remote disconnected",
        "temporarily unavailable",
        "network",
    )
    return any(marker in msg for marker in retry_markers)


def insert_entity_rows(entity: str, ch, rows: List[list]):
    if not rows:
        return ch

    table_name = ENTITY_TABLES[entity]
    insert_columns = get_insert_columns(entity, ch)
    insert_settings = get_insert_settings()

    for attempt in range(1, INSERT_MAX_RETRIES + 1):
        try:
            ch.insert(
                table_name,
                rows,
                column_names=insert_columns,
                settings=insert_settings,
            )
            return ch
        except Exception as ex:
            if attempt >= INSERT_MAX_RETRIES or not _is_retryable_insert_error(ex):
                raise

            sleep_s = min(60.0, INSERT_RETRY_BASE_SLEEP * (2 ** (attempt - 1))) + random.random()
            print(
                f"[INSERT RETRY] entity={entity} rows={len(rows)} attempt={attempt}/{INSERT_MAX_RETRIES} "
                f"sleep={sleep_s:.1f}s err={ex}",
                flush=True,
            )

            try:
                ch.close()
            except Exception:
                pass

            time.sleep(sleep_s)
            ch = get_ch_client()

    return ch


def flush_entity_rows(entity: str, ch, row_buffer: List[list], force: bool = False):
    while row_buffer and (force or len(row_buffer) >= INSERT_BATCH_SIZE):
        batch_len = min(len(row_buffer), INSERT_BATCH_SIZE)
        batch = row_buffer[:batch_len]
        ch = insert_entity_rows(entity, ch, batch)
        del row_buffer[:batch_len]
    return ch


def iso_leq(a: str, b: str) -> bool:
    da = parse_iso_utc(a) if a else None
    db = parse_iso_utc(b) if b else None
    if da is None or db is None:
        return False
    return da <= db


# -------------------------
# Fetch/insert loop
# -------------------------
def pick_order(entity: str) -> str:
    url = ENDPOINTS[entity]
    for order in (ORDER_PRIMARY, ORDER_FALLBACK):
        params = {"limit": str(PAGE_LIMIT), "offset": "0", "order": order, "ascending": "false"}
        data, _meta = safe_get_json(url, params)
        items = data
        if isinstance(data, dict):
            for k in ("data", entity, "results"):
                if k in data and isinstance(data[k], list):
                    items = data[k]
                    break
        if isinstance(items, list):
            return order
    raise RuntimeError(f"Failed to fetch {entity}: API returned non-list for both orders.")


def fetch_and_insert(entity: str, checkpoint: Dict[str, str], ch) -> Tuple[int, Optional[str], object]:
    url = ENDPOINTS[entity]
    last_cp = checkpoint.get(entity, "")
    best_seen = last_cp

    order_used = pick_order(entity)
    print(f"[FETCH] {entity} order={order_used} checkpoint={(last_cp or '(none)')}")

    norm_rows: List[list] = []
    total_written = 0

    for page in range(MAX_PAGES):
        offset = page * PAGE_LIMIT
        params = {"limit": str(PAGE_LIMIT), "offset": str(offset), "order": order_used, "ascending": "false"}
        data, meta = safe_get_json(url, params)
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
        collected_at = utc_now_dt()

        for obj in items:
            if not isinstance(obj, dict):
                continue
            updated = obj.get("updatedAt") or obj.get("updated_at") or ""
            if last_cp and order_used.lower() == "updatedat" and iso_leq(updated, last_cp):
                stop = True
                break

            rk = uuid7()
            row = build_entity_row(entity, obj, collected_at, rk, ch)
            if row:
                norm_rows.append(row)
                total_written += 1

            if updated and (not best_seen or not iso_leq(updated, best_seen)):
                best_seen = updated

        ch = flush_entity_rows(entity, ch, norm_rows)

        print(f"[{entity}] page={page+1} offset={offset} inserted={total_written} http_status={meta.get('http_status')}")
        if stop:
            print(f"[{entity}] reached checkpoint -> stop")
            break
        if len(items) < PAGE_LIMIT:
            break
        if BASE_SLEEP > 0:
            time.sleep(BASE_SLEEP)

    ch = flush_entity_rows(entity, ch, norm_rows, force=True)
    return total_written, (best_seen if best_seen else None), ch


def optimize_after_batch(ch) -> None:
    optimize_random_partitions(
        ch,
        [
            f"{CH_DB}.{EVENT_TABLE}",
            f"{CH_DB}.{MARKET_TABLE}",
            f"{CH_DB}.{SERIES_TABLE}",
        ],
    )


def main():
    if not GH_TOKEN:
        raise RuntimeError("Need GH_TOKEN or GITHUB_TOKEN (Actions provides GITHUB_TOKEN automatically).")
    ch = get_ch_client()
    print(
        f"[CONFIG] insert_batch_size={INSERT_BATCH_SIZE} "
        f"max_partitions_per_insert_block={INSERT_MAX_PARTITIONS_PER_BLOCK} "
        f"throw_on_max_partitions_per_insert_block={1 if INSERT_THROW_ON_MAX_PARTITIONS_PER_BLOCK else 0}",
        flush=True,
    )

    checkpoint = load_checkpoint()
    new_checkpoint = dict(checkpoint)

    wrote_total = 0
    for entity in ("events", "markets", "series"):
        wrote, new_cp, ch = fetch_and_insert(entity, checkpoint, ch)
        wrote_total += wrote
        if new_cp:
            new_checkpoint[entity] = new_cp

    save_checkpoint(new_checkpoint)
    optimize_after_batch(ch)
    print(f"\nDONE. inserted_objects={wrote_total}")


if __name__ == "__main__":
    main()