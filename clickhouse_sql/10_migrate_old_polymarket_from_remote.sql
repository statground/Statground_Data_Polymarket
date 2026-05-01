SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

/*
One-time migration from the old ClickHouse schema statground_polymarket into the new clustered Data_Prediction_* Polymarket schema.

Before running, replace these placeholders:
- OLD_CLICKHOUSE_HOST:9000
- OLD_USER
- OLD_PASSWORD

Recommended order:
1) Run 00-04 DDL first, so raw -> service latest and raw -> mart materialized views are already attached.
2) Run this migration into the Distributed raw snapshot tables.
3) Validate counts and latest rows.

Old tables are ReplacingMergeTree latest tables. FINAL is used to avoid migrating stale duplicate versions.
If the old cluster rejects FINAL on remote(), run OPTIMIZE TABLE statground_polymarket.<table> FINAL on the old cluster first, then remove FINAL here.
*/

INSERT INTO `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot
(
    event_id, raw_key, collected_at_utc, created_at_utc, updated_at_utc,
    title, ticker, slug, description,
    active, archived, closed, restricted,
    start_date_utc, end_date_utc, closed_time_utc, creation_date_utc,
    series_slug, series_ids, market_ids,
    icon_url, image_url, volume, raw_json, ingested_at
)
SELECT
    event_id,
    raw_key,
    collected_at_utc,
    created_at_utc,
    updated_at_utc,
    title,
    ticker,
    slug,
    description,
    active,
    archived,
    closed,
    restricted,
    start_date_utc,
    end_date_utc,
    closed_time_utc,
    creation_date_utc,
    series_slug,
    series_ids,
    market_ids,
    icon_url,
    image_url,
    volume,
    toJSONString(raw_json) AS raw_json,
    ingested_at
FROM remote('OLD_CLICKHOUSE_HOST:9000', 'statground_polymarket', 'polymarket_event', 'OLD_USER', 'OLD_PASSWORD') FINAL
SETTINGS insert_distributed_sync = 1;

INSERT INTO `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot
(
    market_id, raw_key, collected_at_utc, created_at_utc, updated_at_utc,
    condition_id, question_id, slug, question, description,
    resolution_source, resolved_by,
    active, approved, archived, closed, restricted, neg_risk,
    start_date_utc, end_date_utc, closed_time_utc,
    best_ask, best_bid, last_trade_price, spread, volume,
    outcomes, outcome_prices, clob_token_ids,
    series_slug, series_ids, event_ids,
    raw_json, ingested_at
)
SELECT
    market_id,
    raw_key,
    collected_at_utc,
    created_at_utc,
    updated_at_utc,
    condition_id,
    question_id,
    slug,
    question,
    description,
    resolution_source,
    resolved_by,
    active,
    approved,
    archived,
    closed,
    restricted,
    neg_risk,
    start_date_utc,
    end_date_utc,
    closed_time_utc,
    best_ask,
    best_bid,
    last_trade_price,
    spread,
    volume,
    outcomes,
    outcome_prices,
    clob_token_ids,
    series_slug,
    series_ids,
    event_ids,
    toJSONString(raw_json) AS raw_json,
    ingested_at
FROM remote('OLD_CLICKHOUSE_HOST:9000', 'statground_polymarket', 'polymarket_market', 'OLD_USER', 'OLD_PASSWORD') FINAL
SETTINGS insert_distributed_sync = 1;

INSERT INTO `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot
(
    series_id, raw_key, collected_at_utc, created_at_utc, updated_at_utc,
    slug, ticker, title,
    active, archived, closed,
    recurrence, series_type,
    liquidity, volume, volume_24h,
    event_ids, raw_json, ingested_at
)
SELECT
    series_id,
    raw_key,
    collected_at_utc,
    created_at_utc,
    updated_at_utc,
    slug,
    ticker,
    title,
    active,
    archived,
    closed,
    recurrence,
    series_type,
    liquidity,
    volume,
    volume_24h,
    event_ids,
    toJSONString(raw_json) AS raw_json,
    ingested_at
FROM remote('OLD_CLICKHOUSE_HOST:9000', 'statground_polymarket', 'polymarket_series', 'OLD_USER', 'OLD_PASSWORD') FINAL
SETTINGS insert_distributed_sync = 1;

/* Validation: raw snapshot counts */
SELECT 'event_raw' AS table_name, count() AS snapshot_rows, uniqExact(event_id) AS uniq_objects FROM `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot;
SELECT 'market_raw' AS table_name, count() AS snapshot_rows, uniqExact(market_id) AS uniq_objects FROM `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot;
SELECT 'series_raw' AS table_name, count() AS snapshot_rows, uniqExact(series_id) AS uniq_objects FROM `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot;

/* Validation: latest service rows. FINAL is only for exact validation; routine analytical queries should avoid unnecessary FINAL. */
SELECT 'event_latest' AS table_name, count() AS latest_rows FROM `Data_Prediction_Polymarket_Service`.polymarket_event_latest FINAL;
SELECT 'market_latest' AS table_name, count() AS latest_rows FROM `Data_Prediction_Polymarket_Service`.polymarket_market_latest FINAL;
SELECT 'series_latest' AS table_name, count() AS latest_rows FROM `Data_Prediction_Polymarket_Service`.polymarket_series_latest FINAL;

/* Validation: ClickHouse-side Polymarket statistics mart */
SELECT
    service,
    entity,
    sum(snapshot_count) AS snapshot_count,
    uniqMerge(object_uniq_state) AS uniq_objects,
    max(last_ingested_at) AS last_ingested_at
FROM `Data_Prediction_Polymarket_Mart`.polymarket_collection_stats_hourly
WHERE service = 'polymarket'
GROUP BY service, entity
ORDER BY service, entity;
