/*
Polymarket materialized views:
1) Polymarket Kafka envelope -> Polymarket raw snapshot/checkpoint tables
2) Polymarket raw snapshot tables -> Polymarket latest service tables
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.mv_polymarket_events_to_event_snapshot
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot
AS
SELECT
    JSONExtractUInt(payload, 'event_id') AS event_id,
    ifNull(toUUIDOrNull(JSONExtractString(payload, 'raw_key')), event_uuid) AS raw_key,
    coalesce(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'collected_at_utc'), 3, 'UTC'), now64(3, 'UTC')) AS collected_at_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'created_at_utc'), ''), 3, 'UTC') AS created_at_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'updated_at_utc'), ''), 3, 'UTC') AS updated_at_utc,
    JSONExtractString(payload, 'title') AS title,
    JSONExtractString(payload, 'ticker') AS ticker,
    JSONExtractString(payload, 'slug') AS slug,
    JSONExtractString(payload, 'description') AS description,
    toUInt8(JSONExtractUInt(payload, 'active')) AS active,
    toUInt8(JSONExtractUInt(payload, 'archived')) AS archived,
    toUInt8(JSONExtractUInt(payload, 'closed')) AS closed,
    toUInt8(JSONExtractUInt(payload, 'restricted')) AS restricted,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'start_date_utc'), ''), 3, 'UTC') AS start_date_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'end_date_utc'), ''), 3, 'UTC') AS end_date_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'closed_time_utc'), ''), 3, 'UTC') AS closed_time_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'creation_date_utc'), ''), 3, 'UTC') AS creation_date_utc,
    JSONExtractString(payload, 'series_slug') AS series_slug,
    JSONExtract(payload, 'series_ids', 'Array(UInt64)') AS series_ids,
    JSONExtract(payload, 'market_ids', 'Array(UInt64)') AS market_ids,
    JSONExtractString(payload, 'icon_url') AS icon_url,
    JSONExtractString(payload, 'image_url') AS image_url,
    toFloat64OrNull(nullIf(replaceRegexpAll(JSONExtractRaw(payload, 'volume'), '^"|"$', ''), 'null')) AS volume,
    JSONExtractString(payload, 'raw_json') AS raw_json,
    ingested_at AS ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_events_local
WHERE event_type = 'polymarket.event_snapshot_raw.v1'
  AND JSONExtractUInt(payload, 'event_id') > 0;

ALTER TABLE `Data_Prediction_Polymarket_Raw`.mv_polymarket_events_to_event_snapshot
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Data_Prediction_Polymarket_Raw.polymarket_events_local to Polymarket event raw snapshots; OLAP parsing layer; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.mv_polymarket_events_to_market_snapshot
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot
AS
SELECT
    JSONExtractUInt(payload, 'market_id') AS market_id,
    ifNull(toUUIDOrNull(JSONExtractString(payload, 'raw_key')), event_uuid) AS raw_key,
    coalesce(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'collected_at_utc'), 3, 'UTC'), now64(3, 'UTC')) AS collected_at_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'created_at_utc'), ''), 3, 'UTC') AS created_at_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'updated_at_utc'), ''), 3, 'UTC') AS updated_at_utc,
    JSONExtractString(payload, 'condition_id') AS condition_id,
    JSONExtractString(payload, 'question_id') AS question_id,
    JSONExtractString(payload, 'slug') AS slug,
    JSONExtractString(payload, 'question') AS question,
    JSONExtractString(payload, 'description') AS description,
    JSONExtractString(payload, 'resolution_source') AS resolution_source,
    nullIf(JSONExtractString(payload, 'resolved_by'), '') AS resolved_by,
    toUInt8(JSONExtractUInt(payload, 'active')) AS active,
    toUInt8(JSONExtractUInt(payload, 'approved')) AS approved,
    toUInt8(JSONExtractUInt(payload, 'archived')) AS archived,
    toUInt8(JSONExtractUInt(payload, 'closed')) AS closed,
    toUInt8(JSONExtractUInt(payload, 'restricted')) AS restricted,
    toUInt8(JSONExtractUInt(payload, 'neg_risk')) AS neg_risk,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'start_date_utc'), ''), 3, 'UTC') AS start_date_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'end_date_utc'), ''), 3, 'UTC') AS end_date_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'closed_time_utc'), ''), 3, 'UTC') AS closed_time_utc,
    toFloat64OrNull(nullIf(replaceRegexpAll(JSONExtractRaw(payload, 'best_ask'), '^"|"$', ''), 'null')) AS best_ask,
    toFloat64OrNull(nullIf(replaceRegexpAll(JSONExtractRaw(payload, 'best_bid'), '^"|"$', ''), 'null')) AS best_bid,
    toFloat64OrNull(nullIf(replaceRegexpAll(JSONExtractRaw(payload, 'last_trade_price'), '^"|"$', ''), 'null')) AS last_trade_price,
    toFloat64OrNull(nullIf(replaceRegexpAll(JSONExtractRaw(payload, 'spread'), '^"|"$', ''), 'null')) AS spread,
    toFloat64OrNull(nullIf(replaceRegexpAll(JSONExtractRaw(payload, 'volume'), '^"|"$', ''), 'null')) AS volume,
    JSONExtract(payload, 'outcomes', 'Array(String)') AS outcomes,
    JSONExtract(payload, 'outcome_prices', 'Array(String)') AS outcome_prices,
    JSONExtract(payload, 'clob_token_ids', 'Array(String)') AS clob_token_ids,
    JSONExtractString(payload, 'series_slug') AS series_slug,
    JSONExtract(payload, 'series_ids', 'Array(UInt64)') AS series_ids,
    JSONExtract(payload, 'event_ids', 'Array(UInt64)') AS event_ids,
    JSONExtractString(payload, 'raw_json') AS raw_json,
    ingested_at AS ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_events_local
WHERE event_type = 'polymarket.market_snapshot_raw.v1'
  AND JSONExtractUInt(payload, 'market_id') > 0;

ALTER TABLE `Data_Prediction_Polymarket_Raw`.mv_polymarket_events_to_market_snapshot
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Data_Prediction_Polymarket_Raw.polymarket_events_local to Polymarket market raw snapshots; OLAP parsing layer; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.mv_polymarket_events_to_series_snapshot
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot
AS
SELECT
    JSONExtractUInt(payload, 'series_id') AS series_id,
    ifNull(toUUIDOrNull(JSONExtractString(payload, 'raw_key')), event_uuid) AS raw_key,
    coalesce(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'collected_at_utc'), 3, 'UTC'), now64(3, 'UTC')) AS collected_at_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'created_at_utc'), ''), 3, 'UTC') AS created_at_utc,
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(payload, 'updated_at_utc'), ''), 3, 'UTC') AS updated_at_utc,
    JSONExtractString(payload, 'slug') AS slug,
    JSONExtractString(payload, 'ticker') AS ticker,
    JSONExtractString(payload, 'title') AS title,
    toUInt8(JSONExtractUInt(payload, 'active')) AS active,
    toUInt8(JSONExtractUInt(payload, 'archived')) AS archived,
    toUInt8(JSONExtractUInt(payload, 'closed')) AS closed,
    JSONExtractString(payload, 'recurrence') AS recurrence,
    JSONExtractString(payload, 'series_type') AS series_type,
    toFloat64OrNull(nullIf(replaceRegexpAll(JSONExtractRaw(payload, 'liquidity'), '^"|"$', ''), 'null')) AS liquidity,
    toFloat64OrNull(nullIf(replaceRegexpAll(JSONExtractRaw(payload, 'volume'), '^"|"$', ''), 'null')) AS volume,
    toFloat64OrNull(nullIf(replaceRegexpAll(JSONExtractRaw(payload, 'volume_24h'), '^"|"$', ''), 'null')) AS volume_24h,
    JSONExtract(payload, 'event_ids', 'Array(UInt64)') AS event_ids,
    JSONExtractString(payload, 'raw_json') AS raw_json,
    ingested_at AS ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_events_local
WHERE event_type = 'polymarket.series_snapshot_raw.v1'
  AND JSONExtractUInt(payload, 'series_id') > 0;

ALTER TABLE `Data_Prediction_Polymarket_Raw`.mv_polymarket_events_to_series_snapshot
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Data_Prediction_Polymarket_Raw.polymarket_events_local to Polymarket series raw snapshots; OLAP parsing layer; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.mv_polymarket_events_to_crawl_checkpoint
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Raw`.polymarket_crawl_checkpoint
AS
SELECT
    ifNull(toUUIDOrNull(JSONExtractString(payload, 'checkpoint_uuid')), event_uuid) AS checkpoint_uuid,
    ifNull(nullIf(JSONExtractString(payload, 'service'), ''), 'polymarket') AS service,
    ifNull(nullIf(JSONExtractString(payload, 'source'), ''), source) AS source,
    JSONExtractRaw(payload, 'checkpoint') AS checkpoint_json,
    coalesce(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'updated_at'), 3, 'Asia/Seoul'), created_at) AS updated_at,
    ingested_at AS ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_events_local
WHERE event_type = 'polymarket.crawl_checkpoint.v1';

ALTER TABLE `Data_Prediction_Polymarket_Raw`.mv_polymarket_events_to_crawl_checkpoint
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Data_Prediction_Polymarket_Raw.polymarket_events_local to Polymarket checkpoint log; OLAP monitoring layer; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Service`.mv_polymarket_event_snapshot_to_latest_local
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Service`.polymarket_event_latest_local
AS
SELECT *
FROM `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot_local;

ALTER TABLE `Data_Prediction_Polymarket_Service`.mv_polymarket_event_snapshot_to_latest_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Polymarket event raw snapshots to Polymarket latest local service table; OLAP service layer; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Service`.mv_polymarket_market_snapshot_to_latest_local
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Service`.polymarket_market_latest_local
AS
SELECT *
FROM `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot_local;

ALTER TABLE `Data_Prediction_Polymarket_Service`.mv_polymarket_market_snapshot_to_latest_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Polymarket market raw snapshots to Polymarket latest local service table; OLAP service layer; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Service`.mv_polymarket_series_snapshot_to_latest_local
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Service`.polymarket_series_latest_local
AS
SELECT *
FROM `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot_local;

ALTER TABLE `Data_Prediction_Polymarket_Service`.mv_polymarket_series_snapshot_to_latest_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Polymarket series raw snapshots to Polymarket latest local service table; OLAP service layer; SSOT 아님';

SELECT
    hostName() AS remote_host,
    database,
    name,
    engine
FROM clusterAllReplicas('statground_cluster', system.tables)
WHERE name LIKE 'mv_polymarket%'
ORDER BY database, name, remote_host;
