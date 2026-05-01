/*
Polymarket parse-error log target tables and ClickHouse-side statistics marts.
GitHub repositories publish raw Polymarket events only; Polymarket statistics are stored and queried from ClickHouse.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Log`.polymarket_kafka_parse_error_local
ON CLUSTER statground_cluster
(
    event_uuid UUID COMMENT 'Polymarket producer UUID v7 if parseable from Kafka JSON; zero UUID only when malformed Kafka rows cannot expose event_uuid',
    kafka_topic LowCardinality(String) COMMENT 'Kafka topic name from _topic virtual column; current topic prediction.events',
    kafka_partition UInt32 COMMENT 'Kafka partition number from _partition virtual column; lineage/debug field',
    kafka_offset UInt64 COMMENT 'Kafka offset from _offset virtual column; lineage/replay/debug field',
    error String COMMENT 'ClickHouse Kafka Engine _error value; parsing/format error message',
    raw_message String COMMENT 'ClickHouse Kafka Engine _raw_message value; raw malformed message; OLAP troubleshooting only; SSOT 아님',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'Error observation timestamp in Asia/Seoul; ORDER BY leading column',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp in Asia/Seoul'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_Prediction_Polymarket_Log/polymarket_kafka_parse_error_local', '{replica}')
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, event_uuid, kafka_partition, kafka_offset)
SETTINGS index_granularity = 8192
COMMENT 'Polymarket Kafka parse error local log table; OLAP only; SSOT 아님; TB-scale ORDER BY starts with created_at';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Log`.polymarket_kafka_parse_error
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Log`.polymarket_kafka_parse_error_local
ENGINE = Distributed('statground_cluster', 'Data_Prediction_Polymarket_Log', 'polymarket_kafka_parse_error_local', cityHash64(kafka_topic, kafka_partition, kafka_offset))
COMMENT 'Polymarket Kafka parse error distributed log table; read interface across statground_cluster; OLAP only; SSOT 아님';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Mart`.polymarket_collection_stats_hourly_local
ON CLUSTER statground_cluster
(
    bucket_at DateTime64(3, 'Asia/Seoul') COMMENT 'Hourly Polymarket statistics bucket in Asia/Seoul; ORDER BY leading column',
    domain LowCardinality(String) COMMENT 'Platform domain code; expected prediction; LowCardinality for grouping',
    service LowCardinality(String) COMMENT 'External data service/provider code; expected polymarket; retained for future union views',
    entity LowCardinality(String) COMMENT 'Polymarket entity type, e.g. event, market, series; LowCardinality for grouping',
    snapshot_count SimpleAggregateFunction(sum, UInt64) COMMENT 'Number of Polymarket raw snapshots collected in the hour; aggregated by sum across shards',
    object_uniq_state AggregateFunction(uniq, UInt64) COMMENT 'uniqState of Polymarket object id/hash; query with uniqMerge(object_uniq_state)',
    last_ingested_at SimpleAggregateFunction(max, DateTime64(3, 'Asia/Seoul')) COMMENT 'Latest ClickHouse ingestion timestamp in Asia/Seoul for this bucket'
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/Data_Prediction_Polymarket_Mart/polymarket_collection_stats_hourly_local', '{replica}')
PARTITION BY toYYYYMM(bucket_at)
ORDER BY (bucket_at, domain, service, entity)
SETTINGS index_granularity = 8192
COMMENT 'Polymarket collection hourly statistics local mart; ClickHouse-side statistics storage; GitHub repository does not store stats; OLAP only; SSOT 아님';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Mart`.polymarket_collection_stats_hourly
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Mart`.polymarket_collection_stats_hourly_local
ENGINE = Distributed('statground_cluster', 'Data_Prediction_Polymarket_Mart', 'polymarket_collection_stats_hourly_local', cityHash64(domain, service, entity, toUInt64(toUnixTimestamp(bucket_at))))
COMMENT 'Polymarket collection hourly statistics distributed mart; query interface across statground_cluster; OLAP only; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Mart`.mv_polymarket_event_snapshot_to_collection_stats_hourly_local
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Mart`.polymarket_collection_stats_hourly_local
AS
SELECT
    toStartOfHour(toTimeZone(collected_at_utc, 'Asia/Seoul')) AS bucket_at,
    'prediction' AS domain,
    'polymarket' AS service,
    'event' AS entity,
    count() AS snapshot_count,
    uniqState(toUInt64(event_id)) AS object_uniq_state,
    max(ingested_at) AS last_ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot_local
GROUP BY bucket_at;

ALTER TABLE `Data_Prediction_Polymarket_Mart`.mv_polymarket_event_snapshot_to_collection_stats_hourly_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Polymarket event raw snapshots to Polymarket hourly collection statistics mart; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Mart`.mv_polymarket_market_snapshot_to_collection_stats_hourly_local
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Mart`.polymarket_collection_stats_hourly_local
AS
SELECT
    toStartOfHour(toTimeZone(collected_at_utc, 'Asia/Seoul')) AS bucket_at,
    'prediction' AS domain,
    'polymarket' AS service,
    'market' AS entity,
    count() AS snapshot_count,
    uniqState(toUInt64(market_id)) AS object_uniq_state,
    max(ingested_at) AS last_ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot_local
GROUP BY bucket_at;

ALTER TABLE `Data_Prediction_Polymarket_Mart`.mv_polymarket_market_snapshot_to_collection_stats_hourly_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Polymarket market raw snapshots to Polymarket hourly collection statistics mart; SSOT 아님';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Mart`.mv_polymarket_series_snapshot_to_collection_stats_hourly_local
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Mart`.polymarket_collection_stats_hourly_local
AS
SELECT
    toStartOfHour(toTimeZone(collected_at_utc, 'Asia/Seoul')) AS bucket_at,
    'prediction' AS domain,
    'polymarket' AS service,
    'series' AS entity,
    count() AS snapshot_count,
    uniqState(toUInt64(series_id)) AS object_uniq_state,
    max(ingested_at) AS last_ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot_local
GROUP BY bucket_at;

ALTER TABLE `Data_Prediction_Polymarket_Mart`.mv_polymarket_series_snapshot_to_collection_stats_hourly_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Polymarket series raw snapshots to Polymarket hourly collection statistics mart; SSOT 아님';

SELECT
    hostName() AS remote_host,
    database,
    name,
    engine
FROM clusterAllReplicas('statground_cluster', system.tables)
WHERE (database = 'Data_Prediction_Polymarket_Log' AND name LIKE 'polymarket%')
   OR (database = 'Data_Prediction_Polymarket_Mart' AND name LIKE 'polymarket%')
   OR name LIKE 'mv_polymarket%'
ORDER BY database, name, remote_host;
