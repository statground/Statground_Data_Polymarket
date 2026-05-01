/*
Polymarket monitoring queries.
These are read-only ClickHouse operations queries for the new polymarket_* table set.
*/

/* Latest Polymarket Kafka envelope ingestion */
SELECT
    service,
    event_type,
    count() AS rows,
    min(created_at) AS min_created_at,
    max(created_at) AS max_created_at,
    max(ingested_at) AS max_ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_events
WHERE created_at >= now64(3, 'Asia/Seoul') - INTERVAL 24 HOUR
GROUP BY service, event_type
ORDER BY rows DESC;

/* Polymarket Kafka parse errors */
SELECT
    kafka_topic,
    kafka_partition,
    count() AS error_rows,
    min(kafka_offset) AS min_offset,
    max(kafka_offset) AS max_offset,
    max(created_at) AS last_error_at
FROM `Data_Prediction_Polymarket_Log`.polymarket_kafka_parse_error
WHERE created_at >= now64(3, 'Asia/Seoul') - INTERVAL 24 HOUR
GROUP BY kafka_topic, kafka_partition
ORDER BY last_error_at DESC;

/* Polymarket collection statistics stored in ClickHouse */
SELECT
    bucket_at,
    service,
    entity,
    sum(snapshot_count) AS snapshot_count,
    uniqMerge(object_uniq_state) AS uniq_objects,
    max(last_ingested_at) AS last_ingested_at
FROM `Data_Prediction_Polymarket_Mart`.polymarket_collection_stats_hourly
WHERE service = 'polymarket'
GROUP BY bucket_at, service, entity
ORDER BY bucket_at DESC, service, entity
LIMIT 200;

/* Latest Polymarket object counts */
SELECT 'event' AS entity, count() AS latest_rows FROM `Data_Prediction_Polymarket_Service`.polymarket_event_latest FINAL
UNION ALL
SELECT 'market' AS entity, count() AS latest_rows FROM `Data_Prediction_Polymarket_Service`.polymarket_market_latest FINAL
UNION ALL
SELECT 'series' AS entity, count() AS latest_rows FROM `Data_Prediction_Polymarket_Service`.polymarket_series_latest FINAL;

/* Check every Polymarket object is present on all active replicas after ON CLUSTER DDL */
SELECT
    hostName() AS remote_host,
    database,
    name,
    engine,
    total_rows
FROM clusterAllReplicas('statground_cluster', system.tables)
WHERE database IN ('Data_Prediction_Polymarket_Raw', 'Data_Prediction_Polymarket_Service', 'Data_Prediction_Polymarket_Log', 'Data_Prediction_Polymarket_Mart')
  AND name LIKE 'polymarket%'
ORDER BY database, name, remote_host;
