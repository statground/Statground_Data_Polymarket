/*
Statground Prediction / Polymarket reset script.
Run this first from DBeaver on the ClickHouse 50005 entrypoint.

Reason for this version:
- Previous generic tables such as prediction_events_* are removed.
- Polymarket ingestion tables are recreated with polymarket_* names.
- ON CLUSTER DDL waits in this session to avoid Code 60 UNKNOWN_TABLE from dependent MV creation.

Runtime baseline: ClickHouse 26.1.2.11
Cluster: statground_cluster
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE DATABASE IF NOT EXISTS `Data_Prediction_Raw`
ON CLUSTER statground_cluster
COMMENT 'Prediction domain raw ingestion database; provider raw snapshots and Kafka envelope tables; OLAP only; SSOT 아님; timezone columns explicitly defined';

CREATE DATABASE IF NOT EXISTS `Data_Prediction_Service`
ON CLUSTER statground_cluster
COMMENT 'Prediction domain service database; provider latest query tables derived from raw snapshots; OLAP only; SSOT 아님; ReplacingMergeTree 기반';

CREATE DATABASE IF NOT EXISTS `Data_Prediction_Log`
ON CLUSTER statground_cluster
COMMENT 'Prediction domain ingestion log database; provider Kafka parse errors and pipeline logs; OLAP only; SSOT 아님; timezone Asia/Seoul';

CREATE DATABASE IF NOT EXISTS `Data_Prediction_Mart`
ON CLUSTER statground_cluster
COMMENT 'Prediction domain mart database; provider-specific ClickHouse-side statistics; GitHub repository no longer stores statistics; OLAP only; SSOT 아님';

/* Drop old/new materialized views first. */
DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_prediction_kafka_queue_to_events ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_prediction_events_to_polymarket_event_snapshot ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_prediction_events_to_polymarket_market_snapshot ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_prediction_events_to_polymarket_series_snapshot ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_prediction_events_to_polymarket_checkpoint ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_polymarket_kafka_queue_to_events ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_polymarket_events_to_event_snapshot ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_polymarket_events_to_market_snapshot ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_polymarket_events_to_series_snapshot ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_polymarket_events_to_crawl_checkpoint ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Service`.mv_polymarket_event_snapshot_to_latest_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Service`.mv_polymarket_market_snapshot_to_latest_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Service`.mv_polymarket_series_snapshot_to_latest_local ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Log`.mv_prediction_kafka_queue_to_parse_error_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Log`.mv_polymarket_kafka_queue_to_parse_error_local ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Mart`.mv_polymarket_event_snapshot_to_collection_stats_hourly_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Mart`.mv_polymarket_market_snapshot_to_collection_stats_hourly_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Mart`.mv_polymarket_series_snapshot_to_collection_stats_hourly_local ON CLUSTER statground_cluster SYNC;

/* Drop old generic ingestion/log/mart tables. */
DROP TABLE IF EXISTS `Data_Prediction_Raw`.prediction_events_kafka_queue ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.prediction_events ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.prediction_events_local ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Log`.prediction_kafka_parse_error ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Log`.prediction_kafka_parse_error_local ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Mart`.prediction_collection_stats_hourly ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Mart`.prediction_collection_stats_hourly_local ON CLUSTER statground_cluster SYNC;

/* Drop Polymarket tables so this package can recreate them cleanly. */
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_events_kafka_queue ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_events ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_events_local ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_event_snapshot ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_event_snapshot_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_market_snapshot ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_market_snapshot_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_series_snapshot ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_series_snapshot_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_crawl_checkpoint ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_crawl_checkpoint_local ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Service`.polymarket_event_latest ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Service`.polymarket_event_latest_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Service`.polymarket_market_latest ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Service`.polymarket_market_latest_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Service`.polymarket_series_latest ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Service`.polymarket_series_latest_local ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Log`.polymarket_kafka_parse_error ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Log`.polymarket_kafka_parse_error_local ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Mart`.polymarket_collection_stats_hourly ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Mart`.polymarket_collection_stats_hourly_local ON CLUSTER statground_cluster SYNC;

SELECT
    hostName() AS remote_host,
    name,
    comment
FROM clusterAllReplicas('statground_cluster', system.databases)
WHERE name IN ('Data_Prediction_Raw', 'Data_Prediction_Service', 'Data_Prediction_Log', 'Data_Prediction_Mart')
ORDER BY name, remote_host;
