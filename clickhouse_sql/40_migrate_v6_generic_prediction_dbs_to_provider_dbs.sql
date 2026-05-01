/*
Migrate data already ingested by the previous generic Data_Prediction_* Polymarket schema
into the provider-specific Data_Prediction_Polymarket_* databases.

Recommended order for existing v6 installs:
1) 00_stop_old_generic_prediction_consumers_on_cluster.sql
2) 00_create_polymarket_provider_databases_on_cluster.sql
3) 01_create_polymarket_ingest_tables_on_cluster.sql
4) 02_create_polymarket_snapshot_and_service_tables_on_cluster.sql
5) 03_create_polymarket_downstream_materialized_views_on_cluster.sql
6) 04_create_polymarket_log_and_mart_tables_on_cluster.sql
7) This migration script
8) 05_attach_polymarket_kafka_materialized_views_on_cluster.sql
9) 06_grants_prediction_databases_on_cluster.sql

This script copies the raw Kafka envelope first. The provider DB downstream materialized views
parse those envelope rows into raw snapshots, service latest tables, checkpoint tables, and marts.
It intentionally does not copy old snapshot tables directly to avoid duplicate snapshot creation.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

/* Copy old generic Polymarket envelope rows into the new provider-specific envelope table. */
INSERT INTO `Data_Prediction_Polymarket_Raw`.polymarket_events
SELECT
    event_uuid,
    kafka_topic,
    event_domain,
    kafka_partition,
    kafka_offset,
    source,
    host,
    uuid_user,
    ip,
    url,
    event_type,
    service,
    payload,
    created_at,
    ingested_at
FROM `Data_Prediction_Raw`.polymarket_events
WHERE startsWith(event_type, 'polymarket.');

/* Copy old generic parse-error rows, if any. This does not trigger downstream views. */
INSERT INTO `Data_Prediction_Polymarket_Log`.polymarket_kafka_parse_error
SELECT
    event_uuid,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    error,
    raw_message,
    created_at,
    ingested_at
FROM `Data_Prediction_Log`.polymarket_kafka_parse_error;

SELECT
    'provider_events' AS table_name,
    count() AS rows,
    max(created_at) AS max_created_at,
    max(ingested_at) AS max_ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_events
UNION ALL
SELECT
    'provider_event_snapshot' AS table_name,
    count() AS rows,
    max(collected_at_utc) AS max_created_at,
    max(ingested_at) AS max_ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot
UNION ALL
SELECT
    'provider_market_snapshot' AS table_name,
    count() AS rows,
    max(collected_at_utc) AS max_created_at,
    max(ingested_at) AS max_ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot
UNION ALL
SELECT
    'provider_series_snapshot' AS table_name,
    count() AS rows,
    max(collected_at_utc) AS max_created_at,
    max(ingested_at) AS max_ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot;
