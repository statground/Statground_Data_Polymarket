# Statground Polymarket ClickHouse SQL v4

This package fixes the Code 60 `UNKNOWN_TABLE` issue by separating table creation from Kafka materialized-view attachment.

Run from DBeaver on ClickHouse `192.168.0.15:50005` in this order:

1. `00_recreate_prediction_databases_and_drop_old_tables_on_cluster.sql`
2. `01_create_polymarket_ingest_tables_on_cluster.sql`
3. `02_create_polymarket_snapshot_and_service_tables_on_cluster.sql`
4. `03_create_polymarket_downstream_materialized_views_on_cluster.sql`
5. `04_create_polymarket_log_and_mart_tables_on_cluster.sql`
6. `05_attach_polymarket_kafka_materialized_views_on_cluster.sql`
7. `06_grants_prediction_databases_on_cluster.sql`
8. `10_migrate_old_polymarket_from_remote.sql` after replacing old ClickHouse placeholders
9. `20_polymarket_monitoring_queries.sql`

Important:

- `01` creates `Data_Prediction_Raw.polymarket_events_kafka_queue` but does not attach Kafka materialized views.
- `05` attaches `mv_polymarket_kafka_queue_to_events` and `mv_polymarket_kafka_queue_to_parse_error_local`.
- Kafka starts consuming `prediction.events` only after `05`.
- Verification queries should show `host_count = 4` on the current 2 shards x 2 replicas cluster.

Provider naming:

- Old generic objects such as `prediction_events_*`, `prediction_kafka_parse_error_*`, and `prediction_collection_stats_hourly_*` are dropped in `00`.
- New provider-specific objects use `polymarket_*` names inside the generic DBs `Data_Prediction_Raw`, `Data_Prediction_Service`, `Data_Prediction_Log`, and `Data_Prediction_Mart`.

Operational baseline:

- ClickHouse 26.1.2.11
- Cluster: `statground_cluster`
- Kafka topic: `prediction.events`
- Consumer group: `clickhouse_data_prediction_polymarket_events_v1`
- UUID: UUID v7 producer strings converted to ClickHouse `UUID`
- Logs and marts are OLAP-only and are not SSOT.
