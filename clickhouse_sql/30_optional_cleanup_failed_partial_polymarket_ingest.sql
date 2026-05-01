/*
OPTIONAL cleanup for a failed early Polymarket Kafka ingest run.

Use only when the new Polymarket database is still in test/bootstrap mode and
you want clean raw/mart counts before rerunning v7 from scratch.

Do NOT run this after migrating old production Polymarket data unless you intend
to remove all current Polymarket raw/service/mart rows.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

TRUNCATE TABLE IF EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_events_local ON CLUSTER statground_cluster;
TRUNCATE TABLE IF EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot_local ON CLUSTER statground_cluster;
TRUNCATE TABLE IF EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot_local ON CLUSTER statground_cluster;
TRUNCATE TABLE IF EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot_local ON CLUSTER statground_cluster;
TRUNCATE TABLE IF EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_crawl_checkpoint_local ON CLUSTER statground_cluster;

TRUNCATE TABLE IF EXISTS `Data_Prediction_Polymarket_Service`.polymarket_event_latest_local ON CLUSTER statground_cluster;
TRUNCATE TABLE IF EXISTS `Data_Prediction_Polymarket_Service`.polymarket_market_latest_local ON CLUSTER statground_cluster;
TRUNCATE TABLE IF EXISTS `Data_Prediction_Polymarket_Service`.polymarket_series_latest_local ON CLUSTER statground_cluster;

TRUNCATE TABLE IF EXISTS `Data_Prediction_Polymarket_Log`.polymarket_kafka_parse_error_local ON CLUSTER statground_cluster;
TRUNCATE TABLE IF EXISTS `Data_Prediction_Polymarket_Mart`.polymarket_collection_stats_hourly_local ON CLUSTER statground_cluster;
