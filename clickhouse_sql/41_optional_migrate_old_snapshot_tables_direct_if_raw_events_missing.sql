/*
Fallback migration only.

Use this script only if the previous generic Data_Prediction_Raw.polymarket_events table is missing
or incomplete. If 40_migrate_v6_generic_prediction_dbs_to_provider_dbs.sql already replayed the old
envelope rows and populated the provider snapshot tables, do not run this script because it can add
append-only duplicate snapshots.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

INSERT INTO `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot
SELECT * FROM `Data_Prediction_Raw`.polymarket_event_snapshot;

INSERT INTO `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot
SELECT * FROM `Data_Prediction_Raw`.polymarket_market_snapshot;

INSERT INTO `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot
SELECT * FROM `Data_Prediction_Raw`.polymarket_series_snapshot;

INSERT INTO `Data_Prediction_Polymarket_Raw`.polymarket_crawl_checkpoint
SELECT * FROM `Data_Prediction_Raw`.polymarket_crawl_checkpoint;

SELECT
    'provider_event_snapshot' AS table_name,
    count() AS rows,
    max(collected_at_utc) AS max_collected_at_utc
FROM `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot
UNION ALL
SELECT
    'provider_market_snapshot' AS table_name,
    count() AS rows,
    max(collected_at_utc) AS max_collected_at_utc
FROM `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot
UNION ALL
SELECT
    'provider_series_snapshot' AS table_name,
    count() AS rows,
    max(collected_at_utc) AS max_collected_at_utc
FROM `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot;
