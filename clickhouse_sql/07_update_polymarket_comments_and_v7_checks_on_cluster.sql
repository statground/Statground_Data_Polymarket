/*
Polymarket v7 comment alignment and sanity checks.

Run this if provider-specific tables already exist and code has been updated to v7.
No data is modified. Only raw_json column comments are updated to match the
producer-side nested-array pruning and large-array trimming policy.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

ALTER TABLE `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot_local
ON CLUSTER statground_cluster
MODIFY COLUMN raw_json String COMMENT 'Polymarket API raw JSON string after producer-side pruning of large nested entity arrays; relation arrays may be capped in Kafka payload to stay below broker message limits; OLAP raw retention only; SSOT 아님';

ALTER TABLE `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot_local
ON CLUSTER statground_cluster
MODIFY COLUMN raw_json String COMMENT 'Polymarket API raw JSON string after producer-side pruning of large nested entity arrays; relation arrays may be capped in Kafka payload to stay below broker message limits; OLAP raw retention only; SSOT 아님';

ALTER TABLE `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot_local
ON CLUSTER statground_cluster
MODIFY COLUMN raw_json String COMMENT 'Polymarket API raw JSON string after producer-side pruning of large nested entity arrays; relation arrays may be capped in Kafka payload to stay below broker message limits; OLAP raw retention only; SSOT 아님';

ALTER TABLE `Data_Prediction_Polymarket_Service`.polymarket_event_latest_local
ON CLUSTER statground_cluster
MODIFY COLUMN raw_json String COMMENT 'Polymarket API raw JSON string after producer-side pruning of large nested entity arrays; relation arrays may be capped in Kafka payload to stay below broker message limits; OLAP service copy; SSOT 아님';

ALTER TABLE `Data_Prediction_Polymarket_Service`.polymarket_market_latest_local
ON CLUSTER statground_cluster
MODIFY COLUMN raw_json String COMMENT 'Polymarket API raw JSON string after producer-side pruning of large nested entity arrays; relation arrays may be capped in Kafka payload to stay below broker message limits; OLAP service copy; SSOT 아님';

ALTER TABLE `Data_Prediction_Polymarket_Service`.polymarket_series_latest_local
ON CLUSTER statground_cluster
MODIFY COLUMN raw_json String COMMENT 'Polymarket API raw JSON string after producer-side pruning of large nested entity arrays; relation arrays may be capped in Kafka payload to stay below broker message limits; OLAP service copy; SSOT 아님';

/* Confirm Polymarket object set exists on every active replica. */
SELECT
    database,
    name,
    engine,
    count() AS host_count,
    groupArray(hostName()) AS hosts
FROM clusterAllReplicas('statground_cluster', system.tables)
WHERE database IN ('Data_Prediction_Polymarket_Raw', 'Data_Prediction_Polymarket_Service', 'Data_Prediction_Polymarket_Log', 'Data_Prediction_Polymarket_Mart')
  AND name LIKE 'polymarket%'
GROUP BY database, name, engine
ORDER BY database, name, engine;

/* Confirm v7 raw_json comments. */
SELECT
    hostName() AS remote_host,
    database,
    table,
    name,
    comment
FROM clusterAllReplicas('statground_cluster', system.columns)
WHERE database IN ('Data_Prediction_Polymarket_Raw', 'Data_Prediction_Polymarket_Service')
  AND table IN (
      'polymarket_event_snapshot_local',
      'polymarket_market_snapshot_local',
      'polymarket_series_snapshot_local',
      'polymarket_event_latest_local',
      'polymarket_market_latest_local',
      'polymarket_series_latest_local'
  )
  AND name = 'raw_json'
ORDER BY database, table, remote_host;
