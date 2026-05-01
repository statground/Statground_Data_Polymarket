/*
Stop old generic Data_Prediction_* Kafka consumers before attaching the new provider-specific
Data_Prediction_Polymarket_* consumers.

Why this is required:
- The old and new Kafka Engine tables use the same Polymarket consumer group so offsets continue.
- If both are attached at the same time, Kafka partitions can be split between old and new DBs.
- This script stops only the old generic Kafka ingestion path. It does not delete old stored data.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_prediction_kafka_queue_to_events ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_polymarket_kafka_queue_to_events ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Log`.mv_prediction_kafka_queue_to_parse_error_local ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Log`.mv_polymarket_kafka_queue_to_parse_error_local ON CLUSTER statground_cluster SYNC;

DROP TABLE IF EXISTS `Data_Prediction_Raw`.prediction_events_kafka_queue ON CLUSTER statground_cluster SYNC;
DROP TABLE IF EXISTS `Data_Prediction_Raw`.polymarket_events_kafka_queue ON CLUSTER statground_cluster SYNC;

SELECT
    database,
    name,
    engine,
    count() AS host_count,
    groupArray(hostName()) AS hosts
FROM clusterAllReplicas('statground_cluster', system.tables)
WHERE database IN ('Data_Prediction_Raw', 'Data_Prediction_Log')
  AND (
      name LIKE '%kafka_queue%'
      OR name LIKE 'mv%kafka%'
  )
GROUP BY database, name, engine
ORDER BY database, name, engine;
