/*
Optional final cleanup.

Run this only after verifying Data_Prediction_Polymarket_* row counts and Kafka ingestion.
This removes the old generic Prediction DBs. It is intentionally separated from migration scripts.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

DROP DATABASE IF EXISTS `Data_Prediction_Raw` ON CLUSTER statground_cluster SYNC;
DROP DATABASE IF EXISTS `Data_Prediction_Service` ON CLUSTER statground_cluster SYNC;
DROP DATABASE IF EXISTS `Data_Prediction_Log` ON CLUSTER statground_cluster SYNC;
DROP DATABASE IF EXISTS `Data_Prediction_Mart` ON CLUSTER statground_cluster SYNC;

SELECT
    hostName() AS remote_host,
    name
FROM clusterAllReplicas('statground_cluster', system.databases)
WHERE name LIKE 'Data_Prediction%'
ORDER BY name, remote_host;
