/*
Statground Polymarket provider database creation.

Provider-specific database policy:
- Data_Prediction_Polymarket_Raw      : Polymarket Kafka envelope, raw snapshots, checkpoints
- Data_Prediction_Polymarket_Service  : Polymarket latest/service query tables
- Data_Prediction_Polymarket_Log      : Polymarket ingestion/parse error logs
- Data_Prediction_Polymarket_Mart     : Polymarket ClickHouse-side statistics/marts

Runtime baseline: ClickHouse 26.1.2.11
Cluster: statground_cluster
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE DATABASE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`
ON CLUSTER statground_cluster
COMMENT 'Polymarket prediction raw ingestion database; Kafka envelope, provider raw snapshots, and checkpoints; OLAP only; SSOT 아님; timezone columns explicitly defined';

CREATE DATABASE IF NOT EXISTS `Data_Prediction_Polymarket_Service`
ON CLUSTER statground_cluster
COMMENT 'Polymarket prediction service database; latest query tables derived from raw snapshots; OLAP only; SSOT 아님; ReplacingMergeTree 기반';

CREATE DATABASE IF NOT EXISTS `Data_Prediction_Polymarket_Log`
ON CLUSTER statground_cluster
COMMENT 'Polymarket prediction ingestion log database; Kafka parse errors and pipeline logs; OLAP only; SSOT 아님; timezone Asia/Seoul';

CREATE DATABASE IF NOT EXISTS `Data_Prediction_Polymarket_Mart`
ON CLUSTER statground_cluster
COMMENT 'Polymarket prediction mart database; ClickHouse-side statistics; GitHub repository does not store statistics; OLAP only; SSOT 아님';

SELECT
    hostName() AS remote_host,
    name,
    comment
FROM clusterAllReplicas('statground_cluster', system.databases)
WHERE name IN (
    'Data_Prediction_Polymarket_Raw',
    'Data_Prediction_Polymarket_Service',
    'Data_Prediction_Polymarket_Log',
    'Data_Prediction_Polymarket_Mart'
)
ORDER BY name, remote_host;
