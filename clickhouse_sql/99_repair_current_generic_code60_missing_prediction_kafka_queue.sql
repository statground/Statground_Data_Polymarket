/*
Repair for current Code 60 error in the earlier generic package:
Unknown table Data_Prediction_Raw.prediction_events_kafka_queue while creating mv_prediction_kafka_queue_to_events.

Use this only if you continue with the earlier generic prediction_events_* naming.
For the provider-specific Polymarket naming package, run 01_create_polymarket_ingest_tables_on_cluster.sql first, then 05_attach_polymarket_kafka_materialized_views_on_cluster.sql.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Raw`.prediction_events_kafka_queue
ON CLUSTER statground_cluster
(
    event_uuid String COMMENT 'UUID v7 string from Kafka JSON message; converted to UUID in materialized view',
    source String COMMENT 'Producer source code from Kafka JSON message',
    host String COMMENT 'Producer host from Kafka JSON message',
    uuid_user Nullable(String) COMMENT 'Nullable UUID v7 user string from Kafka JSON message',
    ip String COMMENT 'IP string from Kafka JSON message; normalized to IPv6 in materialized view',
    url String COMMENT 'URL string from Kafka JSON message',
    event_type String COMMENT 'Event type string from Kafka JSON message',
    payload String COMMENT 'Payload JSON string from Kafka JSON message',
    created_at String COMMENT 'Asia/Seoul timestamp string from Kafka JSON message; parsed to DateTime64(3, Asia/Seoul)'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-platform:19092',
    kafka_topic_list = 'prediction.events',
    kafka_group_name = 'clickhouse_data_prediction_events_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream'
COMMENT 'Prediction Kafka Engine queue; consumes prediction.events; OLAP ingestion buffer only; SSOT 아님';

SELECT
    database,
    name,
    engine,
    count() AS host_count,
    groupArray(hostName()) AS hosts
FROM clusterAllReplicas('statground_cluster', system.tables)
WHERE database = 'Data_Prediction_Raw'
  AND name = 'prediction_events_kafka_queue'
GROUP BY database, name, engine;

DROP TABLE IF EXISTS `Data_Prediction_Raw`.mv_prediction_kafka_queue_to_events ON CLUSTER statground_cluster;

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Raw`.mv_prediction_kafka_queue_to_events
ON CLUSTER statground_cluster
TO `Data_Prediction_Raw`.prediction_events_local
AS
SELECT
    ifNull(toUUIDOrNull(event_uuid), toUUID('00000000-0000-0000-0000-000000000000')) AS event_uuid,
    _topic AS kafka_topic,
    replaceRegexpOne(_topic, '\..*$', '') AS event_domain,
    toUInt32(_partition) AS kafka_partition,
    toUInt64(_offset) AS kafka_offset,
    source AS source,
    host AS host,
    toUUIDOrNull(nullIf(uuid_user, '')) AS uuid_user,
    toIPv6OrDefault(ip) AS ip,
    url AS url,
    event_type AS event_type,
    splitByChar('.', event_type)[1] AS service,
    payload AS payload,
    coalesce(parseDateTime64BestEffortOrNull(created_at, 3, 'Asia/Seoul'), now64(3, 'Asia/Seoul')) AS created_at,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_Prediction_Raw`.prediction_events_kafka_queue
WHERE length(ifNull(_error, '')) = 0;

ALTER TABLE `Data_Prediction_Raw`.mv_prediction_kafka_queue_to_events
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from prediction Kafka Engine queue to Data_Prediction_Raw.prediction_events_local; valid rows only; OLAP ingestion layer; SSOT 아님';
