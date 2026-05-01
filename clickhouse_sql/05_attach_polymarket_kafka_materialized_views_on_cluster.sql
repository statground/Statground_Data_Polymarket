/*
Final Kafka attach step for Polymarket.

Run this only after 00, 01, 02, 03, 04 have succeeded and the verification query in 01 shows
polymarket_events_kafka_queue on all 4 storage nodes. Kafka Engine consumption starts as soon as
these materialized views are attached, so do not run this before downstream snapshot/latest/mart views exist.

Runtime baseline: ClickHouse 26.1.2.11
Cluster: statground_cluster
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.mv_polymarket_kafka_queue_to_events
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Raw`.polymarket_events_local
AS
SELECT
    ifNull(toUUIDOrNull(event_uuid), toUUID('00000000-0000-0000-0000-000000000000')) AS event_uuid,
    _topic AS kafka_topic,
    replaceRegexpOne(_topic, '\\..*$', '') AS event_domain,
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
FROM `Data_Prediction_Polymarket_Raw`.polymarket_events_kafka_queue
WHERE length(ifNull(_error, '')) = 0
  AND startsWith(event_type, 'polymarket.');

ALTER TABLE `Data_Prediction_Polymarket_Raw`.mv_polymarket_kafka_queue_to_events
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Polymarket Kafka Engine queue to Data_Prediction_Polymarket_Raw.polymarket_events_local; valid Polymarket rows only; one local insert per ClickHouse Kafka consumer';

CREATE MATERIALIZED VIEW IF NOT EXISTS `Data_Prediction_Polymarket_Log`.mv_polymarket_kafka_queue_to_parse_error_local
ON CLUSTER statground_cluster
TO `Data_Prediction_Polymarket_Log`.polymarket_kafka_parse_error_local
AS
SELECT
    ifNull(toUUIDOrNull(event_uuid), toUUID('00000000-0000-0000-0000-000000000000')) AS event_uuid,
    _topic AS kafka_topic,
    toUInt32(_partition) AS kafka_partition,
    toUInt64(_offset) AS kafka_offset,
    _error AS error,
    _raw_message AS raw_message,
    now64(3, 'Asia/Seoul') AS created_at,
    now64(3, 'Asia/Seoul') AS ingested_at
FROM `Data_Prediction_Polymarket_Raw`.polymarket_events_kafka_queue
WHERE length(ifNull(_error, '')) > 0;

ALTER TABLE `Data_Prediction_Polymarket_Log`.mv_polymarket_kafka_queue_to_parse_error_local
ON CLUSTER statground_cluster
MODIFY COMMENT 'Materialized view from Polymarket Kafka Engine queue to Polymarket parse error log; malformed rows only; OLAP monitoring layer; SSOT 아님';

/* Verification after Kafka MV attach. Expected host_count = 4 for both materialized views. */
SELECT
    database,
    name,
    engine,
    count() AS host_count,
    groupArray(hostName()) AS hosts
FROM clusterAllReplicas('statground_cluster', system.tables)
WHERE (database = 'Data_Prediction_Polymarket_Raw' AND name = 'mv_polymarket_kafka_queue_to_events')
   OR (database = 'Data_Prediction_Polymarket_Log' AND name = 'mv_polymarket_kafka_queue_to_parse_error_local')
GROUP BY database, name, engine
ORDER BY database, name, engine;
