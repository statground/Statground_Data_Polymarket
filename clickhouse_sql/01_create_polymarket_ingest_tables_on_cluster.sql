/*
Polymarket Kafka envelope ingestion.

This provider-specific ingestion layer consumes the current Prediction-domain Kafka topic `prediction.events`,
but stores only Polymarket events in polymarket_* tables.

For future Kalshi ingestion, create a parallel kalshi_events_* set with its own consumer group and event_type filter.

DBeaver-safe: this script creates tables only. Kafka materialized views are attached later in 05_attach_polymarket_kafka_materialized_views_on_cluster.sql.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_events_local
ON CLUSTER statground_cluster
(
    event_uuid UUID COMMENT 'Polymarket producer event UUID v7; ClickHouse stores UUID type; producer sends 36-char UUID v7 string',
    kafka_topic LowCardinality(String) COMMENT 'Kafka topic name from _topic virtual column; current topic prediction.events',
    event_domain LowCardinality(String) COMMENT 'Domain derived from Kafka topic prefix, e.g. prediction; LowCardinality for grouping',
    kafka_partition UInt32 COMMENT 'Kafka partition number from _partition virtual column; lineage/debug field',
    kafka_offset UInt64 COMMENT 'Kafka offset from _offset virtual column; lineage/replay/debug field',
    source LowCardinality(String) COMMENT 'Polymarket producer source code such as github_actions, django, crawler',
    host LowCardinality(String) COMMENT 'Producer host or runtime name; LowCardinality for TB-scale grouping',
    uuid_user Nullable(UUID) COMMENT 'Nullable UUID v7 user identifier; Nullable is not used in ORDER BY',
    ip IPv6 COMMENT 'Client or producer IP normalized to IPv6; invalid or missing values become ::',
    url String COMMENT 'Target URL or crawled Polymarket API URL related to the event',
    event_type LowCardinality(String) COMMENT 'Polymarket event type code, e.g. polymarket.market_snapshot_raw.v1',
    service LowCardinality(String) COMMENT 'Prediction provider/service code parsed from event_type; expected polymarket',
    payload String COMMENT 'Polymarket raw event payload JSON string; parsed by downstream materialized views; OLAP only; SSOT 아님',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'Producer event timestamp in Asia/Seoul; ORDER BY leading column',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp in Asia/Seoul'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_Prediction_Polymarket_Raw/polymarket_events_local', '{replica}')
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, event_uuid)
SETTINGS index_granularity = 8192
COMMENT 'Polymarket raw Kafka envelope local replicated table; OLAP only; SSOT 아님; TB-scale ORDER BY (created_at, event_uuid)';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_events
ON CLUSTER statground_cluster
(
    event_uuid UUID COMMENT 'Polymarket producer event UUID v7; ClickHouse stores UUID type; producer sends 36-char UUID v7 string',
    kafka_topic LowCardinality(String) COMMENT 'Kafka topic name from _topic virtual column; current topic prediction.events',
    event_domain LowCardinality(String) COMMENT 'Domain derived from Kafka topic prefix, e.g. prediction; LowCardinality for grouping',
    kafka_partition UInt32 COMMENT 'Kafka partition number from _partition virtual column; lineage/debug field',
    kafka_offset UInt64 COMMENT 'Kafka offset from _offset virtual column; lineage/replay/debug field',
    source LowCardinality(String) COMMENT 'Polymarket producer source code such as github_actions, django, crawler',
    host LowCardinality(String) COMMENT 'Producer host or runtime name; LowCardinality for TB-scale grouping',
    uuid_user Nullable(UUID) COMMENT 'Nullable UUID v7 user identifier; Nullable is not used in ORDER BY',
    ip IPv6 COMMENT 'Client or producer IP normalized to IPv6; invalid or missing values become ::',
    url String COMMENT 'Target URL or crawled Polymarket API URL related to the event',
    event_type LowCardinality(String) COMMENT 'Polymarket event type code, e.g. polymarket.market_snapshot_raw.v1',
    service LowCardinality(String) COMMENT 'Prediction provider/service code parsed from event_type; expected polymarket',
    payload String COMMENT 'Polymarket raw event payload JSON string; parsed by downstream materialized views; OLAP only; SSOT 아님',
    created_at DateTime64(3, 'Asia/Seoul') COMMENT 'Producer event timestamp in Asia/Seoul; storage local table ORDER BY leading column',
    ingested_at DateTime64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp in Asia/Seoul'
)
ENGINE = Distributed('statground_cluster', 'Data_Prediction_Polymarket_Raw', 'polymarket_events_local', cityHash64(toString(event_uuid)))
COMMENT 'Polymarket raw Kafka envelope distributed table; insert/read interface across statground_cluster; routes by event_uuid';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_events_kafka_queue
ON CLUSTER statground_cluster
(
    event_uuid String COMMENT 'Polymarket UUID v7 string from Kafka JSON message; converted to UUID in materialized view',
    source String COMMENT 'Producer source code from Kafka JSON message',
    host String COMMENT 'Producer host from Kafka JSON message',
    uuid_user Nullable(String) COMMENT 'Nullable UUID v7 user string from Kafka JSON message',
    ip String COMMENT 'IP string from Kafka JSON message; normalized to IPv6 in materialized view',
    url String COMMENT 'URL string from Kafka JSON message',
    event_type String COMMENT 'Polymarket event type string from Kafka JSON message',
    payload String COMMENT 'Polymarket payload JSON string from Kafka JSON message',
    created_at String COMMENT 'Producer timestamp string from Kafka JSON message; parsed to DateTime64(3, Asia/Seoul)'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-platform:19092',
    kafka_topic_list = 'prediction.events',
    kafka_group_name = 'clickhouse_data_prediction_polymarket_events_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream'
COMMENT 'Polymarket Kafka Engine queue; consumes prediction.events with a Polymarket-specific consumer group; OLAP ingestion buffer only; SSOT 아님';

/* Verification before attaching Kafka materialized views. Expected on current 2 shards x 2 replicas cluster: host_count = 4. */
SELECT
    database,
    name,
    engine,
    count() AS host_count,
    groupArray(hostName()) AS hosts
FROM clusterAllReplicas('statground_cluster', system.tables)
WHERE database = 'Data_Prediction_Polymarket_Raw'
  AND name IN ('polymarket_events_local', 'polymarket_events', 'polymarket_events_kafka_queue')
GROUP BY database, name, engine
ORDER BY database, name, engine;
