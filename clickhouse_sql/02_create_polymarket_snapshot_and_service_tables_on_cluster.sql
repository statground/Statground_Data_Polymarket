/*
Polymarket raw snapshot, checkpoint, and latest service tables.
Raw tables are append-only snapshots from Kafka payloads.
Service tables keep latest object state with ReplicatedReplacingMergeTree.
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot_local
ON CLUSTER statground_cluster
(
    event_id UInt64 COMMENT 'Polymarket event id from Gamma API',
    raw_key UUID COMMENT 'UUID v7 per collected snapshot; producer sends 36-char UUID v7 string; ClickHouse stores UUID type',
    collected_at_utc DateTime64(3, 'UTC') COMMENT 'API collection timestamp in UTC; ORDER BY leading column',
    created_at_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket event.createdAt in UTC; Nullable is not used in ORDER BY',
    updated_at_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket event.updatedAt in UTC; Nullable is not used in ORDER BY',
    title String COMMENT 'Polymarket event.title',
    ticker LowCardinality(String) COMMENT 'Polymarket event.ticker; LowCardinality for grouping/filtering',
    slug LowCardinality(String) COMMENT 'Polymarket event.slug; LowCardinality for grouping/filtering',
    description String COMMENT 'Polymarket event.description',
    active UInt8 COMMENT 'Polymarket event.active status; 1=true, 0=false',
    archived UInt8 COMMENT 'Polymarket event.archived status; 1=true, 0=false',
    closed UInt8 COMMENT 'Polymarket event.closed status; 1=true, 0=false',
    restricted UInt8 COMMENT 'Polymarket event.restricted status; 1=true, 0=false',
    start_date_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket event.startDate in UTC; Nullable is not used in ORDER BY',
    end_date_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket event.endDate in UTC; Nullable is not used in ORDER BY',
    closed_time_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket event.closedTime in UTC; Nullable is not used in ORDER BY',
    creation_date_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket event.creationDate in UTC; Nullable is not used in ORDER BY',
    series_slug LowCardinality(String) COMMENT 'Polymarket event.seriesSlug; LowCardinality for grouping/filtering',
    series_ids Array(UInt64) COMMENT 'Connected Polymarket series ids extracted from payload',
    market_ids Array(UInt64) COMMENT 'Connected Polymarket market ids extracted from payload',
    icon_url String COMMENT 'Polymarket event.icon URL',
    image_url String COMMENT 'Polymarket event.image URL',
    volume Nullable(Float64) COMMENT 'Polymarket event.volume; Nullable metric',
    raw_json String COMMENT 'Polymarket API raw JSON string after producer-side pruning of large nested entity arrays; OLAP raw retention only; SSOT 아님',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp in Asia/Seoul'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_Prediction_Polymarket_Raw/polymarket_event_snapshot_local', '{replica}')
PARTITION BY toYYYYMM(collected_at_utc)
ORDER BY (collected_at_utc, event_id, raw_key)
SETTINGS index_granularity = 8192
COMMENT 'Polymarket event raw snapshot local table; append-only OLAP raw data; SSOT 아님; TB-scale ORDER BY starts with collection time';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot_local
ENGINE = Distributed('statground_cluster', 'Data_Prediction_Polymarket_Raw', 'polymarket_event_snapshot_local', cityHash64(event_id))
COMMENT 'Polymarket event raw snapshot distributed table; insert/read interface across statground_cluster; routes by event_id';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot_local
ON CLUSTER statground_cluster
(
    market_id UInt64 COMMENT 'Polymarket market id from Gamma API',
    raw_key UUID COMMENT 'UUID v7 per collected snapshot; producer sends 36-char UUID v7 string; ClickHouse stores UUID type',
    collected_at_utc DateTime64(3, 'UTC') COMMENT 'API collection timestamp in UTC; ORDER BY leading column',
    created_at_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket market.createdAt in UTC; Nullable is not used in ORDER BY',
    updated_at_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket market.updatedAt in UTC; Nullable is not used in ORDER BY',
    condition_id String COMMENT 'Polymarket market.conditionId hex string',
    question_id String COMMENT 'Polymarket market.questionID/questionId string',
    slug LowCardinality(String) COMMENT 'Polymarket market.slug; LowCardinality for grouping/filtering',
    question String COMMENT 'Polymarket market.question',
    description String COMMENT 'Polymarket market.description',
    resolution_source String COMMENT 'Polymarket market.resolutionSource',
    resolved_by Nullable(String) COMMENT 'Polymarket market.resolvedBy address/string; Nullable is not used in ORDER BY',
    active UInt8 COMMENT 'Polymarket market.active status; 1=true, 0=false',
    approved UInt8 COMMENT 'Polymarket market.approved status; 1=true, 0=false',
    archived UInt8 COMMENT 'Polymarket market.archived status; 1=true, 0=false',
    closed UInt8 COMMENT 'Polymarket market.closed status; 1=true, 0=false',
    restricted UInt8 COMMENT 'Polymarket market.restricted status; 1=true, 0=false',
    neg_risk UInt8 COMMENT 'Polymarket market.negRisk status; 1=true, 0=false',
    start_date_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket market.startDate in UTC; Nullable is not used in ORDER BY',
    end_date_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket market.endDate in UTC; Nullable is not used in ORDER BY',
    closed_time_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket market.closedTime in UTC; Nullable is not used in ORDER BY',
    best_ask Nullable(Float64) COMMENT 'Polymarket market.bestAsk; Nullable metric',
    best_bid Nullable(Float64) COMMENT 'Polymarket market.bestBid; Nullable metric',
    last_trade_price Nullable(Float64) COMMENT 'Polymarket market.lastTradePrice; Nullable metric',
    spread Nullable(Float64) COMMENT 'Polymarket market.spread; Nullable metric',
    volume Nullable(Float64) COMMENT 'Polymarket market.volume; Nullable metric',
    outcomes Array(String) COMMENT 'Polymarket market.outcomes array',
    outcome_prices Array(String) COMMENT 'Polymarket market.outcomePrices array as strings',
    clob_token_ids Array(String) COMMENT 'Polymarket market.clobTokenIds array',
    series_slug LowCardinality(String) COMMENT 'Polymarket market.seriesSlug; LowCardinality for grouping/filtering',
    series_ids Array(UInt64) COMMENT 'Connected Polymarket series ids extracted from payload',
    event_ids Array(UInt64) COMMENT 'Connected Polymarket event ids extracted from payload',
    raw_json String COMMENT 'Polymarket API raw JSON string after producer-side pruning of large nested entity arrays; OLAP raw retention only; SSOT 아님',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp in Asia/Seoul'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_Prediction_Polymarket_Raw/polymarket_market_snapshot_local', '{replica}')
PARTITION BY toYYYYMM(collected_at_utc)
ORDER BY (collected_at_utc, market_id, raw_key)
SETTINGS index_granularity = 8192
COMMENT 'Polymarket market raw snapshot local table; append-only OLAP raw data; SSOT 아님; TB-scale ORDER BY starts with collection time';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot_local
ENGINE = Distributed('statground_cluster', 'Data_Prediction_Polymarket_Raw', 'polymarket_market_snapshot_local', cityHash64(market_id))
COMMENT 'Polymarket market raw snapshot distributed table; insert/read interface across statground_cluster; routes by market_id';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot_local
ON CLUSTER statground_cluster
(
    series_id UInt64 COMMENT 'Polymarket series id from Gamma API',
    raw_key UUID COMMENT 'UUID v7 per collected snapshot; producer sends 36-char UUID v7 string; ClickHouse stores UUID type',
    collected_at_utc DateTime64(3, 'UTC') COMMENT 'API collection timestamp in UTC; ORDER BY leading column',
    created_at_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket series.createdAt in UTC; Nullable is not used in ORDER BY',
    updated_at_utc Nullable(DateTime64(3, 'UTC')) COMMENT 'Polymarket series.updatedAt in UTC; Nullable is not used in ORDER BY',
    slug LowCardinality(String) COMMENT 'Polymarket series.slug; LowCardinality for grouping/filtering',
    ticker LowCardinality(String) COMMENT 'Polymarket series.ticker; LowCardinality for grouping/filtering',
    title String COMMENT 'Polymarket series.title',
    active UInt8 COMMENT 'Polymarket series.active status; 1=true, 0=false',
    archived UInt8 COMMENT 'Polymarket series.archived status; 1=true, 0=false',
    closed UInt8 COMMENT 'Polymarket series.closed status; 1=true, 0=false',
    recurrence LowCardinality(String) COMMENT 'Polymarket series.recurrence; LowCardinality for grouping/filtering',
    series_type LowCardinality(String) COMMENT 'Polymarket series.seriesType; LowCardinality for grouping/filtering',
    liquidity Nullable(Float64) COMMENT 'Polymarket series.liquidity; Nullable metric',
    volume Nullable(Float64) COMMENT 'Polymarket series.volume; Nullable metric',
    volume_24h Nullable(Float64) COMMENT 'Polymarket series.volume24hr or volume24h; Nullable metric',
    event_ids Array(UInt64) COMMENT 'Connected Polymarket event ids extracted from payload',
    raw_json String COMMENT 'Polymarket API raw JSON string after producer-side pruning of large nested entity arrays; OLAP raw retention only; SSOT 아님',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp in Asia/Seoul'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_Prediction_Polymarket_Raw/polymarket_series_snapshot_local', '{replica}')
PARTITION BY toYYYYMM(collected_at_utc)
ORDER BY (collected_at_utc, series_id, raw_key)
SETTINGS index_granularity = 8192
COMMENT 'Polymarket series raw snapshot local table; append-only OLAP raw data; SSOT 아님; TB-scale ORDER BY starts with collection time';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot_local
ENGINE = Distributed('statground_cluster', 'Data_Prediction_Polymarket_Raw', 'polymarket_series_snapshot_local', cityHash64(series_id))
COMMENT 'Polymarket series raw snapshot distributed table; insert/read interface across statground_cluster; routes by series_id';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Service`.polymarket_event_latest_local
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Raw`.polymarket_event_snapshot_local
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_Prediction_Polymarket_Service/polymarket_event_latest_local', '{replica}', collected_at_utc)
PARTITION BY cityHash64(event_id) % 128
ORDER BY event_id
SETTINGS index_granularity = 8192
COMMENT 'Polymarket event latest local table; OLAP service layer; ReplacingMergeTree keeps latest per event_id by collected_at_utc; SSOT 아님';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Service`.polymarket_event_latest
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Service`.polymarket_event_latest_local
ENGINE = Distributed('statground_cluster', 'Data_Prediction_Polymarket_Service', 'polymarket_event_latest_local', cityHash64(event_id))
COMMENT 'Polymarket event latest distributed table; read interface across statground_cluster; use FINAL only when exact latest rows are required';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Service`.polymarket_market_latest_local
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Raw`.polymarket_market_snapshot_local
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_Prediction_Polymarket_Service/polymarket_market_latest_local', '{replica}', collected_at_utc)
PARTITION BY cityHash64(market_id) % 128
ORDER BY market_id
SETTINGS index_granularity = 8192
COMMENT 'Polymarket market latest local table; OLAP service layer; ReplacingMergeTree keeps latest per market_id by collected_at_utc; SSOT 아님';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Service`.polymarket_market_latest
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Service`.polymarket_market_latest_local
ENGINE = Distributed('statground_cluster', 'Data_Prediction_Polymarket_Service', 'polymarket_market_latest_local', cityHash64(market_id))
COMMENT 'Polymarket market latest distributed table; read interface across statground_cluster; use FINAL only when exact latest rows are required';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Service`.polymarket_series_latest_local
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Raw`.polymarket_series_snapshot_local
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/Data_Prediction_Polymarket_Service/polymarket_series_latest_local', '{replica}', collected_at_utc)
PARTITION BY cityHash64(series_id) % 128
ORDER BY series_id
SETTINGS index_granularity = 8192
COMMENT 'Polymarket series latest local table; OLAP service layer; ReplacingMergeTree keeps latest per series_id by collected_at_utc; SSOT 아님';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Service`.polymarket_series_latest
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Service`.polymarket_series_latest_local
ENGINE = Distributed('statground_cluster', 'Data_Prediction_Polymarket_Service', 'polymarket_series_latest_local', cityHash64(series_id))
COMMENT 'Polymarket series latest distributed table; read interface across statground_cluster; use FINAL only when exact latest rows are required';


CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_crawl_checkpoint_local
ON CLUSTER statground_cluster
(
    checkpoint_uuid UUID COMMENT 'UUID v7 checkpoint event id; producer sends 36-char UUID v7 string; ClickHouse stores UUID type',
    service LowCardinality(String) COMMENT 'Prediction provider/service code; expected polymarket',
    source LowCardinality(String) COMMENT 'Checkpoint source or crawler mode such as polymarket_ingest_new',
    checkpoint_json String COMMENT 'Checkpoint map JSON string; OLAP monitoring only; SSOT 아님',
    updated_at DateTime64(3, 'Asia/Seoul') COMMENT 'Checkpoint update timestamp in Asia/Seoul; ORDER BY leading column',
    ingested_at DateTime64(3, 'Asia/Seoul') DEFAULT now64(3, 'Asia/Seoul') COMMENT 'ClickHouse ingestion timestamp in Asia/Seoul'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/Data_Prediction_Polymarket_Raw/polymarket_crawl_checkpoint_local', '{replica}')
PARTITION BY toYYYYMM(updated_at)
ORDER BY (updated_at, source, checkpoint_uuid)
SETTINGS index_granularity = 8192
COMMENT 'Polymarket crawler checkpoint log local table; OLAP monitoring only; SSOT 아님; TB-scale ORDER BY starts with updated_at';

CREATE TABLE IF NOT EXISTS `Data_Prediction_Polymarket_Raw`.polymarket_crawl_checkpoint
ON CLUSTER statground_cluster
AS `Data_Prediction_Polymarket_Raw`.polymarket_crawl_checkpoint_local
ENGINE = Distributed('statground_cluster', 'Data_Prediction_Polymarket_Raw', 'polymarket_crawl_checkpoint_local', cityHash64(toString(checkpoint_uuid)))
COMMENT 'Polymarket crawler checkpoint distributed table; read interface across statground_cluster; OLAP only; SSOT 아님';

SELECT
    hostName() AS remote_host,
    database,
    name,
    engine
FROM clusterAllReplicas('statground_cluster', system.tables)
WHERE (database = 'Data_Prediction_Polymarket_Raw' AND name LIKE 'polymarket%')
   OR (database = 'Data_Prediction_Polymarket_Service' AND name LIKE 'polymarket%')
ORDER BY database, name, remote_host;
