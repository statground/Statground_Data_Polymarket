/*
Repair for Code 60 with the provider-specific Polymarket package.

The fix is execution order, not a new table design:
1) Run 01_create_polymarket_ingest_tables_on_cluster.sql.
2) Confirm the verification query shows polymarket_events_kafka_queue with host_count = 4.
3) Run 05_attach_polymarket_kafka_materialized_views_on_cluster.sql.
*/

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
