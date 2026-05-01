/*
Prediction domain grants.
Run after Polymarket databases/tables are created.
Users are created by current ClickHouse docker init scripts:
- statground_ch_app: application/read-write user
- trino_ch_statground: Trino read-only user
*/

SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'none_only_active';

GRANT ON CLUSTER statground_cluster SELECT, INSERT ON `Data_Prediction_Polymarket_Raw`.* TO statground_ch_app;
GRANT ON CLUSTER statground_cluster SELECT ON `Data_Prediction_Polymarket_Service`.* TO statground_ch_app;
GRANT ON CLUSTER statground_cluster SELECT ON `Data_Prediction_Polymarket_Log`.* TO statground_ch_app;
GRANT ON CLUSTER statground_cluster SELECT ON `Data_Prediction_Polymarket_Mart`.* TO statground_ch_app;

GRANT ON CLUSTER statground_cluster SELECT ON `Data_Prediction_Polymarket_Raw`.* TO trino_ch_statground;
GRANT ON CLUSTER statground_cluster SELECT ON `Data_Prediction_Polymarket_Service`.* TO trino_ch_statground;
GRANT ON CLUSTER statground_cluster SELECT ON `Data_Prediction_Polymarket_Log`.* TO trino_ch_statground;
GRANT ON CLUSTER statground_cluster SELECT ON `Data_Prediction_Polymarket_Mart`.* TO trino_ch_statground;

SHOW GRANTS FOR statground_ch_app;
SHOW GRANTS FOR trino_ch_statground;
