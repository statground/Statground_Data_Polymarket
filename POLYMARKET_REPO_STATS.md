# Polymarket Repo Stats
Updated: 2026-01-13 01:08:15 UTC
Owner: statground
Prefix: Statground_Data_Polymarket (auto-detect main + year repos)

## Summary (All Repos)
- Total files: 922,255
- JSON files: 922,255
- event JSON: 150,811 (excluding meta)
- market JSON: 309,510 (excluding meta)
- series JSON: 804 (excluding meta)
- event meta: 150,811
- market meta: 309,510
- series meta: 804

## Per Repository
Repository | Total files | JSON files | event JSON | market JSON | series JSON | event meta | market meta | series meta | Note
---|---:|---:|---:|---:|---:|---:|---:|---:|---
Statground_Data_Polymarket | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | counts file missing (will appear after next daily crawl)
Statground_Data_Polymarket_2022 | 16,073 | 16,073 | 3,171 | 4,860 | 5 | 3,171 | 4,860 | 5 | 
Statground_Data_Polymarket_2023 | 10,633 | 10,633 | 1,436 | 3,868 | 12 | 1,436 | 3,868 | 12 | 
Statground_Data_Polymarket_2024 | 44,299 | 44,299 | 5,912 | 16,233 | 4 | 5,912 | 16,233 | 4 | 
Statground_Data_Polymarket_2025 | 733,405 | 733,405 | 116,162 | 249,812 | 728 | 116,162 | 249,812 | 728 | 
Statground_Data_Polymarket_2026 | 117,845 | 117,845 | 24,130 | 34,737 | 55 | 24,130 | 34,737 | 55 | 

### Notes
- Counts are read from `POLYMARKET_COUNTS.json` in each repo.
- That file is updated during the **daily crawl** (once per UTC day).
- This stats workflow runs hourly, so timestamps update hourly even if counts do not change.
