# Polymarket Repo Stats
Updated: 2026-02-28 06:40:09 UTC
Owner: statground
Prefix: Statground_Data_Polymarket (auto-detect main + year repos)

## Summary (All Repos)
- Total files: 1,400,357
- JSON files: 1,400,357
- event JSON: 225,600 (excluding meta)
- market JSON: 473,407 (excluding meta)
- series JSON: 1,169 (excluding meta)
- event meta: 225,600
- market meta: 473,407
- series meta: 1,169

## Per Repository
Repository | Total files | JSON files | event JSON | market JSON | series JSON | event meta | market meta | series meta | Note
---|---:|---:|---:|---:|---:|---:|---:|---:|---
Statground_Data_Polymarket | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | counts file missing (will appear after next daily crawl)
Statground_Data_Polymarket_2022 | 16,073 | 16,073 | 3,171 | 4,860 | 5 | 3,171 | 4,860 | 5 | 
Statground_Data_Polymarket_2023 | 10,633 | 10,633 | 1,436 | 3,868 | 12 | 1,436 | 3,868 | 12 | 
Statground_Data_Polymarket_2024 | 44,299 | 44,299 | 5,912 | 16,233 | 4 | 5,912 | 16,233 | 4 | 
Statground_Data_Polymarket_2025 | 733,735 | 733,735 | 116,169 | 249,970 | 728 | 116,169 | 249,970 | 728 | 
Statground_Data_Polymarket_2026 | 595,617 | 595,617 | 98,912 | 198,476 | 420 | 98,912 | 198,476 | 420 | 

### Notes
- Counts are read from `POLYMARKET_COUNTS.json` in each repo.
- That file is updated during the **daily crawl** (once per UTC day).
- This stats workflow runs hourly, so timestamps update hourly even if counts do not change.
