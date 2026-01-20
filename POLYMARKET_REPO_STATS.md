# Polymarket Repo Stats
Updated: 2026-01-20 12:40:09 UTC
Owner: statground
Prefix: Statground_Data_Polymarket (auto-detect main + year repos)

## Summary (All Repos)
- Total files: 999,029
- JSON files: 999,029
- event JSON: 166,420 (excluding meta)
- market JSON: 332,260 (excluding meta)
- series JSON: 832 (excluding meta)
- event meta: 166,420
- market meta: 332,260
- series meta: 832

## Per Repository
Repository | Total files | JSON files | event JSON | market JSON | series JSON | event meta | market meta | series meta | Note
---|---:|---:|---:|---:|---:|---:|---:|---:|---
Statground_Data_Polymarket | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | counts file missing (will appear after next daily crawl)
Statground_Data_Polymarket_2022 | 16,073 | 16,073 | 3,171 | 4,860 | 5 | 3,171 | 4,860 | 5 | 
Statground_Data_Polymarket_2023 | 10,633 | 10,633 | 1,436 | 3,868 | 12 | 1,436 | 3,868 | 12 | 
Statground_Data_Polymarket_2024 | 44,299 | 44,299 | 5,912 | 16,233 | 4 | 5,912 | 16,233 | 4 | 
Statground_Data_Polymarket_2025 | 733,503 | 733,503 | 116,168 | 249,855 | 728 | 116,168 | 249,855 | 728 | 
Statground_Data_Polymarket_2026 | 194,521 | 194,521 | 39,733 | 57,444 | 83 | 39,733 | 57,444 | 83 | 

### Notes
- Counts are read from `POLYMARKET_COUNTS.json` in each repo.
- That file is updated during the **daily crawl** (once per UTC day).
- This stats workflow runs hourly, so timestamps update hourly even if counts do not change.
