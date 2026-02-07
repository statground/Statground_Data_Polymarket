# Polymarket Repo Stats
Updated: 2026-02-07 18:33:19 UTC
Owner: statground
Prefix: Statground_Data_Polymarket (auto-detect main + year repos)

## Summary (All Repos)
- Total files: 1,168,125
- JSON files: 1,168,125
- event JSON: 191,274 (excluding meta)
- market JSON: 391,743 (excluding meta)
- series JSON: 1,043 (excluding meta)
- event meta: 191,274
- market meta: 391,743
- series meta: 1,043

## Per Repository
Repository | Total files | JSON files | event JSON | market JSON | series JSON | event meta | market meta | series meta | Note
---|---:|---:|---:|---:|---:|---:|---:|---:|---
Statground_Data_Polymarket | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | counts file missing (will appear after next daily crawl)
Statground_Data_Polymarket_2022 | 16,073 | 16,073 | 3,171 | 4,860 | 5 | 3,171 | 4,860 | 5 | 
Statground_Data_Polymarket_2023 | 10,633 | 10,633 | 1,436 | 3,868 | 12 | 1,436 | 3,868 | 12 | 
Statground_Data_Polymarket_2024 | 44,299 | 44,299 | 5,912 | 16,233 | 4 | 5,912 | 16,233 | 4 | 
Statground_Data_Polymarket_2025 | 733,613 | 733,613 | 116,168 | 249,910 | 728 | 116,168 | 249,910 | 728 | 
Statground_Data_Polymarket_2026 | 363,507 | 363,507 | 64,587 | 116,872 | 294 | 64,587 | 116,872 | 294 | 

### Notes
- Counts are read from `POLYMARKET_COUNTS.json` in each repo.
- That file is updated during the **daily crawl** (once per UTC day).
- This stats workflow runs hourly, so timestamps update hourly even if counts do not change.
