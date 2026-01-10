# Polymarket Repo Stats

- Updated: **2026-01-10 14:23:15 UTC**
- Owner: `statground` (mode: `auto`)
- Prefix: `Statground_Data_Polymarket` (auto-detect main + year repos)

## Summary (All Repos)

- Total files: **713,850**
- JSON files: **713,848**
- event JSON: **137,624** (excluding meta)
- market JSON: **218,530** (excluding meta)
- series JSON: **770** (excluding meta)
- event meta: **137,624**
- market meta: **218,530**
- series meta: **770**

## Per Repository

| Repository | Total files | JSON files | event JSON | market JSON | series JSON | event meta | market meta | series meta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `Statground_Data_Polymarket` | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `Statground_Data_Polymarket_2022` | 16,072 | 16,072 | 3,171 | 4,860 | 5 | 3,171 | 4,860 | 5 |
| `Statground_Data_Polymarket_2023` | 10,632 | 10,632 | 1,436 | 3,868 | 12 | 1,436 | 3,868 | 12 |
| `Statground_Data_Polymarket_2024` | 44,298 | 44,298 | 5,912 | 16,233 | 4 | 5,912 | 16,233 | 4 |
| `Statground_Data_Polymarket_2025` | 579,284 | 579,284 | 116,159 | 172,755 | 728 | 116,159 | 172,755 | 728 |
| `Statground_Data_Polymarket_2026` | 63,562 | 63,562 | 10,946 | 20,814 | 21 | 10,946 | 20,814 | 21 |

### Notes
- Repo listing: tries `/orgs/{owner}/repos` then `/users/{owner}/repos`. If token causes 404 masking, retries without token.
- Entity counts are `event_/market_/series_` excluding `*.meta.json`. Meta counts shown separately.
- Uses `git clone --filter=blob:none --no-checkout --depth=1` + `git ls-tree -r` for scalable listing.
