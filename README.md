# Single-workflow scheduler: hourly run, daily crawl + hourly stats

## What it does
- One GitHub Actions workflow runs every hour.
- It decides whether to crawl (once per UTC day).
- If crawl is due: **crawl -> stats** (strict order).
- If crawl is not due: **stats only**.
- Scheduler state is stored in `.state/polymarket_scheduler.json` via GitHub Contents API (SHA-safe).

## Required secret
- `POLYMARKET_PAT` (fine-grained PAT)
  - Contents: Read & write (required)
  - Administration: Read & write (required only if crawler auto-creates year repos)

## Expected existing scripts
- `scripts/polymarket_crawl_and_fanout.py`  (PAT v2 that updates checkpoint via Contents API)
- `scripts/update_repo_stats.py`
