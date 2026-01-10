# Polymarket: single workflow (every 6 hours) + daily crawl (UTC) + every 6 hours stats

## What you get
- **One** workflow: `.github/workflows/polymarket_every 6 hours_scheduler.yml`
- Runs **every hour** (UTC)
- Crawl runs **once per UTC day**
- Stats runs **every hour**
- If crawl is due: **crawl -> stats**

## Required secret
Create a fine-grained PAT and add it as repo secret:

- Secret name: `POLYMARKET_PAT`
- Permissions (recommended):
  - Contents: Read & write (required)
  - Administration: Read & write (required if auto-create year repos is desired)

## How stats work (scalable)
Hourly stats does **not** clone huge repos.
Instead, the daily crawler writes `POLYMARKET_COUNTS.json` into each year repo on every crawl.
Hourly stats reads those small files via GitHub API and updates `POLYMARKET_REPO_STATS.md` in the orchestrator repo.

## Files written in orchestrator repo (via GitHub API)
- `.state/polymarket_scheduler.json`  (scheduler state)
- `.state/polymarket_checkpoint.json` (crawl checkpoint)
- `POLYMARKET_REPO_STATS.md`          (every 6 hours stats output)


## Note: fine-grained PAT org listing 404
The every 6 hours stats script no longer requires listing org repositories. The daily crawler writes `.state/polymarket_targets.json` into the orchestrator repo, and stats uses that file.
