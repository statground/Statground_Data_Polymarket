# Polymarket Crawler (PAT + auto-create year repos)

This orchestrator runs in **Statground_Data_Polymarket** and will:
1) Crawl Polymarket Gamma API (events/markets/series) incrementally
2) Write `by_created/<entity>/<YYYY>/<MM>/...` with **JSON + .meta.json**
3) Fan-out commits into year repos (`Statground_Data_Polymarket_YYYY`)
4) Auto-create year repos if missing (requires PAT)

## Required Secret

Create a **fine-grained PAT** and add it as a repository secret in `Statground_Data_Polymarket`:

- Secret name: `POLYMARKET_PAT`

Recommended token setup (GitHub):
- Resource owner: `statground` organization
- Repository access: **All repositories** (or at least: orchestrator repo + all year repos; but for auto-create, all is easiest)
- Permissions:
  - **Contents: Read and write** (push commits)
  - **Administration: Read and write** (to create new repositories)  
    (If your org policy separates this, ensure the token can call `POST /orgs/{org}/repos`.)

Note: Fine-grained tokens require the org setting **"Allow access via fine-grained personal access tokens"**.

## Files
- `.github/workflows/polymarket_crawl_daily.yml`
- `scripts/polymarket_crawl_and_fanout.py`

## Checkpoint
- Stored in `.state/polymarket_checkpoint.json` inside the orchestrator repo.
- Uses `updatedAt` ordering to stop early; falls back to `id` ordering if needed.

## Safety knobs
- `MAX_PAGES` is a safety cap to avoid runaway full scans.
- If the API ever stops supporting `order=updatedAt`, you can switch to a different field in workflow env.
