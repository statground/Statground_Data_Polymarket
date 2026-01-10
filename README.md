# Polymarket Daily Crawl (Year Repo)

1) Copy these files into the target repo:
- scripts/crawl_polymarket_incremental.py
- .github/workflows/polymarket_daily_crawl.yml

2) Edit `.github/workflows/polymarket_daily_crawl.yml` and set:
- TARGET_YEAR: "2026" for Statground_Data_Polymarket_2026
- TARGET_YEAR: "2025" for Statground_Data_Polymarket_2025
- etc.
- For Statground_Data_Polymarket (null bucket): set TARGET_YEAR to empty string, and optionally modify should_accept_dt() to accept dt=None too.

3) Ensure repo Settings → Actions → Workflow permissions → Read and write.
