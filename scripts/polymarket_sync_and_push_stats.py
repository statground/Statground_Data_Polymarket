#!/usr/bin/env python3
"""Generate Polymarket stats on top of the latest remote branch and push safely.

Why this exists:
- The ingestion/refresh jobs may update the repository indirectly (checkpoint via GitHub
  Contents API), so the local checkout can become stale during the workflow.
- If two workflows try to commit the stats report at similar times, a naïve
  `git commit -> git pull --rebase -> git push` flow can conflict on
  reports/polymarket_stats/README.md.

Strategy:
1) Reset the working tree to the latest origin/<branch>.
2) Regenerate reports/polymarket_stats from ClickHouse.
3) Commit only that report directory.
4) Try to push.
5) If push is rejected because origin advanced, discard the local commit,
   reset to the newest origin/<branch>, regenerate, and retry.

This avoids textual/binary merge conflict handling entirely and keeps the result
fully deterministic from the latest ClickHouse state.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = REPO_ROOT / "reports" / "polymarket_stats"


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=REPO_ROOT, check=check, text=True)


def checkout_latest(branch: str) -> None:
    run(["git", "fetch", "origin", branch])
    run(["git", "checkout", "-B", branch, f"origin/{branch}"])
    run(["git", "reset", "--hard", f"origin/{branch}"])


def generate_stats() -> None:
    run([sys.executable, "-m", "scripts.polymarket_stats_report"])


def stage_stats() -> bool:
    run(["git", "add", "-A", str(REPORT_DIR.relative_to(REPO_ROOT))])
    diff = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=REPO_ROOT,
        text=True,
    )
    return diff.returncode != 0


def commit_stats(message: str) -> None:
    run(["git", "commit", "-m", message])


def push(branch: str) -> bool:
    result = subprocess.run(
        ["git", "push", "origin", f"HEAD:{branch}"],
        cwd=REPO_ROOT,
        text=True,
    )
    return result.returncode == 0


def ensure_git_identity() -> None:
    run(["git", "config", "user.name", "github-actions[bot]"])
    run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"])


def main() -> int:
    branch = os.getenv("DEFAULT_BRANCH", "main")
    commit_message = os.getenv("STATS_COMMIT_MESSAGE", "chore(stats): update polymarket stats")
    max_attempts = int(os.getenv("STATS_PUSH_MAX_ATTEMPTS", "4"))

    ensure_git_identity()

    for attempt in range(1, max_attempts + 1):
        print(f"[STATS] attempt {attempt}/{max_attempts}", flush=True)
        checkout_latest(branch)
        generate_stats()

        if not stage_stats():
            print("[STATS] No report changes detected. Nothing to push.", flush=True)
            return 0

        commit_stats(commit_message)
        if push(branch):
            print("[STATS] Push succeeded.", flush=True)
            return 0

        print("[STATS] Push rejected. Retrying from latest remote state...", flush=True)

    print("[STATS] Failed to push stats after retries.", file=sys.stderr, flush=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
