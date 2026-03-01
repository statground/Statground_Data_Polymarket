#!/usr/bin/env python3
"""ClickHouse OPTIMIZE helpers for ReplacingMergeTree tables.

We trigger OPTIMIZE TABLE ... FINAL on random partitions to encourage merge/cleanup.
This is intentionally probabilistic to avoid heavy load on every hourly run.
"""
from __future__ import annotations

import os
import random
from typing import Iterable

def optimize_random_partitions(ch, tables: Iterable[str]) -> None:
    """Run OPTIMIZE TABLE ... FINAL on a few random partitions.

    Env controls:
      - OPTIMIZE_AFTER_RUN: '1' (default) / '0'
      - OPTIMIZE_PROB: float in [0,1], default 0.15
      - OPTIMIZE_PARTITIONS: int, default 128 (matches DDL cityHash64(...) % 128)
      - OPTIMIZE_MAX_PARTITIONS: int, default 4
    """
    if os.getenv("OPTIMIZE_AFTER_RUN", "1") not in ("1", "true", "TRUE", "yes", "YES"):
        return

    try:
        prob = float(os.getenv("OPTIMIZE_PROB", "0.15"))
    except Exception:
        prob = 0.15

    if prob <= 0:
        return
    if random.random() > prob:
        return

    try:
        n_partitions = int(os.getenv("OPTIMIZE_PARTITIONS", "128"))
    except Exception:
        n_partitions = 128

    try:
        k = int(os.getenv("OPTIMIZE_MAX_PARTITIONS", "4"))
    except Exception:
        k = 4
    k = max(1, min(k, n_partitions))

    picked = random.sample(range(n_partitions), k=k)
    for t in tables:
        for p in picked:
            q = f"OPTIMIZE TABLE {t} PARTITION {p} FINAL"
            try:
                ch.command(q)
                print(f"[OPTIMIZE] ok: {t} partition={p}")
            except Exception as e:
                # Don't fail the workflow because optimize is best-effort
                print(f"[OPTIMIZE] skip: {t} partition={p} err={e}")
