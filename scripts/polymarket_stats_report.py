#!/usr/bin/env python3
"""Polymarket 통계 리포트 (ClickHouse) -> 레포지토리에 커밋

요구사항 반영:
- series / event / market 각각
- 2가지 기준 모두 생성:
  1) 생성 시점 기준(created_at_utc): Polymarket에서 객체가 만들어진 시각
  2) 수집 시점 기준(collected_at_utc): 현재 테이블에 남아있는 '최신 스냅샷 행'이 마지막으로 수집된 시각
    (주의: 현재 테이블들이 ReplacingMergeTree로 최신 1행 유지이므로, '최초 수집(first_seen)' 통계는 아님)

- total / yearly / monthly / daily / hourly
- 시점별(구간별 건수) + 누적(누적합) 을 '하나의 차트'에서 동시에 표시
- 텍스트/라벨/섹션은 한국어
- 산출물은 레포 내 아래 2종만 생성(그 외 파일 생성 금지)
  - reports/polymarket_stats/README.md
  - reports/polymarket_stats/charts/*.png

이 스크립트는 GitHub Actions에서 실행되며, 워크플로가 생성물만 커밋/푸시한다.
"""

import os
from datetime import datetime, timezone

import pandas as pd

# Headless backend for GitHub Actions
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import scripts.polymarket_crawl_to_clickhouse as pm


REPORT_DIR = os.path.join("reports", "polymarket_stats")
CHART_DIR = os.path.join(REPORT_DIR, "charts")


def q1(ch, sql):
    r = ch.query(sql)
    return r.result_rows[0][0] if r.result_rows else None


def q_df(ch, sql, cols):
    r = ch.query(sql)
    return pd.DataFrame(r.result_rows, columns=cols)


def _bucket_sql(dt_col: str, period: str) -> str:
    if period == "hourly":
        return f"toStartOfHour({dt_col})"
    if period == "daily":
        return f"toDate({dt_col})"
    if period == "monthly":
        return f"toStartOfMonth({dt_col})"
    if period == "yearly":
        return f"toStartOfYear({dt_col})"
    raise ValueError(period)


def _recent_where(dt_col: str, period: str) -> str:
    # 너무 길면 가독성이 떨어져서 최근 구간만 시각화
    if period == "hourly":
        return f"{dt_col} >= now() - INTERVAL 72 HOUR"
    if period == "daily":
        return f"{dt_col} >= now() - INTERVAL 120 DAY"
    if period == "monthly":
        return f"{dt_col} >= now() - INTERVAL 48 MONTH"
    if period == "yearly":
        return "1"
    return "1"


def build_time_series(ch, db: str, table: str, dt_col: str, period: str, include_null_guard: bool) -> pd.DataFrame:
    bucket = _bucket_sql(dt_col, period)
    where_recent = _recent_where(dt_col, period)

    wh = []
    if include_null_guard:
        wh.append(f"{dt_col} IS NOT NULL")
    wh.append(where_recent)
    where_clause = " AND ".join(wh) if wh else "1"

    sql = f"""
    SELECT
      {bucket} AS bucket,
      count() AS cnt
    FROM {db}.{table}
    WHERE {where_clause}
    GROUP BY bucket
    ORDER BY bucket
    """
    df = q_df(ch, sql, ["bucket", "cnt"])
    if df.empty:
        return df

    df["bucket"] = pd.to_datetime(df["bucket"])
    df["cnt"] = df["cnt"].astype(int)
    df["cum"] = df["cnt"].cumsum()
    return df


def plot_bar_and_cumline(df: pd.DataFrame, title: str, xlabel: str, out_path: str):
    if df.empty:
        return False

    x = df["bucket"]
    y = df["cnt"]
    y2 = df["cum"]

    fig, ax = plt.subplots(figsize=(12, 4))

    # 막대: 구간별 건수
    ax.bar(x, y)
    ax.set_ylabel("구간별 건수")
    ax.set_xlabel(xlabel)

    # 누적 라인: 누적합
    ax2 = ax.twinx()
    ax2.plot(x, y2, marker="o", linewidth=1)
    ax2.set_ylabel("누적 건수")

    ax.set_title(title)
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    return True


def _safe_name(s: str) -> str:
    return s.replace(" ", "_").replace("/", "_")


def main():
    ch = pm.get_ch_client()
    db = pm.CH_DB

    os.makedirs(CHART_DIR, exist_ok=True)

    run_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    entities = [
        ("시리즈", pm.SERIES_TABLE, "series_id"),
        ("이벤트", pm.EVENT_TABLE, "event_id"),
        ("마켓", pm.MARKET_TABLE, "market_id"),
    ]

    기준들 = [
        ("생성 시점", "created_at_utc", True),
        ("수집 시점", "collected_at_utc", False),
    ]

    periods = [
        ("연도별", "yearly"),
        ("월별", "monthly"),
        ("일별", "daily"),
        ("시간별", "hourly"),
    ]

    # 상단 요약
    summary_rows = []
    for ent_kr, tbl, id_col in entities:
        rows = q1(ch, f"SELECT count() FROM {db}.{tbl}")
        uniq_ids = q1(ch, f"SELECT uniqExact({id_col}) FROM {db}.{tbl}")
        null_created = q1(ch, f"SELECT countIf(created_at_utc IS NULL) FROM {db}.{tbl}")
        min_created = q1(ch, f"SELECT min(created_at_utc) FROM {db}.{tbl}")
        max_effective_updated = q1(ch, f"SELECT max(ifNull(updated_at_utc, collected_at_utc)) FROM {db}.{tbl}")
        max_collected = q1(ch, f"SELECT max(collected_at_utc) FROM {db}.{tbl}")
        summary_rows.append((
            ent_kr,
            int(rows or 0),
            int(uniq_ids or 0),
            int(null_created or 0),
            str(min_created) if min_created is not None else "-",
            str(max_effective_updated) if max_effective_updated is not None else "-",
            str(max_collected) if max_collected is not None else "-",
        ))

    md = []
    md.append("# Polymarket 수집 통계\n")
    md.append(f"- 실행 시각: {run_at_utc}")
    md.append(f"- 대상 DB: `{db}`\n")
    md.append("> **주의**: series/event/market 테이블은 ReplacingMergeTree 기반 최신 1행 유지 구조입니다.\n")
    md.append("> 따라서 **수집 시점 기준 통계는 ‘최신 스냅샷이 마지막으로 수집된 시점’ 분포**이며, ‘최초 발견(first_seen)’ 통계는 아닙니다.\n")

    md.append("## 요약\n")
    md.append("| 구분 | 행 수 | 고유 ID 수 | 생성시각 NULL 행 | 최소 생성 시각 | 최대 갱신(유효) 시각 | 최대 수집 시각 |")
    md.append("|---|---:|---:|---:|---|---|---|")
    for r in summary_rows:
        md.append(f"| {r[0]} | {r[1]:,} | {r[2]:,} | {r[3]:,} | {r[4]} | {r[5]} | {r[6]} |")
    md.append("")

    # 본문(차트)
    for ent_kr, tbl, _ in entities:
        md.append(f"## {ent_kr}\n")
        for 기준_kr, dt_col, guard_null in 기준들:
            md.append(f"### {기준_kr} 기준\n")

            # total
            if guard_null:
                total_cnt = q1(ch, f"SELECT count() FROM {db}.{tbl} WHERE {dt_col} IS NOT NULL")
            else:
                total_cnt = q1(ch, f"SELECT count() FROM {db}.{tbl}")
            md.append(f"- 총 {기준_kr} 기준 집계 대상 행 수: **{int(total_cnt or 0):,}**\n")

            for period_kr, period_key in periods:
                df = build_time_series(ch, db, tbl, dt_col, period_key, include_null_guard=guard_null)
                fname = _safe_name(f"{ent_kr}__{기준_kr}__{period_kr}.png")
                out_png = os.path.join(CHART_DIR, fname)

                if not df.empty:
                    xlabel = period_kr.replace("별", "")
                    title = f"{ent_kr} · {기준_kr} · {period_kr} (구간별 + 누적)"
                    plot_bar_and_cumline(df, title=title, xlabel=xlabel, out_path=out_png)

                open_default = period_key in ("yearly", "monthly")
                md.append(f"<details{' open' if open_default else ''}>")
                md.append(f"<summary>{period_kr} (구간별 + 누적)</summary>\n")
                if df.empty:
                    md.append("- 표시할 데이터가 없습니다. (최근 구간 기준)\n")
                else:
                    md.append(f"![](./charts/{fname})\n")
                md.append("</details>\n")

    out_md = os.path.join(REPORT_DIR, "README.md")
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md).rstrip() + "\n")

    print(f"DONE. wrote {out_md} and charts under {CHART_DIR}")


if __name__ == "__main__":
    main()
