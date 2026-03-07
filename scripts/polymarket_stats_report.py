#!/usr/bin/env python3
"""polymarket_stats_report

Polymarket 공개 데이터 흐름 관점의 통계 리포트 생성 스크립트.

산출물(커밋 대상):
- reports/polymarket_stats/README.md
- reports/polymarket_stats/charts/*.png

통계 기준(2종):
1) 생성 시점: Polymarket에서 각 항목이 처음 만들어진 시점을 기준으로 집계
2) 반영 시점: 데이터 흐름에 각 항목이 최근 반영된 시점을 기준으로 집계

주기:
- 전체 / 연도별 / 월별 / 일별 / 시간별

차트:
- "구간별(막대) + 누적(라인)"을 한 장의 PNG에 동시에 표시 (이중 Y축)

문자(한글) 폰트:
- GitHub Actions(ubuntu-latest)에서는 fonts-noto-cjk 설치를 전제로 "Noto Sans CJK KR" 사용
- 폰트가 없으면 자동으로 영어 라벨로 fallback
"""
from __future__ import annotations

import os
from datetime import timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import font_manager

import scripts.polymarket_crawl_to_clickhouse as pm


# ----------------------
# Korean font handling
# ----------------------
def _has_font(name: str) -> bool:
    try:
        for f in font_manager.fontManager.ttflist:
            if f.name == name:
                return True
    except Exception:
        return False
    return False


def setup_fonts():
    """
    Try to enable Korean labels. If not possible, fallback to English.
    Returns: use_korean(bool)
    """
    preferred = ["Noto Sans CJK KR", "Noto Sans KR", "AppleGothic", "NanumGothic"]
    for name in preferred:
        if _has_font(name):
            plt.rcParams["font.family"] = name
            plt.rcParams["axes.unicode_minus"] = False
            return True
    # fallback (avoid broken squares)
    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.rcParams["axes.unicode_minus"] = False
    return False


# ----------------------
# Data store helpers
# ----------------------
def q_df(ch, sql: str, cols):
    r = ch.query(sql)
    return pd.DataFrame(r.result_rows, columns=cols)


def _bucket_expr(dt_col: str, period: str) -> str:
    if period == "hourly":
        return f"toStartOfHour({dt_col})"
    if period == "daily":
        return f"toDate({dt_col})"
    if period == "monthly":
        return f"toStartOfMonth({dt_col})"
    if period == "yearly":
        return f"toStartOfYear({dt_col})"
    raise ValueError(period)


def build_created_series(ch, db: str, tbl: str, id_col: str, created_col: str, period: str) -> pd.DataFrame:
    """
    Creation-time series:
    - bucket: period bucket of the original created_at_utc
    - cnt: uniqExact(id_col) per bucket
    """
    bucket = _bucket_expr(created_col, period)
    sql = f"""
    SELECT
        {bucket} AS bucket,
        uniqExact({id_col}) AS cnt
    FROM {db}.{tbl}
    WHERE {created_col} IS NOT NULL
    GROUP BY bucket
    ORDER BY bucket
    """
    df = q_df(ch, sql, ["bucket", "cnt"])
    if df.empty:
        return df
    df["bucket"] = pd.to_datetime(df["bucket"], utc=True, errors="coerce")
    df["cnt"] = df["cnt"].astype(int)
    df = df.sort_values("bucket")
    df["cum"] = df["cnt"].cumsum()
    return df


def build_latest_collected_series(ch, db: str, tbl: str, id_col: str, collected_col: str, period: str) -> pd.DataFrame:
    """
    Reflection-time series:
    - latest_collected_at is max(collected_at_utc) per id
    - bucket: period bucket of latest_collected_at
    - cnt: count() per bucket (1 row per id after subquery)
    """
    bucket = _bucket_expr("latest_collected_at", period)
    sql = f"""
    SELECT
        {bucket} AS bucket,
        count() AS cnt
    FROM
    (
        SELECT
            {id_col} AS entity_id,
            max({collected_col}) AS latest_collected_at
        FROM {db}.{tbl}
        WHERE {collected_col} IS NOT NULL
        GROUP BY entity_id
    )
    GROUP BY bucket
    ORDER BY bucket
    """
    df = q_df(ch, sql, ["bucket", "cnt"])
    if df.empty:
        return df
    df["bucket"] = pd.to_datetime(df["bucket"], utc=True, errors="coerce")
    df["cnt"] = df["cnt"].astype(int)
    df = df.sort_values("bucket")
    df["cum"] = df["cnt"].cumsum()
    return df


def latest_collected_minmax(ch, db: str, tbl: str, id_col: str, collected_col: str):
    sql = f"""
    SELECT
        min(latest_collected_at) AS min_collected,
        max(latest_collected_at) AS max_collected
    FROM
    (
        SELECT
            {id_col} AS entity_id,
            max({collected_col}) AS latest_collected_at
        FROM {db}.{tbl}
        WHERE {collected_col} IS NOT NULL
        GROUP BY entity_id
    )
    """
    r = ch.query(sql).result_rows[0]
    return r[0], r[1]


# ----------------------
# Plotting
# ----------------------
def plot_bar_and_cum(df: pd.DataFrame, title: str, xlabel: str, out_path: str, use_korean: bool) -> bool:
    if df is None or df.empty:
        return False

    x = df["bucket"]
    y = df["cnt"]
    c = df["cum"]

    # For very dense series, avoid markers
    dense = len(df) > 400

    fig, ax1 = plt.subplots(figsize=(12.5, 4.2))
    ax1.bar(x, y)
    ax1.set_ylabel("구간별 수" if use_korean else "Bucket count")
    ax1.set_xlabel(xlabel)
    ax1.tick_params(axis="x", labelrotation=45)

    ax2 = ax1.twinx()
    ax2.plot(x, c, linewidth=1.2, marker=None if dense else "o")
    ax2.set_ylabel("누적" if use_korean else "Cumulative")

    ax1.set_title(title)
    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    return True


# ----------------------
# Markdown helpers
# ----------------------
def _fmt_bucket(period: str, dt: pd.Timestamp) -> str:
    if pd.isna(dt):
        return ""
    dt = dt.to_pydatetime().astimezone(timezone.utc)
    if period == "yearly":
        return dt.strftime("%Y")
    if period == "monthly":
        return dt.strftime("%Y-%m")
    if period == "daily":
        return dt.strftime("%Y-%m-%d")
    if period == "hourly":
        return dt.strftime("%Y-%m-%d %H:00")
    return dt.isoformat().replace("+00:00", "Z")


def write_report(md_path: str, use_korean: bool, sections: list):
    title = "Polymarket 통계 리포트"
    subtitle = "생성 시점 / 최근 반영 시점 기준"
    note_kr = (
        "본 문서는 Polymarket의 공개 데이터를 기준으로 집계한 결과입니다.\n"
        "생성 시점은 원본 createdAt, 반영 시점은 각 id의 최신 collected_at_utc를 기준으로 계산합니다.\n"
        "내부 저장/구현 방식과 무관하게, 공개 데이터 흐름 관점에서 정리된 통계만 제공합니다.\n"
    )
    note_en = (
        "This document aggregates public Polymarket data.\n"
        "Creation time uses the original createdAt, and reflection time uses the latest collected_at_utc per id.\n"
        "It intentionally avoids internal storage/implementation details.\n"
    )

    with open(md_path, "w", encoding="utf-8") as f:
        if use_korean:
            f.write(f"# {title}\n\n{subtitle}\n\n{note_kr}\n")
        else:
            f.write("# Polymarket Statistics Report\n\nBased on created/reflected time.\n\n" + note_en + "\n")

        for sec in sections:
            f.write(sec)
            if not sec.endswith("\n"):
                f.write("\n")
            f.write("\n")


# ----------------------
# Main
# ----------------------
def main():
    use_korean = setup_fonts()

    db = os.environ.get("CLICKHOUSE_DATABASE", "statground_polymarket")
    out_dir = os.path.join("reports", "polymarket_stats")
    charts_dir = os.path.join(out_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    ch = pm.get_ch_client()

    entities = [
        {
            "key": "series",
            "kr": "시리즈",
            "en": "Series",
            "table": "polymarket_series",
            "id_col": "series_id",
            "created": "created_at_utc",
            "collected": "collected_at_utc",
        },
        {
            "key": "event",
            "kr": "이벤트",
            "en": "Event",
            "table": "polymarket_event",
            "id_col": "event_id",
            "created": "created_at_utc",
            "collected": "collected_at_utc",
        },
        {
            "key": "market",
            "kr": "마켓",
            "en": "Market",
            "table": "polymarket_market",
            "id_col": "market_id",
            "created": "created_at_utc",
            "collected": "collected_at_utc",
        },
    ]

    periods = [
        ("yearly", "연도별" if use_korean else "Yearly", "연도" if use_korean else "Year"),
        ("monthly", "월별" if use_korean else "Monthly", "월" if use_korean else "Month"),
        ("daily", "일별" if use_korean else "Daily", "일" if use_korean else "Day"),
        ("hourly", "시간별" if use_korean else "Hourly", "시간" if use_korean else "Hour"),
    ]

    sections = []

    # Overview / totals
    for e in entities:
        label = e["kr"] if use_korean else e["en"]
        tbl = e["table"]

        sql_total = f"SELECT uniqExact({e['id_col']}) FROM {db}.{tbl}"
        total = ch.query(sql_total).result_rows[0][0]

        sql_created_minmax = f"""
        SELECT
            minIf({e['created']}, {e['created']} IS NOT NULL) AS min_created,
            maxIf({e['created']}, {e['created']} IS NOT NULL) AS max_created
        FROM {db}.{tbl}
        """
        min_created, max_created = ch.query(sql_created_minmax).result_rows[0]
        min_collected, max_collected = latest_collected_minmax(ch, db, tbl, e["id_col"], e["collected"])

        def fmt(dt):
            if dt is None:
                return "-"
            return str(pd.to_datetime(dt, utc=True))

        if use_korean:
            sec = (
                f"## {label}\n\n"
                f"- 전체 고유 개수: **{int(total):,}**\n"
                f"- 생성 시점 범위: {fmt(min_created)} ~ {fmt(max_created)}\n"
                f"- 최근 반영 시점 범위: {fmt(min_collected)} ~ {fmt(max_collected)}\n"
            )
        else:
            sec = (
                f"## {label}\n\n"
                f"- Total unique count: **{int(total):,}**\n"
                f"- Created time range: {fmt(min_created)} ~ {fmt(max_created)}\n"
                f"- Latest reflected time range: {fmt(min_collected)} ~ {fmt(max_collected)}\n"
            )
        sections.append(sec)

    # Charts + top buckets
    for e in entities:
        label = e["kr"] if use_korean else e["en"]
        tbl = e["table"]

        for period_key, period_label, xlabel in periods:
            # Created-time stats
            df_created = build_created_series(ch, db, tbl, e["id_col"], e["created"], period_key)
            created_slug = f"{e['key']}_created_{period_key}.png"
            created_path = os.path.join(charts_dir, created_slug)
            created_title = f"{label} / {'생성 시점' if use_korean else 'Created time'} / {period_label}"
            plot_bar_and_cum(df_created, created_title, xlabel, created_path, use_korean)

            if df_created is not None and not df_created.empty:
                top_created = df_created.sort_values("cnt", ascending=False).head(5)
                rows = "\n".join(
                    [f"  - {_fmt_bucket(period_key, r.bucket)}: {int(r.cnt):,}" for r in top_created.itertuples(index=False)]
                )
                sec = (
                    f"### {label} / {'생성 시점' if use_korean else 'Created time'} / {period_label}\n\n"
                    f"![](charts/{created_slug})\n\n"
                    f"{'상위 구간' if use_korean else 'Top buckets'}:\n{rows}\n"
                )
                sections.append(sec)

            # Latest-collected-time stats
            df_collected = build_latest_collected_series(ch, db, tbl, e["id_col"], e["collected"], period_key)
            collected_slug = f"{e['key']}_collected_{period_key}.png"
            collected_path = os.path.join(charts_dir, collected_slug)
            collected_title = f"{label} / {'최근 반영 시점' if use_korean else 'Latest reflected time'} / {period_label}"
            plot_bar_and_cum(df_collected, collected_title, xlabel, collected_path, use_korean)

            if df_collected is not None and not df_collected.empty:
                top_collected = df_collected.sort_values("cnt", ascending=False).head(5)
                rows = "\n".join(
                    [f"  - {_fmt_bucket(period_key, r.bucket)}: {int(r.cnt):,}" for r in top_collected.itertuples(index=False)]
                )
                sec = (
                    f"### {label} / {'최근 반영 시점' if use_korean else 'Latest reflected time'} / {period_label}\n\n"
                    f"![](charts/{collected_slug})\n\n"
                    f"{'상위 구간' if use_korean else 'Top buckets'}:\n{rows}\n"
                )
                sections.append(sec)

    md_path = os.path.join(out_dir, "README.md")
    write_report(md_path, use_korean, sections)
    print(f"DONE. report={md_path}")


if __name__ == "__main__":
    main()
