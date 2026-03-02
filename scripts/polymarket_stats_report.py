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

import os
from datetime import timezone

import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
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


def build_series(ch, db: str, tbl: str, id_col: str, dt_col: str, period: str) -> pd.DataFrame:
    """
    Full-period time series:
    - bucket: period bucket
    - cnt: uniqExact(id_col) per bucket
    """
    bucket = _bucket_expr(dt_col, period)
    sql = f"""
    SELECT
        {bucket} AS bucket,
        uniqExact({id_col}) AS cnt
    FROM {db}.{tbl}
    WHERE {dt_col} IS NOT NULL
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


# ----------------------
# Plotting
# ----------------------
def plot_bar_and_cum(df: pd.DataFrame, title: str, xlabel: str, out_path: str) -> bool:
    if df is None or df.empty:
        return False

    x = df["bucket"]
    y = df["cnt"]
    c = df["cum"]

    # For very dense series, avoid markers
    dense = len(df) > 400

    fig, ax1 = plt.subplots(figsize=(12.5, 4.2))
    ax1.bar(x, y)
    ax1.set_ylabel("구간별 수" if " " in title or "월" in title or "일" in title or "시간" in title else ("구간별 수"))
    ax1.set_xlabel(xlabel)
    ax1.tick_params(axis="x", labelrotation=45)

    ax2 = ax1.twinx()
    ax2.plot(x, c, linewidth=1.2, marker=None if dense else "o")
    ax2.set_ylabel("누적" if "누적" in title else "누적")

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
    subtitle = "생성 시점 / 반영 시점 기준"
    note_kr = (
        "본 문서는 Polymarket의 공개 데이터를 기준으로 집계한 결과입니다.\n"
        "내부 저장/구현 방식과 무관하게, 공개 데이터 흐름 관점에서 정리된 통계만 제공합니다.\n"
    )
    note_en = (
        "This document aggregates public Polymarket data.\n"
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

        sql_minmax = f"""
        SELECT
            minIf({e['created']}, {e['created']} IS NOT NULL) AS min_created,
            maxIf({e['created']}, {e['created']} IS NOT NULL) AS max_created,
            minIf({e['collected']}, {e['collected']} IS NOT NULL) AS min_collected,
            maxIf({e['collected']}, {e['collected']} IS NOT NULL) AS max_collected
        FROM {db}.{tbl}
        """
        r = ch.query(sql_minmax).result_rows[0]
        min_created, max_created, min_collected, max_collected = r

        def fmt(dt):
            if dt is None:
                return "-"
            try:
                return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            except Exception:
                return str(dt)

        if use_korean:
            sec = f"## {label}\n\n- 총 {label} 수(고유 ID): **{int(total)}**\n- 생성 시점 범위: {fmt(min_created)} ~ {fmt(max_created)}\n- 수집 시점 범위: {fmt(min_collected)} ~ {fmt(max_collected)}\n\n"
        else:
            sec = f"## {label}\n\n- Total unique IDs: **{int(total)}**\n- Created range: {fmt(min_created)} ~ {fmt(max_created)}\n- Collected range: {fmt(min_collected)} ~ {fmt(max_collected)}\n\n"
        sections.append(sec)

        # Two bases: created vs collected
        bases = [
            ("created", e["created"], "생성 시점" if use_korean else "Created time"),
            ("collected", e["collected"], "수집 시점" if use_korean else "Collected time"),
        ]

        for base_key, dt_col, base_label in bases:
            if use_korean:
                sections.append(f"### {base_label} 기준\n")
            else:
                sections.append(f"### {base_label}\n")

            for period_key, period_label, xlabel in periods:
                df = build_series(ch, db, tbl, e["id_col"], dt_col, period_key)

                chart_name = f"{e['key']}_{base_key}_{period_key}.png"
                chart_path = os.path.join(charts_dir, chart_name)

                # Titles
                if use_korean:
                    title = f"{label} · {base_label} · {period_label} (구간별 + 누적)"
                else:
                    title = f"{label} · {base_label} · {period_label} (Bucket + Cumulative)"

                ok = plot_bar_and_cum(df, title=title, xlabel=xlabel, out_path=chart_path)

                # Markdown section (fold long ones)
                if use_korean:
                    subhead = f"#### {period_label}\n"
                else:
                    subhead = f"#### {period_label}\n"

                if not ok:
                    sections.append(subhead + "\n(데이터 없음)\n\n")
                    continue

                rel_chart = f"charts/{chart_name}"
                sections.append(subhead + f"\n![]({rel_chart})\n\n")

    md_path = os.path.join(out_dir, "README.md")
    write_report(md_path, use_korean, sections)


if __name__ == "__main__":
    main()
