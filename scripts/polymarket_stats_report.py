#!/usr/bin/env python3
"""
Polymarket stats report (ClickHouse) -> artifacts.

Creates chart-heavy HTML (+ JSON/CSV) reports for:
- created_at_utc-based distribution (when the entity was created on Polymarket)
- collected_at_utc-based distribution (when our crawler last collected the latest snapshot row)

Granularity:
- total / yearly / monthly / daily / hourly
- For dense granularities we only show recent windows and hide long sections in <details>.

Outputs:
- artifacts/polymarket_stats_report.json
- artifacts/polymarket_stats_report.md
- artifacts/polymarket_stats_report.html
- artifacts/charts/*.png
- artifacts/data/*.csv
"""

import os, json
from datetime import datetime, timezone

import pandas as pd

# Headless backend for GitHub Actions
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import scripts.polymarket_crawl_to_clickhouse as pm


def _dt_to_iso(dt):
    if dt is None:
        return None
    try:
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return str(dt)


def q1(ch, sql):
    r = ch.query(sql)
    return r.result_rows[0][0] if r.result_rows else None


def q_df(ch, sql, cols):
    r = ch.query(sql)
    return pd.DataFrame(r.result_rows, columns=cols)


def _bucket_sql(dt_col: str, period: str) -> str:
    # dt_col must be DateTime64 / DateTime; period: hourly/daily/monthly/yearly
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
    # Keep charts readable
    if period == "hourly":
        return f"{dt_col} >= now() - INTERVAL 72 HOUR"
    if period == "daily":
        return f"{dt_col} >= now() - INTERVAL 120 DAY"
    if period == "monthly":
        return f"{dt_col} >= now() - INTERVAL 48 MONTH"
    if period == "yearly":
        return "1"
    return "1"


def _plot_series(df: pd.DataFrame, title: str, out_path: str):
    if df.empty:
        return False
    x = df["bucket"]
    y = df["count"]
    plt.figure(figsize=(12, 4))
    plt.plot(x, y, marker="o", linewidth=1)
    plt.title(title)
    plt.xlabel("bucket")
    plt.ylabel("count")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return True


def _plot_cumsum(df: pd.DataFrame, title: str, out_path: str):
    if df.empty:
        return False
    d2 = df.copy()
    d2["cumulative"] = d2["count"].cumsum()
    x = d2["bucket"]
    y = d2["cumulative"]
    plt.figure(figsize=(12, 4))
    plt.plot(x, y, marker="o", linewidth=1)
    plt.title(title)
    plt.xlabel("bucket")
    plt.ylabel("cumulative")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return True


def _write_csv(df: pd.DataFrame, out_path: str):
    df.to_csv(out_path, index=False, encoding="utf-8")


def build_time_series(ch, db, tbl, dt_col, period, allow_null=False):
    bucket = _bucket_sql(dt_col, period)
    where_recent = _recent_where(dt_col, period)

    where_parts = []
    if allow_null:
        where_parts.append(f"{dt_col} IS NOT NULL")
    where_parts.append(where_recent)
    where_clause = " AND ".join(where_parts) if where_parts else "1"

    sql = f"""
    SELECT
        {bucket} AS bucket,
        count() AS count
    FROM {db}.{tbl}
    WHERE {where_clause}
    GROUP BY bucket
    ORDER BY bucket
    """
    df = q_df(ch, sql, ["bucket", "count"])
    # Normalize types for CSV/plot
    df["bucket"] = pd.to_datetime(df["bucket"])
    df["count"] = df["count"].astype(int)
    return df


def main():
    ch = pm.get_ch_client()
    os.makedirs("artifacts", exist_ok=True)
    os.makedirs("artifacts/charts", exist_ok=True)
    os.makedirs("artifacts/data", exist_ok=True)

    db = pm.CH_DB
    raw = pm.RAW_TABLE
    ev = pm.EVENT_TABLE
    mk = pm.MARKET_TABLE
    se = pm.SERIES_TABLE

    run_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    report = {
        "run_at_utc": run_at,
        "db": db,
        "tables": {},
        "notes": [
            "created_at_utc = Polymarket object creation time (nullable).",
            "collected_at_utc = crawler collection time for the latest snapshot row (tables keep latest 1 row per id by ReplacingMergeTree).",
        ],
    }

    entities = [
        ("series", se, "series_id"),
        ("event", ev, "event_id"),
        ("market", mk, "market_id"),
    ]

    # Basic coverage
    for name, tbl, id_col in entities:
        report["tables"][name] = {
            "rows": q1(ch, f"SELECT count() FROM {db}.{tbl}"),
            "uniq_ids": q1(ch, f"SELECT uniqExact({id_col}) FROM {db}.{tbl}"),
            "created_at_null_rows": q1(ch, f"SELECT countIf(created_at_utc IS NULL) FROM {db}.{tbl}"),
            "min_created_at_utc": _dt_to_iso(q1(ch, f"SELECT min(created_at_utc) FROM {db}.{tbl}")),
            "max_effective_updated_at_utc": _dt_to_iso(q1(ch, f"SELECT max(ifNull(updated_at_utc, collected_at_utc)) FROM {db}.{tbl}")),
            "max_collected_at_utc": _dt_to_iso(q1(ch, f"SELECT max(collected_at_utc) FROM {db}.{tbl}")),
            "uniq_ids_updated_last_24h": q1(
                ch,
                f"""SELECT uniqExact({id_col})
                    FROM {db}.{tbl}
                    WHERE ifNull(updated_at_utc, collected_at_utc) >= now() - INTERVAL 24 HOUR"""
            ),
            "time_series": {
                "by_created_at_utc": {},
                "by_collected_at_utc": {},
            }
        }

    # RAW quick stats
    report["tables"]["raw"] = {
        "rows": q1(ch, f"SELECT count() FROM {db}.{raw}"),
        "by_entity": {
            "series": q1(ch, f"SELECT count() FROM {db}.{raw} WHERE entity='series'"),
            "event": q1(ch, f"SELECT count() FROM {db}.{raw} WHERE entity='event'"),
            "market": q1(ch, f"SELECT count() FROM {db}.{raw} WHERE entity='market'"),
        },
        "max_collected_at_utc": _dt_to_iso(q1(ch, f"SELECT max(collected_at_utc) FROM {db}.{raw}")),
    }

    periods = ["yearly", "monthly", "daily", "hourly"]

    # Build time series data + charts
    html_sections = []

    def add_entity_section(name, tbl):
        html_sections.append(f"<h2>{name}</h2>")

        # Created-at
        html_sections.append(f"<h3>Created at (created_at_utc)</h3>")
        for period in periods:
            df = build_time_series(ch, db, tbl, "created_at_utc", period, allow_null=True)
            data_csv = f"artifacts/data/{name}__created_at__{period}.csv"
            _write_csv(df, data_csv)

            img1 = f"artifacts/charts/{name}__created_at__{period}.png"
            img2 = f"artifacts/charts/{name}__created_at__{period}__cumsum.png"
            ok1 = _plot_series(df, f"{name} created_at_utc ({period})", img1)
            ok2 = _plot_cumsum(df, f"{name} created_at_utc cumulative ({period})", img2)

            report["tables"][name]["time_series"]["by_created_at_utc"][period] = {
                "csv": os.path.basename(data_csv),
                "points": int(len(df)),
            }

            # Collapse dense sections
            open_attr = " open" if period in ("yearly", "monthly") else ""
            html_sections.append(f"<details{open_attr}><summary>{period}</summary>")
            if df.empty:
                html_sections.append("<p>(no data in window)</p>")
            else:
                if ok1:
                    html_sections.append(f'<p><img src="charts/{os.path.basename(img1)}" style="max-width:100%;"></p>')
                if ok2:
                    html_sections.append(f'<p><img src="charts/{os.path.basename(img2)}" style="max-width:100%;"></p>')
                html_sections.append(f'<p><a href="data/{os.path.basename(data_csv)}">download csv</a></p>')
            html_sections.append("</details>")

        # Collected-at
        html_sections.append(f"<h3>Crawled at (collected_at_utc)</h3>")
        for period in periods:
            df = build_time_series(ch, db, tbl, "collected_at_utc", period, allow_null=False)
            data_csv = f"artifacts/data/{name}__collected_at__{period}.csv"
            _write_csv(df, data_csv)

            img1 = f"artifacts/charts/{name}__collected_at__{period}.png"
            img2 = f"artifacts/charts/{name}__collected_at__{period}__cumsum.png"
            ok1 = _plot_series(df, f"{name} collected_at_utc ({period})", img1)
            ok2 = _plot_cumsum(df, f"{name} collected_at_utc cumulative ({period})", img2)

            report["tables"][name]["time_series"]["by_collected_at_utc"][period] = {
                "csv": os.path.basename(data_csv),
                "points": int(len(df)),
            }

            open_attr = " open" if period in ("yearly", "monthly") else ""
            html_sections.append(f"<details{open_attr}><summary>{period}</summary>")
            if df.empty:
                html_sections.append("<p>(no data in window)</p>")
            else:
                if ok1:
                    html_sections.append(f'<p><img src="charts/{os.path.basename(img1)}" style="max-width:100%;"></p>')
                if ok2:
                    html_sections.append(f'<p><img src="charts/{os.path.basename(img2)}" style="max-width:100%;"></p>')
                html_sections.append(f'<p><a href="data/{os.path.basename(data_csv)}">download csv</a></p>')
            html_sections.append("</details>")

    for name, tbl, _ in entities:
        add_entity_section(name, tbl)

    # Write JSON
    json_path = os.path.join("artifacts", "polymarket_stats_report.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # Write Markdown (compact)
    md_lines = []
    md_lines.append("# Polymarket Stats Report\n")
    md_lines.append(f"- Run at (UTC): {run_at}\n")
    md_lines.append(f"- Database: `{db}`\n")
    md_lines.append("## Summary\n")
    for name, tbl, _ in entities:
        v = report["tables"][name]
        md_lines.append(f"### {name}\n")
        md_lines.append(f"- rows: {v['rows']}\n")
        md_lines.append(f"- uniq_ids: {v['uniq_ids']}\n")
        md_lines.append(f"- created_at_null_rows: {v['created_at_null_rows']}\n")
        md_lines.append(f"- min_created_at_utc: {v['min_created_at_utc']}\n")
        md_lines.append(f"- max_effective_updated_at_utc: {v['max_effective_updated_at_utc']}\n")
        md_lines.append(f"- max_collected_at_utc: {v['max_collected_at_utc']}\n")
        md_lines.append(f"- uniq_ids_updated_last_24h: {v['uniq_ids_updated_last_24h']}\n\n")
    md_lines.append("### raw\n")
    md_lines.append(f"- rows: {report['tables']['raw']['rows']}\n")
    md_lines.append(f"- max_collected_at_utc: {report['tables']['raw']['max_collected_at_utc']}\n")
    md_lines.append(f"- by_entity: {report['tables']['raw']['by_entity']}\n")

    md_path = os.path.join("artifacts", "polymarket_stats_report.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    # Write HTML (chart-heavy)
    html = []
    html.append("<!doctype html><html><head><meta charset='utf-8'>")
    html.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
    html.append("<title>Polymarket Stats Report</title>")
    html.append("<style>body{font-family:system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin:16px; line-height:1.45} details{margin:10px 0} summary{cursor:pointer; font-weight:600}</style>")
    html.append("</head><body>")
    html.append("<h1>Polymarket Stats Report</h1>")
    html.append(f"<p><b>Run at (UTC)</b>: {run_at}<br><b>DB</b>: <code>{db}</code></p>")
    html.append("<p><b>Downloads:</b> <a href='polymarket_stats_report.json'>json</a> · <a href='polymarket_stats_report.md'>md</a></p>")
    html.append("<details open><summary>Notes</summary><ul>")
    for n in report["notes"]:
        html.append(f"<li>{n}</li>")
    html.append("</ul></details>")
    html.extend(html_sections)
    html.append("</body></html>")

    html_path = os.path.join("artifacts", "polymarket_stats_report.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html))

    print(f"DONE. wrote {json_path}, {md_path}, {html_path} + charts/data")


if __name__ == "__main__":
    main()
