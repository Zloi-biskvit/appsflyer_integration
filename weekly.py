#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, Dict, List

import pandas as pd

# === Настройки ===
INPUT_DIRS = [Path("./report"), Path(".")]   # папки с daily_report*.csv
OUT_XLSX = Path("./summary_readable.xlsx")   # читаемый Excel
OUT_CSV  = Path("./summary.csv")             # сводный CSV (по желанию)

# === Вспомогательные ===
def parse_filename(p: Path) -> Tuple[Optional[datetime], Optional[datetime], Optional[str]]:
    m = re.match(r"^daily_report(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(.+)\.csv$", p.name)
    if not m:
        return None, None, None
    d1 = datetime.strptime(m.group(1), "%Y-%m-%d").date()
    d2 = datetime.strptime(m.group(2), "%Y-%m-%d").date()
    return d1, d2, m.group(3)

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    col_map = {
        "Date": "date",
        "Agency/PMD (af_prt)": "agency",
        "Media Source (pid)": "media_source",
        "Campaign (c)": "campaign",
        "Impressions": "impressions",
        "Clicks": "clicks",
        "CTR": "ctr",
        "Installs": "installs",
        "Conversion Rate": "cr",
        "Sessions": "sessions",
        "Loyal Users": "loyal_users",
        "Loyal Users/Installs": "loyal_ratio",
        "Total Cost": "total_cost",
        "Average eCPI": "ecpi"
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for c in ["impressions","clicks","ctr","installs","cr","sessions","loyal_users","loyal_ratio","total_cost","ecpi"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "media_source" in df.columns:
        df["media_source"] = df["media_source"].fillna("Organic")
    if "campaign" in df.columns:
        df["campaign"] = df["campaign"].where(df["campaign"].notna(), None)
    return df

def summarize_week(df: pd.DataFrame) -> Dict[str, float]:
    installs = int(df.get("installs", pd.Series(dtype=float)).sum())
    loyal_users = int(df.get("loyal_users", pd.Series(dtype=float)).sum())
    total_cost = float(df.get("total_cost", pd.Series(dtype=float)).sum())
    ecpi = (total_cost / installs) if installs and total_cost else None
    loyal_ratio = (100 * loyal_users / installs) if installs else None
    clicks = float(df.get("clicks", pd.Series(dtype=float)).sum())
    impressions = float(df.get("impressions", pd.Series(dtype=float)).sum())
    ctr_pct = (100 * clicks / impressions) if impressions else None
    cr_pct = (100 * installs / clicks) if clicks else None
    sessions = int(df.get("sessions", pd.Series(dtype=float)).sum())
    return {
        "installs": installs,
        "loyal_users": loyal_users,
        "loyal_ratio_pct": round(loyal_ratio, 2) if loyal_ratio is not None else None,
        "total_cost": round(total_cost, 2) if total_cost else None,
        "ecpi": round(ecpi, 4) if ecpi else None,
        "impressions": int(impressions) if impressions else None,
        "clicks": int(clicks) if clicks else None,
        "ctr_pct": round(ctr_pct, 2) if ctr_pct is not None else None,
        "cr_pct": round(cr_pct, 2) if cr_pct is not None else None,
        "sessions": sessions,
    }

def compute_summary(csv_files: List[Path]) -> pd.DataFrame:
    rows, index_map = [], {}
    for p in csv_files:
        d1, d2, app = parse_filename(p)
        if not app:
            continue
        df = normalize_df(pd.read_csv(p))
        stats = summarize_week(df)
        rows.append({
            "app": app, "start_date": d1, "end_date": d2, **stats, "source_file": p.name
        })
        index_map[(app, d2)] = stats["installs"]
    summary = pd.DataFrame(rows).sort_values(["app","end_date"]).reset_index(drop=True)
    prev_vals = []
    for _, r in summary.iterrows():
        app, end_d = r["app"], r["end_date"]
        prev_end = end_d - timedelta(days=7) if pd.notna(end_d) else None
        prev_inst = index_map.get((app, prev_end))
        w2w_pct = round(100.0 * (r["installs"] - prev_inst) / prev_inst, 2) if prev_inst else None
        prev_vals.append((prev_inst, w2w_pct))
    summary["prev_installs"] = [v[0] for v in prev_vals]
    summary["w2w_pct"]       = [v[1] for v in prev_vals]
    # порядок колонок
    summary = summary[
        ["app","start_date","end_date","installs","prev_installs","w2w_pct",
         "loyal_users","loyal_ratio_pct","total_cost","ecpi",
         "impressions","clicks","ctr_pct","cr_pct","sessions","source_file"]
    ]
    return summary

def main():
    csvs = []
    for d in INPUT_DIRS:
        if d.exists():
            csvs += list(d.glob("daily_report*.csv"))
    if not csvs:
        print("Файлы daily_report*.csv не найдены.")
        return
    summary = compute_summary(csvs)
    summary.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
        summary.to_excel(w, index=False, sheet_name="Summary")
    # Краткий вывод
    for _, r in summary.sort_values(["app","end_date"]).iterrows():
        print(f"{r['app']} {r['start_date']}–{r['end_date']} | installs={r['installs']} | prev={r['prev_installs']} | w2w={r['w2w_pct']}%")
    print(f"\nOK: summary -> {OUT_CSV} и {OUT_XLSX}")

if __name__ == "__main__":
    main()
