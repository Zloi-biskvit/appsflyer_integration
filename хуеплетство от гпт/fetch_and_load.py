import os
import io
import sys
import csv
import argparse
import logging
from datetime import datetime
from typing import Optional

import requests
import pandas as pd
import psycopg2
import psycopg2.extras
from dateutil.parser import isoparse

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s")
log = logging.getLogger("af-load")

def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    return v.strip() if isinstance(v, str) else v

AF_API_TOKEN = env("AF_API_TOKEN")
AF_APP_ID    = env("AF_APP_ID")
AF_TZ        = env("AF_TIMEZONE", "UTC")
PG_DSN       = env("PG_DSN", "host=localhost port=5432 dbname=af user=afuser password=afpass")

# Два популярных CSV-эндпоинта AF: партнёры по дням И/ИЛИ инсталлы.
ENDPOINTS = [
    "https://hq.appsflyer.com/export/{app_id}/partners_by_date_report/v5",
    "https://hq.appsflyer.com/export/{app_id}/installs_report/v5",
]

def fetch_csv(app_id: str, token: str, date_from: str, date_to: str, timezone: str) -> str:
    headers = {"accept": "text/csv", "Authorization": f"Bearer {token}"}
    params = {
        "from": date_from,
        "to": date_to,
        "timezone": timezone,
        "additional_fields": ",".join([
            "campaign","adset","ad","country","impressions","clicks","cost","revenue","media_source","install_time"
        ]),
    }
    last_err = None
    for tmpl in ENDPOINTS:
        url = tmpl.format(app_id=app_id)
        log.info("GET %s", url)
        try:
            r = requests.get(url, headers=headers, params=params, timeout=120)
            r.raise_for_status()
            if not r.text.strip():
                raise RuntimeError("Empty CSV")
            return r.text
        except Exception as e:
            last_err = e
            log.warning("Endpoint failed: %s -> %s", url, e)
    raise SystemExit(f"All endpoints failed. Last error: {last_err}")

def normalize(csv_text: str, app_id: str, app_name: str = "", platform: str = "") -> pd.DataFrame:
    # Пробуем прочитать CSV даже если разделители/кавычки необычные
    df = pd.read_csv(io.StringIO(csv_text))
    # Универсальные переименования
    ren = {
        "Date": "event_date", "date": "event_date", "Install Time": "install_time", "install_time": "install_time",
        "Media Source": "media_source", "media_source": "media_source",
        "Campaign": "campaign", "campaign": "campaign",
        "Adset": "adset", "adset": "adset",
        "Ad": "ad", "ad": "ad",
        "Country Code": "country", "Country": "country", "country": "country",
        "Impressions": "impressions", "impressions": "impressions",
        "Clicks": "clicks", "clicks": "clicks",
        "Installs": "installs", "installs": "installs",
        "Cost": "cost", "cost": "cost",
        "Revenue": "revenue", "af_revenue": "revenue"
    }
    have = {k: v for k, v in ren.items() if k in df.columns}
    df = df.rename(columns=have)

    # event_date: из Date или Install Time
    if "event_date" in df.columns:
        ed = pd.to_datetime(df["event_date"], errors="coerce")
    elif "install_time" in df.columns:
        ed = pd.to_datetime(df["install_time"], errors="coerce")
    else:
        ed = pd.Timestamp.utcnow()
    df["event_date"] = pd.to_datetime(ed, errors="coerce").dt.date

    # Заполним отсутствующие поля
    for c in ["media_source","campaign","adset","ad","country"]:
        if c not in df.columns: df[c] = ""
    for c in ["impressions","clicks","installs"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0).astype(int)
    for c in ["cost","revenue"]:
        df[c] = pd.to_numeric(df.get(c, 0.0), errors="coerce").fillna(0.0).astype(float)

    # retention если нет — нули
    for c in ["d1_retained","d7_retained"]:
        if c not in df.columns: df[c] = 0

    df["app_id"] = app_id
    df["app_name"] = app_name
    df["platform"] = platform

    keep = [
        "event_date","app_id","app_name","platform","media_source","campaign","adset","ad","country",
        "impressions","clicks","installs","cost","revenue","d1_retained","d7_retained"
    ]
    # оставляем только существующие
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()
    return df

def safe_div(n, d):
    try:
        n = float(n); d = float(d)
        return n/d if d else 0.0
    except Exception:
        return 0.0

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    keys = ["event_date","app_id","media_source","campaign","country"]
    g = df.groupby(keys, dropna=False, as_index=False).agg({
        "impressions":"sum","clicks":"sum","installs":"sum","cost":"sum","revenue":"sum",
        "d1_retained":"sum","d7_retained":"sum"
    })
    g["cpi"] = g.apply(lambda r: safe_div(r["cost"], r["installs"]), axis=1)
    g["ctr"] = g.apply(lambda r: safe_div(r["clicks"], r["impressions"]), axis=1)
    g["cvr"] = g.apply(lambda r: safe_div(r["installs"], r["clicks"]), axis=1)
    g["roas"] = g.apply(lambda r: safe_div(r["revenue"], r["cost"]), axis=1)
    g["arpu"] = g.apply(lambda r: safe_div(r["revenue"], r["installs"]), axis=1)
    g["d1_retention"] = g.apply(lambda r: safe_div(r["d1_retained"], r["installs"]), axis=1)
    g["d7_retention"] = g.apply(lambda r: safe_div(r["d7_retained"], r["installs"]), axis=1)
    return g

def pg_conn():
    return psycopg2.connect(PG_DSN)

def insert_raw(conn, df: pd.DataFrame):
    if df.empty: return
    cols = ["event_date","app_id","app_name","platform","media_source","campaign","adset","ad","country",
            "impressions","clicks","installs","cost","revenue","d1_retained","d7_retained"]
    # приведём недостающие колонки к None
    for c in cols:
        if c not in df.columns:
            df[c] = None
    rows = [tuple(df.loc[i, cols]) for i in df.index]
    with conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, f"""
            INSERT INTO af_raw_installs ({",".join(cols)}) VALUES %s
        """, rows)

def upsert_metrics(conn, df: pd.DataFrame):
    if df.empty: return
    cols = ["event_date","app_id","media_source","campaign","country","impressions","clicks","installs",
            "cost","revenue","cpi","ctr","cvr","roas","arpu","d1_retention","d7_retention"]
    recs = [tuple(df.loc[i, cols]) for i in df.index]
    with conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, f"""
        INSERT INTO af_metrics_daily ({",".join(cols)}) VALUES %s
        ON CONFLICT (event_date, app_id, media_source, campaign, country) DO UPDATE SET
          impressions=EXCLUDED.impressions,
          clicks=EXCLUDED.clicks,
          installs=EXCLUDED.installs,
          cost=EXCLUDED.cost,
          revenue=EXCLUDED.revenue,
          cpi=EXCLUDED.cpi,
          ctr=EXCLUDED.ctr,
          cvr=EXCLUDED.cvr,
          roas=EXCLUDED.roas,
          arpu=EXCLUDED.arpu,
          d1_retention=EXCLUDED.d1_retention,
          d7_retention=EXCLUDED.d7_retention
        """, recs)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    p.add_argument("--app-id", dest="app_id", default=AF_APP_ID)
    p.add_argument("--token", dest="token", default=AF_API_TOKEN)
    p.add_argument("--tz", dest="tz", default=AF_TZ)
    args = p.parse_args()

    if not args.token or not args.app_id:
        sys.exit("Set AF_API_TOKEN and AF_APP_ID (env or args)")

    # валидация дат
    try:
        _ = isoparse(args.date_from); _ = isoparse(args.date_to)
    except Exception:
        sys.exit("Dates must be in format YYYY-MM-DD")

    csv_text = fetch_csv(args.app_id, args.token, args.date_from, args.date_to, args.tz)
    df = normalize(csv_text, app_id=args.app_id)

    with pg_conn() as conn:
        insert_raw(conn, df)
        metrics = compute_metrics(df)
        upsert_metrics(conn, metrics)

    log.info("Loaded: raw=%d rows, metrics=%d rows", len(df), len(metrics))

if __name__ == "__main__":
    main()
