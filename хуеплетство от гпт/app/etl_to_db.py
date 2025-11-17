from __future__ import annotations
import os
import logging
import pandas as pd
import psycopg2
import psycopg2.extras
from af_client import AFClient

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("etl")

def safe_div(n, d):
    return float(n)/float(d) if d not in (0, None) and d != 0 else 0.0

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
    uri = os.getenv("POSTGRES_URI")
    if not uri:
        uri = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST','postgres')}:{os.getenv('PG_PORT','5432')}/{os.getenv('PG_DB')}"
    return psycopg2.connect(uri)

def insert_raw(conn, df: pd.DataFrame):
    if df.empty: return
    cols = ["event_date","app_id","app_name","platform","media_source","campaign","adset","ad","country",
            "impressions","clicks","installs","cost","revenue","d1_retained","d7_retained"]
    rows = [tuple(df.fillna({c:None})[c] for c in cols) for _, df in df.iterrows()]
    with conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, f"""
            INSERT INTO af_raw_installs ({",".join(cols)}) VALUES %s
        """, rows)

def upsert_metrics(conn, df: pd.DataFrame):
    if df.empty: return
    cols = ["event_date","app_id","media_source","campaign","country","impressions","clicks","installs",
            "cost","revenue","cpi","ctr","cvr","roas","arpu","d1_retention","d7_retention"]
    recs = [tuple(row[c] for c in cols) for _, row in df.iterrows()]
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
    token = os.getenv("AF_API_TOKEN")
    app_id = os.getenv("AF_APP_ID")
    date_from = os.getenv("AF_FROM_DATE")
    date_to = os.getenv("AF_TO_DATE")
    tz = os.getenv("AF_TIMEZONE", "UTC")
    if not all([token, app_id, date_from, date_to]):
        raise SystemExit("Set AF_API_TOKEN, AF_APP_ID, AF_FROM_DATE, AF_TO_DATE in .env")

    client = AFClient(api_token=token)
    csv_text = client.fetch_agg_daily_csv(app_id, date_from, date_to, tz)
    df = client.to_normalized_df(csv_text, app_id=app_id)

    with pg_conn() as conn:
        insert_raw(conn, df)
        metrics = compute_metrics(df)
        upsert_metrics(conn, metrics)
        log.info("Inserted raw=%d, metrics=%d", len(df), len(metrics))

if __name__ == "__main__":
    main()
