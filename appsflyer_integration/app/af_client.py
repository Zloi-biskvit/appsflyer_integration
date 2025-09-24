from __future__ import annotations
import os
import io
import logging
import requests
import pandas as pd

log = logging.getLogger("af-client")

class AFError(Exception): ...

class AFClient:
    BASE = os.getenv("AF_BASE_URL", "https://hq.appsflyer.com")

    def __init__(self, api_token: str):
        if not api_token:
            raise AFError("AF API token is empty")
        self.token = api_token

    def fetch_agg_daily_csv(self, app_id: str, date_from: str, date_to: str, timezone: str = "UTC") -> str:
        """
        Тянем агрегированный DAILY отчёт (v5) как CSV.
        Важно: endpoint/поля могут различаться по настройкам аккаунта AF.
        """
        url = f"{self.BASE}/export/{app_id}/partners_by_date_report/v5"
        # NOTE: при необходимости поменяй на нужный путь в твоём аккаунте:
        #   instals_report/v5, aggregated/.., partners_by_date_report/v5 и т.п.
        headers = {
            "accept": "text/csv",
            "Authorization": f"Bearer {self.token}",
        }
        params = {
            "from": date_from,
            "to": date_to,
            "timezone": timezone,
            "additional_fields": ",".join([
                "campaign","adset","ad","country","impressions","clicks","cost","af_prt","media_source","revenue"
            ]),
        }
        log.info("GET %s", url)
        r = requests.get(url, headers=headers, params=params, timeout=120)
        if r.status_code >= 400:
            raise AFError(f"AF error {r.status_code}: {r.text[:300]}")
        return r.text

    def to_normalized_df(self, csv_text: str, app_id: str, app_name: str = "", platform: str = "") -> pd.DataFrame:
        df = pd.read_csv(io.StringIO(csv_text))
        # мягкое переименование распространённых колонок
        rename_map = {
            "Date": "event_date",
            "date": "event_date",
            "Media Source": "media_source",
            "media_source": "media_source",
            "Campaign": "campaign",
            "campaign": "campaign",
            "Adset": "adset",
            "adset": "adset",
            "Ad": "ad",
            "ad": "ad",
            "Country Code": "country",
            "Country": "country",
            "country": "country",
            "Impressions": "impressions",
            "Clicks": "clicks",
            "Installs": "installs",
            "Cost": "cost",
            "Revenue": "revenue",
            "af_revenue": "revenue",
        }
        # применим только пересечение колонок
        rename_present = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=rename_present)

        # обязательные колонки
        for col in ["media_source","campaign","country","adset","ad"]:
            if col not in df.columns: df[col] = ""

        for col in ["impressions","clicks","installs"]:
            if col not in df.columns: df[col] = 0
        if "cost" not in df.columns: df["cost"] = 0.0
        if "revenue" not in df.columns: df["revenue"] = 0.0

        # типы
        df["event_date"] = pd.to_datetime(df.get("event_date", pd.Timestamp.utcnow()), errors="coerce").dt.date
        for col in ["impressions","clicks","installs"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        for col in ["cost","revenue"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

        df["app_id"] = app_id
        df["app_name"] = app_name
        df["platform"] = platform
        # retention если есть
        for rcol in ["d1_retained","d7_retained"]:
            if rcol not in df.columns: df[rcol] = 0

        # оставляем только нужное
        keep = [
            "event_date","app_id","app_name","platform","media_source","campaign","adset","ad","country",
            "impressions","clicks","installs","cost","revenue","d1_retained","d7_retained"
        ]
        return df[[c for c in keep if c in df.columns]]
