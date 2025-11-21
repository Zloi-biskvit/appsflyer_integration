# integration_service.py
from __future__ import annotations

from typing import Any, Dict, List

from .config import Config
from .appsflyer_client import AppsFlyerClient
from .postgresql_adapter import PostgresqlAdapter
from .models import AppsFlyerRecord

# Маппинг "как в CSV" -> "как в таблице Postgres"
COLUMN_RENAME_MAP: Dict[str, str] = {
    "Date": "date",
    "Country": "country",
    "Agency/PMD (af_prt)": "agency_pmd",
    "Media Source (pid)": "media_source",
    "Campaign (c)": "campaign",
    "Impressions": "impressions",
    "Clicks": "clicks",
    "CTR": "ctr",
    "Installs": "installs",
    "Conversion Rate": "conversion_rate",
    "Sessions": "sessions",
    "Loyal Users": "loyal_users",
    "Loyal Users/Installs": "loyal_users_per_install",
    "Total Revenue": "total_revenue",
    "Total Cost": "total_cost",
    "ROI": "roi",
    "ARPU": "arpu",
    "Average eCPI": "avg_ecpi",
    "af_complete_registration (Unique users)": "af_complete_registration_unique_users",
    "af_complete_registration (Event counter)": "af_complete_registration_event_counter",
    "af_complete_registration (Sales in USD)": "af_complete_registration_sales_usd",
    "af_purchase (Unique users)": "af_purchase_unique_users",
    "af_purchase (Event counter)": "af_purchase_event_counter",
    "af_purchase (Sales in USD)": "af_purchase_sales_usd",
}

def to_pydantic_dict(row: dict) -> dict:
    """
    Превращает одну строку в объект AppsFlyerRecord
    и возвращает готовый dict для записи в БД.
    """
    model = AppsFlyerRecord(**row)
    return model.model_dump(exclude_none=True)

def normalize_rows(rows):
    normed = []

    for r in rows:
        clean = {}
        for k, v in r.items():
            new_key = COLUMN_RENAME_MAP.get(k.strip(), k.strip())
            clean[new_key] = v

        # теперь валидируем через pydantic
        parsed = to_pydantic_dict(clean)
        normed.append(parsed)

    print("Normalization complete")
    return normed




class IntegrationService:
    """
    Склеивает всё вместе:
    - берёт конфиг
    - ходит в AppsFlyer
    - нормализует данные
    - пишет в Postgres
    """

    def __init__(self, config: Config, client: AppsFlyerClient) -> None:
        self._config = config
        self._client = client

    def _insert_to_db(self, data: List[Dict[str, Any]]) -> None:
        if not data:
            print("No rows to insert")
            return

        print(f"Insert {len(data)} rows into {self._config.destination_table}")
        PostgresqlAdapter.insert(
            data=data,
            destination_table=self._config.destination_table,
            destination_uri=self._config.destination_uri,
            on_duplicate="update",
        )

    def run(self) -> None:
        """
        Главный сценарий:
        для каждого отчёта и приложения → забрать данные → нормализовать → вставить в БД.
        """
        for report_type in self._config.agg_report_types:
            print(f"\n=== AGG {report_type} ===")

            for idx, app in enumerate(self._config.apps, 1):
                all_rows: List[Dict[str, Any]] = []
                print(f"[{idx}/{len(self._config.apps)}] {app.name} ({app.id})")
                try:
                    rows = self._client.fetch_agg_report(app, report_type)
                    all_rows.extend(rows)
                    print(f"  +{len(rows)} rows")
                except Exception as e:
                    print("  ERROR:", repr(e))

                all_rows = normalize_rows(all_rows)
                self._insert_to_db(all_rows)