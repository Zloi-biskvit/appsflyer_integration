import csv
import io
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from postgresql_adapter import PostgresqlAdapter

# ===============================
# Конфиг
# ===============================

CONFIG_PATH = Path("config.json")
BASE_URL = "https://hq1.appsflyer.com"
AGG_MIN_INTERVAL_SEC = 65  # 1 запрос/мин на (app, report)

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 180
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

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
}


@dataclass
class AppConfig:
    id: str
    name: str
    platform: str


@dataclass
class Config:
    apps: List[AppConfig]
    api_token: str
    agg_report_types: List[str]
    from_date: str
    to_date: str
    timezone: str
    retargeting: str
    destination_table: str
    destination_uri: str


def load_config(path: Path = CONFIG_PATH) -> Config:
    if not path.exists():
        raise RuntimeError(f"Config file not found: {path}")

    raw: Dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))

    apps = [AppConfig(**a) for a in raw.get("apps", [])]
    if not apps:
        raise RuntimeError("`apps` in config.json is empty")

    return Config(
        apps=apps,
        api_token=raw["api_token"],
        agg_report_types=raw.get("agg_report_types", ["geo_by_date_report"]),
        from_date=raw["from_date"],
        to_date=raw["to_date"],
        timezone=raw.get("timezone", "UTC"),
        retargeting=raw.get("retargeting", "false"),
        destination_table=raw["destination_table"],
        destination_uri=raw["destination_uri"],
    )


def make_session(api_token: str) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5, status=5,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8))
    session.headers.update({"Authorization": f"Bearer {api_token}", "Connection": "keep-alive"})
    return session


# ===============================
# HTTP / утилиты
# ===============================

def _rate_limit(last_call: Dict[Tuple[str, str], float], app_id: str, report_type: str) -> None:
    key = (app_id, report_type)
    now = time.time()
    last = last_call.get(key, 0.0)
    delta = now - last
    if delta < AGG_MIN_INTERVAL_SEC:
        time.sleep(AGG_MIN_INTERVAL_SEC - delta)
    last_call[key] = time.time()


def _download_csv_bytes(
    session: requests.Session,
    app_id: str,
    report_type: str,
    cfg: Config,
) -> bytes:
    url = f"{BASE_URL}/api/agg-data/export/app/{app_id}/{report_type}/v5"
    params = {
        "from": cfg.from_date,
        "to": cfg.to_date,
        "timezone": cfg.timezone,
        "retargeting": cfg.retargeting,
    }
    r = session.get(url, params=params, timeout=TIMEOUT)
    if r.status_code == 200:
        return r.content
    raise Exception(f"HTTP {r.status_code}: {r.text[:500]}")


def _parse_csv(content: bytes):
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = [dict(row) for row in reader]
    return reader.fieldnames or [], rows


def _insert_to_bd(
    data: List[Dict[str, Any]],
    destination_table: str,
    destination_uri: str,
) -> None:
    if not data:
        print("No rows to insert, skipping insert.")
        return

    print(f"Insert {len(data)} rows into {destination_table}")
    PostgresqlAdapter.insert(
        data=data,
        destination_table=destination_table,
        destination_uri=destination_uri,
        on_duplicate="update",
    )

def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Приводит ключи к нужным именам и формирует data: list[dict].
    """
    normalized: List[Dict[str, Any]] = []

    for row in rows:
        new_row: Dict[str, Any] = {}
        for key, value in row.items():
            # если есть маппинг — переименуем, иначе оставим как есть
            new_key = COLUMN_RENAME_MAP.get(key, key)
            new_row[new_key] = value
        normalized.append(new_row)

    return normalized


# ===============================
# Основная логика
# ===============================

def main():
    cfg = load_config()
    session = make_session(cfg.api_token)

    last_call_ts: Dict[Tuple[str, str], float] = {}

    for report_type in cfg.agg_report_types:
        all_rows: List[Dict[str, Any]] = []
        print(f"\n=== AGG {report_type} ===")

        for idx, app in enumerate(cfg.apps, 1):
            print(f"[{idx}/{len(cfg.apps)}] {app.name} ({app.id})")
            try:
                _rate_limit(last_call_ts, app.id, report_type)
                content = _download_csv_bytes(session, app.id, report_type, cfg)
                headers, rows = _parse_csv(content)

                # проклейка идентификаторов
                for r in rows:
                    r["app_id"] = app.id
                    r["app_name"] = app.name
                    r["app_platform"] = app.platform
                    r["report_type"] = report_type

                all_rows.extend(rows)
                print(f"  +{len(rows)} rows")
            except Exception as e:
                print("  ERROR:", repr(e))

        if all_rows:
            # здесь приводим к виду data: list[dict] + переименовываем колонки
            data: List[Dict[str, Any]] = normalize_rows(all_rows)

            _insert_to_bd(
                data=data,
                destination_table=cfg.destination_table,
                destination_uri=cfg.destination_uri,
            )
        else:
            print("No data for this report")


if __name__ == "__main__":
    main()
