# appsflyer_client.py
from __future__ import annotations

import csv
import io
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import AppConfig

BASE_URL = "https://hq1.appsflyer.com"
AGG_MIN_INTERVAL_SEC = 65  # 1 запрос/мин на (app, report)

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 180
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)


def _make_session(api_token: str) -> requests.Session:
    """Создаёт HTTP-сессию с retry и нужными заголовками."""
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8))
    session.headers.update({"Authorization": f"Bearer {api_token}", "Connection": "keep-alive"})
    return session


def _parse_csv(content: bytes) -> List[Dict[str, Any]]:
    """Парсит CSV в список dict'ов."""
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


@dataclass
class AppsFlyerClient:
    api_token: str
    from_date: str
    to_date: str
    timezone: str = "UTC"
    retargeting: str = "false"

    def __post_init__(self) -> None:
        self._session = _make_session(self.api_token)
        # для rate-limit: (app_id, report_type) -> timestamp последнего вызова
        self._last_call_ts: Dict[Tuple[str, str], float] = {}

    def _rate_limit(self, app_id: str, report_type: str) -> None:
        key = (app_id, report_type)
        now = time.time()
        last = self._last_call_ts.get(key, 0.0)
        delta = now - last
        if delta < AGG_MIN_INTERVAL_SEC:
            time.sleep(AGG_MIN_INTERVAL_SEC - delta)
        self._last_call_ts[key] = time.time()

    def _download_csv_bytes(self, app_id: str, report_type: str) -> bytes:
        url = f"{BASE_URL}/api/agg-data/export/app/{app_id}/{report_type}/v5"
        params = {
            "from": self.from_date,
            "to": self.to_date,
            "timezone": self.timezone,
            "retargeting": self.retargeting,
        }
        r = self._session.get(url, params=params, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.content

        preview = r.content[:200]
        raise Exception(f"HTTP {r.status_code}, raw preview: {preview!r}")

    def fetch_agg_report(self, app: AppConfig, report_type: str) -> List[Dict[str, Any]]:
        """
        Возвращает строки отчёта для одной пары (app, report_type).
        Каждая строка — dict (без переименования колонок).
        """
        self._rate_limit(app.id, report_type)
        content = self._download_csv_bytes(app.id, report_type)
        rows = _parse_csv(content)

        # добавляем тех.колонки про приложение
        for r in rows:
            r["app_id"] = app.id
            r["app_name"] = app.name
            r["app_platform"] = app.platform
            r["report_type"] = report_type

        return rows
