from __future__ import annotations

import os

import requests
import csv
import io
import math
import time
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, List

from sqlalchemy.testing import rowset

log = logging.getLogger(__name__)


# ===== Модели / типы =====


@dataclass(frozen=True)
class AppInfo:
    id: str
    name: Optional[str]
    platform: Optional[str]


class ReportType(str, Enum):
    DAILY = "daily_report"
    PARTNERS = "partners_report"
    GEO = "geo_report"


class AppsFlyerError(RuntimeError):
    def __init__(self, status: int, message: str, url: str):
        super().__init__(f"HTTP {status} at {url}: {message}")
        self.status = status
        self.url = url


# ===== Клиент =====

class AppsFlyerClient:
    DEFAULT_BASE_URL = "https://hq1.appsflyer.com"
    PATH_LIST_APPS = "/api/mng/apps"
    PATH_EXPORT_AGG = "/api/agg-data/export/app/{app_id}/{report_type}/v5"

    def __init__(
        self,
        api_token: str,
        base_url: str | None = None,
        *,
        timeout_sec: float = 60.0,
        max_retries: int = 5,
        backoff_base_sec: float = 1.0,
    ):
        if not api_token:
            raise ValueError("api_token is required")

        self.api_token = api_token
        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/")
        self.session = requests.Session()
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.backoff_base_sec = backoff_base_sec

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = {"Authorization": f"Bearer {self.api_token}"}

        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout_sec,
                )
            except requests.RequestException as e:
                if attempt <= self.max_retries:
                    sleep = min(self.backoff_base_sec * (2 ** (attempt - 1)), 60)
                    log.warning("Network error %s %s: %s. Retry in %.1fs", method, url, e, sleep)
                    time.sleep(sleep)
                    continue
                raise

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code in (429, 500, 502, 503, 504) and attempt <= self.max_retries:
                retry_after = resp.headers.get("Retry-After")
                sleep = int(retry_after) if retry_after and retry_after.isdigit() else min(
                    self.backoff_base_sec * (2 ** (attempt - 1)), 120
                )
                log.warning("HTTP %s on %s. Retry in %.1fs (attempt %d/%d)",
                            resp.status_code, url, sleep, attempt, self.max_retries)
                time.sleep(sleep)
                continue

            raise AppsFlyerError(resp.status_code, resp.text[:2048], url)

    def list_apps(
        self,
        *,
        capability: str | None = "protect_360",
        limit: int = 1000,
    ) -> List[AppInfo]:
        """Возвращает список приложений (батч, без генератора)."""
        if not (1 <= limit <= 1000):
            raise ValueError("limit must be in 1..1000")

        apps: list[AppInfo] = []
        offset = 0

        while True:
            params = {"limit": limit, "offset": offset}
            if capability:
                params["capabilities"] = capability

            resp = self._request("GET", self.PATH_LIST_APPS, params=params)
            try:
                payload = resp.json()
            except ValueError:
                raise AppsFlyerError(resp.status_code, "Invalid JSON in response", resp.url)

            items = payload.get("data") or []
            if not items:
                break

            for app in items:
                attrs = app.get("attributes") or {}
                apps.append(
                    AppInfo(
                        id=str(app.get("id") or ""),
                        name=attrs.get("name"),
                        platform=attrs.get("platform"),
                    )
                )

            if len(items) < limit:
                break
            offset += limit

        return apps

    def download_agg_report_to_file(
        self,
        file_path: str,
        apps: list[AppInfo],
        # app_id: str,
        # app_name: str,
        report: ReportType | str,
        *,
        date_from: str,
        date_to: str,
        timezone: str = "UTC",
        retargeting: bool | str = False,
        extra_params: Optional[Dict[str, Any]] = None,
        columns_mapping: Optional[Dict[str, Optional[str]]] = None,


    ) -> None:
        row = []
        """Скачивает отчёт и сохраняет в файл (целиком)."""
        rows = self.fetch_agg_report_rows(
            apps=apps,
            report=report,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            retargeting=retargeting,
            extra_params=extra_params,
            columns_mapping=columns_mapping,
        )
        if not rows:
            return


        with open(file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    def fetch_agg_report_rows(
        self,
        apps,  # list[AppInfo] или list[dict] с ключами id/name/platform
        report: ReportType | str,
        *,
        date_from: str,
        date_to: str,
        timezone: str = "UTC",
        retargeting: bool | str = False,
        extra_params: Optional[Dict[str, Any]] = None,
        columns_mapping: Optional[Dict[str, Optional[str]]] = None,
        drop_default: bool = True,
        encoding: str = "utf-8",
        null_tokens: Optional[List[str]] = None,
        empty_as_null: bool = True,
    ) -> List[Dict[str, Any]]:
        """Собирает единый датасет по списку приложений и возвращает список словарей."""

        if not date_from or not date_to:
            raise ValueError("date_from/date_to are required")

        # helper: извлечь поля из AppInfo или dict
        def _extract_app_fields(app) -> tuple[str, Optional[str], Optional[str]]:
            if isinstance(app, dict):
                return (
                    str(app.get("id") or ""),
                    app.get("name"),
                    app.get("platform") or app.get("device"),
                )
            # AppInfo-like
            return (
                str(getattr(app, "id", "") or ""),
                getattr(app, "name", None),
                getattr(app, "platform", None),
            )

        report_type = report.value if isinstance(report, ReportType) else str(report)

        all_rows: list[Dict[str, Any]] = []
        drop_set = {"id", "name"} if drop_default else set()
        tokens = {t.lower() for t in (null_tokens or ["nan", "na", "n/a", "null", "none", "-", "—"])}

        def normalize(v: Any) -> Any:
            if v is None:
                return None
            if isinstance(v, float) and math.isnan(v):
                return None
            if isinstance(v, str):
                s = v.strip().replace("\u00a0", " ")
                if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
                    s = s[1:-1].strip()
                if empty_as_null and s == "":
                    return None
                if s.lower() in tokens:
                    return None
                return s
            return v

        for app in apps:
            app_id, app_name, app_platform = _extract_app_fields(app)
            if not app_id:
                # пропускаем некорректный элемент
                continue

            path = self.PATH_EXPORT_AGG.format(app_id=app_id, report_type=report_type)
            params = {
                "from": date_from,
                "to": date_to,
                "timezone": timezone,
                "retargeting": "true" if retargeting in (True, "true", "True", "1", 1) else "false",
            }
            if extra_params:
                params.update(extra_params)

            resp = self._request("GET", path, params=params)
            text = resp.content.decode(encoding, errors="replace")
            if text.startswith("\ufeff"):
                text = text.lstrip("\ufeff")

            f = io.StringIO(text, newline="")
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                # пустой отчёт для этого приложения
                continue

            for raw_row in reader:
                # маппинг/дроп колонок
                out: Dict[str, Any] = {}
                if columns_mapping:
                    for src, value in raw_row.items():
                        if src in drop_set:
                            continue
                        new_name = columns_mapping.get(src, src)
                        if not new_name:  # None/"" => дроп колонки
                            continue
                        out[new_name] = normalize(value)
                else:
                    for src, value in raw_row.items():
                        if src in drop_set:
                            continue
                        out[src] = normalize(value)

                # добавляем сведения о приложении
                out["app_name"] = app_name
                out["device"] = app_platform

                all_rows.append(out)

        return all_rows