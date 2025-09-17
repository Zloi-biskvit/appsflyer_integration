from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, List, Iterable, Iterator

import requests
import csv
import io
import math

__all__ = [
    "AppsFlyerClient",
    "AppsFlyerError",
    "AppInfo",
    "ReportType",
]

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
    """
    Надёжный тонкий клиент:
      - requests.Session
      - таймауты
      - экспоненциальные ретраи на 429/5xx + уважение Retry-After
      - пагинация для списка приложений
      - стриминг выгрузок (chunked)
    """

    # При необходимости замени на свой базовый домен
    DEFAULT_BASE_URL = "https://hq1.appsflyer.com"

    # Список приложений
    PATH_LIST_APPS = "/api/mng/apps"

    # Агрегированные отчёты v5
    PATH_EXPORT_AGG = "/api/agg-data/export/app/{app_id}/{report_type}/v5"

    def __init__(
        self,
        api_token: str,
        base_url: str | None = None,
        *,
        session: Optional[requests.Session] = None,
        timeout_sec: float = 60.0,
        max_retries: int = 5,
        backoff_base_sec: float = 1.0,
    ):
        if not api_token:
            raise ValueError("api_token is required")
        self.api_token = api_token
        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/")
        self.session = session or requests.Session()
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.backoff_base_sec = backoff_base_sec

    # --- низкоуровневый запрос с ретраями ---
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        stream: bool = False,
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
                    stream=stream,
                )
            except requests.RequestException as e:
                if attempt <= self.max_retries:
                    sleep = min(self.backoff_base_sec * (2 ** (attempt - 1)), 60)
                    log.warning("Network error on %s %s: %s. Retry in %.1fs", method, url, e, sleep)
                    time.sleep(sleep)
                    continue
                raise

            if 200 <= resp.status_code < 300:
                return resp

            if resp.status_code in (429, 500, 502, 503, 504) and attempt <= self.max_retries:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep = int(retry_after)
                else:
                    sleep = min(self.backoff_base_sec * (2 ** (attempt - 1)), 120)
                log.warning("HTTP %s on %s. Retry in %.1fs (attempt %d/%d)",
                            resp.status_code, url, sleep, attempt, self.max_retries)
                time.sleep(sleep)
                continue

            # фатальная ошибка
            raise AppsFlyerError(resp.status_code, resp.text[:2048], url)

    # ===== Публичные методы =====

    def list_apps(
        self,
        *,
        capability: str = "protect_360",
        limit: int = 1000,
    ) -> Iterable[AppInfo]:
        """
        Возвращает все приложения постранично (генератор).
        """
        if limit <= 0 or limit > 1000:
            raise ValueError("limit must be in 1..1000")

        offset = 0
        while True:
            params = {"capabilities": capability, "limit": limit, "offset": offset}
            resp = self._request("GET", self.PATH_LIST_APPS, params=params, stream=False)
            data = resp.json() if resp.content else {}
            items = data.get("data", [])

            if not items:
                return  # конец

            for app in items:
                app_id = app.get("id")
                attrs = app.get("attributes", {}) or {}
                yield AppInfo(
                    id=str(app_id) if app_id is not None else "",
                    name=attrs.get("name"),
                    platform=attrs.get("platform"),
                )

            # если элементов меньше лимита — выходим
            if len(items) < limit:
                return
            offset += limit

    def iter_agg_report_csv(
        self,
        app_id: str,
        report: ReportType | str,
        *,
        date_from: str,
        date_to: str,
        timezone: str = "UTC",
        retargeting: bool | str = False,
        chunk_size: int = 1024 * 1024,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[bytes]:
        """
        Стримит CSV кусками (bytes). Удобно для больших выгрузок.

        Пример:
            with open("out.csv", "wb") as f:
                for chunk in client.iter_agg_report_csv(app_id, ReportType.DAILY, date_from="2025-09-01", date_to="2025-09-02"):
                    f.write(chunk)
        """
        if not app_id:
            raise ValueError("app_id is required")
        if not date_from or not date_to:
            raise ValueError("date_from/date_to are required")

        report_type = report.value if isinstance(report, ReportType) else str(report)
        path = self.PATH_EXPORT_AGG.format(app_id=app_id, report_type=report_type)

        params = {
            "from": date_from,
            "to": date_to,
            "timezone": timezone,
            "retargeting": "true" if retargeting in (True, "true", "True", "1", 1) else "false",
        }
        if extra_params:
            params.update(extra_params)

        resp = self._request("GET", path, params=params, stream=True)
        for chunk in resp.iter_content(chunk_size=chunk_size):
            if chunk:  # keep-alive
                yield chunk

    def download_agg_report_to_file(
        self,
        file_path: str,
        app_id: str,
        report: ReportType | str,
        *,
        date_from: str,
        date_to: str,
        timezone: str = "UTC",
        retargeting: bool | str = False,
        chunk_size: int = 1024 * 1024,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Скачивает отчёт и сохраняет в файл (стриминг, без загрузки всего в память).
        """
        with open(file_path, "wb") as f:
            for chunk in self.iter_agg_report_csv(
                app_id=app_id,
                report=report,
                date_from=date_from,
                date_to=date_to,
                timezone=timezone,
                retargeting=retargeting,
                chunk_size=chunk_size,
                extra_params=extra_params,
            ):
                f.write(chunk)

    def fetch_agg_report_rows(
        self,
        app_id: str,
        report: ReportType | str,
        *,
        date_from: str,
        date_to: str,
        timezone: str = "UTC",
        retargeting: bool | str = False,
        extra_params: Optional[Dict[str, Any]] = None,
        # маппинг "исходная_колонка" -> "новое_имя"; None/"" — дропнуть колонку
        columns_mapping: Optional[Dict[str, Optional[str]]] = None,
        # по умолчанию дропаем служебные 'id' и 'name'
        drop_default: bool = True,
        # кодировка CSV
        encoding: str = "utf-8",
        # нормализация пустых значений
        null_tokens: Optional[Iterable[str]] = None,
        empty_as_null: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Загружает агрегированный отчёт целиком и возвращает список словарей (строки CSV).
        Применяет маппинг колонок и замену "пустых" значений на None (NULL в БД).
        """
        if not app_id:
            raise ValueError("app_id is required")
        if not date_from or not date_to:
            raise ValueError("date_from/date_to are required")

        report_type = report.value if isinstance(report, ReportType) else str(report)
        path = self.PATH_EXPORT_AGG.format(app_id=app_id, report_type=report_type)

        params = {
            "from": date_from,
            "to": date_to,
            "timezone": timezone,
            "retargeting": "true" if retargeting in (True, "true", "True", "1", 1) else "false",
        }
        if extra_params:
            params.update(extra_params)

        # Получаем весь файл (без stream)
        resp = self._request("GET", path, params=params, stream=False)

        text = resp.content.decode(encoding, errors="replace")
        # Срезаем BOM, если есть
        if text.startswith("\ufeff"):
            text = text.lstrip("\ufeff")

        # Парсим CSV
        f = io.StringIO(text, newline="")
        reader = csv.DictReader(f)

        # какие столбцы дропать по умолчанию
        drop_set = {"id", "name"} if drop_default else set()

        # Набор токенов, которые считаем "пустыми" (-> None)
        DEFAULT_NULL_TOKENS = {
            "nan", "na", "n/a", "null", "none", "-", "—", "n\\a", "not available", "b"
        }
        tokens = {t.lower() for t in (null_tokens or DEFAULT_NULL_TOKENS)}

        def normalize_value(v: Any) -> Any:
            """Превращает разные пустые формы в None; чистит строки."""
            if v is None:
                return None
            if isinstance(v, float) and math.isnan(v):
                return None
            if isinstance(v, (bytes, bytearray)):
                v = v.decode(encoding, errors="ignore")
            if isinstance(v, str):
                s = v.replace("\u00a0", " ").strip()  # NBSP -> space, trim
                # убираем обрамляющие кавычки "..." / '...'
                if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
                    s = s[1:-1].strip()
                if empty_as_null and s == "":
                    return None
                if s.lower() in tokens:
                    return None
                return s
            return v  # числа/булевы/прочее — как есть

        def transform(row: Dict[str, Any]) -> Dict[str, Any]:
            """Применяет дроп/переименование колонок и нормализацию значений."""
            if not columns_mapping:
                return {k: normalize_value(v) for k, v in row.items() if k not in drop_set}

            out: Dict[str, Any] = {}
            for src, value in row.items():
                if src in drop_set:
                    continue
                new_name = columns_mapping.get(src, src)  # если нет в маппинге — оставляем имя
                if new_name is None or new_name == "":
                    continue  # явный дроп колонки
                out[new_name] = normalize_value(value)
            return out

        # Если нет заголовка — вернём пусто
        if reader.fieldnames is None:
            return []

        return [transform(row) for row in reader]
