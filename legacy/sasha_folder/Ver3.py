
#Должен ходить по id прил и собирать их в один csv


# appsflyer_agg_export.py
import csv
import io
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# ===============================
# Конфигурация (правь под себя)
# ===============================
apps: List[Dict[str, str]] = [

        {'id': 'id1434091635', 'name': 'Black Lines', 'platform': 'ios'},
        {'id': 'id6743713912', 'name': 'MotivaRise', 'platform': 'ios'},
        {'id': 'id6746459914', 'name': 'Glyphoria', 'platform': 'ios'},
        {'id': 'id6747419978', 'name': 'Mount Quizmore', 'platform': 'ios'},
        {'id': 'id6747675833', 'name': 'Written in Stone', 'platform': 'ios'},
        {'id': 'id6747964884', 'name': 'SoundsScape', 'platform': 'ios'},
        {'id': 'id6748456624', 'name': 'AviaQR', 'platform': 'ios'},
        {'id': 'id6748908145', 'name': 'Bubloonies', 'platform': 'ios'},
        {'id': 'id6749228997', 'name': 'Flagleap', 'platform': 'ios'},
        {'id': 'id6749614124', 'name': 'GlyphsQuest', 'platform': 'ios'},
        {'id': 'id6751264734', 'name': "Fysherman's Key", 'platform': 'ios'},
        {'id': 'id6751265804', 'name': 'AirCroco Bombino', 'platform': 'ios'},
        {'id': 'id6751882513', 'name': 'Updraft Balloon', 'platform': 'ios'},
        {'id': 'id6752244390', 'name': 'OceaniaMoons', 'platform': 'ios'},
        {'id': 'id6752688000', 'name': 'Rabbit Trails', 'platform': 'ios'},
        {'id': 'id6752852232', 'name': 'DropsFlavor', 'platform': 'ios'},
        {'id': 'id6752889529', 'name': 'Carrae Ball', 'platform': 'ios'},
        {'id': 'id6753283611', 'name': 'Sky Lores', 'platform': 'ios'},
        {'id': 'id6753668687', 'name': 'CheckYouFerma', 'platform': 'ios'},
        {'id': 'id6753740122', 'name': 'NestKeeeper', 'platform': 'ios'},
        {'id': 'id6753740688', 'name': 'Rabbit Mood', 'platform': 'ios'},
        {'id': 'id6753748304', 'name': 'Moona:Daily Energy Journal', 'platform': 'ios'},
        {'id': 'id6754025150', 'name': 'PlishkoBuild', 'platform': 'ios'},
        {'id': 'id6754025975', 'name': 'SearchMyFormula', 'platform': 'ios'},
        {'id': 'id6754026899', 'name': 'Bon-Bon Story Quest', 'platform': 'ios'},
        {'id': 'id6754033791', 'name': 'Olymera Elements', 'platform': 'ios'},
        {'id': 'id6754906767', 'name': 'KiloCalcAndy', 'platform': 'ios'}
    ]



API_TOKEN = "eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.Li6W7ltorTZaE8LwTiqP6NPHmSuKAOp4_tXDl3_7a4gshv2Ilc7UOg.pHYYwtBweI_g7coR.FrcIKUATiUNqLy_V5tsJF5kjQ6vHEU4oGjK-3qaMm66aYXoPSpTFpaCBS3dMnpeV5_Nd9c4ctWfm6bz9T7HSnk6cWbZEo05f0vtDeR0ly6dxgAzpBi5Hf2Cee_rW4e7eYGnvyr-qQ7rvEV88PgP8YeC32XQ-FiH858q1DpuMN9vma5_RaEI5npUxRx7aphZuaCiDyvyVLTMg-ANeJAGLiCAwfpHG1Yi4T6aEh9aRWvHtWOW8XbWXYffGZcXRJNMVCQqMWWH-02NvZ9Gb2_JR1wZBOkiBzG2CV8sAfY8tn1dCN7PCgiRNHUVIShjQoQNVtibSKIH7lm0AvcW1s96pnFsc.DmdBqF2VDtcEpP4TcIJx7A"  # noqa: E501
# Границы выгрузки. Для «всё, что есть» — ставь START_DATE к началу работы апп.
AGG_REPORT_TYPES = [
    #"daily_report",
    "geo_by_date_report"
]

FROM_DATE = "2025-11-10"
TO_DATE   = "2025-11-16"
TIMEZONE  = "UTC"
RETARGETING = "false"

OUTPUT_DIR = Path("report")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ===============================
# Константы HTTP / лимиты
# ===============================
BASE_URL = "https://hq1.appsflyer.com"
AGG_MIN_INTERVAL_SEC = 65  # 1 запрос/мин на (app, report)

SESSION = requests.Session()
_retry = Retry(
    total=5, connect=5, read=5, status=5,
    backoff_factor=1.2,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET"}),
    respect_retry_after_header=True,
    raise_on_status=False,
)
SESSION.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=8, pool_maxsize=8))
SESSION.headers.update({"Authorization": f"Bearer {API_TOKEN}", "Connection": "keep-alive"})

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 180
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

# ===============================
# Утилиты
# ===============================
def _rate_limit(last_call: Dict[Tuple[str, str], float], app_id: str, report_type: str) -> None:
    key = (app_id, report_type)
    now = time.time()
    last = last_call.get(key, 0.0)
    delta = now - last
    if delta < AGG_MIN_INTERVAL_SEC:
        time.sleep(AGG_MIN_INTERVAL_SEC - delta)
    last_call[key] = time.time()

def _download_csv_bytes(app_id: str, report_type: str) -> bytes:
    url = f"{BASE_URL}/api/agg-data/export/app/{app_id}/{report_type}/v5"
    params = {"from": FROM_DATE, "to": TO_DATE, "timezone": TIMEZONE, "retargeting": RETARGETING}
    r = SESSION.get(url, params=params, timeout=TIMEOUT)
    if r.status_code == 200:
        return r.content
    raise Exception(f"HTTP {r.status_code}: {r.text[:500]}")

def _parse_csv(content: bytes):
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = [dict(row) for row in reader]
    return reader.fieldnames or [], rows

def _write_csv(path: Path, headers: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

# ===============================
# Основная логика
# ===============================
def main():
    if not apps:
        print("Список apps пуст."); return

    last_call_ts: Dict[Tuple[str, str], float] = {}

    for report_type in AGG_REPORT_TYPES:
        all_headers: List[str] = []
        all_rows: List[Dict[str, str]] = []
        print(f"\n=== AGG {report_type} ===")

        for idx, app in enumerate(apps, 1):
            app_id = app["id"]
            print(f"[{idx}/{len(apps)}] {app.get('name')} ({app_id})")
            try:
                _rate_limit(last_call_ts, app_id, report_type)
                content = _download_csv_bytes(app_id, report_type)
                headers, rows = _parse_csv(content)

                # проклейка идентификаторов
                extra = ["app_id", "app_name", "app_platform", "report_type"]
                merged = list(headers or [])
                for c in extra:
                    if c not in merged:
                        merged.append(c)
                for r in rows:
                    r["app_id"] = app.get("id", "")
                    r["app_name"] = app.get("name", "")
                    r["app_platform"] = app.get("platform", "")
                    r["report_type"] = report_type

                # объединяем схему
                if not all_headers:
                    all_headers = merged
                else:
                    for h in merged:
                        if h not in all_headers:
                            all_headers.append(h)

                all_rows.extend(rows)
                print(f"  +{len(rows)} строк")
            except Exception as e:
                print(f"  ✗ ошибка: {e}")

        if all_rows:
            out = OUTPUT_DIR / f"{report_type}_{FROM_DATE}_{TO_DATE}_ALL.csv"
            _write_csv(out, all_headers, all_rows)
            print(f"✓ saved: {out.resolve()}")
        else:
            print("∅ данных для этого отчёта")

if __name__ == "__main__":
    main()