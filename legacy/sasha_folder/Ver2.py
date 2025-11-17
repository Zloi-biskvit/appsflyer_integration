
#Должен ходить по id прил и собирать их в один csv



# appsflyer_service.py
import requests
import csv
import io
import time
from pathlib import Path
from typing import List, Dict, Tuple

# ===============================
# Конфигурация (правь под себя)
# ===============================
apps = [
     #{'id': 'id6747419978', 'name': 'Mount Quizmore', 'platform': 'ios'},
     #{'id': 'id6747675833', 'name': 'Written in Stone', 'platform': 'ios'},
     {'id': 'id6751264734', 'name': 'Fyshermans Key', 'platform': 'ios'},
     #{'id': 'id6751265804', 'name': 'AirCroco Bombino', 'platform': 'ios'},
     #{'id': 'id6752688000', 'name': 'Rabbit Trails', 'platform': 'ios'},
     #{'id': 'id6749614124', 'name': 'GlyphsQuest', 'platform': 'ios'},
     #{'id': 'id6751264734', 'name': 'Fyshermans Key', 'platform': 'ios'},
     #{'id': 'id6752852232', 'name': 'DropsFlavor', 'platform': 'ios'},
     #{'id': 'id6752889529', 'name': 'Carrae Ball', 'platform': 'ios'},
     #{'id': 'id6753283611', 'name': 'Sky Lores', 'platform': 'ios'},
     #{'id': 'id6753668687', 'name': 'CheckYouFerma', 'platform': 'ios'},
     #{'id': 'id6753740122', 'name': 'NestKeeeper', 'platform': 'ios'},
     #{'id': 'id6753740688', 'name': 'Rabbit Mood', 'platform': 'ios'}
]

API_TOKEN = "eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.Li6W7ltorTZaE8LwTiqP6NPHmSuKAOp4_tXDl3_7a4gshv2Ilc7UOg.pHYYwtBweI_g7coR.FrcIKUATiUNqLy_V5tsJF5kjQ6vHEU4oGjK-3qaMm66aYXoPSpTFpaCBS3dMnpeV5_Nd9c4ctWfm6bz9T7HSnk6cWbZEo05f0vtDeR0ly6dxgAzpBi5Hf2Cee_rW4e7eYGnvyr-qQ7rvEV88PgP8YeC32XQ-FiH858q1DpuMN9vma5_RaEI5npUxRx7aphZuaCiDyvyVLTMg-ANeJAGLiCAwfpHG1Yi4T6aEh9aRWvHtWOW8XbWXYffGZcXRJNMVCQqMWWH-02NvZ9Gb2_JR1wZBOkiBzG2CV8sAfY8tn1dCN7PCgiRNHUVIShjQoQNVtibSKIH7lm0AvcW1s96pnFsc.DmdBqF2VDtcEpP4TcIJx7A"  # noqa: E501
REPORT_TYPE = "daily_report"   # например: daily_report, partners_report
FROM_DATE = "2025-10-27"
TO_DATE = "2025-11-02"
TIMEZONE = "UTC"
RETARGETING = "false"

OUTPUT_DIR = Path("report")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / f"{REPORT_TYPE}_{FROM_DATE}_{TO_DATE}_ALL.csv"

# ===============================
# Константы/тайминги
# ===============================
BASE_URL = "https://hq1.appsflyer.com"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
TIMEOUT = 120

# Короткие ретраи на сетевые/5xx/429
RETRIES_SOFT = 3
BACKOFF_BASE = 2  # 1s,2s,4s ...

# Длинные ретраи при 403 Limit reached
LIMIT_RETRIES = 5
LIMIT_SLEEP_SCHEDULE = [60, 120, 240, 480, 900]  # 1m,2m,4m,8m,15m

# Пауза между приложениями — снижает шанс лимита
INTER_APP_DELAY = 2

# Если лимит не снимается — продолжить остальные приложения (True) или падать (False)
FAIL_ON_LIMIT = False


def _is_limit_reached(resp: requests.Response) -> bool:
    if resp.status_code != 403:
        return False
    txt = (resp.text or "").lower()
    return "limit reached" in txt or "limit" in txt


def _download_csv_bytes(app_id: str) -> bytes:
    url = f"{BASE_URL}/api/agg-data/export/app/{app_id}/{REPORT_TYPE}/v5"
    params = {
        "from": FROM_DATE,
        "to": TO_DATE,
        "timezone": TIMEZONE,
        "retargeting": RETARGETING,
    }

    # 1) быстрые ретраи (429/5xx/сети)
    last_err = None
    for attempt in range(1, RETRIES_SOFT + 1):
        try:
            print(f"[soft {attempt}/{RETRIES_SOFT}] GET {url} params={params}")
            r = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.content

            if _is_limit_reached(r):
                print("→ Получен 403 Limit reached — переходим к длинным ретраям.")
                break  # перейдём к блоку длинных ретраев

            if r.status_code in (429, 500, 502, 503, 504):
                last_err = Exception(f"HTTP {r.status_code}: {r.text[:300]}")
            else:
                raise Exception(f"HTTP {r.status_code}: {r.text[:1000]}")
        except Exception as e:
            last_err = e

        if attempt < RETRIES_SOFT:
            sleep_s = BACKOFF_BASE ** (attempt - 1)
            time.sleep(sleep_s)
    else:
        # Если вышли без break и так и не получили 200 — кидаем последнюю ошибку
        if last_err:
            raise last_err

    # 2) длинные ретраи под лимиты (403 limit)
    for i in range(LIMIT_RETRIES):
        wait = LIMIT_SLEEP_SCHEDULE[min(i, len(LIMIT_SLEEP_SCHEDULE) - 1)]
        print(f"[limit {i+1}/{LIMIT_RETRIES}] Ждём {wait}s из-за лимита...")
        time.sleep(wait)

        r = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.content
        if not _is_limit_reached(r):
            if r.status_code in (429, 500, 502, 503, 504):
                # один дополнительный короткий бэкоф после длинного ожидания
                time.sleep(2)
                continue
            raise Exception(f"HTTP {r.status_code}: {r.text[:1000]}")

    raise Exception("Лимит не снялся после длинных ретраев (403 Limit reached).")


def _parse_csv(content: bytes) -> Tuple[List[str], List[Dict[str, str]]]:
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = [dict(row) for row in reader]
    return reader.fieldnames or [], rows


def fetch_report_for_app(app: Dict[str, str]) -> Tuple[List[str], List[Dict[str, str]]]:
    app_id = app.get("id")
    content = _download_csv_bytes(app_id)
    headers, rows = _parse_csv(content)

    extra_cols = ["app_id", "app_name", "app_platform"]
    header_set = set(headers or [])
    merged_headers = list(headers or []) + [c for c in extra_cols if c not in header_set]

    for r in rows:
        r["app_id"] = app.get("id", "")
        r["app_name"] = app.get("name", "")
        r["app_platform"] = app.get("platform", "")

    return merged_headers, rows


def _write_csv(path: Path, headers: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main():
    if not apps:
        print("Список apps пуст.")
        return

    all_headers: List[str] = []
    all_rows: List[Dict[str, str]] = []

    for idx, app in enumerate(apps, 1):
        print(f"\n=== [{idx}/{len(apps)}] {app.get('name')} ({app.get('id')}) ===")
        try:
            headers, rows = fetch_report_for_app(app)

            if not all_headers:
                all_headers = headers[:]
            else:
                for h in headers:
                    if h not in all_headers:
                        all_headers.append(h)

            all_rows.extend(rows)
            print(f"✓ {app.get('name')} — строк: {len(rows)}")
        except Exception as e:
            msg = str(e)
            print(f"✗ {app.get('name')} — ошибка: {msg}")
            if FAIL_ON_LIMIT or "Limit" not in msg:
                # Либо настроено падать, либо это не лимит — выходим
                raise
            # Иначе — просто пропускаем это приложение и идём дальше
            print("→ Пропускаем приложение из-за лимита и продолжаем со следующим.")

        time.sleep(INTER_APP_DELAY)

    if not all_rows:
        print("Нет данных для записи.")
        return

    _write_csv(OUTPUT_FILE, all_headers, all_rows)
    print(f"\n✅ Сводный CSV: {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()