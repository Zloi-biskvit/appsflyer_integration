# appsflyer_service.py
import requests

# ===============================
# Конфигурация (правь под себя)
# ===============================
apps = [
    {'id': 'id1434091635', 'name': 'Black Lines', 'platform': 'ios'}
    , {'id': 'id6743713912', 'name': 'MotivaRise', 'platform': 'ios'}
    , {'id': 'id6746459914', 'name': 'Glyphoria', 'platform': 'ios'}
    , {'id': 'id6747419978', 'name': 'Mount Quizmore', 'platform': 'ios'}
    , {'id': 'id6747675833', 'name': 'Written in Stone', 'platform': 'ios'}
    , {'id': 'id6747964884', 'name': 'SoundsScape', 'platform': 'ios'}
    , {'id': 'id6748456624', 'name': 'AviaQR', 'platform': 'ios'}
    , {'id': 'id6748908145', 'name': 'Bubloonies', 'platform': 'ios'}
    , {'id': 'id6749228997', 'name': 'Flagleap', 'platform': 'ios'}
    , {'id': 'id6749614124', 'name': 'GlyphsQuest', 'platform': 'ios'}
 ]



for i in range(len(apps)):
    # app_id = apps[i]['id']
 #
    APP = apps[i]  # TODO выбор приложения
    API_TOKEN = "eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.Li6W7ltorTZaE8LwTiqP6NPHmSuKAOp4_tXDl3_7a4gshv2Ilc7UOg.pHYYwtBweI_g7coR.FrcIKUATiUNqLy_V5tsJF5kjQ6vHEU4oGjK-3qaMm66aYXoPSpTFpaCBS3dMnpeV5_Nd9c4ctWfm6bz9T7HSnk6cWbZEo05f0vtDeR0ly6dxgAzpBi5Hf2Cee_rW4e7eYGnvyr-qQ7rvEV88PgP8YeC32XQ-FiH858q1DpuMN9vma5_RaEI5npUxRx7aphZuaCiDyvyVLTMg-ANeJAGLiCAwfpHG1Yi4T6aEh9aRWvHtWOW8XbWXYffGZcXRJNMVCQqMWWH-02NvZ9Gb2_JR1wZBOkiBzG2CV8sAfY8tn1dCN7PCgiRNHUVIShjQoQNVtibSKIH7lm0AvcW1s96pnFsc.DmdBqF2VDtcEpP4TcIJx7A"
    APP_ID = APP.get('id')            # ID риложения из get_apps_id.py
    REPORT_TYPE = "daily_report"        # https://chatgpt.com/s/t_68b7215da13481919630daec39bd6710 partners_report daily_report
    FROM_DATE = "2025-09-01"               # формат YYYY-MM-DD
    TO_DATE = "2025-09-02"                 # формат YYYY-MM-DD
    TIMEZONE = "UTC"                       # например Europe/Berlin
    RETARGETING = "false"                  # "true" или "false"
    OUTPUT_FILE = f"report/{REPORT_TYPE}{FROM_DATE}_{TO_DATE}_{APP.get('name')}.csv"

    # ===============================
    # Логика
    # ===============================
    BASE_URL = "https://hq1.appsflyer.com"

    def fetch_report():
        url = f"{BASE_URL}/api/agg-data/export/app/{APP_ID}/{REPORT_TYPE}/v5"
        params = {
            "from": FROM_DATE,
            "to": TO_DATE,
            "timezone": TIMEZONE,
            "retargeting": RETARGETING
        }
        headers = {"Authorization": f"Bearer {API_TOKEN}"}

        print(f"Запрос: {url}")
        r = requests.get(url, headers=headers, params=params, timeout=120)
        if r.status_code != 200:
            raise Exception(f"Ошибка {r.status_code}: {r.text}")

        with open(OUTPUT_FILE, "wb") as f:
            f.write(r.content)
        print(f"✅ Отчёт сохранён в {OUTPUT_FILE}")

if __name__ == "__main__":
    fetch_report()
