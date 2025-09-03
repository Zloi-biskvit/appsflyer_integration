# TODO Берем все приложения что бы потом запрашивать для них статистику

import requests

# Ваш API V2.0 токен (создаётся в AppsFlyer Console -> API Tokens)
API_TOKEN = "eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.Li6W7ltorTZaE8LwTiqP6NPHmSuKAOp4_tXDl3_7a4gshv2Ilc7UOg.pHYYwtBweI_g7coR.FrcIKUATiUNqLy_V5tsJF5kjQ6vHEU4oGjK-3qaMm66aYXoPSpTFpaCBS3dMnpeV5_Nd9c4ctWfm6bz9T7HSnk6cWbZEo05f0vtDeR0ly6dxgAzpBi5Hf2Cee_rW4e7eYGnvyr-qQ7rvEV88PgP8YeC32XQ-FiH858q1DpuMN9vma5_RaEI5npUxRx7aphZuaCiDyvyVLTMg-ANeJAGLiCAwfpHG1Yi4T6aEh9aRWvHtWOW8XbWXYffGZcXRJNMVCQqMWWH-02NvZ9Gb2_JR1wZBOkiBzG2CV8sAfY8tn1dCN7PCgiRNHUVIShjQoQNVtibSKIH7lm0AvcW1s96pnFsc.DmdBqF2VDtcEpP4TcIJx7A"

# Базовый URL
BASE_URL = "https://hq1.appsflyer.com/api/mng/apps"

# Параметры запроса
params = {
    "capabilities": "protect_360",  # или другой capability, например: cost, raw_data, aggregate
    "limit": 1000,
    "offset": 0
}

# Заголовки
headers = {
    "Authorization": f"Bearer {API_TOKEN}"
}

# Выполняем запрос
response = requests.get(BASE_URL, headers=headers, params=params)

# Проверяем статус
if response.status_code == 200:
    data = response.json()
    apps = data.get("data", [])
    app_dict = []
    for app in apps:
        app_id = app.get("id")
        name = app.get("attributes", {}).get("name")
        platform = app.get("attributes", {}).get("platform")
        app_dict.append({"id": app_id, "name": name, "platform": platform})
    print(app_dict)
else:
    print(f"Ошибка: {response.status_code}, {response.text}")


