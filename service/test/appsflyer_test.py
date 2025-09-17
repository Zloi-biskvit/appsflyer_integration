# from operator import truediv
#
# from dotenv import load_dotenv
# import os
#
# from service.appsflyer_service import AppsFlyerClient, ReportType, AppsFlyerError, AppInfo
# # ищем .env в корне проекта
# from pathlib import Path
# env_path = Path(__file__).resolve().parent.parent / ".env"
# load_dotenv(dotenv_path=env_path)
# # print(env_path)
# # print("Token:", os.getenv("APPSFLYER_API_TOKEN"))
# # print("App ID:", os.getenv("APPSFLYER_APP_ID"))
#
# token = os.getenv("APPSFLYER_API_TOKEN")
# url = os.getenv("APPSFLYER_BASE_URL")
#
# base_url = os.getenv("APPSFLYER_BASE_URL")  # можешь не задавать, есть дефолт
#
# if not token:
#     raise RuntimeError("APPSFLYER_API_TOKEN пуст. Проверь .env и путь env_path.")
#
# client = AppsFlyerClient(api_token=token, base_url=base_url)
#
#
# from pathlib import Path
# from datetime import date, timedelta
#
# def download_agg_for_all_apps(client: AppsFlyerClient,
#                               report: ReportType,
#                               date_from: str,
#                               date_to: str,
#                               out_dir: Path) -> None:
#     out_dir.mkdir(parents=True, exist_ok=True)
#     apps = list(client.list_apps(limit=200))
#     for app in apps:
#         if not app.id:
#             continue
#         fname = out_dir / f"{app.id}_{report.value}_{date_from}_{date_to}.csv"
#         try:
#             client.download_agg_report_to_file(
#                 file_path=str(fname),
#                 app_id=app.id,
#                 report=report,
#                 date_from=date_from,
#                 date_to=date_to,
#                 timezone="UTC",
#                 retargeting=False,
#                 extra_params=None,  # сюда можно добавить обычные фильтры AppsFlyer (media_source, campaign, country и т.п., но не phone)
#             )
#             print(f"[OK] {fname}")
#         except AppsFlyerError as e:
#             print(f"[FAIL] {app.id}: {e}")
#
# # пример вызова
# today = date.today()
# yesterday = today - timedelta(days=1)
# download_agg_for_all_apps(
#     client=client,
#     report=ReportType.DAILY,                 # или PARTNERS / GEO
#     date_from=yesterday.isoformat(),
#     date_to=today.isoformat(),
#     out_dir=Path("af_agg_reports"),
# )


from dotenv import load_dotenv
import os
from pathlib import Path
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
# print(env_path)
# print("Token:", os.getenv("APPSFLYER_API_TOKEN"))
# print("App ID:", os.getenv("APPSFLYER_APP_ID"))

token = os.getenv("APPSFLYER_API_TOKEN")
url = os.getenv("APPSFLYER_BASE_URL")

from pathlib import Path
from datetime import date, timedelta

from service.appsflyer_service import AppsFlyerClient, ReportType, AppsFlyerError

client = AppsFlyerClient(api_token=token, base_url=url)


def print_agg_for_all_apps(client: AppsFlyerClient,
                           report: ReportType,
                           date_from: str,
                           date_to: str) -> None:
    """
    Для каждого приложения делает запрос аггр. отчёта и печатает CSV в консоль.
    """
    apps = list(client.list_apps(limit=200))
    for app in apps:
        if not app.id:
            continue
        print("=" * 80)
        print(f"App: {app.id} | {app.name} | {app.platform} | Report={report.value}")
        try:
            chunks = client.iter_agg_report_csv(
                app_id=app.id,
                report=report,
                date_from=date_from,
                date_to=date_to,
                timezone="UTC",
                retargeting=False,
                extra_params=None,
            )
            for chunk in chunks:
                print(chunk.decode("utf-8", errors="ignore"), end="")
        except AppsFlyerError as e:
            print(f"[FAIL] {app.id}: {e}")


# пример вызова
today = date.today()
yesterday = today - timedelta(days=1)

print_agg_for_all_apps(
    client=client,
    report=ReportType.DAILY,     # DAILY / PARTNERS / GEO
    date_from=yesterday.isoformat(),
    date_to=today.isoformat(),
)
