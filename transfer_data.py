from adapters.postgresql_adapter import PostgresqlAdapter
from service.appsflyer_service import AppsFlyerClient, ReportType, AppsFlyerError
from dotenv import load_dotenv
import os
from pathlib import Path
out_dir = Path(__file__).resolve().parent / "reports"
out_dir.mkdir(exist_ok=True)
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)
from datetime import date, timedelta

token = os.getenv("APPSFLYER_API_TOKEN")
url = os.getenv("APPSFLYER_BASE_URL")
uri = os.getenv("POSTGRES_URI")

DATE_FROM="2025-09-08",
DATE_TO="2025-09-22",
# Инициализация клиента
apps_client = AppsFlyerClient(api_token=token)
# Получаем id приложения
apps = apps_client.list_apps()

apps_client.download_agg_report_to_file(
        apps=apps,
        file_path = out_dir / f"{date.today().strftime('%m%d')}_report.csv",
        report=ReportType.DAILY,
        date_from=DATE_FROM,
        date_to=DATE_TO,
        columns_mapping = {
                "Date": "date",
                "Agency/PMD (af_prt)": "agency_pmd",
                "Media Source (pid)": "media_source",
                "Campaign (c)": "campaign",
                "Impressions": "impressions",
                "Clicks": "clicks",
                "CTR":"ctr",
                "Installs":"installs",
                "Conversion Rate":"conversion_rate",
                "Sessions":"sessions",
                "Loyal Users":"loyal_users",
                "Loyal Users/Installs": "loyal_users_installs",
                "Total Cost":"total_cost",
                "Average eCPI":"average_ecpi",
                # "apps_name": apps_id[i].name,
                # "device" : apps_id[i].platform
        }
)

# Собираем репорты и записываем это в csv
# for i in range(len(apps)):
#     apps_client.download_agg_report_to_file(
#         file_path = out_dir / f"{date.today().strftime('%m%d')}_report_{apps[i].name}_{i}.csv",
#         app_id=apps[i].id,
#         app_name=apps[i].name,
#         report=ReportType.DAILY,
#         date_from=DATE_FROM,
#         date_to=DATE_TO,
#         columns_mapping = {
#                 "Date": "date",
#                 "Agency/PMD (af_prt)": "agency_pmd",
#                 "Media Source (pid)": "media_source",
#                 "Campaign (c)": "campaign",
#                 "Impressions": "impressions",
#                 "Clicks": "clicks",
#                 "CTR":"ctr",
#                 "Installs":"installs",
#                 "Conversion Rate":"conversion_rate",
#                 "Sessions":"sessions",
#                 "Loyal Users":"loyal_users",
#                 "Loyal Users/Installs": "loyal_users_installs",
#                 "Total Cost":"total_cost",
#                 "Average eCPI":"average_ecpi",
#                 # "apps_name": apps_id[i].name,
#                 # "device" : apps_id[i].platform
#         },
#

    # )



#TODO fetch all row

# for i in range(len(apps_id)):
#     print(apps_id[i])
#     rows = apps_client.fetch_agg_report_rows(
#         app_id=apps_id[i].id,
#         report=ReportType.DAILY,
#         date_from="2025-08-01",
#         date_to="2025-09-02",
#         drop_default=True,            # автоматически дропнет 'id' и 'name'
#         null_tokens=["nan", "n/a", "—", "b"],  # можно расширять/менять
#         empty_as_null=True,
#     )
#     print(rows)

# TODO in db

# result = PostgresqlAdapter.insert(
#     data=rows,
#     destination_table="daily_report",
#     destination_uri=os.getenv("PSQL_URI"),
#     schema_name="public",
#     batch_size=50000,
#     on_duplicate="update",
# )
# print(result)