from adapters.postgresql_adapter import PostgresqlAdapter
from service.appsflyer_service import AppsFlyerClient, ReportType, AppsFlyerError

from dotenv import load_dotenv
import os
from pathlib import Path
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)
from datetime import date, timedelta

token = os.getenv("APPSFLYER_API_TOKEN")
url = os.getenv("APPSFLYER_BASE_URL")
uri = os.getenv("POSTGRES_URI")

apps_client = AppsFlyerClient(api_token=token, base_url=url)

rows = apps_client.fetch_agg_report_rows(
    app_id="id6747419978",
    report=ReportType.DAILY,
    date_from="2025-08-01",
    date_to="2025-09-02",
    columns_mapping={
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
        "Average eCPI":"average_ecpi"
    },
    drop_default=True,            # автоматически дропнет 'id' и 'name'
    null_tokens={"nan", "n/a", "—", "b"},  # можно расширять/менять
    empty_as_null=True,
)

result = PostgresqlAdapter.insert(
    data=rows,
    destination_table="daily_report",
    destination_uri=os.getenv("PSQL_URI"),
    schema_name="public",
    batch_size=50000,
    on_duplicate="update",
)
print(result)