from typing import Optional
from pydantic import BaseModel, field_validator
from datetime import datetime


class AppsFlyerRecord(BaseModel):
    date: Optional[datetime]
    country: Optional[str]
    agency_pmd: Optional[str]
    media_source: Optional[str]
    campaign: Optional[str]

    impressions: Optional[int]
    clicks: Optional[int]
    ctr: Optional[str]
    installs: Optional[int]
    conversion_rate: Optional[str]
    sessions: Optional[int]
    loyal_users: Optional[int]
    loyal_users_per_install: Optional[float]
    total_revenue: Optional[float]
    total_cost: Optional[float]
    roi: Optional[float]
    arpu: Optional[float]
    avg_ecpi: Optional[float]

    af_complete_registration_unique_users: Optional[int]
    af_complete_registration_event_counter: Optional[int]
    af_complete_registration_sales_usd: Optional[float]

    af_purchase_unique_users: Optional[int]
    af_purchase_event_counter: Optional[int]
    af_purchase_sales_usd: Optional[float]

    app_id: Optional[str]
    app_name: Optional[str]
    app_platform: Optional[str]
    report_type: Optional[str]

    @field_validator(
        "impressions",
        "clicks",
        "installs",
        "sessions",
        "loyal_users",
        "loyal_users_per_install",
        "total_revenue",
        "total_cost",
        "roi",
        "arpu",
        "avg_ecpi",
        "af_complete_registration_unique_users",
        "af_complete_registration_event_counter",
        "af_complete_registration_sales_usd",
        "af_purchase_unique_users",
        "af_purchase_event_counter",
        "af_purchase_sales_usd",
        mode="before",
    )
    @classmethod
    def empty_or_na_to_none(cls, v):
        """
        Конвертирует 'N/A', 'NA', '-', '' -> None перед разбором числа.
        """
        if v in (None, "", "N/A", "NA", "-", "null", "Null", "NULL"):
            return None
        return v
