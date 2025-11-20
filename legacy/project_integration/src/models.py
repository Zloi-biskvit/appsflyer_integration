from typing import Optional
from pydantic import BaseModel, field_validator
from datetime import datetime


class AppsFlyerRecord(BaseModel):
    date: Optional[datetime] = None
    country: Optional[str] = None
    agency_pmd: Optional[str] = None
    media_source: Optional[str] = None
    campaign: Optional[str] = None
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    ctr: Optional[str] = None
    installs: Optional[int] = None
    conversion_rate: Optional[str] = None
    sessions: Optional[int] = None
    loyal_users: Optional[int] = None
    loyal_users_per_install: Optional[float] = None
    total_revenue: Optional[float] = None
    total_cost: Optional[float] = None
    roi: Optional[float] = None
    arpu: Optional[float] = None
    avg_ecpi: Optional[float] = None
    af_complete_registration_unique_users: Optional[int] = None
    af_complete_registration_event_counter: Optional[int] = None
    af_complete_registration_sales_usd: Optional[float] = None
    af_purchase_unique_users: Optional[int] = None
    af_purchase_event_counter: Optional[int] = None
    af_purchase_sales_usd: Optional[float] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    app_platform: Optional[str] = None
    report_type: Optional[str] = None

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
        if v in (None, "", "N/A", "NA", "-", "null", "Null", "NULL"):
            return None
        return v
