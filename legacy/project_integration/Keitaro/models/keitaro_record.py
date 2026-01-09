from typing import Optional
import datetime as dt

from pydantic import BaseModel, field_validator


class KeitaroRecord(BaseModel):
    subid: Optional[str] = None
    datetime: Optional[dt.datetime] = None

    ip: Optional[str] = None
    campaign: Optional[str] = None
    stream: Optional[str] = None
    stream_id: Optional[int] = None
    offer: Optional[str] = None

    country: Optional[str] = None
    country_flag: Optional[str] = None

    sub_id_2: Optional[str] = None
    sub_id_5: Optional[str] = None

    os: Optional[str] = None
    os_version: Optional[str] = None
    browser: Optional[str] = None

    connection_type: Optional[str] = None
    device_type: Optional[str] = None
    device_model: Optional[str] = None

    is_bot: Optional[bool] = None
    is_unique: Optional[bool] = None

    sale: Optional[int] = None
    lead: Optional[int] = None

    user_agent: Optional[str] = None
    isp: Optional[str] = None
    operator: Optional[str] = None
    campaign_group: Optional[str] = None

    # ---------- validators ----------

    @field_validator("datetime", mode="before")
    @classmethod
    def parse_datetime(cls, v):
        if v in (None, "", "null", "NULL"):
            return None
        if isinstance(v, dt.datetime):
            return v
        try:
            return dt.datetime.strptime(str(v), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    @field_validator("is_bot", "is_unique", mode="before")
    @classmethod
    def parse_bool(cls, v):
        if v in (None, "", "null", "NULL"):
            return None
        if str(v).lower() in ("1", "true", "yes", "y"):
            return True
        if str(v).lower() in ("0", "false", "no", "n"):
            return False
        return None

    @field_validator("sale", "lead", "stream_id", mode="before")
    @classmethod
    def parse_int(cls, v):
        if v in (None, "", "null", "NULL"):
            return None
        try:
            return int(v)
        except Exception:
            return None

    @field_validator(
        "subid", "ip", "campaign", "stream", "offer",
        "country", "country_flag",
        "sub_id_2", "sub_id_5",
        "os", "os_version", "browser",
        "connection_type", "device_type", "device_model",
        "user_agent", "isp", "operator", "campaign_group",
        mode="before",
    )
    @classmethod
    def empty_or_nan_to_none(cls, v):
        import pandas as pd
        if v in (None, "", "null", "NULL", "N/A", "NA"):
            return None
        if isinstance(v, float) and pd.isna(v):
            return None
        return v
