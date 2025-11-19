from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
import json

CONFIG_PATH = Path("config.json")


@dataclass
class AppConfig:
    id: str
    name: str
    platform: str


@dataclass
class Config:
    apps: List[AppConfig]
    api_token: str
    agg_report_types: List[str]
    from_date: str
    to_date: str
    timezone: str
    retargeting: str
    destination_table: str
    destination_uri: str


def load_config(path: Path = CONFIG_PATH) -> Config:
    if not path.exists():
        raise RuntimeError(f"Config file not found: {path}")

    raw: Dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))

    apps = [AppConfig(**a) for a in raw.get("apps", [])]
    if not apps:
        raise RuntimeError("`apps` in config.json is empty")

    return Config(
        apps=apps,
        api_token=raw["api_token"],
        agg_report_types=raw.get("agg_report_types", ["geo_by_date_report"]),
        from_date=raw["from_date"],
        to_date=raw["to_date"],
        timezone=raw.get("timezone", "UTC"),
        retargeting=raw.get("retargeting", "false"),
        destination_table=raw["destination_table"],
        destination_uri=raw["destination_uri"],
    )
