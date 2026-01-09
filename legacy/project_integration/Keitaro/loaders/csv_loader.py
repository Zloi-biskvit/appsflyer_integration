import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

from Keitaro.models.keitaro_record import KeitaroRecord


# ЯВНЫЙ МАППИНГ: CSV -> модель
CSV_COLUMNS_MAP: Dict[str, str] = {
    "Дата и время": "datetime",
    "Subid": "subid",
    "IP": "ip",
    "Кампания": "campaign",
    "Поток": "stream",
    "ID потока": "stream_id",
    "Оффер": "offer",
    "Страна": "country",
    "Флаг страны": "country_flag",
    "Sub ID 2": "sub_id_2",
    "Sub ID 5": "sub_id_5",
    "ОС": "os",
    "Версия ОС": "os_version",
    "Браузер": "browser",
    "Тип соединения": "connection_type",
    "Тип устройства": "device_type",
    "Модель устройства": "device_model",
    "Бот": "is_bot",
    "Уник. (кампания)": "is_unique",
    "Продажа": "sale",
    "Лид": "lead",
    "User Agent": "user_agent",
    "Провайдер": "isp",
    "Оператор": "operator",
    "Группа кампании": "campaign_group",
}


class KeitaroCSVLoader:
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path

    def load(self) -> List[KeitaroRecord]:
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

        df = pd.read_csv(
            self.csv_path,
            sep=";",
            quotechar='"',
            encoding="utf-8",
        )

        records: List[KeitaroRecord] = []

        for idx, row in enumerate(df.to_dict(orient="records"), start=1):
            try:
                mapped: Dict[str, Any] = {}

                for csv_col, model_field in CSV_COLUMNS_MAP.items():
                    mapped[model_field] = row.get(csv_col)

                record = KeitaroRecord(**mapped)
                records.append(record)

            except Exception as e:
                # Принципиально НЕ роняем весь процесс
                print(f"[CSV][ROW {idx}] parse error: {e}")

        print(f"[CSV] Loaded {len(records)} records from {self.csv_path.name}")
        return records
