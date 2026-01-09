from pathlib import Path
from Keitaro.loaders.csv_loader import KeitaroCSVLoader
from Keitaro.db.repository import KeitaroRepository
from Keitaro.models.keitaro_record import KeitaroRecord


class KeitaroService:
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.loader = KeitaroCSVLoader(csv_path)
        self.repository = KeitaroRepository()

    def run(self):
        print(f"[SERVICE] Starting Keitaro import from {self.csv_path}")

        records: list[KeitaroRecord] = self.loader.load()
        if not records:
            print("[SERVICE] No records loaded, stopping")
            return

        print(f"[SERVICE] Loaded {len(records)} records. Inserting to DB...")

        self.repository.insert_many(records)

        print("[SERVICE] Keitaro import finished successfully ✅")
