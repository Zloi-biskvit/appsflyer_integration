import os
from dotenv import load_dotenv

load_dotenv()  # загружает переменные окружения из .env (если используешь)

def load_config() -> dict:
    return {
        "POSTGRESQL_URL": os.getenv("POSTGRESQL_URL", "postgresql://user:password@localhost:5432/your_db_name"),
        "DESTINATION_TABLE": os.getenv("DESTINATION_TABLE", "keitaro_clicks"),
    }