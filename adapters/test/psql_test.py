from adapters.postgresql_adapter import PostgresqlAdapter

from dotenv import load_dotenv
import os
from pathlib import Path
env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

uri = os.getenv('PSQL_URI')
query = 'select * from public.daily_report dr'

client = PostgresqlAdapter().extract(uri=uri, query=query)

print(client)