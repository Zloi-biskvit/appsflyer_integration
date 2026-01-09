import psycopg2
import psycopg2.extras
from config import config  # Убедись, что config возвращает словарь с параметрами подключения


class PostgresqlAdapter:
    def __init__(self):
        self.connection = psycopg2.connect(**config())

    def execute_batch(self, query: str, values: list[tuple]):
        with self.connection.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, query, values)
        self.connection.commit()
