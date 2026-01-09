from Keitaro.models.keitaro_record import KeitaroRecord
from Keitaro.db.postgresql_adapter import PostgresqlAdapter



class KeitaroRepository:
    def __init__(self):
        self.adapter = PostgresqlAdapter()

    def insert_many(self, records: list[KeitaroRecord]):
        if not records:
            return

        rows = [record.model_dump() for record in records]

        # Получаем список всех колонок из первой строки
        columns = list(rows[0].keys())

        # Подготавливаем VALUES для SQL
        values = [
            tuple(row.get(col) for col in columns)
            for row in rows
        ]

        # Генерируем SQL-запрос
        sql = f"""
        INSERT INTO keitaro_clicks ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        """

        print(f"[DB] Inserting {len(values)} rows into keitaro_clicks...")
        self.adapter.execute_batch(sql, values)
        print(f"[DB] Insert completed ✅")
