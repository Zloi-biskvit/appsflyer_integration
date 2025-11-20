from __future__ import annotations

import logging
from datetime import timedelta
from typing import Generator, List, Union

from sqlalchemy import create_engine, text, Table, MetaData, inspect, select, func
from sqlalchemy.dialects.postgresql import insert
# from sqlalchemy.engine import Result
# from sqlalchemy.schema import AddConstraint, DropConstraint
from jinja2 import Template


# noinspection PyUnusedLocal
class PostgresqlAdapter:
    """
    Implements logic to interact with mysql database
    """

    @classmethod
    def get_engine(cls, uri):
        uri = uri.replace('postgres://', 'postgresql://')
        engine = create_engine(uri, connect_args={'connect_timeout': 1800}, json_serializer=lambda x: x,
                               hide_parameters=True)
        # By default, SQLAlchemy will automatically cll json.dumps on values assigned to a JSON or JSONB column, so it
        # isn't necessary to call it yourself - in fact this will lead to double-encoded values as seen in the question.

        # if 'postgresql' in uri:
        # with engine.connect() as connection:
        #     schemas_exclude = {'public', 'information_schema', 'pg_catalog', 'pg_toast'}
        #     schemas_result = connection.execute(text('SELECT schema_name FROM information_schema.schemata;')).all()
        #     schemas = set(row['schema_name'] for row in list(map(dict, schemas_result)))
        #     search_path = (schemas_exclude ^ schemas) & schemas
        #     result = connection.execute(text(f"SET SEARCH_PATH = {','.join(search_path)}, '$user';"))

        return engine

    @classmethod
    def insert(
        cls,
        *,
        data: list[dict],
        destination_table: str,
        destination_uri: str,
        schema_name: str | None = None,
        on_duplicate: str | None = None,
        mode: str | None = None,
        batch_size: int = 80000,
        **kwargs,
    ):
        """
        Универсальный insert с поддержкой батчей и on_duplicate.
        data: список dict (ключи = имена колонок в таблице).
        on_duplicate:
            - None / 'no_check'  -> обычный INSERT, пусть БД сама ругается на дубликаты
            - 'ignore'           -> ON CONFLICT DO NOTHING
            - 'update'           -> ON CONFLICT DO UPDATE SET ... = EXCLUDED....
        """
        if not data:
            logging.info("PostgresqlAdapter.insert: no data to insert")
            return {"affected_rows": 0, "affected_columns": []}

        engine = cls.get_engine(destination_uri)
        affected = 0
        columns_list: list[str] = []

        with engine.begin() as connection:
            # schema / search_path
            if schema_name:
                connection.execute(text(f"SET LOCAL SEARCH_PATH = {schema_name}"))

            metadata = MetaData()
            table_object = Table(destination_table, metadata, autoload_with=connection)
            inspector = inspect(connection)

            # autoincrement / PK / уникальный constraint
            columns_data = inspector.get_columns(destination_table)
            autoincrement_columns = [c["name"] for c in columns_data if c.get("autoincrement")]

            ucs = inspector.get_unique_constraints(destination_table)
            unique_constraint_name = ucs[0]["name"] if ucs else None

            pk = inspector.get_pk_constraint(destination_table)
            primary_constraint_name = pk.get("name")
            pk_columns = pk.get("constrained_columns") or []

            constraint_name = unique_constraint_name or primary_constraint_name

            if not constraint_name and on_duplicate in ("update", "partial_update", "ignore"):
                raise ValueError(
                    "No unique or primary key constraint found, but on_duplicate requires a constraint"
                )

            # полное множество колонок, которые реально передаём (без автоинкремента)
            all_keys = list(data[0].keys())
            insert_columns = [c for c in all_keys if c not in autoincrement_columns]
            columns_list = insert_columns

            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                if not batch:
                    continue

                # нормализуем батч: только нужные колонки
                batch_rows: list[dict] = []
                for row in batch:
                    # выкидываем колонки, которых нет в insert_columns
                    normalized_row = {col: row.get(col) for col in insert_columns}
                    batch_rows.append(normalized_row)

                # строим INSERT без .values(...)
                stmt = insert(table_object)

                if on_duplicate == "update":
                    # обновляем все вставляемые колонки (кроме автогенерируемых)
                    update_fields = {col: stmt.excluded[col] for col in insert_columns}
                    stmt = stmt.on_conflict_do_update(
                        constraint=constraint_name,
                        set_=update_fields,
                    )
                elif on_duplicate == "ignore":
                    stmt = stmt.on_conflict_do_nothing()
                elif on_duplicate in ("no_check", None):
                    # обычный insert, без on_conflict
                    pass
                else:
                    raise ValueError(f"Unsupported on_duplicate mode: {on_duplicate}")

                # executemany: SQLAlchemy сам создаёт bind-параметры под dict
                result = connection.execute(stmt, batch_rows)
                if result.rowcount is not None:
                    affected += result.rowcount

        engine.dispose()
        return {"affected_rows": affected, "affected_columns": columns_list}
