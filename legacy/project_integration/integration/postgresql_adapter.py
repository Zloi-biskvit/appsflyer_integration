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
    def transfer(cls, source_uri, destination_table, query, destination_uri, destination_session=None,
                 on_duplicate=None, **kwargs):

        data = cls.extract(uri=source_uri, query=query, **kwargs)

        cls.insert(
            data=data,
            destination_session=destination_session,
            destination_uri=destination_uri,
            destination_table=destination_table,
            on_duplicate=on_duplicate,
            **kwargs
        )

    @staticmethod
    def _batch_generator(gen: Generator[dict, None, None], batch_size: int) -> Generator[List[dict], None, None]:
        """Yield batches from a generator."""
        batch = []
        for item in gen:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    @classmethod
    def insert(cls, *, data: list[dict], destination_table, destination_uri, schema_name=None,
           on_duplicate=None, mode=None, batch_size=80000, **kwargs):

        engine = cls.get_engine(destination_uri)
        affected = 0
        columns_list = []

        # ОДНА транзакция на всё: begin/commit управляется контекстом
        with engine.begin() as connection:
            # session-level настройки только локально в этой транзакции
            # connection.execute(text("SET LOCAL work_mem = '8GB'"))
            if schema_name:
                connection.execute(text(f"SET LOCAL SEARCH_PATH = {schema_name}"))

            table_object = Table(destination_table, MetaData(), autoload_with=connection)
            inspector = inspect(connection)

            columns_data = inspector.get_columns(destination_table)
            autoincrement_columns = [c['name'] for c in columns_data if c.get('autoincrement')]

            ucs = inspector.get_unique_constraints(destination_table)
            unique_constraint_names = [uc['name'] for uc in ucs]
            if unique_constraint_names:
                uc = unique_constraint_names[0]
                u_constraint_columns = next(
                    (uc_def['column_names'] for uc_def in ucs if uc_def['name'] == uc), []
                )
            else:
                u_constraint_columns = []

            pk = inspector.get_pk_constraint(destination_table)
            primary_constraint_name = pk['name']
            p_constraint_columns = pk['constrained_columns']

            constraint_columns = u_constraint_columns if u_constraint_columns else p_constraint_columns

            if isinstance(data, Generator):
                for batch in cls._batch_generator(data, 1000):
                    columns_list = list(batch[0].keys())
                    affected += cls._insert_batch(
                        data=batch,
                        destination_table=destination_table,
                        table_object=table_object,
                        autoincrement_columns=autoincrement_columns,
                        unique_constraint_names=unique_constraint_names,
                        primary_constraint_name=primary_constraint_name,
                        constraint_columns=constraint_columns,
                        connection=connection,
                        on_duplicate=on_duplicate,
                        mode=mode,
                        manage_tx=False,         # <— ВАЖНО: не начинаем транзакцию внутри
                        **kwargs,
                    )
            else:
                if data:
                    columns_list = list(data[0].keys())
                for i in range(0, len(data), batch_size):
                    batch = data[i:i+batch_size]
                    affected += cls._insert_batch(
                        data=batch,
                        destination_table=destination_table,
                        table_object=table_object,
                        autoincrement_columns=autoincrement_columns,
                        unique_constraint_names=unique_constraint_names,
                        primary_constraint_name=primary_constraint_name,
                        constraint_columns=constraint_columns,
                        connection=connection,
                        on_duplicate=on_duplicate,
                        mode=mode,
                        manage_tx=False,         # <— тут тоже
                        **kwargs,
                    )

            # здесь контекст сам сделает COMMIT

        engine.dispose()
        return {'affected_rows': affected, 'affected_columns': columns_list}

    @classmethod
    def _insert_batch_v2(cls, *, data: list[dict], destination_table, connection,
                      table_object, autoincrement_columns, unique_constraint_names, primary_constraint_name,
                      constraint_columns, on_duplicate=None, mode=None, **kwargs):

        if not data:
            logging.info("no data")
            return 0

        # Step 1: Filter out autoincrement columns and normalize rows
        columns = [col for col in data[0].keys() if col not in autoincrement_columns]

        for row in data:
            keys_to_remove = set(row.keys()) - set(columns)
            for k in keys_to_remove:
                del row[k]

        stmt = insert(table_object).values(data)  #on_duplicate == 'no_check' or on_duplicate is None
        if on_duplicate in ('update', 'partial_update', 'ignore'):


            if not constraint_columns:
                raise ValueError("No unique or primary key columns available for diffing.")

            logging.info(f"Using constraint columns: {constraint_columns}")

            # Step 3: Query existing rows for those keys
            def make_key(row):
                return tuple(row[col] for col in constraint_columns)

            # input_keys = [make_key(row) for row in data]
            key_cols_clause = ' AND '.join([
                f"{col} = :{col}" for col in constraint_columns
            ])

            # Flatten unique keys to dicts for SQL query
            keys_set = {make_key(row) for row in data}
            keys_list = [dict(zip(constraint_columns, key)) for key in keys_set]

            if not keys_list:
                return 0

            # Build where clause for composite keys
            where_conditions = " OR ".join(["(" + key_cols_clause + ")" for _ in keys_list])

            # flattened_params = []
            # for key_dict in keys_list:
            #     flattened_params.extend(key_dict.values())

            select_sql = f"""
                SELECT {', '.join(columns)}
                FROM {destination_table}
                WHERE {where_conditions}
            """
            result = connection.execute(text(select_sql), keys_list).mappings().all()
            existing = {make_key(row): row for row in result}

            logging.info(f"Found {len(existing)} matching rows in target table.")

            # Step 4: Compare rows by hash
            def row_hash(row):
                return hash(tuple(str(row[col]) for col in columns))

            new_rows = []
            for row in data:
                key = make_key(row)
                if key not in existing:
                    new_rows.append(row)
                else:
                    existing_row = existing[key]
                    if row_hash(row) != row_hash(existing_row):
                        new_rows.append(row)

            logging.info(f"{len(new_rows)} new or changed rows to insert.")

            if not new_rows:
                return 0

            # Step 5: Insert changed rows
            stmt = insert(table_object).values(new_rows)

            if on_duplicate == 'update':
                update_fields = stmt.excluded
                update_fields['meta_updated_at'] = func.current_timestamp()
                stmt = stmt.on_conflict_do_update(
                    constraint=unique_constraint_names[0] if unique_constraint_names else primary_constraint_name,
                    set_=update_fields
                )

            elif on_duplicate == 'partial_update':
                update_fields = {col: stmt.excluded[col] for col in columns}
                update_fields['meta_updated_at'] = func.current_timestamp()
                stmt = stmt.on_conflict_do_update(
                    constraint=unique_constraint_names[0] if unique_constraint_names else primary_constraint_name,
                    set_=update_fields
                )
            elif on_duplicate == 'ignore':
                stmt = stmt.on_conflict_do_nothing()

        elif on_duplicate == 'scd2' or on_duplicate == 'append':
            # within one transaction:
            # 1. insert new rows
            # 2. update existing rows with the same business key, set valid_to to current date
            # and set is_current to false

            # columns valid_from, valid_to, is_current, business_key, version, version_hash should be added

            # create constraint in the db
            # CREATE UNIQUE INDEX unique_current_record
            # ON dimension_table (business_key)
            # WHERE is_current = 1;
            # get business_key list from data

            # # check if current data already has the same meta_version_hash
            # select_query = f"""
            #     SELECT business_key
            #     FROM {destination_table}
            #     WHERE meta_is_current = 1
            #       AND meta_version_hash in {tuple(row['meta_version_hash'] for row in batch)}
            # """
            #
            # not_changed = connection.execute(text(rendered_query)).mappings().all()

            # implemented simultaneously two behaviors:
            #  1) insert version A - add row, then version B - add row, then version B - nothing
            #  1) insert version A - add row, then version B - add row, then version A - add row

            business_keys = [row['business_key'] for row in data]
            hashes = [row['meta_version_hash'] for row in data]

            business_keys_query = tuple(business_keys) if len(business_keys) > 1 else f"('{business_keys[0]}')"

            logging.info(f'business keys: {len(business_keys)}, unique: {len(set(business_keys))}')
            logging.info(f'hashes: {len(hashes)}, unique: {len(set(hashes))}')

            # select last hash for each business key
            select_query = f"""
                    SELECT business_key, meta_version_hash, meta_valid_from, meta_version_number
                    FROM {destination_table}
                    WHERE business_key in {business_keys_query}
                        AND meta_is_current = 1
                """
            last_hash = connection.execute(text(select_query)).mappings().all()
            last_hashes = set([row['meta_version_hash'] for row in last_hash])

            logging.info(f'row for business keys extracted: {len(last_hash)}')

            # exclude from batch rows with the same hash
            data = [row for row in data if row['meta_version_hash'] not in last_hashes]
            logging.info(f'batch after excluding existing hashes: {len(data)}')
            # scd2_counter += len(data)

            if not data:
                logging.info('no new data')
                return 0

            last_version = {row['business_key']: row['meta_version_number'] for row in last_hash}
            for row in data:
                row['meta_version_number'] = last_version.get(row['business_key'], 0) + 1

            if mode == 'backfill':
                # set valid from to the valid from of current version + 1 second
                last_row_dates = {row['business_key']: row['meta_valid_from'] for row in last_hash}

                for row in data:
                    row['meta_valid_from'] = last_row_dates.get(row['business_key'],
                                                                row['created_at']) + timedelta(seconds=1)

            business_keys_update_query = (
                tuple([row['business_key'] for row in data])
                if len(data) > 1
                else f"('{data[0]['business_key']}')"
            )

            update_query = f"""
                    UPDATE {destination_table}
                    SET meta_is_current = 0, 
                        meta_valid_to = CURRENT_TIMESTAMP
                    WHERE business_key in {business_keys_update_query}
                      AND meta_is_current = 1;
                """

            stmt = insert(table_object).values(data)

        # begin transaction
        transaction = connection.begin()
        # perform insert and update in a single transaction
        try:
            if on_duplicate == 'scd2' or on_duplicate == 'append':
                # logging.info('update existing rows')
                # update existing rows
                connection.execute(text(update_query))
            # insert new rows
            # logging.info(' execute insert statement')
            result = connection.execute(stmt)
            transaction.commit()
        except Exception as e:
            # print('problem')
            transaction.rollback()
            logging.error(f'Error: {e}')
            raise e

            # # execute inserting
            # connection.execute(stmt)
        return result.rowcount

    @classmethod
    def _insert_batch(cls, *, data: list[dict], destination_table, connection,
                      table_object, autoincrement_columns, unique_constraint_names, primary_constraint_name,
                      on_duplicate=None, mode=None, **kwargs):
        if data:
            columns = list(data[0].keys())
            columns = [col for col in columns if col not in autoincrement_columns]

            for row in data:
                for column_to_delete in [column for column in row.keys() if column not in columns]:
                    del row[column_to_delete]

            stmt = insert(table_object).values(data)
            if on_duplicate == 'update':
                stmt = stmt.on_conflict_do_update(
                    constraint=unique_constraint_names[0] if unique_constraint_names else primary_constraint_name,
                    set_=stmt.excluded
                )
            elif on_duplicate == 'ignore':
                stmt = stmt.on_conflict_do_nothing()
            elif on_duplicate == 'no_check' or on_duplicate is None:
                pass
            elif on_duplicate == 'partial_update':
                stmt = stmt.on_conflict_do_update(
                    constraint=unique_constraint_names[0] if unique_constraint_names else primary_constraint_name,
                    set_={col: stmt.excluded[col] for col in columns}
                )

            elif on_duplicate == 'scd2' or on_duplicate == 'append':
                # within one transaction:
                # 1. insert new rows
                # 2. update existing rows with the same business key, set valid_to to current date
                # and set is_current to false

                # columns valid_from, valid_to, is_current, business_key, version, version_hash should be added

                # create constraint in the db
                # CREATE UNIQUE INDEX unique_current_record
                # ON dimension_table (business_key)
                # WHERE is_current = 1;
                # get business_key list from data

                # # check if current data already has the same meta_version_hash
                # select_query = f"""
                #     SELECT business_key
                #     FROM {destination_table}
                #     WHERE meta_is_current = 1
                #       AND meta_version_hash in {tuple(row['meta_version_hash'] for row in batch)}
                # """
                #
                # not_changed = connection.execute(text(rendered_query)).mappings().all()

                # implemented simultaneously two behaviors:
                #  1) insert version A - add row, then version B - add row, then version B - nothing
                #  1) insert version A - add row, then version B - add row, then version A - add row

                business_keys = [row['business_key'] for row in data]
                hashes = [row['meta_version_hash'] for row in data]

                business_keys_query = tuple(business_keys) if len(business_keys) > 1 else f"('{business_keys[0]}')"

                logging.info(f'business keys: {len(business_keys)}, unique: {len(set(business_keys))}')
                logging.info(f'hashes: {len(hashes)}, unique: {len(set(hashes))}')

                # select last hash for each business key
                select_query = f"""
                    SELECT business_key, meta_version_hash, meta_valid_from, meta_version_number
                    FROM {destination_table}
                    WHERE business_key in {business_keys_query}
                        AND meta_is_current = 1
                """
                last_hash = connection.execute(text(select_query)).mappings().all()
                last_hashes = set([row['meta_version_hash'] for row in last_hash])

                logging.info(f'row for business keys extracted: {len(last_hash)}')

                # exclude from batch rows with the same hash
                data = [row for row in data if row['meta_version_hash'] not in last_hashes]
                logging.info(f'batch after excluding existing hashes: {len(data)}')
                # scd2_counter += len(data)

                if not data:
                    logging.info('no new data')
                    return 0

                last_version = {row['business_key']: row['meta_version_number'] for row in last_hash}
                for row in data:
                    row['meta_version_number'] = last_version.get(row['business_key'], 0) + 1

                if mode == 'backfill':
                    # set valid from to the valid from of current version + 1 second
                    last_row_dates = {row['business_key']: row['meta_valid_from'] for row in last_hash}

                    for row in data:
                        row['meta_valid_from'] = last_row_dates.get(row['business_key'],
                                                                    row['created_at']) + timedelta(seconds=1)

                business_keys_update_query = (
                    tuple([row['business_key'] for row in data])
                    if len(data) > 1
                    else f"('{data[0]['business_key']}')"
                )

                update_query = f"""
                    UPDATE {destination_table}
                    SET meta_is_current = 0, 
                        meta_valid_to = CURRENT_TIMESTAMP
                    WHERE business_key in {business_keys_update_query}
                      AND meta_is_current = 1;
                """

                stmt = insert(table_object).values(data)
            # print(stmt)
            # begin transaction
            # transaction = connection.begin()
            # perform insert and update in a single transaction
            try:
                if on_duplicate == 'scd2' or on_duplicate == 'append':
                    # logging.info('update existing rows')
                    # update existing rows
                    connection.execute(text(update_query))
                # insert new rows
                # logging.info(' execute insert statement')
                connection.execute(stmt)
                # transaction.commit()
            except Exception as e:
                # print('problem')
                # transaction.rollback()
                logging.error(f'Error: {e}')
                raise e

                # # execute inserting
                # connection.execute(stmt)

        else:
            logging.info('no data')

        return len(data)

    @classmethod
    def generate(cls, *, query, destination_table, destination_uri, template, schema_name=None, on_duplicate=None,
                 batch_size=80000, **kwargs):

        # method to execute insert from select query
        # steps:
        # 1. get template by template_path
        # 2. render template with kwargs
        # 3. wrap rendered query with insert from select query using sqlalchemy
        # 4. add on_duplicate logic using on_conflict_do_update or on_conflict_do_nothing
        #   a. exclude autoincrement columns from insert statement
        #       1. get table columns
        #       2. get autoincrement columns
        #       3. get one row from the query result in order to get column names
        # 5. execute query

        # rendered_query = query

        # get engine
        engine = cls.get_engine(destination_uri)
        connection = engine.connect()

        try:

            # set work_mem
            connection.execute(text("SET work_mem = '8GB'"))
            logging.info('set work_mem to 8GB')

            # set search_path if schema_name is provided
            if schema_name:
                logging.info(f'schema_name: {schema_name}')
                connection.execute(text(f'SET search_path TO {schema_name};'))

            # execute the rendered query and fetch one row to get column names
            kwargs.update({'offset': 0, 'limit': 1})
            one_row_query = Template(template).render(kwargs)
            result = connection.execute(text(one_row_query))
            logging.info('one row query executed to get column names')
            columns = list(result.keys())
            # columns_object = result.keys()

            search_path = connection.execute(text('show search_path;')).mappings().all()
            search_path = [dict(row) for row in search_path]
            logging.info(f'search_path: {search_path}')

            table = Table(destination_table, MetaData(), autoload_with=connection)

            # exclude autoincrement columns from insert statement
            inspector = inspect(connection)
            columns_data = inspector.get_columns(destination_table)
            autoincrement_columns = [col['name'] for col in columns_data if col.get('autoincrement')]

            columns = [col for col in columns if col not in autoincrement_columns]

            # remove first keyword select/SELECT from rendered query. workaround for sqlalchemy select,
            # sqlalchemy select() always adds select keyword at the beginning of the query
            if template.startswith('SELECT'):
                template = template[template.index('SELECT') + 6:]
            elif template.startswith('select'):
                template = template[template.index('select') + 6:]
            # remove spaces in the end of the query
            template = template.strip()
            # remove ; in the end of the query
            if template.endswith(';'):
                template = template[:-1]

            # batch_size = 80000
            offset = 0
            counter = 0
            while True:
                kwargs.update({'offset': offset, 'limit': batch_size})
                batch_query = Template(template).render(kwargs)
                logging.info(f'OFFSET {offset} LIMIT {batch_size}')
                logging.info(batch_query)
                # construct insert statement
                insert_stmt = insert(table).from_select(columns, select(text(batch_query)))
                # add on_duplicate logic
                if on_duplicate == 'update':
                    insert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=[col for col in table.primary_key.columns.values()],
                        set_={col: insert_stmt.excluded[col] for col in columns}
                    )
                elif on_duplicate == 'ignore':
                    insert_stmt = insert_stmt.on_conflict_do_nothing()
                elif on_duplicate == 'partial_update':
                    unique_constraints = inspector.get_unique_constraints(destination_table)
                    unique_constraint_names = [uc['name'] for uc in unique_constraints]
                    primary_key_constraint = inspector.get_pk_constraint(destination_table)
                    primary_constraint_name = primary_key_constraint['name']
                    insert_stmt = insert_stmt.on_conflict_do_update(
                        constraint=unique_constraint_names[0] if unique_constraint_names else primary_constraint_name,
                        set_={col: insert_stmt.excluded[col] for col in columns}
                    )
                row_cnt = connection.execute(insert_stmt)
                affected_rows = row_cnt.rowcount  # Количество затронутых строк
                counter += affected_rows
                logging.info(f'Affected rows: {affected_rows}')
                if affected_rows == 0:
                    logging.info('Affected rows is 0, no new data')
                    break
                # На случай когда в template отсутствуют эти параметры, что бы избежать бесконечного цикла
                elif '{{ offset }}' not in template or '{{ limit }}' not in template:
                    logging.info('Without offset parameters')
                    break
                offset += batch_size
        finally:
            connection.close()
            engine.dispose()

        logging.info(f'successful execution of insert from select query')

        return {'affected_rows': counter, 'affected_columns': columns}

    @classmethod
    def perform_query(cls, *, query, uri, schema_name=None, **kwargs):

        logging.info(f'connection uri: {uri}')

        rendered_query = query
        engine = cls.get_engine(uri)

        connection = engine.connect()

        try:

            # set work_mem
            connection.execute(text("SET work_mem = '8GB'"))
            logging.info('set work_mem to 8GB')

            transaction = connection.begin()

            if schema_name:
                connection.execute(text(f'SET search_path TO {schema_name};'))

            search_path = connection.execute(text('show search_path;')).mappings().all()
            search_path = [dict(row) for row in search_path]
            logging.info(f'search_path: {search_path}')

            result = connection.execute(text(rendered_query))
            # Attempt to fetch rows if the query used RETURNING
            # try:
            #     affected_columns = list(result.keys())
            # except Exception as e:
            # Likely no RETURNING clause was used
            affected_columns = []
            transaction.commit()

        finally:
            connection.close()
            engine.dispose()

        logging.info(f'affected rows: {result.rowcount}')

        return {'affected_rows': result.rowcount, 'affected_columns': affected_columns}

    @classmethod
    def extract(
            cls,
            *,
            query,
            uri,
            generator=False,
            source_schema_name=None,
            **kwargs
    ) -> Union[list[dict], Generator[dict, None, None]]:
        logging.info(f'connection uri: {uri}')
        rendered_query = query
        engine = cls.get_engine(uri)

        if generator:
            logging.info(f'generator mode: {generator}')
            return cls._extract_generator(engine, rendered_query, source_schema_name=source_schema_name)
        else:
            return cls._extract_all(engine, rendered_query, source_schema_name=source_schema_name)

    @staticmethod
    def _extract_all(engine, query, source_schema_name=None) -> list[dict]:
        with engine.connect() as connection:
            # set work_mem
            connection.execute(text("SET work_mem = '8GB'"))
            logging.info('set work_mem to 8GB')
            # Optional: adjust schema path
            if source_schema_name:
                connection.execute(text(f'SET search_path TO {source_schema_name};'))

            # Debug the current search path
            search_path = connection.execute(text('show search_path;')).mappings().all()
            logging.info(f"Current search_path: {[dict(row) for row in search_path]}")

            data = connection.execute(text(query)).mappings().all()
            data = [dict(row) for row in data]

            logging.info(f'extracted rows: {len(data)}')
            return data

    @staticmethod
    def _extract_generator(engine, query, source_schema_name=None) -> Generator[dict, None, None]:
        row_count = 0
        with engine.connect() as connection:
            # set work_mem
            connection.execute(text("SET work_mem = '8GB'"))
            logging.info('set work_mem to 8GB')
            # Optional: adjust schema path
            if source_schema_name:
                connection.execute(text(f'SET search_path TO {source_schema_name};'))
            result = connection.execution_options(stream_results=True).execute(text(query))
            for row in result:
                row_count += 1
                yield dict(row)
        logging.info(f'successfully extracted rows: {row_count}')

    @classmethod
    def sync_schema(cls, source_uri, target_uri, tables_to_sync, schema_name=None):

        # Create engines
        source_engine = cls.get_engine(source_uri)
        target_engine = cls.get_engine(target_uri)

        # Metadata
        source_metadata = MetaData()
        target_metadata = MetaData()

        # Reflect tables
        # source_metadata.reflect(source_engine, only=tables_to_sync)
        # target_metadata.reflect(target_engine, only=tables_to_sync)

        # Sync columns for each table
        for table_name in tables_to_sync:
            source_table = Table(table_name, source_metadata, autoload_with=source_engine)
            target_table = Table(table_name, target_metadata, autoload_with=target_engine)

            source_columns = {col.name: col for col in source_table.columns}
            target_columns = {col.name: col for col in target_table.columns}

            missing_columns = set(source_columns.keys()) - set(target_columns.keys())
            if missing_columns:
                # Add missing columns to the target table
                with target_engine.connect() as conn:
                    for column_name in missing_columns:
                        column = source_columns[column_name]
                        add_column_ddl = f'ALTER TABLE "{table_name}" ADD COLUMN {column.name} {column.type}'

                        conn.execute(add_column_ddl)
                        print(f'Added column {column.name} to {table_name}')

        print("Columns synced successfully")

    @classmethod
    def get_constrained_columns(cls, destination_table, destination_uri, schema_name=None) -> list[str]:
        engine = cls.get_engine(destination_uri)
        connection = engine.connect()

        try:
            if schema_name:
                connection.execute(text(f"SET SEARCH_PATH = {schema_name}"))

            inspector = inspect(connection)

            # unique constraint columns
            unique_constraints = inspector.get_unique_constraints(destination_table)
            constrained_columns = unique_constraints[0]['column_names'] if unique_constraints else []
            if not constrained_columns:
                columns_data = inspector.get_columns(destination_table)
                constrained_columns = [col['name'] for col in columns_data if col.get('primary_key')]

            print('constrained_columns:', constrained_columns)

            return constrained_columns

        finally:
            connection.close()
            engine.dispose()
