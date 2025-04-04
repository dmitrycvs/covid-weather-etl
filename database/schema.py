from sqlglot import parse_one
import psycopg2
from .utils import execute_query, connect_db

schema_name = "log"

country_sql = f"""
CREATE TABLE {schema_name}.country (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT NOT NULL
)
"""

api_sql = f"""
CREATE TABLE {schema_name}.api (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL
)
"""

import_logs_sql = f"""
CREATE TABLE {schema_name}.import_logs (
    id SERIAL PRIMARY KEY,
    country_id INTEGER NOT NULL REFERENCES {schema_name}.country(id) ON DELETE CASCADE,
    batch_timestamp BIGINT NOT NULL,
    import_directory_name TEXT,
    import_file_name TEXT,
    file_created_date TIMESTAMP,
    file_last_modified_date TIMESTAMP,
    start_backfill_date TIMESTAMP,
    end_backfill_date TIMESTAMP
)
"""

api_import_logs_sql = f"""
CREATE TABLE {schema_name}.api_import_logs (
    id SERIAL PRIMARY KEY,
    country_id INTEGER NOT NULL REFERENCES {schema_name}.country(id) ON DELETE CASCADE,
    api_id INTEGER NOT NULL REFERENCES {schema_name}.api(id) ON DELETE CASCADE,
    import_logs_id INTEGER REFERENCES {schema_name}.import_logs(id) ON DELETE SET NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    code_response INTEGER NOT NULL,
    error_message TEXT
)
"""

schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
execute_query(schema_sql)

tables = {
    "country": parse_one(country_sql),
    "api": parse_one(api_sql),
    "import_logs": parse_one(import_logs_sql),
    "api_import_logs": parse_one(api_import_logs_sql)
}

def generate_tables(logger):
    for table_id, table_expr in tables.items():
        table_name = table_expr.this.name or table_id
        
        try:
            sql = table_expr.sql()
            execute_query(sql)
            logger.info(f"Table {table_name} created successfully!")
        except psycopg2.errors.DuplicateTable:
            logger.info(f"Table {table_name} already exists, skipping creation.")
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
            conn = connect_db()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            conn.rollback()
    
    logger.info("Schema generation completed.")


