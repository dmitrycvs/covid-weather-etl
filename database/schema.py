from sqlglot import parse_one
from .utils import execute_query

extract_schema_name = "extract"
transform_schema_name = "transform"
load_schema_name = "load"

country_sql = f"""
CREATE TABLE IF NOT EXISTS {extract_schema_name}.country (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT NOT NULL
)
"""

api_sql = f"""
CREATE TABLE IF NOT EXISTS {extract_schema_name}.api (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL
)
"""

import_logs_sql = f"""
CREATE TABLE IF NOT EXISTS {extract_schema_name}.import_logs (
    id SERIAL PRIMARY KEY,
    country_id INTEGER NOT NULL REFERENCES {extract_schema_name}.country(id) ON DELETE CASCADE,
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
CREATE TABLE IF NOT EXISTS {extract_schema_name}.api_import_logs (
    id SERIAL PRIMARY KEY,
    api_id INTEGER NOT NULL REFERENCES {extract_schema_name}.api(id) ON DELETE CASCADE,
    import_logs_id INTEGER REFERENCES {extract_schema_name}.import_logs(id) ON DELETE SET NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    code_response INTEGER NOT NULL,
    error_message TEXT
)
"""

transform_logs_sql = f"""
CREATE TABLE IF NOT EXISTS {transform_schema_name}.logs (
    id SERIAL PRIMARY KEY,
    import_logs_id INTEGER UNIQUE NOT NULL REFERENCES {extract_schema_name}.import_logs(id) ON DELETE CASCADE,
    processed_directory_name TEXT NOT NULL,
    processed_file_name TEXT NOT NULL,
    status VARCHAR(15) NOT NULL
)
"""

load_logs_sql = f"""
CREATE TABLE IF NOT EXISTS {load_schema_name}.logs (
    id SERIAL PRIMARY KEY,
    transform_logs_id INTEGER NOT NULL REFERENCES {transform_schema_name}.logs(id) ON DELETE CASCADE,
    status VARCHAR(15) NOT NULL
)
"""

weather_table_sql = f"""
CREATE TABLE IF NOT EXISTS {load_schema_name}.weather (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    tavg REAL,
    tmin REAL,
    tmax REAL,
    prcp REAL,
    snow REAL,
    wdir REAL,
    wspd REAL,
    wpgt REAL,
    pres REAL,
    tsun REAL,
    country TEXT NOT NULL
)
"""

covid_table_sql = f"""
CREATE TABLE IF NOT EXISTS {load_schema_name}.covid (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    confirmed INTEGER,
    deaths INTEGER,
    recovered INTEGER,
    confirmed_diff INTEGER,
    deaths_diff INTEGER,
    recovered_diff INTEGER,
    active INTEGER,
    active_diff INTEGER,
    fatality_rate REAL,
    country TEXT NOT NULL
)
"""


extract_tables = {
    "country": parse_one(country_sql),
    "api": parse_one(api_sql),
    "import_logs": parse_one(import_logs_sql),
    "api_import_logs": parse_one(api_import_logs_sql)
}

transform_tables = {
    "logs": parse_one(transform_logs_sql)
}

load_tables = {
    "logs": parse_one(load_logs_sql),
    "weather": parse_one(weather_table_sql),
    "covid": parse_one(covid_table_sql)
}

def generate_schema_and_tables(logger, schema_name, tables_dict):
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
    execute_query(schema_sql)
    logger.info(f"Schema {schema_name} created or already exists.")

    for table_id, table_expr in tables_dict.items():
        table_name = table_expr.this.name or table_id 
        sql = table_expr.sql()
        execute_query(sql)
        logger.info(f"Table {table_name} created or already exists.")
    
    logger.info(f"{schema_name.capitalize()} schema generation completed!")

def generate_extract_schema_and_tables(logger):
    generate_schema_and_tables(logger, extract_schema_name, extract_tables)

def generate_transform_schema_and_tables(logger):
    generate_schema_and_tables(logger, transform_schema_name, transform_tables)

def generate_load_schema_and_tables(logger):
    generate_schema_and_tables(logger, load_schema_name, load_tables)