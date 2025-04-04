from .utils import execute_query
from .schema import schema_name

def get_or_create_api(api_type):
    query = f"SELECT id FROM {schema_name}.api WHERE type = %s"
    result = execute_query(query, (api_type,), fetch=True)
    if result:
        return result[0][0]
    insert_query = f"INSERT INTO {schema_name}.api (type) VALUES (%s) RETURNING id"
    return execute_query(insert_query, (api_type,), fetch=True)[0][0]

def get_or_create_country(name, code):
    query = f"SELECT id FROM {schema_name}.country WHERE name = %s"
    result = execute_query(query, (name,), fetch=True)
    if result:
        return result[0][0]
    insert_query = f"INSERT INTO {schema_name}.country (name, code) VALUES (%s, %s) RETURNING id"
    return execute_query(insert_query, (name, code), fetch=True)[0][0]

def insert_import_log(data):
    query = f"""
    INSERT INTO {schema_name}.import_logs (country_id, batch_timestamp, import_directory_name, import_file_name, file_created_date, file_last_modified_date, start_backfill_date, end_backfill_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """
    return execute_query(query, data, fetch=True)[0][0]

def insert_api_import_log(data):
    query = f"""
    INSERT INTO {schema_name}.api_import_logs (country_id, api_id, import_logs_id, start_time, end_time, code_response, error_message)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    execute_query(query, data)