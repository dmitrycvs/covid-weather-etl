from .utils import execute_query
from .schema import extract_schema_name, transform_schema_name, load_schema_name

def get_or_create_api_id(api_type):
    query = f"SELECT id FROM {extract_schema_name}.api WHERE type = %s"
    result = execute_query(query, (api_type,), fetch=True)
    if result:
        return result[0][0]
    insert_query = f"INSERT INTO {extract_schema_name}.api (type) VALUES (%s) RETURNING id"
    return execute_query(insert_query, (api_type,), fetch=True)[0][0]

def get_or_create_country_id(name, code):
    query = f"SELECT id FROM {extract_schema_name}.country WHERE name = %s"
    result = execute_query(query, (name,), fetch=True)
    if result:
        return result[0][0]
    insert_query = f"INSERT INTO {extract_schema_name}.country (name, code) VALUES (%s, %s) RETURNING id"
    return execute_query(insert_query, (name, code), fetch=True)[0][0]

def get_country_name(id):
    query = f"SELECT name FROM {extract_schema_name}.country WHERE id = %s"
    result = execute_query(query, (id,), fetch=True)
    return result[0][0]

def insert_import_log(data):
    query = f"""
    INSERT INTO {extract_schema_name}.import_logs (country_id, batch_timestamp, import_directory_name, import_file_name, file_created_date, file_last_modified_date, start_backfill_date, end_backfill_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """
    return execute_query(query, data, fetch=True)[0][0]

def insert_api_import_log(data):
    query = f"""
    INSERT INTO {extract_schema_name}.api_import_logs (api_id, import_logs_id, start_time, end_time, code_response, error_message)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    execute_query(query, data)

def insert_transform_logs(data):
    query = f"""
    INSERT INTO {transform_schema_name}.logs (import_logs_id, processed_directory_name, processed_file_name, status)
    VALUES (%s, %s, %s, %s)
    """
    execute_query(query, data)

def insert_load_logs(data):
    query = f"""
    INSERT INTO {load_schema_name}.logs (transform_logs_id, status)
    VALUES (%s, %s) RETURNING id
    """
    execute_query(query, data)

def insert_weather(data):
    query = f"""
    INSERT INTO {load_schema_name}.weather (
        date, tavg, tmin, tmax, prcp, snow, wdir,
        wspd, wpgt, pres, tsun, country
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    execute_query(query, data)

def insert_covid(data):
    query = f"""
    INSERT INTO {load_schema_name}.covid (
        date, confirmed, deaths, recovered, confirmed_diff,
        deaths_diff, recovered_diff, active, active_diff,
        fatality_rate, country
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    execute_query(query, data)

def get_file_path_for_period(start_date, end_date, processed=False):
    if processed:
        query = f"""
        SELECT t.id, t.processed_directory_name, t.processed_file_name
        FROM {extract_schema_name}.import_logs AS i
        LEFT JOIN {transform_schema_name}.logs AS t
          ON t.import_logs_id = i.id
        WHERE t.status = 'Processed'
          AND i.start_backfill_date = %s
          AND i.end_backfill_date = %s;
        """
    else:
        query = f"""
        SELECT i.id, i.country_id, i.import_directory_name, i.import_file_name
        FROM {extract_schema_name}.import_logs AS i
        WHERE i.start_backfill_date = %s AND i.end_backfill_date = %s;
        """

    return execute_query(query, (start_date, end_date), fetch=True)