from .schema import extract_schema_name, load_schema_name, transform_schema_name
from .utils import execute_query


def get_or_create_api_id(api_type):
    query = f"SELECT id FROM {extract_schema_name}.api WHERE type = %s"
    result = execute_query(query, (api_type,), fetch=True)
    if result:
        return result[0][0]
    insert_query = (
        f"INSERT INTO {extract_schema_name}.api (type) VALUES (%s) RETURNING id"
    )
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
    INSERT INTO {extract_schema_name}.import_logs (country_id, batch_timestamp, import_directory_name, import_file_name, file_created_date, file_last_modified_date, backfill_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
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


def insert_weather(data_dict):
    query = f"""
    INSERT INTO {load_schema_name}.weather (
        date, tavg, tmin, tmax, prcp, snow, wdir,
        wspd, wpgt, pres, tsun, country
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        data_dict.get("date"),
        data_dict.get("tavg"),
        data_dict.get("tmin"),
        data_dict.get("tmax"),
        data_dict.get("prcp"),
        data_dict.get("snow"),
        data_dict.get("wdir"),
        data_dict.get("wspd"),
        data_dict.get("wpgt"),
        data_dict.get("pres"),
        data_dict.get("tsun"),
        data_dict.get("country"),
    )
    execute_query(query, values)


def insert_covid(data_dict):
    query = f"""
    INSERT INTO {load_schema_name}.covid (
        date, confirmed, deaths, recovered, confirmed_diff,
        deaths_diff, recovered_diff, active, active_diff,
        fatality_rate, country
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        data_dict.get("date"),
        data_dict.get("confirmed"),
        data_dict.get("deaths"),
        data_dict.get("recovered"),
        data_dict.get("confirmed_diff"),
        data_dict.get("deaths_diff"),
        data_dict.get("recovered_diff"),
        data_dict.get("active"),
        data_dict.get("active_diff"),
        data_dict.get("fatality_rate"),
        data_dict.get("country"),
    )
    execute_query(query, values)


def get_info_for_date_range(start_date, end_date, transformed=False):
    if transformed:
        query = f"""
        SELECT t.id, t.processed_directory_name, t.processed_file_name
        FROM {extract_schema_name}.import_logs AS i
        LEFT JOIN {transform_schema_name}.logs AS t
          ON t.import_logs_id = i.id
        WHERE t.status = 'Processed'
          AND i.backfill_date BETWEEN %s AND %s
        """
    else:
        query = f"""
        SELECT i.id, i.country_id, i.import_directory_name, i.import_file_name
        FROM {extract_schema_name}.import_logs AS i
        WHERE i.backfill_date BETWEEN %s AND %s
        """

    return execute_query(query, (start_date, end_date), fetch=True)


def get_import_id_if_backfill_date_exists(date, country_id, api_id):
    query = f"""
    SELECT i.id
    FROM {extract_schema_name}.import_logs AS i
    JOIN {extract_schema_name}.api_import_logs AS a
      ON i.id = a.import_logs_id
    WHERE i.backfill_date = %s
      AND i.country_id = %s
      AND a.api_id = %s
    """

    result = execute_query(query, (date, country_id, api_id), fetch=True)
    return result[0][0] if result else None


def get_transform_logs_id(import_id):
    query = f"""
    SELECT t.id
    FROM {transform_schema_name}.logs AS t
    WHERE t.import_logs_id = %s
    """

    result = execute_query(query, (import_id,), fetch=True)
    return result[0][0] if result else None


def update_import_logs(data):
    query = f"""
    UPDATE {extract_schema_name}.import_logs
    SET batch_timestamp = %s, import_directory_name = %s, import_file_name = %s, file_last_modified_date = %s 
    WHERE id = %s
    """
    execute_query(query, data)


def update_transform_logs(data):
    query = f"""
    UPDATE {transform_schema_name}.logs
    SET processed_directory_name = %s, processed_file_name = %s, status = %s
    WHERE id = %s
    """
    execute_query(query, data)


def check_weather_record_exists(data_dict):
    query = f"""
    SELECT COUNT(*) FROM {load_schema_name}.weather 
    WHERE date = %s AND country = %s
      AND tavg = %s AND tmin = %s AND tmax = %s
    """
    values = (
        data_dict.get("date"),
        data_dict.get("country"),
        data_dict.get("tavg"),
        data_dict.get("tmin"),
        data_dict.get("tmax"),
    )
    result = execute_query(query, values, fetch=True)
    return result[0][0] > 0


def check_covid_record_exists(data_dict):
    query = f"""
    SELECT COUNT(*) FROM {load_schema_name}.covid 
    WHERE date = %s AND country = %s
      AND confirmed = %s AND deaths = %s AND recovered = %s
    """
    values = (
        data_dict.get("date"),
        data_dict.get("country"),
        data_dict.get("confirmed"),
        data_dict.get("deaths"),
        data_dict.get("recovered"),
    )
    result = execute_query(query, values, fetch=True)
    return result[0][0] > 0


def identify_api_type(transform_logs_id=None, import_logs_id=None):
    if transform_logs_id:
        query = f"""
        SELECT a.type 
        FROM {extract_schema_name}.api a
        JOIN {extract_schema_name}.api_import_logs ail ON a.id = ail.api_id
        JOIN {extract_schema_name}.import_logs il ON ail.import_logs_id = il.id
        JOIN {transform_schema_name}.logs tl ON il.id = tl.import_logs_id
        WHERE tl.id = %s
        """
        params = (transform_logs_id,)
    elif import_logs_id:
        query = f"""
        SELECT a.type 
        FROM {extract_schema_name}.api a
        JOIN {extract_schema_name}.api_import_logs ail ON a.id = ail.api_id
        JOIN {extract_schema_name}.import_logs il ON ail.import_logs_id = il.id
        WHERE il.id = %s
        """
        params = (import_logs_id,)
    else:
        return None

    result = execute_query(query, params, fetch=True)
    if result:
        return result[0][0].lower()

    return None
