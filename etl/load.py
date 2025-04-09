import os
import sys
import json
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import identify_api_type, setup_logging
from database import get_file_path_for_period
from database import insert_weather, insert_covid, insert_load_logs, generate_load_schema_and_tables

def load_process(logger, transform_logs_id, file_path):
    try:
        api_type = identify_api_type(file_path)
        with open(file_path, 'r') as file:
            json_data = json.load(file)

        for content in json_data["data"]:
            entry = content[0]
            if api_type == "weather":
                values = (
                    entry["date"], entry["tavg"], entry["tmin"], entry["tmax"], entry["prcp"],
                    entry["snow"], entry["wdir"], entry["wspd"], entry["wpgt"],
                    entry["pres"], entry["tsun"], entry["country"]
                )
                logger.info(f"Inserted record for {entry['country']} on {entry['date']} for WEATHER api")
                insert_weather(values)
            elif api_type == "covid":
                values = (
                    entry["date"], entry["confirmed"], entry["deaths"], entry["recovered"],
                    entry["confirmed_diff"], entry["deaths_diff"], entry["recovered_diff"],
                    entry["active"], entry["active_diff"], entry["fatality_rate"],
                    entry["country"]
                )
                insert_covid(values)
                logger.info(f"Inserted record for {entry['country']} on {entry['date']} for COVID api")

            insert_load_logs((transform_logs_id, "Success"))

    except Exception as e:
        insert_load_logs((transform_logs_id, "Error"))
        logger.error(f"Load process failed with error: {e}")
        raise

def load():
    try:
        logger = setup_logging()
        logger.info("Starting load process")
        generate_load_schema_and_tables(logger)

        start_date = datetime(2021, 4, 1)
        end_date = datetime(2021, 4, 3)

        period_data = get_file_path_for_period(start_date, end_date, processed=True)

        for record in period_data:
            id = record[0]
            directory_name = record[1]
            file_name = record[2]
            file_path = os.path.join(directory_name, file_name)
            load_process(logger, id, file_path)
        
        logger.info("Load process completed successfully")
    except Exception as e:
        logger.error(f"Load process failed with error: {e}")
        raise

# load()