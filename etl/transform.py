import sys
import os
import json
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import get_file_path_for_period, get_country_name, insert_transform_logs, generate_transform_schema_and_tables
from utils import setup_logging, identify_api_type

def process_file(logger, import_id, country_id, file_path):
    try:
        api_type = identify_api_type(file_path)

        with open(file_path, "r") as file:
            data = file.read()
        json_data = json.loads(data)
        logger.info(f"Processing the file for the {api_type} API from {file_path}")

        country_name = get_country_name((country_id))
        has_empty_values = False
        if api_type == "covid":
            for content in json_data["data"]:
                entry = content[0]
                entry["country"] = country_name
                del entry["region"]
                del entry["last_update"]
                for value in entry.values():
                    if not value and value != 0:
                        has_empty_values = True
                        break

        elif api_type == "weather":
            for content in json_data["data"]:
                entry = content[0]
                entry["country"] = country_name
                for key, value in entry.items():
                    if not value and value != 0:
                        entry[key] = 0.0

        if has_empty_values:
            logger.error("File is corrupted, processing aborted.")
            raise ValueError()

        output_file_path = file_path.replace("/raw/", "/processed/")
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        with open(output_file_path, 'w') as file:
            json.dump(json_data, file, indent=2)
        
        processed_directory_name = os.path.dirname(output_file_path)
        processed_file_name = os.path.basename(output_file_path)
        insert_transform_logs((import_id, processed_directory_name, processed_file_name, "Processed"))
        logger.info(f"File was processed and saved into {output_file_path}")
    except ValueError:
        output_file_path = file_path.replace("/raw/", "/error/")
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        with open(output_file_path, 'w') as file:
            json.dump(json_data, file, indent=2)

        processed_directory_name = os.path.dirname(output_file_path)
        processed_file_name = os.path.basename(output_file_path)
        insert_transform_logs((import_id, processed_directory_name, processed_file_name, "Error"))
        logger.error(f"File is corrupted and was saved into {output_file_path}")

def transform():
    logger = setup_logging()
    logger.info("Starting transformation process")

    generate_transform_schema_and_tables(logger)

    try:
        start_date = datetime(2021, 4, 1)
        end_date = datetime(2021, 4, 3)
        period_data = get_file_path_for_period(start_date, end_date)

        for record in period_data:
            import_id = record[0]
            country_id = record[1]
            directory_name = record[2]
            file_name = record[3]
            file_path = os.path.join(directory_name, file_name)
            process_file(logger, import_id, country_id, file_path)
            
        logger.info("Transformation process completed successfully")
    except Exception as e:
        logger.error(f"Transformation process failed with error: {e}")
        raise

# transform()