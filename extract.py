from time import time, sleep
from datetime import datetime, timedelta
from data.countries import countries
from database import get_or_create_api, get_or_create_country, insert_import_log, insert_api_import_log, generate_tables
from utils import setup_logging
import os
import json
import requests

def extract_monthly_data(logger, api_type, url, start_date, end_date, timestamp):
    logger.info(f"Starting data extraction for {api_type} from {start_date} to {end_date}")
    api_id = get_or_create_api(api_type)
    headers = {
        "x-rapidapi-host": os.getenv(f"{api_type}_API_HOST"),
        "x-rapidapi-key": os.getenv(f"{api_type}_API_KEY")
    }

    for country_name, country_data in countries.items():
        logger.info(f"Processing {api_type} data for {country_name}")
        country_id = get_or_create_country(country_name, country_data['iso'])
        data = []
        current_date = start_date

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            full_url = (f"{url}?station={country_data['meteostat_id']}&start={date_str}&end={date_str}" if api_type == "WEATHER" 
                        else f"{url}?iso={country_data['iso']}&date={date_str}")
            
            try:
                start_time = datetime.now()
                response = requests.get(full_url, headers=headers)
                end_time = datetime.now()
                response.raise_for_status()
                result = response.json()["data"][0]
                if result:
                    data.append([result])
                code_response = response.status_code
                error_message = None
                logger.debug(f"Successfully fetched {api_type} data for {country_name} on {date_str}")
            except requests.exceptions.HTTPError as e:
                code_response = e.response.status_code
                error_message = f"Client Error: {e.response.reason} for url: {e.response.url}"
                logger.error(f"HTTP Error while fetching {api_type} data for {country_name}: {error_message}")
                break
            except Exception as e:
                logger.error(f"Unexpected error fetching {api_type} data for {country_name}: {str(e)}")
                break
            
            sleep(1)
            current_date += timedelta(days=1)

        if data:
            folder_path = f"S3/raw/batch_{timestamp}"
            os.makedirs(folder_path, exist_ok=True)
            file_name = f"{country_data['iso']}_{api_type}_MONTHLY_{timestamp}"
            file_path = os.path.join(folder_path, file_name)

            with open(file_path, "w", encoding="utf-8") as file:
                json.dump({"data": data}, file, indent=2)

            import_log_id = insert_import_log((country_id, timestamp, file_path, file_name, datetime.now(), datetime.now()))
            insert_api_import_log((country_id, api_id, import_log_id, start_time, end_time, code_response, error_message))
            logger.info(f"Successfully added {api_type} data for {country_name} to PostgreSQL")
        else:
            logger.error(f"Error! {api_type} data for {country_name} was NOT added to PostgreSQL")


def extract():
    logger = setup_logging()
    logger.info("Starting extracting process")
    
    try:
        logger.info("Generating tables")
        generate_tables(logger)
        
        start_date = datetime(2021, 4, 1)
        end_date = datetime(2021, 4, 3)
        timestamp = round(time())
        
        logger.info(f"Starting data extraction for period: {start_date} to {end_date}")
        extract_monthly_data(logger, "WEATHER", os.getenv("WEATHER_API_URL"), start_date, end_date, timestamp)
        extract_monthly_data(logger, "COVID", os.getenv("COVID_API_URL"), start_date, end_date, timestamp)
        logger.info("Extracting process completed successfully")
    except Exception as e:
        logger.error(f"Extracting process failed with error: {str(e)}", exc_info=True)
        raise


extract()