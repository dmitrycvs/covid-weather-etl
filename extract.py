from models import Country, Api, ImportLogs, ApiImportLogs
from database import init_session
from data.countries import countries
from time import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
import os
import json
import time as time_module

def extract_monthly_data(api_type, url, start_date, end_date, timestamp, session):
    api = session.query(Api).filter_by(type=api_type).first()
    if not api:
        api = Api(type=api_type)
        session.add(api)
        session.commit()
    
    for country_name, country_data in countries.items():
        country = session.query(Country).filter_by(name=country_name).first()
        if not country:
            country = Country(name=country_name, code=country_data['iso'])
            session.add(country)
            session.commit()
        
        data = []
        api_import_logs_list = [] 
        
        headers = {
            "x-rapidapi-host": os.getenv(f"{api_type}_API_HOST"),
            "x-rapidapi-key": os.getenv(f"{api_type}_API_KEY")
        }
            
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
                
            if api_type == "WEATHER":
                full_url = f"{url}?station={country_data['meteostat_id']}&start={date_str}&end={date_str}"
            elif api_type == "COVID":
                full_url = f"{url}?iso={country_data['iso']}&date={date_str}"
                

            try:
                response_start_time = datetime.now()
                response = requests.get(full_url, headers=headers)
                response_end_time = datetime.now()
                
                response.raise_for_status()
                day_data = response.json()
                data.extend([day_data["data"][0]])
                    
                api_import_logs = ApiImportLogs(
                    country_id=country.id,
                    api_id=api.id,
                    start_time=response_start_time,
                    end_time=response_end_time,
                    code_response=response.status_code,  
                )
                api_import_logs_list.append(api_import_logs)

                print(f"Successfully fetched {api_type} data for {country_name} on {date_str}")

                time_module.sleep(1)
                current_date += timedelta(days=1)

            except requests.exceptions.HTTPError as e:
                api_import_logs = ApiImportLogs(
                    country_id=country.id,
                    api_id=api.id,
                    start_time=response_start_time,
                    end_time=response_end_time,
                    code_response= e.response.status_code,
                    error_message= f"Client Error: {e.response.reason} for url: {e.response.url}"
                )
                    
                api_import_logs_list.append(api_import_logs)

                print(e.args[0])
                break
            
        if data:
            final_json = {"data": data}
                
            folder_path = f"S3/raw/batch_{timestamp}"
            os.makedirs(folder_path, exist_ok=True)
            file_name = f"{country_data['iso']}_{api_type}_MONTHLY_{timestamp}"
            file_path = os.path.join(folder_path, file_name)
                
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(final_json, file, indent=2)
            file_created_date = datetime.now()
                
            import_logs = ImportLogs(
                batch_timestamp=timestamp,
                country_id=country.id,
                import_directory_name=file_path,
                import_file_name=file_name,
                file_created_date=file_created_date,
                file_last_modified_date=file_created_date,
            )
            session.add(import_logs)
            session.flush()


            for api_logs in api_import_logs_list:
                api_logs.import_logs_id = import_logs.id

            session.add_all(api_import_logs_list)
            session.commit()
                
            print(f"Monthly {api_type} data for {country_name} was successfully added to PGSQL!")
        else:
            session.add_all(api_import_logs_list)
            session.commit()

            print(f"Error! Monthly {api_type} data for {country_name} was NOT added to PGSQL!")

try:
    load_dotenv()
    
    session = init_session()
    
    start_date = datetime(2021, 4, 1)
    end_date = datetime(2021, 4, 30)

    timestamp = round(time())
    
    url_weather = os.getenv("WEATHER_API_URL")
    url_covid = os.getenv("COVID_API_URL")
    
    extract_monthly_data("WEATHER", url_weather, start_date, end_date, timestamp, session)
    extract_monthly_data("COVID", url_covid, start_date, end_date, timestamp, session)
    
    session.close()

except Exception as e:
    print("Connection failed:", e)