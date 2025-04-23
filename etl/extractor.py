from etl import BaseETL

import json
import os
import sys
from datetime import datetime, timedelta
from time import sleep
import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import database as db
from data.countries import countries


class Extractor(BaseETL):
    def __init__(self, api_type, url, start_date, end_date, timestamp, logger=None):
        super().__init__(logger)
        self.api_type = api_type
        self.url = url
        self.start_date = start_date
        self.end_date = end_date
        self.api_id = None
        self.headers = {
            "x-rapidapi-host": os.environ.get(f"{api_type}_API_HOST"),
            "x-rapidapi-key": os.environ.get(f"{api_type}_API_KEY"),
        }
        self.timestamp = timestamp

    def _get_full_url(self, country_data, date_str):
        if self.api_type == "WEATHER":
            return f"{self.url}?lat={country_data['lat']}&lon={country_data['lon']}&start={date_str}&end={date_str}"
        elif self.api_type == "COVID":
            return f"{self.url}?iso={country_data['iso']}&date={date_str}"

    def _process_country(self, country_name, country_data):
        current_date = self.start_date
        folder_path = f"S3/raw/batch_{self.timestamp}"
        os.makedirs(folder_path, exist_ok=True)
        country_id = db.get_or_create_country_id(country_name, country_data["iso"])
        file_paths = []

        self.logger.info(f"Processing country: {country_name} ({country_data['iso']})")

        while current_date <= self.end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            import_time = datetime.now()

            file_name = f"{country_data['iso']}_{self.api_type}_{current_date.date()}"
            file_path = os.path.join(folder_path, file_name)
            file_paths.append(file_path)

            self.logger.debug(f"Checking import log for {country_name} on {date_str}")
            import_log_id = db.get_import_id_if_backfill_date_exists(
                current_date, country_id, self.api_id
            )
            if import_log_id:
                self.logger.info(
                    f"Found existing import log (ID: {import_log_id}) for {country_name} on {date_str}, updating."
                )
                db.update_import_logs(
                    (self.timestamp, folder_path, file_name, import_time, import_log_id)
                )
            else:
                import_log_id = db.insert_import_log(
                    (
                        country_id,
                        self.timestamp,
                        folder_path,
                        file_name,
                        import_time,
                        import_time,
                        current_date,
                    )
                )
                self.logger.info(
                    f"Created new import log (ID: {import_log_id}) for {country_name} on {date_str}"
                )

            full_url = self._get_full_url(country_data, date_str)
            self.logger.info(f"Requesting data for {country_name} on {current_date}")

            try:
                start_time = datetime.now()
                response = requests.get(full_url, headers=self.headers)
                response.raise_for_status()
                result = response.json()["data"][0]
                end_time = datetime.now()

                self.logger.debug(
                    f"Received response for {country_name} on {date_str} (Status: {response.status_code})"
                )

                if result:
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump({"data": [result]}, f, indent=2)
                    self.logger.info(
                        f"Data collected and written to file {file_path} for {country_name} on {date_str}"
                    )

                db.insert_api_import_log(
                    (
                        self.api_id,
                        import_log_id,
                        start_time,
                        end_time,
                        response.status_code,
                        None,
                    )
                )

            except requests.exceptions.HTTPError as e:
                error_msg = f"HTTP Error {e.response.status_code} for {country_name} on {date_str}: {str(e)}"
                self.logger.error(error_msg)
                db.insert_api_import_log(
                    (
                        self.api_id,
                        import_log_id,
                        start_time,
                        datetime.now(),
                        e.response.status_code,
                        str(e),
                    )
                )

            sleep(1)
            current_date += timedelta(days=1)

        return file_paths

    def run(self):
        self.logger.info(f"Starting extraction for API: {self.api_type}")
        db.generate_extract_schema_and_tables(self.logger)

        self.api_id = db.get_or_create_api_id(self.api_type)
        all_file_paths = []

        for country, info in countries.items():
            self.logger.info(f"Starting extraction for {country}")
            country_file_paths = self._process_country(country, info)
            all_file_paths.extend(country_file_paths)
            self.logger.info(f"Finished extraction for {country}")

        self.logger.info(f"Extraction complete. {len(all_file_paths)} files saved.")
        return all_file_paths
