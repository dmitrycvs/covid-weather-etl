from etl import BaseETL

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import database as db


class Loader(BaseETL):
    def __init__(self, start_date, end_date, logger=None):
        super().__init__(logger)
        self.start_date = start_date
        self.end_date = end_date

    def _load_file(self, transform_id, file_path):
        self.logger.info(f"Starting to load data from: {file_path}")

        api_type = db.identify_api_type(transform_logs_id=transform_id)
        self.logger.debug(f"Identified API type as: {api_type}")

        with open(file_path) as f:
            json_data = json.load(f)

        inserted_count = 0
        skipped_count = 0

        for idx, content in enumerate(json_data["data"]):
            entry = content[0] if isinstance(content, list) else content

            entry_date = entry.get("date")
            entry_country = entry.get("country")

            try:
                if api_type == "weather":
                    exists = db.check_weather_record_exists(entry)
                elif api_type == "covid":
                    exists = db.check_covid_record_exists(entry)
                else:
                    self.logger.warning(f"Unknown API type: {api_type}")
                    continue

                if not exists:
                    if api_type == "weather":
                        db.insert_weather(entry)
                    elif api_type == "covid":
                        db.insert_covid(entry)
                    inserted_count += 1
                    self.logger.debug(
                        f"Inserted {api_type} record for {entry_country} on {entry_date}"
                    )
                else:
                    skipped_count += 1
                    self.logger.warning(
                        f"Duplicate {api_type} record for {entry_country} on {entry_date}"
                    )
            except Exception as e:
                self.logger.error(
                    f"Failed to load entry {idx} for {entry_country} on {entry_date}: {e}"
                )

        db.insert_load_logs((transform_id, "Success"))
        self.logger.info(
            f"Completed loading file: {file_path} — Inserted: {inserted_count}, Skipped: {skipped_count}"
        )

    def run(self):
        self.logger.info(
            f"Starting load process for range {self.start_date} to {self.end_date}"
        )
        db.generate_load_schema_and_tables(self.logger)

        records = db.get_info_for_date_range(
            self.start_date, self.end_date, transformed=True
        )
        self.logger.debug(f"Retrieved {len(records)} records to load")

        processed = set()

        for log_id, dir_name, file_name in records:
            path = os.path.join(dir_name, file_name)

            if path in processed:
                self.logger.debug(f"Skipping already processed file: {path}")
                continue

            try:
                self._load_file(log_id, path)
                processed.add(path)
            except Exception as e:
                db.insert_load_logs((log_id, "Error"))
                self.logger.error(f"Load failed for file: {path}. Error: {e}")

        self.logger.info("Load process complete.")
