from etl import BaseETL

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import database as db


class Transformer(BaseETL):
    def __init__(self, start_date, end_date, logger=None):
        super().__init__(logger)
        self.start_date = start_date
        self.end_date = end_date

    def _process(self, import_id, country_id, file_path):
        self.logger.info(f"Processing file: {file_path} for import ID: {import_id}")

        api_type = db.identify_api_type(import_logs_id=import_id)
        self.logger.debug(f"Identified API type: {api_type}")

        with open(file_path) as f:
            json_data = json.load(f)

        country = db.get_country_name(country_id)
        self.logger.debug(f"Fetched country name: {country} for country ID: {country_id}")
        has_empty = False

        for idx, content in enumerate(json_data["data"]):
            entry = content[0] if isinstance(content, list) else content
            entry["country"] = country

            if api_type == "covid":
                if any(v is None for v in entry.values()):
                    has_empty = True
                    self.logger.warning(f"Empty field detected in COVID entry {idx} for {country}")
                    break

                entry.pop("last_update", None)
                entry.pop("region", None)

            elif api_type == "weather":
                for k, v in entry.items():
                    if (k == "snow" or k == "tsun") and v is None:
                        entry[k] = 0.0
                        self.logger.debug(f"Set missing weather value to 0.0 for key: {k} in entry {idx}")
                    elif v is None and k != "snow" and k != "tsun":
                        has_empty = True
                        self.logger.warning(f"Empty field detected in weather entry {idx} for {country}")
                        break

        if has_empty:
            raise ValueError(f"Corrupted data in {file_path} for country: {country}")

        output_path = file_path.replace("/raw/", "/processed/")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(json_data, f, indent=2)

        dir_name = os.path.dirname(output_path)
        file_name = os.path.basename(output_path)

        transform_log_id = db.get_transform_logs_id(import_id)
        if transform_log_id:
            db.update_transform_logs((dir_name, file_name, "Processed", transform_log_id))
        else:
            db.insert_transform_logs((import_id, dir_name, file_name, "Processed"))

        self.logger.info(f"Transformation successful. Processed file saved to: {output_path}")
        return output_path

    def run(self):
        self.logger.info(f"Starting transformation from {self.start_date} to {self.end_date}")
        db.generate_transform_schema_and_tables(self.logger)

        data = db.get_info_for_date_range(self.start_date, self.end_date)
        self.logger.debug(f"Fetched {len(data)} files to process")

        processed = []
        error_count = 0
        total_count = len(data)

        for import_id, country_id, dir_name, file_name in data:
            file_path = os.path.join(dir_name, file_name)
            try:
                self.logger.info(f"Transforming file: {file_path}")
                result_path = self._process(import_id, country_id, file_path)
                processed.append(result_path)
            except Exception as e:
                error_count += 1
                with open(file_path) as f:
                    json_data = json.load(f)

                output_path = file_path.replace("/raw/", "/error/")
                os.makedirs(os.path.dirname(output_path), exist_ok=True)

                with open(output_path, "w") as f:
                    json.dump(json_data, f, indent=2)

                dir_name = os.path.dirname(output_path)
                file_name = os.path.basename(output_path)

                transform_log_id = db.get_transform_logs_id(import_id)
                if transform_log_id:
                    db.update_transform_logs((dir_name, file_name, "Error", transform_log_id))
                else:
                    db.insert_transform_logs((import_id, dir_name, file_name, "Error"))

                self.logger.error(f"Failed to transform {file_path}: {e}")

        if total_count > 0:
            error_percentage = (error_count / total_count) * 100
            self.logger.info(f"Error percentage: {error_percentage:.2f}% ({error_count} out of {total_count})")
            
            if error_percentage >= 50:
                message = (f"WARNING: High error rate detected in transformation process!\n"
                            f"Error rate: {error_percentage:.2f}% ({error_count} out of {total_count} files)\n"
                            f"Date range: {self.start_date} to {self.end_date}")
                self.logger.critical(message)

        self.logger.info(f"Transformation completed. {len(processed)} files processed successfully.")
        return processed