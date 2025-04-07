from utils import indentify_api_type
import json
import os

def process_file(logger, file_path):
    try:
        api_type = indentify_api_type(file_path)

        with open(file_path, "r") as file:
            data = file.read()
        json_data = json.loads(data)

        logger.info(f"Processing the file for the {api_type} API from {file_path}")
        has_empty_values = False
        if api_type == "covid":
            for content in json_data["data"]:
                entry = content[0]
                del entry["region"]
                del entry["last_update"]
                for value in entry.values():
                    if not value and value != 0:
                        has_empty_values = True
                        break

        elif api_type == "weather":
            for content in json_data["data"]:
                entry = content[0]
                for key, value in entry.items():
                    if not value and value != 0:
                        entry[key] = 0.0

        target_folder = "/error/" if has_empty_values else "/processed/"
        output_file_path = file_path.replace("/raw/", target_folder)
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        with open(output_file_path, 'w') as file:
            json.dump(json_data, file, indent=2)
        logger.info(f"File was processed and saved into {output_file_path}")
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")