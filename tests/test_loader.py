import unittest
from unittest.mock import patch, MagicMock, mock_open
import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from etl import Loader


class TestLoader(unittest.TestCase):
    def setUp(self):
        self.start_date = datetime.datetime(2023, 1, 1)
        self.end_date = datetime.datetime(2023, 1, 3)
        self.logger = MagicMock()

        self.weather_data = {
            "data": [
                [{"date": "2023-01-01", "country": "United States", "temp": 25.5, "humidity": 60, "snow": 0.0, "tsun": 0.0, "pressure": 1013}]
            ]
        }

        self.covid_data = {
            "data": [
                [{"date": "2023-01-01", "country": "United States", "confirmed": 100, "deaths": 5, "recovered": 90}]
            ]
        }

        self.test_file_path = "S3/processed/batch_20230101/USA_WEATHER_2023-01-01"
        self.test_covid_file_path = "S3/processed/batch_20230101/USA_COVID_2023-01-01"

    @patch("etl.loader.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.loader.json.load")
    def test_successful_weather_loading(self, mock_load, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "weather"
        mock_db.check_weather_record_exists.return_value = False
        mock_db.get_info_for_date_range.return_value = [(1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))]
        mock_load.return_value = self.weather_data

        loader = Loader(self.start_date, self.end_date, self.logger)
        loader.run()

        mock_db.insert_weather.assert_called_once()
        mock_db.insert_load_logs.assert_called_once_with((1, "Success"))
        self.logger.error.assert_not_called()

    @patch("etl.loader.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.loader.json.load")
    def test_successful_covid_loading(self, mock_load, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "covid"
        mock_db.check_covid_record_exists.return_value = False
        mock_db.get_info_for_date_range.return_value = [(1, os.path.dirname(self.test_covid_file_path), os.path.basename(self.test_covid_file_path))]
        mock_load.return_value = self.covid_data

        loader = Loader(self.start_date, self.end_date, self.logger)
        loader.run()

        mock_db.insert_covid.assert_called_once()
        mock_db.insert_load_logs.assert_called_once_with((1, "Success"))
        self.logger.error.assert_not_called()

    @patch("etl.loader.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.loader.json.load")
    def test_duplicate_record_handling(self, mock_load, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "weather"
        mock_db.check_weather_record_exists.return_value = True
        mock_db.get_info_for_date_range.return_value = [(1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))]
        mock_load.return_value = self.weather_data

        loader = Loader(self.start_date, self.end_date, self.logger)
        loader.run()

        mock_db.insert_weather.assert_not_called()
        mock_db.insert_load_logs.assert_called_once_with((1, "Success"))
        self.logger.warning.assert_called()

    @patch("etl.loader.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.loader.json.load")
    def test_unknown_api_type_handling(self, mock_load, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "unknown"
        mock_db.get_info_for_date_range.return_value = [(1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))]
        mock_load.return_value = self.weather_data

        loader = Loader(self.start_date, self.end_date, self.logger)
        loader.run()

        mock_db.insert_weather.assert_not_called()
        mock_db.insert_covid.assert_not_called()
        mock_db.insert_load_logs.assert_called_once_with((1, "Success"))
        self.logger.warning.assert_called()

    @patch("etl.loader.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.loader.json.load")
    def test_error_during_insertion(self, mock_load, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "weather"
        mock_db.check_weather_record_exists.return_value = False
        mock_db.insert_weather.side_effect = Exception("Database error")
        mock_db.get_info_for_date_range.return_value = [(1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))]
        mock_load.return_value = self.weather_data

        loader = Loader(self.start_date, self.end_date, self.logger)
        loader.run()

        mock_db.insert_load_logs.assert_called_once_with((1, "Success"))
        self.logger.error.assert_called()

    @patch("etl.loader.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.loader.json.load")
    def test_file_open_error(self, mock_load, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "weather"
        mock_file.side_effect = FileNotFoundError("File not found")
        mock_db.get_info_for_date_range.return_value = [(1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))]

        loader = Loader(self.start_date, self.end_date, self.logger)
        loader.run()

        mock_db.insert_load_logs.assert_called_once_with((1, "Error"))
        self.logger.error.assert_called()

    @patch("etl.loader.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.loader.json.load")
    def test_no_files_to_process(self, mock_load, mock_file, mock_db):
        mock_db.get_info_for_date_range.return_value = []

        loader = Loader(self.start_date, self.end_date, self.logger)
        loader.run()

        mock_db.insert_load_logs.assert_not_called()
        self.logger.info.assert_any_call("Load process complete.")

    @patch("etl.loader.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.loader.json.load")
    def test_multiple_files_processing(self, mock_load, mock_file, mock_db):
        mock_db.identify_api_type.side_effect = ["weather", "covid"]
        mock_db.check_weather_record_exists.return_value = False
        mock_db.check_covid_record_exists.return_value = False
        mock_db.get_info_for_date_range.return_value = [
            (1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path)),
            (2, os.path.dirname(self.test_covid_file_path), os.path.basename(self.test_covid_file_path))
        ]
        mock_load.side_effect = [self.weather_data, self.covid_data]

        loader = Loader(self.start_date, self.end_date, self.logger)
        loader.run()

        self.assertEqual(mock_db.insert_load_logs.call_count, 2)
        mock_db.insert_weather.assert_called_once()
        mock_db.insert_covid.assert_called_once()

    @patch("etl.loader.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.loader.json.load")
    def test_duplicate_file_paths(self, mock_load, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "weather"
        mock_db.check_weather_record_exists.return_value = False
        mock_db.get_info_for_date_range.return_value = [
            (1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path)),
            (2, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))
        ]
        mock_load.return_value = self.weather_data

        loader = Loader(self.start_date, self.end_date, self.logger)
        loader.run()

        mock_db.insert_weather.assert_called_once()
        self.assertEqual(mock_db.insert_load_logs.call_count, 1)

    @patch("etl.loader.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.loader.json.load")
    def test_mixed_success_and_failures(self, mock_load, mock_file, mock_db):
        mock_db.identify_api_type.side_effect = ["weather", "covid"]
        mock_db.check_weather_record_exists.return_value = False
        mock_file.side_effect = [mock_open().return_value, FileNotFoundError("File not found")]
        mock_db.get_info_for_date_range.return_value = [
            (1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path)),
            (2, os.path.dirname(self.test_covid_file_path), os.path.basename(self.test_covid_file_path))
        ]
        mock_load.return_value = self.weather_data

        loader = Loader(self.start_date, self.end_date, self.logger)
        loader.run()

        mock_db.insert_weather.assert_called_once()
        mock_db.insert_load_logs.assert_any_call((1, "Success"))
        mock_db.insert_load_logs.assert_any_call((2, "Error"))
