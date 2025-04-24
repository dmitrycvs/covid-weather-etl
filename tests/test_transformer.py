import unittest
from unittest.mock import patch, MagicMock, mock_open
import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from etl import Transformer


class TestTransformer(unittest.TestCase):
    def setUp(self):
        self.start_date = datetime.datetime(2023, 1, 1)
        self.end_date = datetime.datetime(2023, 1, 3)
        self.logger = MagicMock()
        
        self.covid_data = {
            "data": [
                [{"confirmed": 100, "deaths": 5, "recovered": 90, "last_update": "2023-01-01", "region": "All"}]
            ]
        }
        
        self.weather_data = {
            "data": [
                [{"temp": 25.5, "humidity": 60, "snow": None, "tsun": None, "pressure": 1013}]
            ]
        }
        
        self.corrupted_weather_data = {
            "data": [
                [{"temp": None, "humidity": 60, "snow": None, "tsun": None, "pressure": 1013}]
            ]
        }
        
        self.test_file_path = "S3/raw/batch_20230101/USA_WEATHER_2023-01-01"
        self.expected_output_path = "S3/processed/batch_20230101/USA_WEATHER_2023-01-01"
        self.expected_error_path = "S3/error/batch_20230101/USA_WEATHER_2023-01-01"

    @patch("etl.transformer.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.transformer.os.makedirs")
    @patch("etl.transformer.json.load")
    @patch("etl.transformer.json.dump")
    def test_successful_weather_transformation(self, mock_dump, mock_load, mock_makedirs, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "weather"
        mock_db.get_country_name.return_value = "United States"
        mock_db.get_transform_logs_id.return_value = None
        mock_db.get_info_for_date_range.return_value = [(1, 1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))]
        mock_load.return_value = self.weather_data
        
        transformer = Transformer(self.start_date, self.end_date, self.logger)
        result = transformer.run()
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], self.expected_output_path)
        mock_dump.assert_called()
        mock_db.insert_transform_logs.assert_called_once()
        mock_db.update_transform_logs.assert_not_called()
        
    @patch("etl.transformer.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.transformer.os.makedirs")
    @patch("etl.transformer.json.load")
    @patch("etl.transformer.json.dump")
    def test_successful_covid_transformation(self, mock_dump, mock_load, mock_makedirs, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "covid"
        mock_db.get_country_name.return_value = "United States"
        mock_db.get_transform_logs_id.return_value = None
        mock_db.get_info_for_date_range.return_value = [(1, 1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))]
        mock_load.return_value = self.covid_data
        
        transformer = Transformer(self.start_date, self.end_date, self.logger)
        result = transformer.run()
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], self.expected_output_path)
        
        mock_dump.assert_called_once()
        dumped_data = mock_dump.call_args[0][0]
        entry = dumped_data["data"][0][0]
        self.assertNotIn("last_update", entry)
        self.assertNotIn("region", entry)
        self.assertEqual(entry["country"], "United States")
        
    @patch("etl.transformer.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.transformer.os.makedirs")
    @patch("etl.transformer.json.load")
    @patch("etl.transformer.json.dump")
    def test_weather_null_fields_handling(self, mock_dump, mock_load, mock_makedirs, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "weather"
        mock_db.get_country_name.return_value = "United States"
        mock_db.get_transform_logs_id.return_value = None
        mock_db.get_info_for_date_range.return_value = [(1, 1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))]
        mock_load.return_value = self.weather_data
        
        transformer = Transformer(self.start_date, self.end_date, self.logger)
        result = transformer.run()
        
        self.assertEqual(len(result), 1)
        
        mock_dump.assert_called_once()
        dumped_data = mock_dump.call_args[0][0]
        entry = dumped_data["data"][0][0]
        self.assertEqual(entry["snow"], 0.0)
        self.assertEqual(entry["tsun"], 0.0)
        
    @patch("etl.transformer.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.transformer.os.makedirs")
    @patch("etl.transformer.json.load")
    @patch("etl.transformer.json.dump")
    def test_corrupted_data_handling(self, mock_dump, mock_load, mock_makedirs, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "weather"
        mock_db.get_country_name.return_value = "United States"
        mock_db.get_transform_logs_id.return_value = None
        mock_db.get_info_for_date_range.return_value = [(1, 1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))]
        
        mock_load.side_effect = [self.corrupted_weather_data, self.corrupted_weather_data]
        
        transformer = Transformer(self.start_date, self.end_date, self.logger)
        result = transformer.run()
        
        self.assertEqual(len(result), 0)
        
        mock_dump.assert_called_once()
        file_calls = mock_file.call_args_list
        self.assertIn(self.expected_error_path, str(file_calls))
        mock_db.insert_transform_logs.assert_called_once()
        status_arg = mock_db.insert_transform_logs.call_args[0][0][3]
        self.assertEqual(status_arg, "Error")
        
    @patch("etl.transformer.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.transformer.os.makedirs")
    @patch("etl.transformer.json.load")
    @patch("etl.transformer.json.dump")
    def test_update_existing_transform_log(self, mock_dump, mock_load, mock_makedirs, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "weather"
        mock_db.get_country_name.return_value = "United States"
        mock_db.get_transform_logs_id.return_value = 99
        mock_db.get_info_for_date_range.return_value = [(1, 1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path))]
        mock_load.return_value = self.weather_data
        
        transformer = Transformer(self.start_date, self.end_date, self.logger)
        result = transformer.run()
        
        self.assertEqual(len(result), 1)
        mock_db.update_transform_logs.assert_called_once()
        mock_db.insert_transform_logs.assert_not_called()
        
    @patch("etl.transformer.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.transformer.os.makedirs")
    @patch("etl.transformer.json.load")
    @patch("etl.transformer.json.dump")
    def test_high_error_rate_warning(self, mock_dump, mock_load, mock_makedirs, mock_file, mock_db):
        mock_db.identify_api_type.return_value = "weather"
        mock_db.get_country_name.return_value = "United States"
        mock_db.get_transform_logs_id.return_value = None
        
        mock_db.get_info_for_date_range.return_value = [
            (1, 1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path)),
            (2, 1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path) + "2")
        ]
        
        mock_load.side_effect = [
            self.corrupted_weather_data, self.corrupted_weather_data, 
            self.corrupted_weather_data, self.corrupted_weather_data
        ]
        
        transformer = Transformer(self.start_date, self.end_date, self.logger)
        result = transformer.run()
        
        self.assertEqual(len(result), 0)
        self.logger.critical.assert_called_once()
        
        critical_msg = self.logger.critical.call_args[0][0]
        self.assertIn("High error rate", critical_msg)
        self.assertIn("100.00%", critical_msg)
        
    @patch("etl.transformer.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.transformer.os.makedirs")
    def test_no_files_to_process(self, mock_makedirs, mock_file, mock_db):
        mock_db.get_info_for_date_range.return_value = []
        
        transformer = Transformer(self.start_date, self.end_date, self.logger)
        result = transformer.run()
        
        self.assertEqual(len(result), 0)
        self.logger.info.assert_any_call("Transformation completed. 0 files processed successfully.")
        
    @patch("etl.transformer.db")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.transformer.os.makedirs")
    @patch("etl.transformer.json.load")
    @patch("etl.transformer.json.dump")
    def test_mixed_success_and_failure(self, mock_dump, mock_load, mock_makedirs, mock_file, mock_db):
        mock_db.identify_api_type.side_effect = ["weather", "weather"]
        mock_db.get_country_name.return_value = "United States"
        mock_db.get_transform_logs_id.return_value = None
        
        mock_db.get_info_for_date_range.return_value = [
            (1, 1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path)),
            (2, 1, os.path.dirname(self.test_file_path), os.path.basename(self.test_file_path) + "2")
        ]
        
        mock_load.side_effect = [
            self.weather_data,  
            self.corrupted_weather_data, self.corrupted_weather_data  
        ]
        
        transformer = Transformer(self.start_date, self.end_date, self.logger)
        result = transformer.run()
        
        self.assertEqual(len(result), 1)
        self.logger.info.assert_any_call("Error percentage: 50.00% (1 out of 2)")
        self.logger.critical.assert_called()
