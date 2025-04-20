import unittest
from unittest.mock import patch, MagicMock, mock_open
import datetime
import os
import sys
import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etl import Extractor

class TestExtractor(unittest.TestCase):
    def setUp(self):
        self.api_type = "WEATHER"
        self.url = "https://test-api.com/data"
        self.start_date = datetime.datetime(2023, 1, 1)
        self.end_date = datetime.datetime(2023, 1, 3)
        self.timestamp = "20230101120000"
        self.logger = MagicMock()
        
        os.environ["WEATHER_API_HOST"] = "test-host"
        os.environ["WEATHER_API_KEY"] = "test-key"
        
        self.test_countries = {
            "TestCountry": {
                "iso": "TST",
                "lat": "10.0",
                "lon": "20.0"
            }
        }
    
    @patch("etl.extractor.db")
    @patch("etl.extractor.requests.get")
    @patch("etl.extractor.os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.extractor.countries", new_callable=MagicMock)
    def test_successful_extraction(self, mock_countries, mock_file, mock_makedirs, mock_get, mock_db):
        mock_countries.items.return_value = self.test_countries.items()
        mock_db.get_or_create_api_id.return_value = 1
        mock_db.get_or_create_country_id.return_value = 1
        mock_db.get_import_id_if_backfill_date_exists.return_value = None
        mock_db.insert_import_log.return_value = 1
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": [{"temp": 25, "humidity": 60}]}
        mock_get.return_value = mock_response
        
        extractor = Extractor(self.api_type, self.url, self.start_date, self.end_date, self.timestamp, self.logger)
        result = extractor.run()
        
        self.assertEqual(len(result), 3)
        mock_get.assert_called()
        mock_file.assert_called()
        mock_db.insert_api_import_log.assert_called()
        
    @patch("etl.extractor.db")
    @patch("etl.extractor.requests.get")
    @patch("etl.extractor.os.makedirs")
    @patch("etl.extractor.countries", new_callable=MagicMock)
    def test_api_error_handling(self, mock_countries, mock_makedirs, mock_get, mock_db):
        mock_countries.items.return_value = self.test_countries.items()
        mock_db.get_or_create_api_id.return_value = 1
        mock_db.get_or_create_country_id.return_value = 1
        mock_db.get_import_id_if_backfill_date_exists.return_value = None
        mock_db.insert_import_log.return_value = 1
        
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_error = requests.exceptions.HTTPError("API Error")
        http_error.response = mock_response
        mock_get.side_effect = http_error
        mock_response = MagicMock()
        
        extractor = Extractor(self.api_type, self.url, self.start_date, self.end_date, self.timestamp, self.logger)
        result = extractor.run()
        
        mock_db.insert_api_import_log.assert_called()
        self.logger.error.assert_called()
    
    @patch("etl.extractor.db")
    @patch("etl.extractor.requests.get")
    @patch("etl.extractor.os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("etl.extractor.countries", new_callable=MagicMock)
    def test_file_path_generation(self, mock_countries, mock_file, mock_makedirs, mock_get, mock_db):
        mock_countries.items.return_value = self.test_countries.items()
        mock_db.get_or_create_api_id.return_value = 1
        mock_db.get_or_create_country_id.return_value = 1
        mock_db.get_import_id_if_backfill_date_exists.return_value = None
        mock_db.insert_import_log.return_value = 1
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": [{"temp": 25, "humidity": 60}]}
        mock_get.return_value = mock_response
        
        extractor = Extractor(self.api_type, self.url, self.start_date, self.end_date, self.timestamp, self.logger)
        result = extractor.run()
        
        expected_path_pattern = f"S3/raw/batch_{self.timestamp}/TST_WEATHER_2023-01-0"
        for path in result:
            self.assertTrue(any(path.startswith(expected_path_pattern + day) for day in ["1", "2", "3"]))
