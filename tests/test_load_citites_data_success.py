import io
import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch, mock_open
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from main import load_cities_data

class TestLoadCitiesDataSuccess(unittest.TestCase):
    @patch('builtins.open', new_callable=mock_open)
    def test_load_cities_data_success(self, mock_file):
        # Mocking a CSV file content
        mock_csv_content = (
            "country,capital,lat,lon\n"
            "argentina,buenos aires,34.2,52.1\n"
            "francia,paris,13.1,22.3\n"
        )

        # Set the mock to return the CSV content when read
        mock_file.return_value = io.StringIO(mock_csv_content)

        # Call the function
        result = load_cities_data('test_file.csv')

        # Expected DataFrame
        expected_df = pd.DataFrame({
            'country': ['argentina', 'francia'],
            'capital': ['buenos aires', 'paris'],
            'lat': [34.2, 13.1],
            'lon': [52.1, 22.3]
        })

        # Check that the function returns the correct DataFrame
        pd.testing.assert_frame_equal(result, expected_df)


if __name__ == '__main__':
    unittest.main(exit=False)