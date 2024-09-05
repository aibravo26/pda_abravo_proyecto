import os
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from main import get_api_key

class TestApiKeyNotSet(unittest.TestCase):
    @patch('main.os.getenv')
    def test_get_api_key_not_set(self, mock_getenv):
        # Simulate the environment variable not being set
        mock_getenv.return_value = None

        # Check that the function raises a ValueError
        with self.assertRaises(ValueError) as context:
            get_api_key()

        self.assertEqual(str(context.exception), "No API key found. Please set the OPENWEATHERMAP_API_KEY environment variable.")

if __name__ == '__main__':
    unittest.main(exit=False)