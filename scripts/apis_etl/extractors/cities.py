"""
This module provides functionality to extract cities data from a CSV file
and handle logging for success or failure of the operation.
"""

import logging  # Standard Library import
import pandas as pd  # Third-party import

def extract_cities_data(cities_file_path):
    """
    Extract cities data from a CSV file.

    Args:
        cities_file_path (str): The file path of the CSV containing the cities data.

    Returns:
        pandas.DataFrame: The extracted cities data as a DataFrame.

    Raises:
        Exception: If an error occurs while reading the CSV file.
    """
    try:
        cities_df = pd.read_csv(cities_file_path)
        logging.info("Successfully extracted cities data from %s", cities_file_path)
        return cities_df
    except Exception as error:
        logging.error("Error extracting cities data from %s: %s", cities_file_path, error)
        raise
