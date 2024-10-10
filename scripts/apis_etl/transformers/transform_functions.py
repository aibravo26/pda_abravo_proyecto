"""
This module provides a function to transform a pandas DataFrame by adding execution timestamps.
The timestamps include the current timestamp in UTC and the current date.
"""

import logging  # Standard library import
from datetime import datetime, timezone  # Standard library import
import pandas as pd  # Third-party import


def transform_execution_dates_addition(df, data_type):
    """Generic function to transform data (weather or population), adding timestamps."""
    try:
        # Add current timestamp in UTC and current date as timestamp types
        df['execution_timestamp_utc'] = pd.Timestamp(datetime.now(timezone.utc))  # Timestamp with timezone
        df['execution_date'] = pd.Timestamp(datetime.now(timezone.utc).date())  # Timestamp for just the date

        # Save the transformed DataFrame
        logging.info("Transformed %s data successfully", data_type)
        return df

    except ValueError as val_err:
        logging.error("ValueError encountered while transforming %s data: %s", data_type, val_err)
        raise

    except Exception as e:
        logging.error("Error transforming %s data: %s", data_type, e)
        raise
