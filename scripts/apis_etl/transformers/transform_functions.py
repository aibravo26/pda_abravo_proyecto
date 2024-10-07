import pandas as pd
from datetime import datetime, timezone
import logging

def transform_execution_dates_addition(df, data_type):
    """Generic function to transform data (weather or population), adding timestamps."""
    try:
        # Add current timestamp in UTC and current date as timestamp types
        df['execution_timestamp_utc'] = pd.Timestamp(datetime.now(timezone.utc))  # Timestamp with timezone
        df['execution_date'] = pd.Timestamp(datetime.now(timezone.utc).date())  # Timestamp for just the date

        # Save the transformed DataFrame
        logging.info(f"Transformed {data_type} successfully")
        return df
    except Exception as e:
        logging.error(f"Error transforming {data_type} data: {e}")
        raise
