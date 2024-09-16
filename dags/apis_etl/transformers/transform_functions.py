import pandas as pd
from datetime import datetime, timezone
import logging

def transform_execution_dates_addition(df, data_type):
    """Generic function to transform data (weather or population), adding timestamps."""
    try:
        # Add current timestamp in UTC and current date
        df['execution_timestamp_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        df['execution_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Save the transformed DataFrame
        logging.info(f"Transformed {data_type} successfully")
        return df
    except Exception as e:
        logging.error(f"Error transforming {data_type} data: {e}")
        raise
