import pandas as pd
from datetime import datetime, timezone
import logging

def transform_execution_dates_addition(input_file, output_file, data_type):
    """Generic function to transform data (weather or population), adding timestamps."""
    try:
        # Load the data from the Parquet file
        df = pd.read_parquet(input_file)

        # Add current timestamp in UTC and current date
        df['execution_timestamp_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        df['execution_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Save the transformed DataFrame to a Parquet file
        df.to_parquet(output_file, index=False)
        logging.info(f"Transformed {data_type} data saved to: {output_file}")
        return output_file
    except Exception as e:
        logging.error(f"Error transforming {data_type} data: {e}")
        raise
