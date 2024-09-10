import pandas as pd
from datetime import datetime, timezone
import logging

def transform_weather_data(input_file, output_file):
    """Transform weather data, adding timestamps."""
    weather_df = pd.read_parquet(input_file)

    # Add current timestamp in UTC and current date
    weather_df['execution_timestamp_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    weather_df['execution_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Save transformed data to Parquet file
    weather_df.to_parquet(output_file, index=False)
    logging.info(f"Transformed weather data saved to: {output_file}")
    return output_file