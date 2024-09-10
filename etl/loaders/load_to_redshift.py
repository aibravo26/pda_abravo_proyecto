import pandas as pd
import logging
import os

def save_to_redshift(parquet_file, table_name, engine):
    """Load data from Parquet file to Redshift."""
    try:
        # Load the Parquet file into a DataFrame
        df = pd.read_parquet(parquet_file)

        # Get the Redshift schema from environment variable
        redshift_schema = os.getenv('REDSHIFT_SCHEMA')

        # Save the DataFrame to Redshift using SQLAlchemy
        df.to_sql(table_name, con=engine, index=False, if_exists='replace', schema=redshift_schema, method='multi')

        logging.info(f"Data successfully uploaded to Redshift schema '{redshift_schema}', table '{table_name}'")

    except Exception as e:
        logging.error(f"Error uploading data to Redshift: {e}")
        raise