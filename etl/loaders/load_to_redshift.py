import pandas as pd
import logging
import os

def save_to_redshift(df, table_name, engine, if_exists='replace'):
    """Load data from Parquet file to Redshift."""
    try:
        # Get the Redshift schema from environment variable
        redshift_schema = os.getenv('REDSHIFT_SCHEMA')

        # Save the DataFrame to Redshift using SQLAlchemy
        df.to_sql(table_name, con=engine, index=False, if_exists=if_exists, schema=redshift_schema, method='multi')

        logging.info(f"Data successfully uploaded to Redshift schema '{redshift_schema}', table '{table_name}' with if_exists='{if_exists}'")

    except Exception as e:
        logging.error(f"Error uploading data to Redshift: {e}")
        raise