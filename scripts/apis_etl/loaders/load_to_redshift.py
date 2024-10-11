"""
This module contains a function to upload data from a pandas DataFrame to an Amazon Redshift 
database using SQLAlchemy. The Redshift schema is retrieved from environment variables.
"""

import os  # Standard Library import
import logging  # Standard library import

def save_to_redshift(dataframe, table_name, engine, if_exists='replace'):
    """Load data from a DataFrame to Redshift."""
    try:
        # Get the Redshift schema from the environment variable
        redshift_schema = os.getenv('REDSHIFT_SCHEMA')

        # Save the DataFrame to Redshift using SQLAlchemy
        dataframe.to_sql(
            table_name, con=engine, index=False, if_exists=if_exists, 
            schema=redshift_schema, method='multi'
        )

        logging.info(
            "Data successfully uploaded to Redshift schema '%s', table '%s' with if_exists='%s'",
            redshift_schema, table_name, if_exists
        )

    except ValueError as val_err:
        logging.error("ValueError encountered: %s", val_err)
        raise

    except Exception as error:
        logging.error("Error uploading data to Redshift: %s", error)
        raise
