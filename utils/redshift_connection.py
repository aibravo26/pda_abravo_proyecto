import os
import logging
from sqlalchemy import create_engine

def connect_to_redshift():
    """Connect to Redshift using SQLAlchemy."""
    try:
        redshift_host = os.getenv('REDSHIFT_HOST')
        redshift_port = os.getenv('REDSHIFT_PORT')
        redshift_dbname = os.getenv('REDSHIFT_DBNAME')
        redshift_user = os.getenv('REDSHIFT_USER')
        redshift_password = os.getenv('REDSHIFT_PASSWORD')

        connection_string = (
            f'postgresql+psycopg2://{redshift_user}:{redshift_password}'
            f'@{redshift_host}:{redshift_port}/{redshift_dbname}'
        )

        engine = create_engine(connection_string, connect_args={"options": ""})
        logging.info("Successfully connected to Redshift using SQLAlchemy")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to Redshift: {e}")
        raise
