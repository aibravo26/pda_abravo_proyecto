import os
import logging
from sqlalchemy import text

def load_incremental_weather_data(engine):
    """Load incremental weather data into fact_weather_metrics."""

    # Fetch schema from environment variable
    schema_name = f'"{os.getenv("REDSHIFT_SCHEMA")}"' 

    try:
        logging.info(f"Starting incremental load for fact_weather_metrics in schema '{schema_name}'.")

        incremental_load_sql = f"""
        BEGIN TRANSACTION;

        -- Insert new records from staging to fact table if they don't already exist
        INSERT INTO {schema_name}.fact_weather_metrics (
            city_id, temperature, feels_like, min_temperature, max_temperature, 
            pressure, humidity, visibility, wind_speed, wind_deg, weather, 
            execution_timestamp_utc, execution_date
        )
        SELECT
            c.id,
            s.temperature,
            s.feels_like,
            s.min_temperature,
            s.max_temperature,
            s.pressure,
            s.humidity,
            s.visibility,
            s.wind_speed,
            s.wind_deg,
            s.weather,
            s.execution_timestamp_utc,
            s.execution_date
        FROM {schema_name}.staging_api_weather_data s
        LEFT JOIN {schema_name}.dim_cities c ON s.capital_city = c.city_name
        LEFT JOIN {schema_name}.fact_weather_metrics f
            ON c.id = f.city_id 
            AND s.execution_timestamp_utc = f.execution_timestamp_utc
        WHERE f.city_id IS NULL;

        COMMIT;
        """

        with engine.connect() as connection:
            logging.info(f"Executing incremental load for fact_weather_metrics in schema '{schema_name}'.")
            connection.execute(incremental_load_sql)
        logging.info(f"fact_weather_metrics updated successfully in schema '{schema_name}'.")
    except Exception as e:
        logging.error(f"Error updating fact_weather_metrics: {e}")
        raise