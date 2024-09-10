import requests
import pandas as pd
import configparser
import logging
import time
import os
from datetime import datetime, timezone
from sqlalchemy import create_engine

# Function to load configuration from the config file
def load_config(config_file='config.ini'):
    # Load the configuration from the config file
    config = configparser.ConfigParser()
    config.read(config_file)

    # Return configuration settings
    return {
        'input_cities_file': config['DEFAULT']['input_cities_file'],
        'output_directory': config['DEFAULT']['output_directory'],
        'extracted_weather_file': config['DEFAULT']['extracted_weather_file'],
        'transformed_weather_file': config['DEFAULT']['transformed_weather_file'],
        'loaded_weather_file': config['DEFAULT']['loaded_weather_file'],
        'pause_duration': float(config['DEFAULT']['pause_duration']),
    }

# Function to connect to redshift
def connect_to_redshift():
    try:
        # Load Redshift connection details from environment variables
        redshift_host = os.getenv('REDSHIFT_HOST')
        redshift_port = os.getenv('REDSHIFT_PORT')
        redshift_dbname = os.getenv('REDSHIFT_DBNAME')
        redshift_user = os.getenv('REDSHIFT_USER')
        redshift_password = os.getenv('REDSHIFT_PASSWORD')

        # Create the Redshift connection string using SQLAlchemy
        connection_string = (
            f'postgresql+psycopg2://{redshift_user}:{redshift_password}'
            f'@{redshift_host}:{redshift_port}/{redshift_dbname}'
        )

        # Ensure that no PostgreSQL-specific options are passed
        engine = create_engine(connection_string, connect_args={"options": ""})

        logging.info("Successfully connected to Redshift using SQLAlchemy")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to Redshift: {e}")
        raise

# Function to ensure the output directory exists
def ensure_output_directory_exists(output_directory):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        logging.info(f"Created output directory: {output_directory}")

# Function to get API key from environment
def get_api_key():
    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    if not api_key:
        raise ValueError("No API key found. Please set the OPENWEATHERMAP_API_KEY environment variable.")
    return api_key

# Extract Phase: Load cities data and process weather data for each city
def extract(input_cities_file, api_key, extracted_weather_file, pause_duration):
    try:
        cities_df = pd.read_csv(input_cities_file)
        weather_data = []
        
        for _, city in cities_df.iterrows():
            logging.info(f"Processing weather data for {city['capital_city']}, {city['country']}")
            city_weather = process_city_weather(city, api_key)
            if city_weather:
                weather_data.append(city_weather)
            time.sleep(pause_duration)
        
        # Save the extracted weather data as CSV
        weather_df = pd.DataFrame(weather_data)
        weather_df.to_parquet(extracted_weather_file, index=False)
        logging.info(f"Extracted weather data saved to: {extracted_weather_file}")
        return extracted_weather_file
    except FileNotFoundError:
        logging.error(f"File not found: {input_cities_file}")
        raise
    except Exception as e:
        logging.error(f"An error occurred during extraction: {e}")
        raise

# Function to get current weather data from OpenWeatherMap API
def get_weather_data(lat, lon, api_key):
    try:
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': api_key,
            'units': 'metric'
        }
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        logging.info(f"Retrieved weather data for lat={lat}, lon={lon}")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except Exception as e:
        logging.error(f"Error retrieving weather data for lat={lat}, lon={lon}, Error: {e}")
    return None

# Function to process weather data for a city
def process_city_weather(city, api_key):
    city_weather = get_weather_data(city["lat"], city["lon"], api_key)
    if city_weather is None:
        return None
    
    return {
        "country": city["country"],
        "capital_city": city["capital_city"],
        "temperature": city_weather["main"]["temp"],
        "feels_like": city_weather["main"]["feels_like"],
        "min_temperature": city_weather["main"]["temp_min"],
        "max_temperature": city_weather["main"]["temp_max"],
        "pressure": city_weather["main"]["pressure"],
        "humidity": city_weather["main"]["humidity"],
        "sea_level": city_weather["main"].get("sea_level"),
        "grnd_level": city_weather["main"].get("grnd_level"),
        "visibility": city_weather.get("visibility", "N/A"),
        "wind_speed": city_weather["wind"]["speed"],
        "wind_deg": city_weather["wind"]["deg"],
        "weather": city_weather["weather"][0]["description"],
        "weather_main": city_weather["weather"][0]["main"],
        "cloudiness": city_weather["clouds"]["all"],
        "sunrise": city_weather["sys"]["sunrise"],
        "sunset": city_weather["sys"]["sunset"],
        "lon": city["lon"],
        "lat": city["lat"]
    }

# Transform Phase: Add new columns with execution timestamp and date
def transform(extracted_weather_file, transformed_weather_file):
    weather_df = pd.read_parquet(extracted_weather_file)

    # Add current timestamp in UTC and current date
    weather_df['execution_timestamp_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    weather_df['execution_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Save the transformed DataFrame to a CSV
    weather_df.to_parquet(transformed_weather_file, index=False)
    logging.info(f"Transformed data saved to: {transformed_weather_file}")
    return transformed_weather_file

# Load Phase: Save the final processed data to the final output Parquet file
def load(transformed_weather_file, loaded_weather_file):
    weather_df = pd.read_parquet(transformed_weather_file)
    weather_df.to_parquet(loaded_weather_file, index=False)
    logging.info(f"Final weather data loaded to: {loaded_weather_file}")
    return loaded_weather_file

# Function to save DataFrame to Redshift using SQLAlchemy
def save_to_redshift(loaded_weather_file, table_name, engine):
    try:
        # Load the Parquet file into a DataFrame
        df = pd.read_parquet(loaded_weather_file)

        # Get the Redshift schema from environment variable
        redshift_schema = os.getenv('REDSHIFT_SCHEMA')

        # Save the DataFrame to Redshift using SQLAlchemy
        df.to_sql(table_name, con=engine, index=False, if_exists='replace', schema=redshift_schema, method='multi')

        logging.info(f"Data successfully uploaded to Redshift schema '{redshift_schema}', table '{table_name}'")

    except Exception as e:
        logging.error(f"Error uploading data to Redshift: {e}")
        raise
    
def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Load configuration
    config = load_config()
    api_key = get_api_key()

    # Ensure output directory exists
    ensure_output_directory_exists(config['output_directory'])

    logging.info("Starting the ETL process.")
    logging.info(f"Input file: {config['input_cities_file']}")
    logging.info(f"Output file: {config['loaded_weather_file']}")

    # Connect to Redshift using SQLAlchemy with redshift_connector
    redshift_engine = connect_to_redshift()

    # Extract Phase: Load cities data, process weather data, and save to a Parquet file
    extracted_weather_file = extract(config['input_cities_file'], api_key, config['extracted_weather_file'], config['pause_duration'])

    # Transform Phase: Add new columns and save the transformed data
    transformed_weather_file = transform(extracted_weather_file, config['transformed_weather_file'])

    # Load Phase: Save the final processed data to the output Parquet file and return the file path
    loaded_weather_file = load(transformed_weather_file, config['loaded_weather_file'])
    
    # Final Step: Save the loaded weather data to Redshift
    save_to_redshift(loaded_weather_file, 'staging_api_weather_data', redshift_engine)

    logging.info("ETL process completed successfully.")


if __name__ == "__main__":
    main()