import requests
import pandas as pd
import configparser
import logging
import time
import os
from datetime import datetime

# Function to get current weather data from OpenWeatherMap API
def get_weather_data(lat, lon, api_key):

    try:
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': api_key,
            'units': 'metric'  # Use 'metric' for Celsius, 'imperial' for Fahrenheit
        }
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Retrieved weather data for lat={lat}, lon={lon}")
        return data
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except Exception as e:
        logging.error(f"Error retrieving weather data for lat={lat}, lon={lon}, Error: {e}")
    return None

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Load configuration from the config file
    config = configparser.ConfigParser()
    config.read('config.ini')

    input_file = config['DEFAULT']['input_file']
    output_file = config['DEFAULT']['output_file']
    pause_duration = float(config['DEFAULT']['pause_duration'])

    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    if not api_key:
        raise ValueError("No API key found. Please set the OPENWEATHERMAP_API_KEY environment variable.")

    logging.info(f"Starting the weather data retrieval process.")
    logging.info(f"Input file: {input_file}")
    logging.info(f"Output file: {output_file}")
    logging.info(f"Pause duration between API requests: {pause_duration} seconds")

    # Load the list of capital cities from the CSV file
    try:
        cities_df = pd.read_csv(input_file)
        logging.info(f"Successfully loaded the input file: {input_file}")
    except FileNotFoundError:
        logging.error(f"File not found: {input_file}")
        raise
    except Exception as e:
        logging.error(f"An error occurred while loading the file: {input_file}, Error: {e}")
        raise

    # Create a list to store the weather data for each capital city
    weather_data = []

    # Loop through the list of capitals and fetch their weather data
    for index, row in cities_df.iterrows():
        logging.info(f"Processing weather data for {row['capital_city']}, {row['country']}")
        city_weather = get_weather_data(row["lat"], row["lon"], api_key)
        
        if city_weather is not None:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            date = datetime.now().strftime('%Y-%m-%d')
            weather_info = {
                "timestamp": timestamp,
                "date": date,
                "country": row["country"],
                "capital_city": row["capital_city"],
                "temperature": city_weather["main"]["temp"],
                "feels_like": city_weather["main"]["feels_like"],
                "min_temperature": city_weather["main"]["temp_min"],
                "max_temperature": city_weather["main"]["temp_max"],
                "pressure": city_weather["main"]["pressure"],
                "humidity": city_weather["main"]["humidity"],
                "visibility": city_weather.get("visibility", "N/A"),
                "wind_speed": city_weather["wind"]["speed"],
                "wind_deg": city_weather["wind"]["deg"],
                "wind_gust": city_weather["wind"].get("gust", "N/A"),
                "weather": city_weather["weather"][0]["description"],
                "weather_main": city_weather["weather"][0]["main"],
                "cloudiness": city_weather["clouds"]["all"],
                "sunrise": city_weather["sys"]["sunrise"],
                "sunset": city_weather["sys"]["sunset"],
                "lon": row["lon"],
                "lat": row["lat"]
            }
            weather_data.append(weather_info)
        
        # Pause to avoid hitting API rate limits
        time.sleep(pause_duration)

    # Convert the weather data into a DataFrame
    if weather_data:
        weather_df = pd.DataFrame(weather_data)

        # Display the DataFrame
        logging.info("Weather data retrieval completed. Saving to output file.")
        print(weather_df)

        # Save the DataFrame to a CSV file
        weather_df.to_csv(output_file, index=False)
        logging.info(f"Weather data successfully saved to: {output_file}")
    else:
        logging.warning("No weather data was retrieved.")



if __name__ == "__main__":
    main()