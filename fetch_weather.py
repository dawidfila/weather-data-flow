import os
from datetime import datetime, timedelta, timezone
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),  
    'password': os.getenv("DB_PASSWORD"),  
    'database': os.getenv("DB_NAME")  
}

API_KEY = os.getenv("API_KEY")

CITY = "Warsaw"

def fetch_data_from_api(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error while fetching data from API: {e}")
        return None

def save_data_to_db(query, data):
    try:
        connection = psycopg2.connect(**DATABASE_CONFIG)
        cursor = connection.cursor()

        cursor.executemany(query, data)

        connection.commit()
        print("Data successfully saved to the database.")
    except psycopg2.Error as e:
        print(f"Error saving data to the database: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()


def fetch_or_create_location(city, country, lat, lon, timezone):
    try:
        connection = psycopg2.connect(**DATABASE_CONFIG)
        cursor = connection.cursor()

        # Check if city exists in table
        cursor.execute("SELECT id FROM locations WHERE city = %s AND country = %s", (city, country))
        location = cursor.fetchone()

        if location:
            print(f"Location {city}, {country} already exists in the database.")
            return location[0]
        else:
            cursor.execute("""
                INSERT INTO locations (city, country, latitude, longitude, timezone)
                VALUES (%s, %s, %s, %s, %s) RETURNING id
            """, (city, country, lat, lon, timezone))
            location_id = cursor.fetchone()[0]
            print(f"Location {city}, {country} added to the database.")
            connection.commit()
            return location_id
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()


def check_last_weather_update(location_id):
    try:
        connection = psycopg2.connect(**DATABASE_CONFIG)
        cursor = connection.cursor()

        # Checking when weather data was last saved for this location
        cursor.execute("""
            SELECT timestamp FROM weather_current WHERE location_id = %s ORDER BY timestamp DESC LIMIT 1
        """, (location_id,))
        last_update = cursor.fetchone()

        if last_update:
            last_timestamp = last_update[0]
            time_diff = datetime.now(timezone.utc) - last_timestamp
            return time_diff
        else:
            # If no data, assume no update
            return timedelta(hours=2)
    except psycopg2.Error as e:
        print(f"Error while checking for the last data update: {e}")
        return timedelta(hours=2)  # Default interval greater than 1h to allow saving new data
    finally:
        if connection:
            cursor.close()
            connection.close()

def weather_current():
    url = "http://api.weatherapi.com/v1/current.json"
    params = {
        "key": API_KEY,
        "q": CITY
    }

    data = fetch_data_from_api(url, params)

    if data:
        location_data = data['location']
        current_weather = data['current']

        city = location_data['name']
        country = location_data['country']
        lat = location_data['lat']
        lon = location_data['lon']
        timezone = location_data['tz_id']

        # Checking locations
        location_id = fetch_or_create_location(city, country, lat, lon, timezone)

        if location_id:
            # Checking the last weather data update
            time_diff = check_last_weather_update(location_id)

            if time_diff > timedelta(hours=1):
                query = """
                    INSERT INTO weather_current (location_id, temp_c, humidity, wind_kph, pressure_mb, cloud, feelslike_c, condition_text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                weather_data = (
                    location_id,
                    current_weather['temp_c'],
                    current_weather['humidity'],
                    current_weather['wind_kph'],
                    current_weather['pressure_mb'],
                    current_weather['cloud'],
                    current_weather['feelslike_c'],
                    current_weather['condition']['text']
                )

                # Save current weather data to database
                save_data_to_db(query, [weather_data])
            else:
                print(f"Too little time has passed since the last update. The last update was {time_diff} ago.")
        else:
            print("Failed to find or add location.")

weather_current()