import requests
import pandas as pd
import sqlite3
import schedule
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

# Extract
def extract_weather_data():
    api_key = 'c8f939fef8908e69e9d56c21ab1fa5ad'
    city = 'London'
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'

    try:
        response = requests.get(url)
        data = response.json()

        if response.status_code == 200:
            weather_data = {
                'city': data['name'],
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'weather': data['weather'][0]['description'],
                'timestamp': pd.to_datetime('now')
            }
            print("Data extracted successfully.")
            return weather_data
        else:
            print(f"Error fetching data: {data.get('message', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"Error in extraction: {e}")
        return None

# Transform
def transform_data(weather_data):
    weather_data['temperature'] -= 273.15
    weather_data['feels_like'] = weather_data['temperature'] - ((100 - weather_data['humidity']) / 5)

    def categorize_humidity(humidity):
        if humidity < 30:
            return 'Low'
        elif 30 <= humidity <= 60:
            return 'Medium'
        else:
            return 'High'

    def categorize_weather(description):
        if 'clear' in description:
            return 'Clear'
        elif 'clouds' in description:
            return 'Cloudy'
        elif 'rain' in description or 'drizzle' in description:
            return 'Rainy'
        elif 'snow' in description:
            return 'Snowy'
        elif 'thunderstorm' in description:
            return 'Thunderstorm'
        else:
            return 'Other'

    def temperature_insight(temp):
        if temp < 0:
            return 'Cold'
        elif 0 <= temp <= 20:
            return 'Warm'
        else:
            return 'Hot'

    weather_data['humidity_category'] = categorize_humidity(weather_data['humidity'])
    weather_data['weather_category'] = categorize_weather(weather_data['weather'])
    weather_data['temperature_insight'] = temperature_insight(weather_data['temperature'])
    weather_data['day_of_week'] = weather_data['timestamp'].strftime('%A')

    print("Data transformed successfully.")
    return weather_data

# Load
def load_data(weather_data):
    if weather_data:
        # Connect to SQLite database (or create it if it doesn't exist)
        conn = sqlite3.connect('weather_data.db')
        cursor = conn.cursor()

        # Drop and recreate the table (development only)
        cursor.execute('DROP TABLE IF EXISTS weather')

        # Create a new table with all relevant fields
        cursor.execute('''
            CREATE TABLE weather (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT,
                temperature REAL,
                feels_like REAL,
                humidity INTEGER,
                humidity_category TEXT,
                weather TEXT,
                weather_category TEXT,
                temperature_insight TEXT,
                day_of_week TEXT,
                timestamp DATETIME
            )
        ''')

        # Ensure timestamp is in string format
        weather_data['timestamp'] = weather_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

        # Insert data into the new table
        cursor.execute('''
            INSERT INTO weather (
                city, temperature, feels_like, humidity, humidity_category, 
                weather, weather_category, temperature_insight, day_of_week, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            weather_data['city'],
            weather_data['temperature'],
            weather_data['feels_like'],
            weather_data['humidity'],
            weather_data['humidity_category'],
            weather_data['weather'],
            weather_data['weather_category'],
            weather_data['temperature_insight'],
            weather_data['day_of_week'],
            weather_data['timestamp']
        ))

        # Commit and close
        conn.commit()
        conn.close()

        print(f"Data loaded into the database: {weather_data['city']}")
    else:
        print("No data to load.")

# ETL Pipeline
def etl_pipeline():
    data = extract_weather_data()
    if data:
        transformed = transform_data(data)
        load_data(transformed)

# Schedule
def schedule_pipeline():
    try:
        schedule.every().hour.do(etl_pipeline)
        print("Pipeline scheduled to run every hour.")
    except Exception as e:
        logging.error(f"Error scheduling pipeline: {e}")
        print(f"Error scheduling pipeline: {e}")

# Main loop
if __name__ == "__main__":
    try:
        etl_pipeline()
        
        schedule_pipeline()
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("Scheduler stopped manually.")
    except Exception as e:
        logging.error(f"Unexpected error in scheduler loop: {e}")
        print(f"Unexpected error in scheduler loop: {e}")
