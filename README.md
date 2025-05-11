# Weather ETL Pipeline

## Data Source

The data for this project comes from the OpenWeather Current Weather Data API, which provides users with up-to-date weather conditions for any location across the globe. Some of these include temperature, humidity, wind speed, and weather conditions like "partly cloudy". 

- API Link: [OpenWeatherMap Current Weather API] (https://openweathermap.org/current)

City: London, UK

Fields Extracted:

- City Name

- Temperature (Kelvin)

- Humidity (%)

- Weather Description

- Timestamp (UTC)

## Transformation

After extraction, the API data goes under a couple of transformations:

Temperature Conversion: Kelvin to Celsius

Feels Like: Approximate "feels like" temperature using humidity

**Humidity Categorization:**

Based on value:

<30% : Low

30–60% : Medium

60%+ : High

**Weather Categorization:** 

Generalized weather info:

Clear, Cloudy, Rainy, Snowy, Thunderstorm, Other

Temperature Insight: Labeled as Cold, Warm, or Hot based on Celsius value

Day of the Week: Extracted and transformed from timestamp within the data

## Destination

Database: SQLite database file (weather_data.db)

Table Name: weather

Stored Fields:

- City

- Temperature (°C)

- Humidity

- Feels Like Temperature (How Temperature and Humidity interact)

- Humidity Category

- Weather Description

- Weather Category

- Temperature Insight

- Day of the Week

- Timestamp

## Automation

The ETL pipeline is scheduled using the schedule Python library, and it is configured to run every hour. The scheduler runs continuously in a loop and can be terminated manually with Ctrl+C.
