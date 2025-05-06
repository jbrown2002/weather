# Weather ETL Pipeline

## Data Source

The data for this project comes from the OpenWeather Current Weather Data API, which provides users with up-to-date weather conditions for any location across the globe. Some of these include temperature, humidity, wind speed, and weather conditions like "partly cloudy". 

- API Link: [OpenWeatherMap Current Weather API] (https://openweathermap.org/current)

City: London (can be modified in the script)

Fields Extracted:

- City Name

- Temperature (Kelvin)

- Humidity (%)

- Weather Description

- Timestamp (UTC)

## Transformation

After extraction, the data undergoes several transformations:

Temperature conversion: Kelvin to Celsius

Feels Like: Approximate "feels like" temperature using humidity

**Humidity Categorization:**

Based on value:

< 30%: Low

30–60%: Medium

> 60%: High

**Weather Categorization:** 

Generalized weather into:

Clear, Cloudy, Rainy, Snowy, Thunderstorm, Other

Temperature Insight: Labeled as Cold, Warm, or Hot based on Celsius value

Day of the Week: Extracted from timestamp



