import dlt
import requests
import os
from dotenv import load_dotenv
import duckdb

# Load API key from environment variable
load_dotenv()
API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

def get_loaded_row_count():
    # Connect to the DuckDB database file used by dlt
    conn = duckdb.connect("weather_pipeline.duckdb")
    # Use the fully qualified table name, including the schema name
    count = conn.execute("SELECT COUNT(*) FROM weather_data.weather").fetchone()[0]
    conn.close()
    return count

def fetch_weather(city="London"):
    # Fetch weather data
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

def load_weather_data(city="London"):
    weather_data = fetch_weather(city)

    pipeline = dlt.pipeline(
        pipeline_name="weather_pipeline",
        destination="duckdb",
        dataset_name="weather_data"
    )

    # Load into DuckDB using dlt
    info = pipeline.run([weather_data], table_name="weather", write_disposition="append")
    print(info)
    
    # Instead of extracting row count from info, query the destination:
    rows_loaded = get_loaded_row_count()
    print(f"📦 Loaded {rows_loaded} row(s) into DuckDB.")



# For testing
if __name__ == "__main__":
    load_weather_data()