import duckdb
import pandas as pd

# Initialize DuckDB connection (in-memory)
conn = duckdb.connect(database="weather_pipeline.duckdb", read_only=False)

def load_data():
    # This creates the table only if it doesn't exist.
    # Not really necessary for my purpose since I will
    # not import data from CSV-files.
    df = pd.read_csv("data/weather_data.csv")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather_data.weather AS 
        SELECT * FROM df
    """)

def fetch_data():
    # Fetch all rows from the table that dlt loaded the API data into
    return conn.execute("SELECT * FROM weather_data.weather").fetchdf()

def get_avg_temp(): # Get average temperature
    result = conn.execute("SELECT AVG(main__temp) AS avg_temp FROM weather_data.weather").fetchone()
    return result[0] if result else None