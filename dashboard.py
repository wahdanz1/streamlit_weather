import streamlit as st
import pandas as pd
import db

# DuckDB connection
data = db.fetch_data()

# Dashboard Components
st.title("🌦️ Weather Dashboard")
st.subheader("Raw Weather Data (from OpenWeatherMap's API)")
st.write(data)

st.subheader("Columns for reference:")
st.write(data.columns)

# Display average temperature
avg_temp = db.get_avg_temp()
if avg_temp is not None:
    st.write(f"🌡️ Average Temperature: **{avg_temp:.1f}°C**")
else:
    st.write("No temperature data available.")