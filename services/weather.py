# services/weather.py
import requests
from config import WEATHER_API_KEY
from services.logging_service import log_audit
from datetime import datetime
from services.db import get_connection, release_connection

def fetch_weather(location, date):
    """Fetch weather for a location and date, caching in DB."""
    conn = get_connection()
    c = conn.cursor()
    # Check cache
    c.execute("SELECT * FROM weather WHERE location = %s AND date = %s", (location, date))
    cached = c.fetchone()
    if cached:
        release_connection(conn)
        return {
            "location": cached[2],
            "date": cached[1],
            "temp": cached[3],
            "wind_speed": cached[4],
            "precipitation": cached[5]
        }
    
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={location}&appid={WEATHER_API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10).json()
        forecast = response["list"][0]  # Use first forecast
        weather = {
            "date": date,
            "location": location,
            "temp": forecast["main"]["temp"],
            "wind_speed": forecast["wind"]["speed"],
            "precipitation": forecast.get("rain", {}).get("3h", 0),
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        }
        c.execute(
            "INSERT INTO weather (date, location, temp, wind_speed, precipitation, timestamp) VALUES (%s, %s, %s, %s, %s, %s)",
            (weather["date"], weather["location"], weather["temp"], weather["wind_speed"], weather["precipitation"], weather["timestamp"])
        )
        conn.commit()
        log_audit(None, "weather_fetch", {"location": location, "date": date})
        return weather
    except Exception as e:
        log_audit(None, "weather_fetch_error", {"location": location, "date": date, "error": str(e)})
        return {"error": str(e)}
    finally:
        release_connection(conn)

def fetch_forecast(location, start_date, days=5):
    # Modify URL to forecast, return list of days