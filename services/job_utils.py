# services/job_utils.py
import requests
from config import WEATHER_API_KEY
from services.logging_service import log_audit
from services.elasticsearch_client import es

def needs_rerouting(job):
    location = job[5] or "Site A"  # job[5] is location
    date = job[2]  # job[2] is start_date
    url = f"https://api.openweathermap.org/data/2.5/weather?q={location}&appid={WEATHER_API_KEY}&units=metric"
    try:
        weather = requests.get(url).json()
        if 'wind' in weather and weather['wind']['speed'] > 15:
            return True
        return False
    except Exception as e:
        log_audit(es, "weather_fetch_failed", None, None, {"error": str(e)})
        return False