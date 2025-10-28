import json 
from matplotlib.font_manager import json_dump
import requests
from pathlib import Path 
from datetime import datetime

def fetch_weather(): 

    raw_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)


    API_URL = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 41.0,
        "longitude": 29.0,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,cloud_cover,wind_speed_10m",
        "timezone": "Europe/Istanbul"
    }

    now = datetime.now()
    filename = f"{now.strftime('%Y-%m-%d-%H')}_multi_feature.json"
    filepath = raw_dir / filename

    response = requests.get(API_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Success: {filepath}")
        return str(filepath)
    else:
        print(f"Error: {response.status_code}")
        raise Exception("FetchWeatherError")
    
if __name__ == "__main__":
    fetch_weather()