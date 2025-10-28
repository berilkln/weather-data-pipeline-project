import json 
from matplotlib.font_manager import json_dump
import requests
from pathlib import Path 
from datetime import datetime


raw_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
raw_dir.mkdir(parents=True, exist_ok=True)


API_URL = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 41.0,
    "longitude": 29.0,
    "hourly": "temperature_2m",
    "timezone": "Europe/Istanbul"
}

now = datetime.now()
filename = f"{now.strftime('%Y-%m-%d-%H')}.json"
filepath = raw_dir / filename

response = requests.get(API_URL, params=params)
if response.status_code == 200:
    data = response.json()
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Success: {filepath}")
else:
    print(f"Error: {filepath}")