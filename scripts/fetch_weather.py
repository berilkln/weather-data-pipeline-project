import json
import requests
from pathlib import Path
from datetime import datetime

def fetch_weather():

    raw_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    API_URL = "https://api.open-meteo.com/v1/forecast"

    cities = [
        {"city": "Istanbul", "region": "Marmara", "lat": 41.0082, "lon": 28.9784},
        {"city": "Ankara", "region": "Ic_Anadolu", "lat": 39.9208, "lon": 32.8541},
        {"city": "Izmir", "region": "Ege", "lat": 38.4192, "lon": 27.1287},
        {"city": "Antalya", "region": "Akdeniz", "lat": 36.8969, "lon": 30.7133},
    ]

    hourly_params = [
        "temperature_2m",
        "apparent_temperature",
        "relative_humidity_2m",
        "precipitation",
        "snowfall",
        "cloud_cover",
        "wind_speed_10m",
        "sunshine_duration"
    ]

    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d-%H")

    for city in cities:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "hourly": ",".join(hourly_params),
            "timezone": "Europe/Istanbul"
        }

        response = requests.get(API_URL, params=params)

        if response.status_code == 200:
            data = response.json()
            data["metadata"] = {
                "city": city["city"],
                "region": city["region"],
                "latitude": city["lat"],
                "longitude": city["lon"],
                "timestamp": timestamp
            }

            filename = f"{timestamp}_{city['city'].lower()}.json"
            filepath = raw_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            print(f"Success: {city['city']} → {filepath}")

        else:
            print(f"Error fetching data for {city['city']}: {response.status_code}")

    print("\n All cities fetched successfully.\n")
    return str(raw_dir)


if __name__ == "__main__":
    fetch_weather()