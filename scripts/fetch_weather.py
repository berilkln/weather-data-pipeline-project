import json
import requests
from pathlib import Path
from datetime import datetime, timedelta

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

    daily_params = [
        "temperature_2m_max",
        "temperature_2m_min",
        "apparent_temperature_max",
        "apparent_temperature_min",
        "precipitation_sum",
        "sunshine_duration",
        "wind_speed_10m_max",
    ]

    start_date = datetime.now().date()
    end_date = start_date + timedelta(days=6)
    timestamp = datetime.now().strftime("%Y-%m-%d")

    for city in cities:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "daily": ",".join(daily_params),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
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
                "forecast_generated_on": timestamp
            }

            filename = f"{timestamp}_{city['city'].lower()}_daily.json"
            filepath = raw_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            print(f"Success: {city['city']} ({start_date} → {end_date})")

        else:
            print(f"Error fetching data for {city['city']}: {response.status_code} | {response.text}")

    print("\n All cities (7-day forecast) fetched successfully.\n")
    return str(raw_dir)


if __name__ == "__main__":
    fetch_weather()
