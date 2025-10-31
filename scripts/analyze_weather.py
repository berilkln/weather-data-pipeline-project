import json
import pandas as pd
from pathlib import Path

def analyze_weather():

    raw_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
    processed_dir = Path(__file__).resolve().parents[1] / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(raw_dir.glob("*_daily.json"))
    if not files:
        print("No daily forecast data files found.")
        return None

    print(f"📂 {len(files)} daily forecast files found. Analysis begins...\n")

    all_data = []

    for file_path in files[-4:]:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        meta = data.get("metadata", {})
        city = meta.get("city", "Unknown")
        region = meta.get("region", "")
        lat = meta.get("latitude", None)
        lon = meta.get("longitude", None)
        forecast_generated_on = meta.get("forecast_generated_on", "")

        print(f"Analyzing: {city}")

        daily = data.get("daily", {})

        df = pd.DataFrame({
            "date": pd.to_datetime(daily.get("time", [])),
            "temp_max": daily.get("temperature_2m_max", []),
            "temp_min": daily.get("temperature_2m_min", []),
            "apparent_max": daily.get("apparent_temperature_max", []),
            "apparent_min": daily.get("apparent_temperature_min", []),
            "precipitation_sum": daily.get("precipitation_sum", []),
            "sunshine_duration": daily.get("sunshine_duration", []),
            "wind_speed_max": daily.get("wind_speed_10m_max", []),
        })

        df["day"] = df["date"].dt.day
        df["month"] = df["date"].dt.month
        df["year"] = df["date"].dt.year
        df["week"] = df["date"].dt.isocalendar().week
        df["weekday_name"] = df["date"].dt.day_name()

        df["city"] = city
        df["region"] = region
        df["latitude"] = lat
        df["longitude"] = lon
        df["forecast_generated_on"] = forecast_generated_on

        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)

    print(f"\n Analysis completed. {len(final_df)} rows created.\n")
    print(final_df.head())
    return final_df


if __name__ == "__main__":
    df = analyze_weather()
    if df is not None:
        print(df.head())
