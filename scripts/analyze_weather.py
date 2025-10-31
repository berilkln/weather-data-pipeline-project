import json
import pandas as pd
from pathlib import Path

def analyze_weather():

    raw_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
    processed_dir = Path(__file__).resolve().parents[1] / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(raw_dir.glob("*.json"))
    if not files:
        print("No data files found yet.")
        return None

    print(f"{len(files)} files found, analysis begins...")

    all_metrics = []

    for file_path in files[-4:]:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        city_meta = data.get("metadata", {})
        city_name = city_meta.get("city", "Unknown")
        region = city_meta.get("region", "")
        lat = city_meta.get("latitude", None)
        lon = city_meta.get("longitude", None)

        print(f"🔍 Analyzing {city_name}...")


        df = pd.DataFrame({
            "time": data["hourly"]["time"],
            "temperature": data["hourly"].get("temperature_2m", []),
            "apparent_temp": data["hourly"].get("apparent_temperature", []),
            "humidity": data["hourly"].get("relative_humidity_2m", []),
            "precipitation": data["hourly"].get("precipitation", []),
            "snowfall": data["hourly"].get("snowfall", []),
            "cloud_cover": data["hourly"].get("cloud_cover", []),
            "wind_speed": data["hourly"].get("wind_speed_10m", []),
            "sunshine_duration": data["hourly"].get("sunshine_duration", []),
        })

        df["time"] = pd.to_datetime(df["time"])
        df["date"] = df["time"].dt.date

        daily = (
            df.groupby("date").agg({
                "temperature": "mean",
                "apparent_temp": "mean",
                "humidity": "mean",
                "precipitation": "sum",
                "snowfall": "sum",
                "cloud_cover": "mean",
                "wind_speed": "mean",
                "sunshine_duration": "sum"
            }).reset_index()
        )


        daily.rename(columns={
            "temperature": "avg_temp",
            "humidity": "avg_humidity",
            "wind_speed": "avg_wind_speed",
            "precipitation": "total_precipitation",
            "snowfall": "total_snowfall"
        }, inplace=True)

    
        daily["day"] = pd.to_datetime(daily["date"]).dt.day
        daily["month"] = pd.to_datetime(daily["date"]).dt.month
        daily["year"] = pd.to_datetime(daily["date"]).dt.year
        daily["week"] = pd.to_datetime(daily["date"]).dt.isocalendar().week
        daily["weekday_name"] = pd.to_datetime(daily["date"]).dt.day_name()


        daily["city"] = city_name
        daily["region"] = region
        daily["latitude"] = lat
        daily["longitude"] = lon

        all_metrics.append(daily)

    final_df = pd.concat(all_metrics, ignore_index=True)

    print(f"Analysis completed. {len(final_df)} line produced.")
    return final_df


if __name__ == "__main__":
    df = analyze_weather()
    if df is not None:
        print(df.head())
