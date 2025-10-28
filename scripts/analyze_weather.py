import json
import pandas as pd
from pathlib import Path

def analyze_weather():

    raw_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
    processed_dir = Path(__file__).resolve().parents[1] / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(raw_dir.glob("*.json"))
    latest_file = files[-1] #last saved file 
    print(f"Analyzing file: {latest_file.name}")

    # Read Json
    with open(latest_file, "r", encoding="utf-8") as f:
        data = json.load(f)


    df = pd.DataFrame({
        "time": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"],
        "humidity": data["hourly"]["relative_humidity_2m"],
        "precipitation": data["hourly"]["precipitation"],
        "cloud_cover": data["hourly"]["cloud_cover"],
        "wind_speed": data["hourly"]["wind_speed_10m"]
    })
    df["time"] = pd.to_datetime(df["time"])
    df["date"] = df["time"].dt.date

    metrics = (
        df.groupby("date").agg({
            "temperature": "mean",
            "humidity": "mean",
            "wind_speed": "mean",
            "precipitation": "sum"
        }).reset_index()
    )

    metrics.rename(columns={
        "temperature": "avg_temp",
            "humidity": "avg_humidity",
            "wind_speed": "avg_wind_speed",
            "precipitation": "total_precipitation"
    }, inplace=True)

    csv_path = processed_dir / "daily_metrics.csv"
    header = not csv_path.exists()
    metrics.to_csv(csv_path, mode="a", index=False, header=header)

    print(f"Metrics appended to: {csv_path}")
    return str(csv_path)
    
    
if __name__ == "__main__":
    analyze_weather()