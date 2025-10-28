import json
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

raw_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
files = sorted(raw_dir.glob("*.json"))
latest_file = files[-1] #last saved file 
print(f"Analyzing file: {latest_file.name}")


with open(latest_file, "r", encoding="utf-8") as f:
    data = json.load(f)


df = pd.DataFrame({
    "time": data["hourly"]["time"],
    "temperature": data["hourly"]["temperature_2m"]
})
df["time"] = pd.to_datetime(df["time"])
df["date"] = df["time"].dt.date

daily_avg = df.groupby("date")["temperature"].mean().reset_index()
daily_avg.rename(columns={"temperature": "avg_temperature"}, inplace=True)
#print(daily_avg)

print(df.head())
 

plt.plot(daily_avg["date"], daily_avg["avg_temperature"], marker="o", linestyle="-")
plt.title("Daily Average Temperature")
plt.xlabel("Date")
plt.ylabel(("Temperature (°C)"))
plt.grid(True)
plt.tight_layout()



processed_dir = Path(__file__).resolve().parents[1] / "data" / "processed"
processed_dir.mkdir(parents=True, exist_ok=True)

output_path = processed_dir / "daily_avg_temp_png"
plt.savefig(output_path)
print(f"Saved Plot: {output_path}")