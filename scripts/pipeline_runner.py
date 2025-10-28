import time
from datetime import datetime
from scripts.fetch_weather import fetch_weather
from scripts.analyze_weather import analyze_weather



try:

    while True:
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Run started at {start_time}")

        fetch_weather()
        analyze_weather()

        print("Pipeline run completed.\n Waiting for the next run...\n")
        time.sleep(10)
except KeyboardInterrupt:
        print("Pipeline stopped manually.")

