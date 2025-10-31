from scripts.fetch_weather import fetch_weather
from scripts.analyze_weather import analyze_weather
from scripts.load_to_db import save_to_db
import traceback
from datetime import datetime

def main():
    """Tam ETL pipeline'ını çalıştırır: Fetch → Analyze → Save"""
    print("Weather Data Pipeline started...\n")

    try:
        print(" Fetching new weather data...")
        fetch_weather()

        
        print("\n Analyzing weather data...")
        df = analyze_weather()
        if df is None or df.empty:
            print("No data found to analyze. Exiting.")
            return

        
        print("\n💾 Saving analyzed data to database...")
        save_to_db(df)

       
        print(f"\n Pipeline completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    except Exception as e:
        print(" Pipeline failed due to an error:")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
