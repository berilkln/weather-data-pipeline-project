from sre_constants import SUCCESS
from scripts.db_manager import create_tables, insert_dim_date, insert_dim_location, insert_fact_weather

def save_to_db(df):
    if df is None or df.empty:
        print("Not found data to save")
        return
    
    print("Database loading process started...")

    create_tables()

    insert_dim_location()

    insert_dim_date(df)

    insert_fact_weather(df)

    print("Data has been successfully saved to the database.")
    return "success"


if __name__ == "__main__":
    from scripts.analyze_weather import analyze_weather
    df = analyze_weather()
    save_to_db(df)
