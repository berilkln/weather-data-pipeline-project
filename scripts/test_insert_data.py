from scripts.analyze_weather import analyze_weather
from scripts.db_manager import insert_dim_date, insert_dim_location, create_table, insert_fact_weather



if __name__ == "__main__":
    
    create_table()
    insert_dim_location()

    df = analyze_weather()
    insert_dim_date(df)
    
    insert_fact_weather(df)
