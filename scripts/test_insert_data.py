from scripts.analyze_weather import analyze_weather
from scripts.db_manager import create_tables, insert_dim_location, insert_dim_date, insert_fact_weather

create_tables()
insert_dim_location()
df = analyze_weather()
insert_dim_date(df)
insert_fact_weather(df)
