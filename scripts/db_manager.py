import sqlite3
from pathlib import Path

DB_PATH =  Path(__file__).resolve().parents[1] / "data" / "db" / "weather_data.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    return conn



def create_table():
    conn = get_connection()
    cursor = conn.cursor()


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_location (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT NOT NULL,
        region TEXT,
        latitude REAL,
        longitude REAL
    );
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT UNIQUE,
        day INTEGER,
        month INTEGER,
        year INTEGER,
        week INTEGER,
        weekday_name TEXT
    );
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        date_id INTEGER,
        avg_temp REAL,
        apparent_temp REAL,
        avg_humidity REAL,
        total_precipitation REAL,
        total_snowfall REAL,
        avg_cloud_cover REAL,
        avg_wind_speed REAL,
        sunshine_duration REAL,
        FOREIGN KEY (location_id) REFERENCES dim_location(id),
        FOREIGN KEY (date_id) REFERENCES dim_date(id)
    );
    """)

    conn.commit()
    conn.close()

    print("Tables created.")


if __name__ == "__main__":
    create_table()