import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "db" / "weather_data.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    return conn


def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_location (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT NOT NULL UNIQUE,
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
        temp_max REAL,
        temp_min REAL,
        apparent_max REAL,
        apparent_min REAL,
        precipitation_sum REAL,
        sunshine_duration REAL,
        wind_speed_max REAL,
        forecast_generated_on TEXT,
        UNIQUE(location_id, date_id),
        FOREIGN KEY (location_id) REFERENCES dim_location(id),
        FOREIGN KEY (date_id) REFERENCES dim_date(id)
    );
    """)

    conn.commit()
    conn.close()
    print(" Tables created successfully.")


def insert_dim_location():
    conn = get_connection()
    cursor = conn.cursor()

    locations = [
        ("Istanbul", "Marmara", 41.0082, 28.9784),
        ("Ankara", "Ic_Anadolu", 39.9208, 32.8541),
        ("Izmir", "Ege", 38.4192, 27.1287),
        ("Antalya", "Akdeniz", 36.8969, 30.7133),
    ]

    for city, region, lat, lon in locations:
        cursor.execute("""
        INSERT OR IGNORE INTO dim_location (city, region, latitude, longitude)
        VALUES (?, ?, ?, ?)
        """, (city, region, lat, lon))

    conn.commit()
    conn.close()
    print(" dim_location inserted/verified.")


def insert_dim_date(df):
    conn = get_connection()
    cursor = conn.cursor()

    date_df = df[["date", "day", "month", "year", "week", "weekday_name"]].drop_duplicates()

    for _, row in date_df.iterrows():
        cursor.execute("""
        INSERT OR IGNORE INTO dim_date (date, day, month, year, week, weekday_name)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            str(row["date"]),
            row["day"], row["month"], row["year"], row["week"], row["weekday_name"]
        ))

    conn.commit()
    conn.close()
    print(f" {len(date_df)} dates inserted or verified.")


def insert_fact_weather(df):
    conn = get_connection()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("SELECT id FROM dim_location WHERE city = ?", (row["city"],))
        loc = cursor.fetchone()
        if not loc:
            print(f"⚠️ Location not found for {row['city']}")
            continue
        location_id = loc[0]

    
        cursor.execute("SELECT id FROM dim_date WHERE date = ?", (str(row["date"]),))
        date_row = cursor.fetchone()
        if not date_row:
            print(f"⚠️ Date not found for {row['date']}")
            continue
        date_id = date_row[0]

    
        cursor.execute("""
        INSERT OR REPLACE INTO fact_weather (
            location_id, date_id,
            temp_max, temp_min, apparent_max, apparent_min,
            precipitation_sum, sunshine_duration, wind_speed_max, forecast_generated_on
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            location_id, date_id,
            row["temp_max"], row["temp_min"], row["apparent_max"], row["apparent_min"],
            row["precipitation_sum"], row["sunshine_duration"], row["wind_speed_max"],
            row["forecast_generated_on"]
        ))

    conn.commit()
    conn.close()
    print(f"{len(df)} weather forecast rows inserted/replaced.")


if __name__ == "__main__":
    create_tables()
    insert_dim_location()
