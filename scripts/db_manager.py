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
    print("dim_location table created.")





def insert_dim_date(df):
    conn = get_connection()
    cursor = conn.cursor()

    date_df = df[["date", "day", "month", "year", "week", "weekday_name"]].drop_duplicates()

    for _, row in date_df.iterrows():
        cursor.execute("""
        INSERT OR IGNORE INTO dim_date (date, day, month, year, week, weekday_name)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (str(row["date"]), row["day"], row["month"], row["year"], row["week"], row["weekday_name"]))

    conn.commit()
    conn.close()
    print(f"new {len(date_df)} date inserted.")




def insert_fact_weather(df):
    conn = get_connection()
    cursor = conn.cursor()


    for _, row in df.iterrows():
        cursor.execute("SELECT id FROM dim_location WHERE city = ?", (row['city'],))
        location_row = cursor.fetchone()
        if not location_row:
            print(f"not found location_id for {row['city']}, continue... ")
            continue
        location_id = location_row[0]


        cursor.execute("SELECT id FROM dim_date WHERE date = ?", (str(row["date"]),))
        date_row = cursor.fetchone()
        if not date_row:
            print(f"not found date_id for {row['date']}, continue...")
            continue
        date_id = date_row[0]

        cursor.execute("SELECT id FROM fact_weather WHERE location_id = ? AND date_id = ?", (location_id, date_id))
        exist = cursor.fetchone()
        if exist:
            print(f"{row['city']} and {row['date']} are avaliable, continue...")
            continue


        cursor.execute("""
        INSERT INTO fact_weather (
            location_id, date_id, avg_temp, apparent_temp,
            avg_humidity, total_precipitation, total_snowfall,
            avg_cloud_cover, avg_wind_speed, sunshine_duration
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            location_id, date_id,
            row["avg_temp"], row["apparent_temp"],
            row["avg_humidity"], row["total_precipitation"],
            row["total_snowfall"], row["cloud_cover"],
            row["avg_wind_speed"], row["sunshine_duration"]
        ))

    conn.commit()
    conn.close()
    print(f"new {len(df)} line inserted.")