import psycopg2
import json
from datetime import datetime

def insert_to_db(rows, dates):
    conn = psycopg2.connect(
        dbname="weather",
        user="user",
        password="password",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    for metric, values_json in rows:
        values = json.loads(values_json)
        for d, v in zip(dates, values):
            if v is not None:
                cur.execute("""
                    INSERT INTO weather_metrics (date, metric, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (date, metric) DO NOTHING
                """, (d, metric, v))
    conn.commit()
    cur.close()
    conn.close()
