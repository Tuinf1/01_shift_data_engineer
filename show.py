import pandas as pd
import psycopg2

conn = psycopg2.connect(
    dbname="weather",
    user="user",
    password="password",
    host="localhost",
    port=5432
)
df = pd.read_sql("SELECT * FROM weather_metrics LIMIT 10;", conn)
print(df)
