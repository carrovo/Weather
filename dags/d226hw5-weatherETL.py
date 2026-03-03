from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import snowflake.connector
import requests

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_226")
    conn = hook.get_conn()
    return conn.cursor()

@task()
def extract(latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily":[
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles",
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

@task()
def transform(data: dict, latitude: float, longitude: float):
    daily = data.get("daily", {})
    dates = daily.get("time", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    precip = daily.get("precipitation_sum", [])
    weather_code = daily.get("weather_code", [])

    records = []
    for i in range(len(dates)):
        records.append(
            [
                float(latitude),
                float(longitude),
                dates[i],
                tmax[i],
                tmin[i],
                precip[i],
                int(weather_code[i]),
            ]
        )
    return records

@task()
def load(target_table: str, records: list):
    cursor = return_snowflake_conn()

    try:
        cursor.execute("BEGIN;")

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                latitude FLOAT,
                longitude FLOAT,
                date DATE,
                tmax FLOAT,
                tmin FLOAT,
                precip FLOAT,
                weather_code INT,
                PRIMARY KEY (latitude, longitude, date)
            );
            """
        )
        # full refresh, delete all existing records before inserting new ones
        cursor.execute(f"DELETE FROM {target_table};")

        insert_sql = f"""
            INSERT INTO {target_table}
               (latitude, longitude, date, tmax, tmin, precip, weather_code)
            VALUES
               (%s, %s, %s, %s, %s, %s, %s);
        """

        for record in records:
            cursor.execute(
                insert_sql,
                (record[0], record[1], record[2], record[3], record[4], record[5], record[6])
            )

        cursor.execute("COMMIT;")
        return len(records)
        
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise

with DAG(
    dag_id="d226hw5-weatherETL",
    start_date=datetime(2026, 3, 2),
    catchup=False,
    tags=["d226", "hw5", "etl"],
    schedule = "30 2 * * *",
) as dag:
    latitude = Variable.get("LATITUDE")
    longitude = Variable.get("LONGITUDE")

    target_table = "RAW.WEATHER_DATA_HW5"
    snowflake_conn_id = "snowflake_226"

    raw_data = extract(latitude, longitude)
    data = transform(raw_data, latitude, longitude)
    load(target_table, data)

    