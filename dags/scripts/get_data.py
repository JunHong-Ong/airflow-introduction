import datetime
import os

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_data(endpoint):

    # Create temporary csv files
    reading_path = f"/opt/airflow/dags/files/readings_{endpoint}.csv"
    station_path = f"/opt/airflow/dags/files/stations_{endpoint}.csv"
    os.makedirs(os.path.dirname(reading_path), exist_ok=True)
    os.makedirs(os.path.dirname(station_path), exist_ok=True)

    # Get current date to pass as parameter
    curr_date = datetime.datetime.today().date().strftime("%Y-%m-%d")
    url = f"https://api.data.gov.sg/v1/environment/{endpoint}"
    response = requests.request("GET", url, params={"date":curr_date}).json()

    # Format response json into csv
    stations = response['metadata']['stations']
    stations = "\n".join([",".join([
        str(station["id"]), str(station["device_id"]),str(station["name"]),
        str(station["location"]["longitude"]), str(station["location"]["latitude"])])
        for station in stations])
    
    readings = response['items']
    readings = "\n".join([",".join([
        str(reading["timestamp"]), str(record["station_id"]),
        endpoint, str(record["value"])])
        for reading in readings for record in reading["readings"]])

    with open(station_path, "w") as file:
        file.write(stations)
    with open(reading_path, "w") as file:
        file.write(readings)

    # Insert data into postgres
    postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    with open(station_path, "r") as file:
        cur.copy_expert(
            "COPY stations_weather_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
            file,
        )
    conn.commit()

    with open(reading_path, "r") as file:
        cur.copy_expert(
            "COPY readings_weather_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
            file,
        )
    conn.commit()