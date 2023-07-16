import datetime
import pendulum
from scripts import get_data as request

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="get-relative-humidity",
    schedule_interval="*/5 * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=1),
)
def ProcessRelativeHumidity():
    create_readings_table = PostgresOperator(
        task_id="create_readings_table",
        postgres_conn_id="pg_conn",
        sql="sql/readings_weather_schema.sql",
    )

    create_readings_temp_table = PostgresOperator(
        task_id="create_readings_temp_table",
        postgres_conn_id="pg_conn",
        sql="sql/readings_weather_temp_schema.sql",
    )

    create_stations_table = PostgresOperator(
        task_id="create_stations_table",
        postgres_conn_id="pg_conn",
        sql="sql/stations_weather_schema.sql",
    )

    create_stations_temp_table = PostgresOperator(
        task_id="create_stations_temp_table",
        postgres_conn_id="pg_conn",
        sql="sql/stations_weather_temp_schema.sql",
    )

    @task
    def get_data():
        request.get_data("relative-humidity")

    @task
    def merge_data():
        stations_query = """
            INSERT INTO stations_weather
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM stations_weather_temp
            ) t
            ON CONFLICT ("station_id") DO UPDATE
            SET
                "device_id" = excluded."device_id",
                "name" = excluded."name",
                "longitude" = excluded."longitude",
                "latitude" = excluded."latitude";
        """

        readings_query = """
            INSERT INTO readings_weather
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM readings_weather_temp
            ) t
            ON CONFLICT ("timestamp", "station_id", "reading_type") DO UPDATE
            SET
                "timestamp" = excluded."timestamp",
                "station_id" = excluded."station_id",
                "reading_type" = excluded."reading_type",
                "value" = excluded."value";
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(stations_query)
            conn.commit()
        except Exception as e:
            return 1
        

        try:
            postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(readings_query)
            conn.commit()
        except Exception as e:
            return 1

        return 0
    [create_stations_table, create_readings_temp_table, create_readings_table, create_stations_temp_table] >> get_data() >> merge_data()


dag = ProcessRelativeHumidity()