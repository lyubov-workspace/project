from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

from common import get_clickhouse_auth, get_clickhouse_url

CH_URL = get_clickhouse_url()
CH_AUTH = get_clickhouse_auth()

# Условие «чистой» поездки для скорости/эффективности.
CLEAN = """(
    trip_distance > 0.1
    AND trip_distance < 100
    AND dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 60 AND 10800
    AND trip_distance / (dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 3600.0) BETWEEN 1 AND 80
)"""


def _ch_query(sql: str) -> str:
    response = requests.post(CH_URL, data=sql, auth=CH_AUTH)
    if response.status_code != 200:
        raise Exception(f"ClickHouse Error: {response.text}")
    return response.text.strip()


def metrics(n_days: int, **context):
    max_loaded_str = _ch_query("SELECT MAX(report_date) FROM default.taxi_daily_metrics")
    if not max_loaded_str or max_loaded_str == '1970-01-01' or max_loaded_str == '\\N':
        max_loaded_date = datetime(2024, 12, 31).date()
    else:
        max_loaded_date = datetime.strptime(max_loaded_str, '%Y-%m-%d').date()

    max_raw_str = _ch_query(
        "SELECT MAX(toDate(tpep_pickup_datetime)) FROM default.fact_taxi_trips"
    )
    if not max_raw_str or max_raw_str == '1970-01-01' or max_raw_str == '\\N':
        return
    max_raw_date = datetime.strptime(max_raw_str, '%Y-%m-%d').date()

    if max_loaded_date >= max_raw_date:
        return

    start_date = max_loaded_date + timedelta(days=1)
    target_date = min(max_loaded_date + timedelta(days=n_days), max_raw_date)

    _ch_query(
        f"""
        ALTER TABLE default.taxi_daily_metrics
        DELETE WHERE report_date >= '{start_date}' AND report_date <= '{target_date}'
        SETTINGS mutations_sync = 1
        """
    )

    speed = (
        "trip_distance / (dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 3600.0)"
    )
    rev_mile = "(fare_amount + tip_amount) / trip_distance"
    rev_min = (
        "(fare_amount + tip_amount)"
        " / (dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0)"
    )

    _ch_query(
        f"""
        INSERT INTO default.taxi_daily_metrics
        SELECT
            toDate(tpep_pickup_datetime) AS report_date,
            count(*) AS total_trips,
            sum(total_amount) AS total_revenue,
            sum(fare_amount + tip_amount) AS driver_revenue,
            quantileIf(0.5)({speed}, {CLEAN}) AS median_speed_mph,
            avgIf({rev_mile}, {CLEAN}) AS revenue_per_mile,
            avgIf({rev_min}, {CLEAN}) AS revenue_per_minute
        FROM default.fact_taxi_trips
        WHERE toDate(tpep_pickup_datetime) >= '{start_date}'
          AND toDate(tpep_pickup_datetime) <= '{target_date}'
          AND toDate(tpep_pickup_datetime) >= '2025-01-01'
          AND toDate(tpep_pickup_datetime) < '2026-01-01'
        GROUP BY report_date
        """
    )


default_args = {'owner': 'lyubov', 'retries': 1, 'retry_delay': timedelta(minutes=1)}

with DAG(
    dag_id='taxi_metrics_incremental_load',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    PythonOperator(
        task_id='calc_metrics_dynamic_window',
        python_callable=metrics,
        op_kwargs={'n_days': 5}
    )
