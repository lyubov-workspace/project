from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests

from common import get_clickhouse_auth, get_clickhouse_url

CH_URL = get_clickhouse_url()
CH_AUTH = get_clickhouse_auth()

DROP_PARTITION_SQL = """ALTER TABLE default.fact_taxi_trips DROP PARTITION '{{ ds_nodash }}'"""

INSERT_SQL = """
INSERT INTO default.fact_taxi_trips
SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, PULocationID, DOLocationID, payment_type, fare_amount, tip_amount, total_amount
FROM postgresql(pg_taxi, table = 'yellow_taxi_trips')
WHERE toDate(tpep_pickup_datetime) = '{{ ds }}'
  AND tpep_pickup_datetime >= toDateTime('2025-01-01 00:00:00')
  AND tpep_pickup_datetime < toDateTime('2026-01-01 00:00:00')
"""


def execute_clickhouse_query(query_template, **context):
    ds = context['ds']
    ds_nodash = context['ds_nodash']
    query = query_template.replace('{{ ds }}', ds).replace('{{ ds_nodash }}', ds_nodash)
    response = requests.post(CH_URL, data=query, auth=CH_AUTH)
    if response.status_code != 200 and "No such partition" not in response.text:
        raise Exception(f"Clickhouse Error: {response.text}")

default_args = {'owner': 'lyubov', 'retries': 1, 'retry_delay': timedelta(minutes=1)}

with DAG(
    dag_id='transfer_postgres_to_clickhouse',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 12, 31),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=3,
) as dag:
    drop_partition_task = PythonOperator(task_id='drop_partition', python_callable=execute_clickhouse_query, op_kwargs={'query_template': DROP_PARTITION_SQL})
    insert_data_task = PythonOperator(task_id='insert_data', python_callable=execute_clickhouse_query, op_kwargs={'query_template': INSERT_SQL})
    drop_partition_task >> insert_data_task
