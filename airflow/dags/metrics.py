from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

from common import get_clickhouse_auth, get_clickhouse_url

CH_URL = get_clickhouse_url()
CH_AUTH = get_clickhouse_auth()

def metrics(n_days: int, **context):
    # 1. Последняя дата в витрине
    query_loaded = "SELECT MAX(report_date) FROM default.taxi_daily_metrics"
    response_loaded = requests.post(CH_URL, data=query_loaded, auth=CH_AUTH)
    max_loaded_str = response_loaded.text.strip()
    if not max_loaded_str or max_loaded_str == '1970-01-01' or max_loaded_str == '\\N':
        max_loaded_date = datetime(2025, 1, 1).date()
    else:
        max_loaded_date = datetime.strptime(max_loaded_str, '%Y-%m-%d').date()
    # 2. Максимальная дата сырых данных
    query_raw = "SELECT MAX(toDate(tpep_pickup_datetime)) FROM default.fact_taxi_trips"
    response_raw = requests.post(CH_URL, data=query_raw, auth=CH_AUTH)
    max_raw_str = response_raw.text.strip()
    if not max_raw_str or max_raw_str == '1970-01-01' or max_raw_str == '\\N':
        return
    max_raw_date = datetime.strptime(max_raw_str, '%Y-%m-%d').date()
    # 3. Расчет окна
    if max_loaded_date >= max_raw_date:
        return

    start_date = max_loaded_date + timedelta(days=1)
    target_date = min(max_loaded_date + timedelta(days=n_days), max_raw_date)
    # 4. Вставка данных
    insert_query = f"""
        INSERT INTO default.taxi_daily_metrics
        SELECT
            toDate(tpep_pickup_datetime) AS report_date,
            count(*) AS total_trips,
            sum(total_amount) AS total_revenue
        FROM default.fact_taxi_trips
        WHERE toDate(tpep_pickup_datetime) >= '{start_date}'
          AND toDate(tpep_pickup_datetime) <= '{target_date}'
        GROUP BY report_date
    """
    response = requests.post(CH_URL, data=insert_query, auth=CH_AUTH)
    if response.status_code != 200:
        raise Exception(f"Clickhouse Error: {response.text}")

default_args = {'owner': 'lyubov', 'retries': 1, 'retry_delay': timedelta(minutes=1)}

with DAG(
    dag_id='taxi_metrics_incremental_load',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    calc_metrics_task = PythonOperator(
        task_id='calc_metrics_dynamic_window',
        python_callable=metrics,
        op_kwargs={'n_days': 5}
    )
    # Триггер для dq_checks
    trigger_dq = TriggerDagRunOperator(
        task_id='trigger_data_quality',
        trigger_dag_id='data_quality_checks',
        wait_for_completion=False
    )

    calc_metrics_task >> trigger_dq
