from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

from common import get_clickhouse_auth, get_clickhouse_url

CH_URL = get_clickhouse_url()
CH_AUTH = get_clickhouse_auth()


def run_checks():
    # 1. Отрицательная выручка
    q_neg = "SELECT count() FROM default.taxi_daily_metrics WHERE total_revenue < 0"
    res_neg = requests.post(CH_URL, data=q_neg, auth=CH_AUTH).text.strip()
    status_neg = 'SUCCESS' if int(res_neg) == 0 else 'FAIL'

    # 2. Пустые даты
    q_null = "SELECT count() FROM default.taxi_daily_metrics WHERE isNull(report_date)"
    res_null = requests.post(CH_URL, data=q_null, auth=CH_AUTH).text.strip()
    status_null = 'SUCCESS' if int(res_null) == 0 else 'FAIL'

    # 3. Дубликаты дат
    q_dupes = """
        SELECT count() FROM (
            SELECT report_date, count(*) as cnt
            FROM default.taxi_daily_metrics
            GROUP BY report_date
            HAVING cnt > 1
        )
    """
    res_dupes = requests.post(CH_URL, data=q_dupes, auth=CH_AUTH).text.strip()
    status_dupes = 'SUCCESS' if int(res_dupes) == 0 else 'FAIL'

    # 4. Отсутствие поездок
    q_zero = "SELECT count() FROM default.taxi_daily_metrics WHERE total_trips <= 0"
    res_zero = requests.post(CH_URL, data=q_zero, auth=CH_AUTH).text.strip()
    status_zero = 'SUCCESS' if int(res_zero) == 0 else 'FAIL'

    # 5. Резкий перепад количества поездок (падение > 50% по сравнению со вчера)
    q_drop = """
        SELECT count() FROM (
            SELECT
                total_trips,
                LAG(total_trips) OVER (ORDER BY report_date) as prev_trips
            FROM default.taxi_daily_metrics
        )
        WHERE prev_trips > 0 AND total_trips <= (prev_trips * 0.5)
    """
    res_drop = requests.post(CH_URL, data=q_drop, auth=CH_AUTH).text.strip()
    status_drop = 'SUCCESS' if int(res_drop) == 0 else 'FAIL'

    log_query = f"""
    INSERT INTO default.dq_log VALUES
    (now(), 'taxi_daily_metrics', 'negative_revenue', '{status_neg}', {res_neg}),
    (now(), 'taxi_daily_metrics', 'null_date', '{status_null}', {res_null}),
    (now(), 'taxi_daily_metrics', 'duplicate_date', '{status_dupes}', {res_dupes}),
    (now(), 'taxi_daily_metrics', 'zero_trips', '{status_zero}', {res_zero}),
    (now(), 'taxi_daily_metrics', 'volume_drop_50_pct', '{status_drop}', {res_drop})
    """
    response = requests.post(CH_URL, data=log_query, auth=CH_AUTH)
    if response.status_code != 200:
        raise Exception(f"Clickhouse Error: {response.text}")


default_args = {'owner': 'lyubov', 'retries': 1, 'retry_delay': timedelta(minutes=1)}

with DAG(
    dag_id='data_quality_checks',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@hourly',
    catchup=False
) as dag:

    dq_task = PythonOperator(
        task_id='run_all_checks',
        python_callable=run_checks
    )
