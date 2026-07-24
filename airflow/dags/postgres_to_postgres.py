"""
DAG: Postgres (taxi_db) → Postgres (taxi_archive), один день за run.

Идемпотентность: DELETE за ds в archive, затем COPY дня из staging.
"""

from __future__ import annotations

import io
import os
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

PG_HOST = os.environ.get("POSTGRES_HOST_DOCKER", "main_postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]
SOURCE_DB = os.environ["POSTGRES_DB"]
ARCHIVE_DB = os.environ.get("POSTGRES_ARCHIVE_DB", "taxi_archive")

ARCHIVE_DDL = Path("/opt/airflow/sql/ddl/postgres/02_taxi_archive_trips.sql")

COPY_COLUMNS = (
    '"VendorID"',
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    '"RatecodeID"',
    "store_and_fwd_flag",
    '"PULocationID"',
    '"DOLocationID"',
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    '"Airport_fee"',
    "cbd_congestion_fee",
)
COLS_SQL = ", ".join(COPY_COLUMNS)


def _connect(dbname: str):
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=dbname,
    )


def ensure_archive_db(**_context):
    """Создаёт БД taxi_archive и таблицу, если их ещё нет."""
    if not ARCHIVE_DB.replace("_", "").isalnum():
        raise ValueError(f"Некорректное имя БД: {ARCHIVE_DB}")

    admin = _connect(SOURCE_DB)
    admin.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with admin.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (ARCHIVE_DB,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{ARCHIVE_DB}" OWNER "{PG_USER}"')
    finally:
        admin.close()

    ddl = ARCHIVE_DDL.read_text()
    target = _connect(ARCHIVE_DB)
    try:
        with target.cursor() as cur:
            cur.execute(ddl)
        target.commit()
    finally:
        target.close()


def transfer_day(**context):
    ds = context["ds"]
    buf = io.StringIO()

    source = _connect(SOURCE_DB)
    try:
        with source.cursor() as cur:
            cur.copy_expert(
                f"""
                COPY (
                    SELECT {COLS_SQL}
                    FROM yellow_taxi_trips
                    WHERE tpep_pickup_datetime >= DATE '{ds}'
                      AND tpep_pickup_datetime < DATE '{ds}' + INTERVAL '1 day'
                      AND tpep_pickup_datetime >= TIMESTAMP '2025-01-01'
                      AND tpep_pickup_datetime < TIMESTAMP '2026-01-01'
                ) TO STDOUT WITH (FORMAT CSV, NULL '\\N')
                """,
                buf,
            )
    finally:
        source.close()

    buf.seek(0)
    target = _connect(ARCHIVE_DB)
    try:
        with target.cursor() as cur:
            cur.execute(
                """
                DELETE FROM yellow_taxi_trips
                WHERE tpep_pickup_datetime >= %s::date
                  AND tpep_pickup_datetime < (%s::date + INTERVAL '1 day')
                """,
                (ds, ds),
            )
            cur.copy_expert(
                f"COPY yellow_taxi_trips ({COLS_SQL}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                buf,
            )
        target.commit()
    finally:
        target.close()


default_args = {"owner": "lyubov", "retries": 1, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="transfer_postgres_to_postgres",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 12, 31),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=3,
    tags=["postgres", "etl"],
) as dag:
    prepare = PythonOperator(task_id="ensure_archive_db", python_callable=ensure_archive_db)
    transfer = PythonOperator(task_id="transfer_day", python_callable=transfer_day)
    prepare >> transfer
