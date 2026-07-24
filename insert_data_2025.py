"""
Загрузка NYC Yellow Taxi 2025: parquet → Postgres (staging).

Схема: sql/ddl/postgres/01_yellow_taxi_trips.sql
Данные: COPY батчами по 100_000 строк.

  set -a && source .env && set +a
  python insert_data_2025.py
  python insert_data_2025.py --months 1 2 3
  python insert_data_2025.py --skip-download --replace
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import urllib.request
from pathlib import Path
from time import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

REQUIRED_ENV = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TABLE_NAME = "yellow_taxi_trips"
BATCH_SIZE = 100_000
DDL_FILE = Path(__file__).resolve().parent / "sql/ddl/postgres/01_yellow_taxi_trips.sql"
DQ_TRIGGER_DDL = Path(__file__).resolve().parent / "sql/ddl/postgres/03_dq_trigger.sql"

# Целочисленные поля DDL (SMALLINT/INTEGER). В CSV должны быть 1, не 1.0.
INT_COLUMNS = (
    "VendorID",
    "passenger_count",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Yellow Taxi 2025 parquet into Postgres")
    parser.add_argument(
        "--months",
        nargs="+",
        type=int,
        default=list(range(1, 13)),
        help="Месяцы для загрузки (1–12). По умолчанию весь год.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "data",
        help="Каталог с parquet-файлами",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Не скачивать файлы, только грузить уже лежащие в data/",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Пересоздать таблицу по DDL и загрузить заново.",
    )
    return parser.parse_args()


def require_env() -> None:
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        print(f"Задайте переменные окружения: {', '.join(missing)}", file=sys.stderr)
        print("Подсказка: cp .env.example .env && set -a && source .env && set +a", file=sys.stderr)
        sys.exit(1)


def month_filename(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def download_month(data_dir: Path, year: int, month: int) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / month_filename(year, month)
    if path.exists() and path.stat().st_size > 0:
        print(f"[skip download] {path.name} уже есть ({path.stat().st_size / 1e6:.1f} MB)")
        return path

    url = f"{TLC_BASE}/{month_filename(year, month)}"
    tmp = path.with_suffix(".parquet.part")
    print(f"[download] {url}")
    urllib.request.urlretrieve(url, tmp)
    tmp.rename(path)
    print(f"[download] готово: {path.name} ({path.stat().st_size / 1e6:.1f} MB)")
    return path


def make_engine():
    return create_engine(
        URL.create(
            drivername="postgresql",
            username=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            database=os.environ["POSTGRES_DB"],
        )
    )


def table_exists(engine) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text("SELECT to_regclass('public.yellow_taxi_trips') IS NOT NULL")
            ).scalar()
        )


def recreate_table(engine) -> None:
    if not DDL_FILE.exists():
        raise FileNotFoundError(DDL_FILE)
    ddl = DDL_FILE.read_text()
    dq_ddl = DQ_TRIGGER_DDL.read_text() if DQ_TRIGGER_DDL.exists() else ""
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS yellow_taxi_trips CASCADE")
            cur.execute(ddl)
            if dq_ddl:
                cur.execute(dq_ddl)
        raw.commit()
    finally:
        raw.close()


def ensure_dq_trigger(engine) -> None:
    if not DQ_TRIGGER_DDL.exists() or not table_exists(engine):
        return
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute(DQ_TRIGGER_DDL.read_text())
        raw.commit()
    finally:
        raw.close()


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in INT_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round().astype("Int64")
    return out


def copy_dataframe(df: pd.DataFrame, raw_conn, columns: list[str]) -> None:
    buf = io.StringIO()
    df.to_csv(
        buf,
        index=False,
        header=False,
        columns=columns,
        na_rep="\\N",
        quoting=csv.QUOTE_MINIMAL,
    )
    buf.seek(0)
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    with raw_conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {TABLE_NAME} ({cols_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
            buf,
        )
    raw_conn.commit()


def load_parquet(path: Path, engine) -> int:
    if not path.exists():
        raise FileNotFoundError(path)

    parquet_file = pq.ParquetFile(path)
    total_rows = 0
    raw_conn = engine.raw_connection()
    try:
        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
            t_start = time()
            df = prepare_dataframe(batch.to_pandas())
            columns = list(df.columns)
            copy_dataframe(df, raw_conn, columns)
            total_rows += len(df)
            print(
                f"  {path.name}: +{len(df)} строк за {time() - t_start:.2f} с "
                f"(файл: {total_rows})"
            )
    finally:
        raw_conn.close()

    return total_rows


def count_rows(engine) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT count(*) FROM {TABLE_NAME}")).scalar())


def main() -> None:
    args = parse_args()
    require_env()

    months = sorted(set(args.months))
    for m in months:
        if m < 1 or m > 12:
            print(f"Некорректный месяц: {m}", file=sys.stderr)
            sys.exit(1)

    year = 2025
    paths: list[Path] = []
    for month in months:
        if args.skip_download:
            path = args.data_dir / month_filename(year, month)
            if not path.exists():
                print(f"Нет файла {path} (и --skip-download)", file=sys.stderr)
                sys.exit(1)
            paths.append(path)
        else:
            paths.append(download_month(args.data_dir, year, month))

    engine = make_engine()
    if args.replace or not table_exists(engine):
        print(f"[ddl] применяю {DDL_FILE.relative_to(Path(__file__).resolve().parent)}")
        recreate_table(engine)
    else:
        ensure_dq_trigger(engine)

    grand_total = 0
    for path in paths:
        print(f"[load] {path.name}")
        grand_total += load_parquet(path, engine)

    db_count = count_rows(engine)
    print(f"Готово. Вставлено за этот запуск: {grand_total}. В таблице сейчас: {db_count}")


if __name__ == "__main__":
    main()
