import os
import sys

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from time import time

REQUIRED_ENV = ('POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB')
missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
if missing:
    print(f"Задайте переменные окружения: {', '.join(missing)}", file=sys.stderr)
    print("Подсказка: cp .env.example .env && source .env", file=sys.stderr)
    sys.exit(1)

file_path = 'yellow_tripdata_2025-01.parquet'

engine = create_engine(URL.create(
    drivername='postgresql',
    username=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD'],
    host=os.environ.get('POSTGRES_HOST', 'localhost'),
    port=int(os.environ.get('POSTGRES_PORT', '5432')),
    database=os.environ['POSTGRES_DB'],
))

parquet_file = pq.ParquetFile(file_path)

is_first_chunk = True
total_rows = 0

for batch in parquet_file.iter_batches(batch_size=100000):
    t_start = time()
    df = batch.to_pandas()

    if is_first_chunk:
        df.to_sql(name='yellow_taxi_trips', con=engine, if_exists='replace', index=False)
        is_first_chunk = False
    else:
        df.to_sql(name='yellow_taxi_trips', con=engine, if_exists='append', index=False)

    t_end = time()
    total_rows += len(df)
    print(f"Вставлено {len(df)} строк (Время: {t_end - t_start:.2f} сек). Всего: {total_rows}")

print(f"Загружено {total_rows} строк")
