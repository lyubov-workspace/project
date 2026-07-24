# NYC Yellow Taxi — pet project

Учебный проект: данные NYC Yellow Taxi за 2025 год.

```
parquet → Postgres → ClickHouse (Airflow) → Superset
```

## Стек

PostgreSQL, ClickHouse, Airflow, Superset. Всё в Docker.

| Сервис | Порт |
|--------|------|
| Postgres | 5432 |
| ClickHouse | 8123 |
| Airflow | 8080 |
| Superset | 8088 |

## Запуск

Все команды — из корня репозитория.

### 1. Переменные окружения

```bash
cp .env.example .env
# задайте пароли и SUPERSET_SECRET_KEY в .env
```

Файл `.env` в git не попадает. Список переменных — в `.env.example`.

### 2. Docker

Сначала базы (создаёт сеть `databases_default`), затем Airflow:

```bash
cd databases && docker compose --env-file ../.env up -d
cd ../airflow && docker compose --env-file ../.env up -d
```

### 3. Таблицы

ClickHouse:

```bash
set -a && source .env && set +a
for f in sql/ddl/clickhouse/*.sql; do
  docker exec -i main_clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" < "$f"
done
python load_taxi_zones.py
```

Postgres: DDL в `sql/ddl/postgres/` применяется скриптом загрузки (`--replace` пересоздаёт таблицу).

### 4. Загрузка данных

```bash
set -a && source .env && set +a
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python insert_data_2025.py
```

Скрипт скачивает parquet в `data/` и загружает их в Postgres.

### 5. Airflow

http://localhost:8080 — UI Airflow (учётные данные задаются при первом `docker compose up` образа, см. [документацию Airflow](https://airflow.apache.org/docs/docker-stack/entrypoint.html#airflow-admin-user-account)).

Включить и запустить:

- `transfer_postgres_to_postgres`
- `transfer_postgres_to_clickhouse`
- `taxi_metrics_incremental_load`
- `data_quality_checks`

### 6. Superset

http://localhost:8088 — логин из `.env` (`SUPERSET_ADMIN_*`).

Подключение ClickHouse: host `clickhouse`, port `8123`, database `default`, user/password из `CLICKHOUSE_*` в `.env`.

Креды Postgres для table function в ClickHouse — named collection `pg_taxi` (`databases/clickhouse/config.d/`).

## После перезагрузки

```bash
cd databases && docker compose --env-file ../.env up -d
cd ../airflow && docker compose --env-file ../.env up -d
```

Данные и дашборды сохраняются в Docker-томах.

## Структура

```
.env.example
insert_data_2025.py
load_taxi_zones.py
data/                      — parquet (не в git)
airflow/dags/              — ETL, метрики, DQ
databases/                 — Postgres, ClickHouse, Superset
sql/ddl/postgres/          — схема staging
sql/ddl/clickhouse/        — схема warehouse
sql/analytics/             — аналитические SQL (скорость и др.)
sql/optimizations/         — 5 кейсов оптимизации ClickHouse
taxi_zone_lookup.csv       — справочник зон NYC
docs/REPORT.md
```

## DAG-и

| DAG | Что делает |
|-----|------------|
| `transfer_postgres_to_postgres` | `taxi_db` → `taxi_archive` по дням |
| `transfer_postgres_to_clickhouse` | Postgres → ClickHouse по дням |
| `taxi_metrics_incremental_load` | витрина `taxi_daily_metrics` |
| `data_quality_checks` | проверки качества → `dq_log` (`@hourly`) |

## Примечания

- На `main_postgres`: БД `taxi_db` (staging) и `taxi_archive` (копия по дням). Отдельно `airflow-postgres` — только метаданные Airflow.
- Parquet в git не входит — скачивается отдельно.
- SQL-скрипты оптимизации (`sql/optimizations/`) запускаются вручную в DBeaver.
