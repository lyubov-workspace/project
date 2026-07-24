# Отчёт: NYC Yellow Taxi Analytics

**Данные:** NYC Yellow Taxi, 2025 (~48.7 млн поездок)

---

## 1. Архитектура

```
data/yellow_tripdata_2025-*.parquet
        │
        ▼
insert_data_2025.py  (pandas + PyArrow + SQLAlchemy)
        │
        ▼
PostgreSQL (staging)          main_postgres :5432
  БД taxi_db: yellow_taxi_trips
  БД taxi_archive: yellow_taxi_trips   — копия по дням (DAG PG→PG)
        │
        ├─ Airflow: transfer_postgres_to_postgres
        │
        ▼  Airflow DAG: transfer_postgres_to_clickhouse
ClickHouse (OLAP)             main_clickhouse :8123
  fact_taxi_trips               — сырые поездки
  taxi_zones                    — справочник зон
  taxi_daily_metrics            — витрина по дням
  dq_log                        — журнал DQ-проверок
        │
        ▼
Superset :8088                  — дашборд NYC Taxi 2025
```

| Компонент | Роль |
|-----------|------|
| **PostgreSQL** | staging (`taxi_db`) и архивная копия (`taxi_archive`) на одном инстансе |
| **ClickHouse** | OLAP: быстрые агрегации на миллионах строк (колоночное хранение) |
| **Airflow** | оркестрация: расписание, цепочка задач, перезапуск без поломки данных |
| **Superset** | BI: дашборды и графики поверх ClickHouse |
| **Docker Compose** | все сервисы поднимаются одной командой, данные в томах |

### Поток данных

**1. parquet → Postgres** (`insert_data_2025.py`)

- Скачивает parquet TLC в `data/` и читает батчами по 100 000 строк (PyArrow).
- Таблица `yellow_taxi_trips` по DDL `sql/ddl/postgres/01_yellow_taxi_trips.sql` (типы, месячные партиции, индекс по `tpep_pickup_datetime`); данные — через `COPY`.
- Креды Postgres берутся из `.env`.

**2. Postgres → Postgres** (DAG `transfer_postgres_to_postgres`)

- Источник: БД `taxi_db`, приёмник: БД `taxi_archive` (тот же `main_postgres`, не метаданные Airflow).
- За день `ds`: `DELETE` в archive + `COPY` строк с pickup за этот день (только 2025).
- Повторный run за тот же день не плодит дубли.

**3. Postgres → ClickHouse** (DAG `transfer_postgres_to_clickhouse`)

- `DROP PARTITION` за день — перед загрузкой удаляется партиция за этот день в `fact_taxi_trips`. Если DAG перезапустить, дубликатов не будет (идемпотентность).
- `INSERT ... SELECT FROM postgresql(pg_taxi, table = 'yellow_taxi_trips')` — ClickHouse читает Postgres через named collection `pg_taxi` (креды в `config.d`, не в тексте запроса / `query_log`).
- Фильтр по дню `toDate(tpep_pickup_datetime) = '{{ ds }}'` и окно pickup ∈ [2025-01-01, 2026-01-01): в ClickHouse не попадают строки с датами вне 2025 (DEFAULT в Postgres).
- Таблица партиционирована по `toYYYYMMDD(tpep_pickup_datetime)` — дневные партиции; DAG на весь 2025 (`end_date` 2025-12-31).

**4. Витрина метрик** (DAG `taxi_metrics_incremental_load`)

- Смотрит `MAX(report_date)` в `taxi_daily_metrics` и `MAX(toDate(...))` в `fact_taxi_trips`.
- Если сырые данные «впереди» витрины — догружает до 5 дней за один запуск.
- Считает `total_trips`, `total_revenue`, `driver_revenue`, а также `median_speed_mph`, `revenue_per_mile`, `revenue_per_minute` (скорость/эффективность — по отфильтрованным поездкам).
- Перед вставкой удаляет то же окно дат (идемпотентность), затем пишет строки заново.

**5. Data Quality**

- DAG `data_quality_checks` — по расписанию `@hourly`, результаты в ClickHouse `dq_log`.

| Проверка | Что проверяет |
|----------|---------------|
| `negative_revenue` | нет отрицательной выручки в витрине |
| `null_date` | нет пустых `report_date` |
| `duplicate_date` | нет дублей по дате |
| `zero_trips` | нет дней с нулём поездок |
| `volume_drop_50_pct` | нет падения объёма >50% день к дню |

- В Postgres на `yellow_taxi_trips`: триггер `trg_yellow_taxi_dq` после `INSERT` вызывает функцию `run_yellow_taxi_dq()` — проверки за последний день pickup, запись в `staging_dq_log` (`sql/ddl/postgres/03_dq_trigger.sql`).

**6. Визуализация** — Superset подключается к ClickHouse и строит чарты на `taxi_daily_metrics`, `fact_taxi_trips` и SQL-датасетах.

---

## 2. Решения

### Модель данных: звезда

В ClickHouse:

- **факт** `fact_taxi_trips` — одна строка = одна поездка (меры: расстояние, суммы; ключи зон и типа оплаты);
- **измерение** `taxi_zones` — атрибуты зоны (`Borough`, `Zone`, `service_zone`) по `LocationID`;
- витрина `taxi_daily_metrics` — агрегат по дням поверх факта.

**Обоснование выбора схемы.** Снежинка дала бы лишний уровень нормализации (например, borough отдельной таблицей) без выигрыша на объёме справочника. Data Vault усложнил бы модель без выгоды при одном стабильном источнике TLC: история изменений и разделение hub/link/satellite здесь не нужны. Остановились на звезде: `fact_taxi_trips` + `taxi_zones`.

### Postgres как staging, ClickHouse как warehouse

Postgres хорош для «положить сырые данные» (pandas, транзакции, `COPY`). ClickHouse — для аналитики: `GROUP BY` на десятках миллионов строк выполняется быстро за счёт колоночного формата и партиций. Это типичная схема landing → warehouse, а не «всё сразу в ClickHouse».

### Идемпотентность ETL

Без `DROP PARTITION` повторный запуск DAG за тот же день добавил бы строки второй раз. Схема «удалить партицию → вставить заново» даёт тот же результат, что и один успешный прогон.

### Партиционирование в ClickHouse

| Таблица | Партиции | Зачем так |
|--------|----------|-----------|
| `fact_taxi_trips` | день pickup (`toYYYYMMDD`) | daily ETL: `DROP PARTITION` + insert за `ds`; в запросах за день читается нужная партиция |
| `taxi_daily_metrics` | нет | ~одна строка на день (~365/год) — отдельный кусок на диске не окупается |
| `dq_log` | нет | журнал проверок, небольшой объём; доступ по `ORDER BY (event_time, check_name)` |

Отдельное субпартиционирование (партиция внутри партиции) не делали: в ClickHouse его нет как в Postgres; нужная селективность внутри дня у факта достигается ключом сортировки `ORDER BY (tpep_pickup_datetime, PULocationID, DOLocationID)`.

### Инкрементальная витрина вместо полного пересчёта

Полный `INSERT ... SELECT ... GROUP BY` за весь месяц при каждом запуске избыточен. DAG догоняет только новые дни — экономит время и нагрузку на ClickHouse.

### DDL в репозитории

Схемы описаны в `sql/ddl/postgres/` и `sql/ddl/clickhouse/` — проект можно развернуть с нуля, не восстанавливая таблицы из памяти или DBeaver.

### Секреты в `.env`

Пароли и ключи — в `.env` (не в git). В репозитории только `.env.example` с заглушками `change_me`. DAG-и и `insert_data_2025.py` читают креды из переменных окружения.

### Docker: Postgres

- `main_postgres` — данные такси: БД `taxi_db` (staging) и `taxi_archive` (дневные копии).
- `airflow-postgres` — служебная БД Airflow (расписание, история запусков), не приёмник ETL.

Сервис Airflow Postgres переименован в `airflow-postgres`, чтобы в общей Docker-сети `databases_default` не было конфликта DNS-имени `postgres`.

### Superset + ClickHouse

Драйвер `clickhouse-connect` ставится при старте контейнера в persistent volume (`superset_home`). Подключение из Superset: host `clickhouse` (имя сервиса в compose), port `8123`, database `default`.

### Дашборд: читаемые зоны

Bar chart по `PULocationID` показывает только цифры (138, 236…). Решение — SQL-датасет с `JOIN taxi_zones`, на графике названия: JFK Airport, Midtown и т.д.

---

## 3. Бенчмарки

**Бенчмарк** — сравнение «до» и «после» оптимизации на одних данных: сколько строк/партиций читает запрос, сколько времени выполняется.

Скрипты: `sql/optimizations/`. Замеры: `EXPLAIN ESTIMATE` и время в DBeaver.

### Кейс 1. Bloom-filter skip index

**Файл:** `01_skip_index.sql`  
**Задача:** найти редкие поездки `PULocationID = 206`.

- **До:** без skip index ClickHouse перебирает все гранулы партиций, попавших в запрос (по `EXPLAIN ESTIMATE` — 31 mark / 253 952 rows).
- **После:** `ADD INDEX ... TYPE bloom_filter()` + `MATERIALIZE INDEX` — для каждой гранулы хранится «есть ли здесь 206», лишние гранулы пропускаются (7 marks / 57 344 rows).

| | До | После |
|---|-----|-------|
| Метод | без skip index | bloom-filter skip index |
| Партиций (parts) | 31 | 7 |
| Строк к чтению (rows) | 253 952 | 57 344 |
| Гранул (marks) | 31 | 7 |

### Кейс 2. Partition pruning

**Файл:** `02_partition.sql`  
**Задача:** `sum(total_amount)` за 15.01.2025.

- **До:** `formatDateTime(tpep_pickup_datetime, ...) = '2025-01-15'` — функция на колонке, оптимизатор не может сопоставить с ключом партиции → читаются все партиции января.
- **После:** `toDate(tpep_pickup_datetime) = '2025-01-15'` — совпадает с ключом партиционирования → читается одна партиция `20250115`.

| | До | После |
|---|-----|-------|
| Условие | `formatDateTime(...)` | `toDate(...)` |
| Партиций (parts) | 31 | 1 |
| Строк к чтению (rows) | 3 475 204 | 126 388 |
| Гранул (marks) | 425 | 16 |

### Кейс 3. Query result cache

**Файл:** `03_cache.sql`  
**Задача:** топ-10 маршрутов `(PULocationID, DOLocationID)`.

- **До:** каждый запуск — полный пересчёт `GROUP BY` на миллионах строк.
- **После:** `SETTINGS use_query_cache = 1` — повторный идентичный запрос (как в дашборде) возвращается из кэша.

| Запуск | Настройка | Время |
|--------|-----------|-------|
| 1-й (без кэша) | без `SETTINGS` | **0,102 с** |
| 2-й (кэш заполняется) | `use_query_cache = 1` | **0,063 с** |
| 3-й (из кэша) | `use_query_cache = 1` | **0,011 с** |

Для сравнения в отчёте: **до** = 1-й запуск (102 мс), **после** = 3-й запуск (11 мс) — ускорение примерно в **9 раз**.

### Кейс 4. Materialized View + POPULATE

**Файл:** `04_populate.sql`  
**Задача:** выручка по часам.

- **До:** каждый раз `GROUP BY toStartOfHour(...)` по `fact_taxi_trips` (~3.5M строк).
- **После:** `CREATE MATERIALIZED VIEW ... ENGINE = SummingMergeTree() ... POPULATE` — агрегаты предрасчитаны, чтение из `mv_hourly` (~768 строк за месяц).

| | До | После |
|---|-----|-------|
| Источник | `fact_taxi_trips` | `mv_hourly` |
| Партиций (parts) | 31 | 2 |
| Строк к чтению (rows) | 3 475 204 | 768 |
| Гранул (marks) | 425 | 2 |

> `CREATE MATERIALIZED VIEW` выдал `TABLE_ALREADY_EXISTS` — витрина была создана ранее, это нормально. Для замера «после» достаточно `EXPLAIN ESTIMATE` по `mv_hourly`.

### Кейс 5. Параллелизм

**Файл:** `05_parallel_processing.sql`  
**Задача:** `GROUP BY payment_type`, `sum(tip_amount)`.

- **До:** `SETTINGS max_threads = 1` — один поток CPU (`MergeTreeSelect ... x 31 -> 1` в плане).
- **После:** по умолчанию — ClickHouse использует **12 потоков** (`ExpressionTransform x 12`, `MergeTreeSelect ... Thread) x 12`).

| | `max_threads = 1` | по умолчанию (12 потоков) |
|---|-------------------|---------------------------|
| Время выполнения | **0,175 с** | **0,035 с** |
| Ускорение | — | ~**5×** |

---

## 4. Дашборд

**Название:** `NYC Taxi 2025`

| Чарт | Тип | Данные |
|------|-----|--------|
| Daily Revenue | Line | `taxi_daily_metrics`, `SUM(total_revenue)` по дням |
| Daily Driver Revenue | Line | `taxi_daily_metrics`, `SUM(driver_revenue)` по дням |
| Daily Trips | Line/Bar | `taxi_daily_metrics`, `SUM(total_trips)` по дням |
| Daily Median Speed (mph) | Line | `taxi_daily_metrics`, `AVG(median_speed_mph)` по дням |
| Top 10 Pickup Zones | Bar | SQL-датасет: `fact_taxi_trips` + `taxi_zones` |
| Payment Type Distribution | Pie | `fact_taxi_trips`, `COUNT(*)` по `payment_type` |

### Скриншоты

![Метрики по дням](images/dashboard_metrics.png)

![Оплата и зоны pickup](images/dashboard_zones.png)

---

## 5. Инсайты

Метрика **доход водителя** = `fare_amount + tip_amount`.

1. **Объём** — ≈ 48.4 млн поездок за 2025.
2. **Доход vs чек** — сумма `total_amount` ≈ $1.30 млрд, доход водителей ≈ $1.03 млрд (~79% чека). Tip rate (чаевые к fare) ≈ **15.4%** за год.
3. **Сезонность** — пик driver_revenue в мае–июне и снова осенью; декабрь даёт максимальный средний чек водителя (≈ $25 за поездку против ≈ $20–22 в остальные месяцы).
4. **Дни недели** — средний доход водителя выше в будни (пн ≈ $22.1), ниже в субботу (≈ $19.9); tip rate выше в начале недели (~16%), ниже в вс (~13.9%).
5. **География pickup** — по сумме дохода лидируют JFK и LaGuardia (высокий средний чек ≈ $65 и ≈ $52), далее центр Manhattan (Midtown, Upper East Side, Times Square) с большим числом поездок, но меньшим средним чеком.
6. **Скорость** — медиана ≈ 9.5 mph за год; днём (примерно 11–17) медиана падает до ≈ 8–8.5 mph, ночью и рано утром выше (≈ 12–17 mph) — типичные пробки Manhattan vs более свободные часы.
7. **Эффективность** — в среднем ≈ $8.9 дохода водителя на милю и ≈ $1.35 на минуту поездки; в декабре выручка на милю заметно выше (≈ $10.6), при сопоставимой медиане скорости.
8. **Оплата** — основная доля у карт (`payment_type = 1`); в сырье есть `payment_type = 0` вне справочника TLC (пропуски/шум).

Запросы к скорости/эффективности: `sql/analytics/01_speed_efficiency.sql`.
