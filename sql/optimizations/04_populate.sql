-- Задача: ускорение повторяющихся запросов (предварительный расчет выручки по часам)

-- 1. Запрос до оптимизации (Чтение сырых данных)
EXPLAIN ESTIMATE
SELECT toStartOfHour(tpep_pickup_datetime) AS pickup_hour, sum(total_amount)
FROM default.fact_taxi_trips
GROUP BY pickup_hour;

CREATE MATERIALIZED VIEW default.mv_hourly
ENGINE = SummingMergeTree()
ORDER BY pickup_hour
POPULATE
AS
SELECT
    toStartOfHour(tpep_pickup_datetime) AS pickup_hour,
    sum(total_amount) AS total_revenue,
    count(*) AS trips_count
FROM default.fact_taxi_trips
GROUP BY pickup_hour;

-- 2. Запрос после оптимизации (чтение готовой витрины)
EXPLAIN ESTIMATE
SELECT pickup_hour, total_revenue
FROM default.mv_hourly;
