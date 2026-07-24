-- Задача: подсчет суммы чаевых и количества поездок по типам оплаты

-- 1. Запрос до оптимизации (ограничение ресурсов)
EXPLAIN PIPELINE
SELECT payment_type, count(*), sum(tip_amount)
FROM default.fact_taxi_trips
GROUP BY payment_type
SETTINGS max_threads = 1;

-- 2. Запрос после оптимизации (многопоточность)
EXPLAIN PIPELINE
SELECT payment_type, count(*), sum(tip_amount)
FROM default.fact_taxi_trips
GROUP BY payment_type;

SELECT payment_type, count(*), sum(tip_amount)
FROM default.fact_taxi_trips
GROUP BY payment_type
SETTINGS max_threads = 1;

SELECT payment_type, count(*), sum(tip_amount)
FROM default.fact_taxi_trips
GROUP BY payment_type;