-- Задача: Поиск суммы выручки за конкретный день (15 января 2025 года)

-- 1. Запрос до оптимизации
-- Оптимизатор не может использовать индекс партиций, так как колонка обернута в formatDateTime
EXPLAIN ESTIMATE
SELECT sum(total_amount)
FROM default.fact_taxi_trips
WHERE formatDateTime(tpep_pickup_datetime, '%Y-%m-%d') = '2025-01-15';

-- 2. Запрос после оптимизации
-- Прямое обращение к ключу партиционирования (toDate)
EXPLAIN ESTIMATE
SELECT sum(total_amount)
FROM default.fact_taxi_trips
WHERE toDate(tpep_pickup_datetime) = '2025-01-15';
