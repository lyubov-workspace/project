-- Задача: ускорение повторяющихся запросов (вывод 10 популярных маршрутов и расчет их средней дальности)

-- 1. Запрос до оптимизации
SELECT
    PULocationID,
    DOLocationID,
    count(*) AS total_trips,
    avg(trip_distance) AS avg_dist
FROM default.fact_taxi_trips
GROUP BY PULocationID, DOLocationID
ORDER BY total_trips DESC
LIMIT 10;

-- 2. Запрос после оптимизации
SELECT
    PULocationID,
    DOLocationID,
    count(*) AS total_trips,
    avg(trip_distance) AS avg_dist
FROM default.fact_taxi_trips
GROUP BY PULocationID, DOLocationID
ORDER BY total_trips DESC
LIMIT 10
SETTINGS use_query_cache = 1;
