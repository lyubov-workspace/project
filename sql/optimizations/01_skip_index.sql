-- Задача: поиск редких событий (поездок из непопулярной локации PULocationID = 206)

-- 1. Запрос до оптимизации (Full Scan)
 EXPLAIN ESTIMATE
SELECT * FROM default.fact_taxi_trips
WHERE PULocationID = 206;

-- 2. Применение оптимизации
-- Удаляем старый индекс, если он был
ALTER TABLE default.fact_taxi_trips DROP INDEX IF EXISTS bf_pickup_loc;

-- Создаем Bloom-filter на колонку PULocationID
ALTER TABLE default.fact_taxi_trips
ADD INDEX bf_pickup_loc PULocationID TYPE bloom_filter() GRANULARITY 1;

-- Создаем индекс для существующих данных
ALTER TABLE default.fact_taxi_trips MATERIALIZE INDEX bf_pickup_loc;

-- 3. Запрос после оптимизации (Bloom-filter)
 EXPLAIN ESTIMATE
SELECT * FROM default.fact_taxi_trips
WHERE PULocationID = 206;
