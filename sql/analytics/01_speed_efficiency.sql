-- Скорость и эффективность. Запуск вручную в клиенте ClickHouse.
-- Фильтр «чистых» поездок: дистанция 0.1–100 миль, длительность 1 мин–3 ч, скорость 1–80 mph.

SELECT
    count() AS trips,
    round(avg(spd), 2) AS avg_mph,
    round(quantile(0.5)(spd), 2) AS median_mph,
    round(avg(rev_per_mile), 2) AS avg_revenue_per_mile,
    round(avg(rev_per_min), 2) AS avg_revenue_per_minute
FROM
(
    SELECT
        trip_distance / (dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 3600.0) AS spd,
        (fare_amount + tip_amount) / trip_distance AS rev_per_mile,
        (fare_amount + tip_amount) / (dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0) AS rev_per_min
    FROM default.fact_taxi_trips
    WHERE toDate(tpep_pickup_datetime) >= '2025-01-01'
      AND toDate(tpep_pickup_datetime) < '2026-01-01'
      AND trip_distance > 0.1
      AND trip_distance < 100
      AND dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 60 AND 10800
)
WHERE spd >= 1 AND spd <= 80;

SELECT
    toHour(tpep_pickup_datetime) AS pickup_hour,
    round(quantile(0.5)(
        trip_distance / (dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 3600.0)
    ), 2) AS median_mph,
    count() AS trips
FROM default.fact_taxi_trips
WHERE toDate(tpep_pickup_datetime) >= '2025-01-01'
  AND toDate(tpep_pickup_datetime) < '2026-01-01'
  AND trip_distance > 0.1
  AND trip_distance < 100
  AND dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 60 AND 10800
  AND trip_distance / (dateDiff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 3600.0) BETWEEN 1 AND 80
GROUP BY pickup_hour
ORDER BY pickup_hour;
