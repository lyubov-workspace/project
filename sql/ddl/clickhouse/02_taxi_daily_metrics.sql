-- Дневная витрина метрик (DAG taxi_metrics_incremental_load).
-- Без PARTITION BY: за год ~365 строк, партиции не дают выигрыша; достаточно ORDER BY report_date.
-- total_revenue = sum(total_amount); driver_revenue = sum(fare_amount + tip_amount).
-- Скорость/эффективность — только по «чистым» поездкам (см. metrics.py).
CREATE TABLE IF NOT EXISTS default.taxi_daily_metrics
(
    report_date          Date,
    total_trips          UInt32,
    total_revenue        Float64,
    driver_revenue       Float64,
    median_speed_mph     Float64,
    revenue_per_mile     Float64,
    revenue_per_minute   Float64
)
ENGINE = MergeTree()
ORDER BY report_date;
