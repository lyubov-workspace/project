-- Дневная витрина метрик (заполняется DAG taxi_metrics_incremental_load).
CREATE TABLE IF NOT EXISTS default.taxi_daily_metrics
(
    report_date   Date,
    total_trips   UInt32,
    total_revenue Float64
)
ENGINE = MergeTree()
ORDER BY report_date;
