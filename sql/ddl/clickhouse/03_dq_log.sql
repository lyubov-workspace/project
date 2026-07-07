-- Журнал проверок качества данных (заполняется DAG data_quality_checks).
CREATE TABLE IF NOT EXISTS default.dq_log
(
    event_time  DateTime,
    table_name  String,
    check_name  String,
    status      String,
    metric_value UInt32
)
ENGINE = MergeTree()
ORDER BY (event_time, check_name);
