-- Журнал DQ на стороне Postgres + триггер после загрузки в staging.

CREATE TABLE IF NOT EXISTS staging_dq_log (
    event_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    table_name   TEXT NOT NULL,
    check_name   TEXT NOT NULL,
    status       TEXT NOT NULL,
    metric_value BIGINT NOT NULL
);

CREATE OR REPLACE FUNCTION run_yellow_taxi_dq()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    check_day date;
    neg_cnt bigint;
    null_pickup_cnt bigint;
BEGIN
    SELECT max(tpep_pickup_datetime)::date INTO check_day FROM yellow_taxi_trips;
    IF check_day IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT count(*) INTO neg_cnt
    FROM yellow_taxi_trips
    WHERE tpep_pickup_datetime >= check_day
      AND tpep_pickup_datetime < check_day + INTERVAL '1 day'
      AND total_amount < 0;

    INSERT INTO staging_dq_log (table_name, check_name, status, metric_value)
    VALUES (
        'yellow_taxi_trips',
        'negative_total_amount',
        CASE WHEN neg_cnt = 0 THEN 'SUCCESS' ELSE 'FAIL' END,
        neg_cnt
    );

    SELECT count(*) INTO null_pickup_cnt
    FROM yellow_taxi_trips
    WHERE tpep_pickup_datetime >= check_day
      AND tpep_pickup_datetime < check_day + INTERVAL '1 day'
      AND trip_distance < 0;

    INSERT INTO staging_dq_log (table_name, check_name, status, metric_value)
    VALUES (
        'yellow_taxi_trips',
        'negative_trip_distance',
        CASE WHEN null_pickup_cnt = 0 THEN 'SUCCESS' ELSE 'FAIL' END,
        null_pickup_cnt
    );

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_yellow_taxi_dq ON yellow_taxi_trips;
CREATE TRIGGER trg_yellow_taxi_dq
    AFTER INSERT ON yellow_taxi_trips
    FOR EACH STATEMENT
    EXECUTE FUNCTION run_yellow_taxi_dq();
