-- Staging: NYC Yellow Taxi trips.
-- Партиции по месяцу pickup (фильтр ETL/DAG по дате).
-- DEFAULT — строки с датами вне 2025 (шум источника TLC).
-- Естественного уникального id поездки в TLC нет — PK не задаём.

CREATE TABLE yellow_taxi_trips (
    "VendorID"              SMALLINT,
    tpep_pickup_datetime    TIMESTAMP NOT NULL,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         SMALLINT,
    trip_distance           DOUBLE PRECISION,
    "RatecodeID"            SMALLINT,
    store_and_fwd_flag      TEXT,
    "PULocationID"          INTEGER,
    "DOLocationID"          INTEGER,
    payment_type            SMALLINT,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    "Airport_fee"           DOUBLE PRECISION,
    cbd_congestion_fee      DOUBLE PRECISION
) PARTITION BY RANGE (tpep_pickup_datetime);

CREATE TABLE yellow_taxi_trips_2025_01 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE yellow_taxi_trips_2025_02 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE yellow_taxi_trips_2025_03 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE yellow_taxi_trips_2025_04 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE yellow_taxi_trips_2025_05 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE yellow_taxi_trips_2025_06 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE yellow_taxi_trips_2025_07 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE yellow_taxi_trips_2025_08 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE yellow_taxi_trips_2025_09 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE yellow_taxi_trips_2025_10 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE yellow_taxi_trips_2025_11 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE yellow_taxi_trips_2025_12 PARTITION OF yellow_taxi_trips
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');
CREATE TABLE yellow_taxi_trips_default PARTITION OF yellow_taxi_trips DEFAULT;

CREATE INDEX idx_yellow_taxi_trips_pickup ON yellow_taxi_trips (tpep_pickup_datetime);
