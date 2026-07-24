-- Таблица в БД taxi_archive (вторая Postgres-БД на main_postgres).
-- Приёмник DAG transfer_postgres_to_postgres.

CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
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
);

CREATE INDEX IF NOT EXISTS idx_archive_yellow_taxi_trips_pickup
    ON yellow_taxi_trips (tpep_pickup_datetime);
