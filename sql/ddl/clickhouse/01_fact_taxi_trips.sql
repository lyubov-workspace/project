-- Фактовая таблица поездок. Партиционирование по дате подачи (совпадает с DAG transfer_postgres_to_clickhouse).
CREATE TABLE IF NOT EXISTS default.fact_taxi_trips
(
    VendorID            UInt8,
    tpep_pickup_datetime   DateTime,
    tpep_dropoff_datetime  DateTime,
    passenger_count     UInt8,
    trip_distance       Float32,
    RatecodeID          UInt8,
    PULocationID        UInt16,
    DOLocationID        UInt16,
    payment_type        UInt8,
    fare_amount         Float32,
    tip_amount          Float32,
    total_amount        Float32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(tpep_pickup_datetime)
ORDER BY (tpep_pickup_datetime, PULocationID, DOLocationID);
