-- Справочник зон TLC (dimension для fact_taxi_trips).
CREATE TABLE IF NOT EXISTS default.taxi_zones
(
    LocationID   UInt16,
    Borough      String,
    Zone         String,
    service_zone String
)
ENGINE = MergeTree()
ORDER BY LocationID;
