CREATE OR REPLACE TABLE iot_analytics.iot_metadata
(
    "rowNumber" UInt64 CODEC(Delta, ZSTD(1)),
    "ownerId" LowCardinality(String) CODEC(ZSTD(1)),
    "factoryId" LowCardinality(String) CODEC(ZSTD(1)),
    "sensorId" String CODEC(ZSTD(1)),
    "sensorType" Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6) CODEC(ZSTD(1))
)
ENGINE = MergeTree()
ORDER BY ("rowNumber", "ownerId", "factoryId", "sensorId")  

CREATE OR REPLACE TABLE iot_analytics.iot_measurements_raw_temp
(
    "ownerId" LowCardinality(String) CODEC(ZSTD(1)),
    "factoryId" LowCardinality(String) CODEC(ZSTD(1)),
    "sensorId" String CODEC(ZSTD(1)),
    "timestamp" DATETIME64 NOT NULL CODEC(ZSTD(1)),
    "sensorType" Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6) CODEC(ZSTD(1)),
    "value" Float64 CODEC(ZSTD(1))
)
ENGINE = MergeTree()
ORDER BY ("ownerId", "factoryId", "sensorId", "timestamp");