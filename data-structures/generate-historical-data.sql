
-- CTEs don't work with insert queries so we need to create a table to hold the generated values
CREATE OR REPLACE TABLE generated_timestamps
(
    `timestamp` DateTime64,
    `rowNumber` UInt64
)
ENGINE = MergeTree
ORDER BY rowNumber
PARTITION BY toYYYYMM(timestamp)

-- generate a time series
INSERT INTO generated_timestamps
SELECT 
    now() - INTERVAL number MILLISECOND AS timestamp,
    rand() % (SELECT count(*) FROM iot_analytics.iot_metadata)
FROM 
    numbers(0, 30000000000)
SETTINGS
    min_insert_block_size_rows = 322122533,
    min_insert_block_size_bytes = 2048576000,
    max_threads = 8,
    max_insert_threads = 8;
-- 1048576 1 MB
-- Generate this query which associates each series with a row number. Now do a join with the metadata table to get the sensor type and generate a value based on that and have this as an intermediate table

INSERT INTO iot_analytics.iot_measurements_raw_temp 
SELECT 
    ownerId,
    factoryId,
    sensorId,
    timestamp,
    sensorType,
    CASE 
        WHEN sensorType = 1 THEN 15 + randCanonical() * 10
        WHEN sensorType = 2 THEN 30 + randCanonical() * 40
        WHEN sensorType = 3 THEN 990 + randCanonical() * 20
        WHEN sensorType = 4 THEN randCanonical() * 5
        WHEN sensorType = 5 THEN randCanonical() * 50
        WHEN sensorType = 6 THEN randCanonical() * 100
    END AS value
FROM generated_timestamps
JOIN iot_analytics.iot_metadata
ON generated_timestamps.rowNumber = iot_metadata.rowNumber
SETTINGS
    min_insert_block_size_rows = 322122533,
    min_insert_block_size_bytes = 2048576000,
    max_threads = 8,
    max_insert_threads = 8;



