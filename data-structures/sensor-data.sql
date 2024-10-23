-- the destination table
CREATE TABLE iot_analytics.iot_measurements_raw
(
    "ownerId" LowCardinality(String) CODEC(ZSTD(1)),
    "factoryId" LowCardinality(String) CODEC(ZSTD(1)),
    "sensorId" String CODEC(ZSTD(1)),
    "timestamp" DATETIME64 NOT NULL CODEC(ZSTD(1)),
    "sensorType" Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6) CODEC(ZSTD(1)),
    "value" Float64 CODEC(ZSTD(1))
)
ENGINE = MergeTree()
ORDER BY ("ownerId", "factoryId", "sensorId", "timestamp")
PARTITION BY toYYYYMM(timestamp);

-- the materialized view to move data from the kafka native table
CREATE MATERIALIZED VIEW iot_analytics.iot_measurements_raw_mv
TO iot_analytics.iot_measurements_raw
AS
SELECT * FROM `service_kafka-gcp-eu`.iot_measurements_kafka_table


-- MV to calculate the avg, quat90 and quant99 of each sensor
CREATE TABLE iot_analytics.measurements_aggregates_per_device
(
    ownerId LowCardinality(String),
    factoryId LowCardinality(String),
    sensorId String,
    sensorType Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6),
    avgValue AggregateFunction(avg, Float64),
    quant90 AggregateFunction(quantile(0.9), Float64),
    quant99 AggregateFunction(quantile(0.99), Float64)
)
ENGINE = AggregatingMergeTree()
ORDER BY ("ownerId", "factoryId", "sensorId");

-- MV
CREATE MATERIALIZED VIEW iot_analytics.measurements_aggregates_per_device_mv
TO iot_analytics.measurements_aggregates_per_device
AS
SELECT
    ownerId,
    factoryId,
    sensorId,
    avgState(value) as avgValue,
    quantileState(0.9)(value) as quant90,
    quantileState(0.99)(value) as quant99
FROM iot_analytics.iot_measurements_raw
GROUP BY ownerId, factoryId, sensorId


-- Read the raw data 
SELECT * FROM iot_analytics.iot_measurements_raw LIMIT 10

-- Read the data
SELECT ownerId, factoryId, sensorId, avgMerge(avgValue) as avgValue, quantileMerge(quant90) as quant90, quantileMerge(quant99) as quant99
FROM iot_analytics.measurements_aggregates_per_device
GROUP BY ownerId, factoryId, sensorId
LIMIT 10


-- Now create a readings per sensor MV
-- Create the destination table

CREATE TABLE num_measurements_per_sensor
(
    ownerId LowCardinality(String),
    factoryId LowCardinality(String),
    sensorId String,
    sensorType Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6),
    numMeasurements AggregateFunction(count, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY ("ownerId", "factoryId", "sensorId");

-- create the MV
CREATE MATERIALIZED VIEW num_measurements_per_sensor_mv
TO num_measurements_per_sensor
AS
SELECT 
    ownerId,
    factoryId,
    sensorId,
    countState() as numMeasurements
FROM iot_analytics.iot_measurements_raw
GROUP BY ownerId, factoryId, sensorId

-- Read the data
SELECT ownerId, factoryId, sensorId, countMerge(numMeasurements) as numMeasurements
FROM num_measurements_per_sensor
GROUP BY ownerId, factoryId, sensorId
ORDER BY numMeasurements DESC
LIMIT 10


-- Create a MV that will compare the latest value with the quant 90 value and send to a new table if it is higher

CREATE OR REPLACE TABLE iot_analytics.high_value_alerts
(
    ownerId LowCardinality(String),
    factoryId LowCardinality(String),
    sensorId String,
    sensorType Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6),
    highValue Float64,
    quant90Value Float64
) ENGINE = MergeTree()
ORDER BY ("ownerId", "factoryId", "sensorId");

-- Create the dictionary to hold the quant values

CREATE OR REPLACE DICTIONARY iot_analytics.quantile_dict
(
    ownerId String,
    factoryId String,
    sensorId String,
    avgValue Float64,
    quant90Value Float64
) PRIMARY KEY ownerId, factoryId, sensorId
SOURCE(CLICKHOUSE(QUERY 'SELECT ownerId, factoryId, sensorId, avgMerge(avgValue) as avgValue, quantileMerge(quant90) as quant90Value FROM iot_analytics.measurements_aggregates_per_device GROUP BY ownerId, factoryId, sensorId'))
LAYOUT(HASHED())
LIFETIME(60)



-- Create the MV
CREATE MATERIALIZED VIEW iot_analytics.high_value_alerts_mv
TO iot_analytics.high_value_alerts
AS
SELECT
    m.ownerId,
    m.factoryId,
    m.sensorId,
    m.value as highValue,
    q.quant90Value
FROM iot_analytics.iot_measurements_raw AS m
ANY LEFT JOIN iot_analytics.quantile_dict AS q
ON m.ownerId = q.ownerId AND m.factoryId = q.factoryId AND m.sensorId = q.sensorId
WHERE highValue > q.quant90Value

-- Read the data
SELECT * FROM iot_analytics.high_value_alerts


-- Send this data to Kafka
