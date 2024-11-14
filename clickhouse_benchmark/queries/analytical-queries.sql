

-- Number of devices per owner

SELECT ownerId, count(sensorId) as numDevicesPerOwner
FROM default.iot_metadata
GROUP BY ownerId, factoryId, sensorId;

-- Fetch one weeks worth of historical data




-- Fetch all sensor values for each sensor type 'Temperature' within each factory

SELECT
    factoryId,
    sensorId,
    value
FROM default.iot_measurements_raw
WHERE sensorType = 1;


-- How many values are stored per owner, per factory

SELECT
    ownerId,
    factoryId,
    count(value) as numValuesPerSensor
FROM default.iot_measurements_raw
GROUP BY ownerId, factoryId;

--
-- values beyond the 95th percentile for sensor type 'Temperature' for a particular owner

WITH quant_90_temp_sensors AS (
    SELECT
        quantile(0.9)(value) as quant90Value
    FROM default.iot_measurements_raw
    WHERE sensorType = 1
),
owners AS (
    SELECT
        ownerId
    FROM default.iot_metadata
    ORDER BY rand()
    LIMIT 1
)
SELECT
    ownerId,
    factoryId,
    count(value) as numValues
FROM default.iot_measurements_raw
WHERE sensorType = 1 AND value > (SELECT quant90Value FROM quant_90_temp_sensors) AND ownerId = (SELECT ownerId FROM owners)
GROUP BY ownerId, factoryId;

-- 90th quantile for each sensor type per factory
SELECT
    factoryId,
    sensorType,
    quantile(0.9)(value) as quant90Value
FROM default.iot_measurements_raw
GROUP BY factoryId, sensorType;

-- avg, 90th quantile and 99th quantile for each sensor type in factory x, y, z
SELECT
    factoryId,
    avg(value) as avgValue,
    quantile(0.9)(value) as quant90Value,
    quantile(0.99)(value) as quant99Value
FROM default.iot_measurements_raw
WHERE factoryId IN ('factory_af88af30-d741-4d99-beee-0e3ac0cd1837', 'factory_008a36db-79c0-47d1-a697-6d6233875f1b', 'factory_ab377f78-56e6-47a1-9252-035034163ed3')
GROUP BY factoryId, sensorType;

-- Fetch all values for 10 random sensors
WITH sensors AS (
    SELECT
        sensorId
    FROM default.iot_metadata
    ORDER BY rand()
    LIMIT 10
)
SELECT
    sensorId,
    value
FROM default.iot_measurements_raw
WHERE sensorId IN (SELECT sensorId FROM sensors);

-- Fetch the number of values for each factory for a specific owner in descending order
WITH owners As (
    SELECT
        ownerId
    FROM default.iot_metadata
    ORDER BY rand()
    LIMIT 1
)
SELECT
    ownerId,
    factoryId,
    count(value) as numValues
FROM default.iot_measurements_raw
WHERE ownerId = (SELECT ownerId FROM owners)
GROUP BY ownerId, factoryId
ORDER BY numValues DESC;

--

SELECT
    database,
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS table_size
FROM system.parts
WHERE database = 'iot_analytics' AND table = 'iot_measurements_raw'
GROUP BY database, table;
