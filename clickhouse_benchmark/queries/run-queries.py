import os

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")

client = clickhouse_connect.get_client(
    host=db_host,
    port=db_port,
    username=db_username,
    password=db_password,
    interface="https",
)


# Query 1 - Get aggregates for all sensors in a subset of factories
get_factory_aggregate_values_query = """
WITH factories AS (
    SELECT factoryId
    FROM default.iot_metadata
    ORDER BY rand()
    LIMIT 3
)
SELECT
    factoryId,
    avg(value) as avgValue,
    quantile(0.9)(value) as quant90Value,
    quantile(0.99)(value) as quant99Value
FROM default.iot_measurements_raw
WHERE factoryId IN (SELECT factoryId FROM factories)
GROUP BY factoryId, sensorType"""

factory_aggregate_results = client.query(get_factory_aggregate_values_query)
print(f"Factory aggregate values query id: {factory_aggregate_results.query_id}")
print(factory_aggregate_results.summary)

# Query 2 - Fetch values for 10 specific sensors
get_all_values_for_specific_sensors_query = """
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
WHERE sensorId IN (SELECT sensorId FROM sensors)"""
all_values_for_specific_sensors_results = client.query(
    get_all_values_for_specific_sensors_query
)
print(
    f"Values for specific sensors query id: {all_values_for_specific_sensors_results.query_id}"
)
print(all_values_for_specific_sensors_results.summary)

# Query 3 - Get the total value count for all sensors for a specific owner
get_total_sensor_count_for_owner_query = """
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
ORDER BY numValues DESC"""

total_sensor_count_for_owner_results = client.query(
    get_total_sensor_count_for_owner_query
)
print(
    f"Total sensor count for owner query id: {total_sensor_count_for_owner_results.query_id}"
)
print(total_sensor_count_for_owner_results.summary)

# Query 4 - Get the number of values beyond the 95th percentile for each factory for one owner
get_values_beyond_95th_percentile_for_one_owner_query = """
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
GROUP BY ownerId, factoryId"""

values_beyond_95th_percentile_for_one_owner_results = client.query(
    get_values_beyond_95th_percentile_for_one_owner_query
)
print(
    f"Values beyond 95th percentile for one owner query id: {values_beyond_95th_percentile_for_one_owner_results.query_id}"
)
print(values_beyond_95th_percentile_for_one_owner_results.summary)

# Query 5 - Get the 90th percentile value for each sensor type per factory
get_90th_percentile_value_per_sensor_type_per_factory_query = """
SELECT
    factoryId,
    sensorType,
    quantile(0.9)(value) as quant90Value
FROM default.iot_measurements_raw
GROUP BY factoryId, sensorType"""

percentile_value_per_sensor_type_per_factory_results = client.query(
    get_90th_percentile_value_per_sensor_type_per_factory_query
)
print(
    f"90th percentile value per sensor type per factory query id: {percentile_value_per_sensor_type_per_factory_results.query_id}"
)
print(percentile_value_per_sensor_type_per_factory_results.summary)

# Query 6 - Fetch all values of type 'Temperature' sensor for each factory
get_all_temperature_values_per_factory_query = """
SELECT
    factoryId,
    sensorId,
    value
FROM default.iot_measurements_raw
WHERE sensorType = 1"""

all_temperature_values_per_factory_results = client.query(
    get_all_temperature_values_per_factory_query
)
print(
    f"All temperature values per factory query id: {all_temperature_values_per_factory_results.query_id}"
)
print(all_temperature_values_per_factory_results.summary)

# Query 7 - Get the number of values per owner per factory
get_num_values_per_owner_per_factory_query = """
SELECT
    ownerId,
    factoryId,
    count(value) as numValuesPerSensor
FROM default.iot_measurements_raw
GROUP BY ownerId, factoryId"""

num_values_per_owner_per_factory_results = client.query(
    get_num_values_per_owner_per_factory_query
)
print(
    f"Number of values per owner per factory query id: {num_values_per_owner_per_factory_results.query_id}"
)
print(num_values_per_owner_per_factory_results.summary)


# Query 8 - Get the total number of devices per owner
get_total_devices_per_owner_query = """
SELECT ownerId, count(sensorId) as numDevicesPerOwner
FROM default.iot_measurements_raw
GROUP BY ownerId, factoryId, sensorId"""

total_devices_per_owner_results = client.query(get_total_devices_per_owner_query)
print(f"Total devices per owner query id: {total_devices_per_owner_results.query_id}")
print(total_devices_per_owner_results.summary)
