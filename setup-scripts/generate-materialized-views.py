import clickhouse_connect
from dotenv import load_dotenv
import os


load_dotenv()

db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')

client = clickhouse_connect.get_client(host=db_host, port=db_port, username=db_username, password=db_password, interface="https")

kafka_cluster_name = os.getenv('KAFKA_CLUSTER_NAME')

# first drop the MV if it exists
drop_iot_sensor_data_kafka_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.iot_measurements_raw_mv"""
client.command(drop_iot_sensor_data_kafka_mv_query)

create_iot_sensor_data_kafka_mv_query = f"""CREATE MATERIALIZED VIEW iot_analytics.iot_measurements_raw_mv
TO iot_analytics.iot_measurements_raw
AS
SELECT * FROM `service_{kafka_cluster_name}`.iot_measurements_kafka_table"""

client.command(create_iot_sensor_data_kafka_mv_query)
print("Materialized view created successfully.")

# create an MV to calculate the avg, quant90 and quant99 values

create_measurements_aggregates_table_query = f"""CREATE OR REPLACE TABLE iot_analytics.measurements_aggregates_per_device
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
ORDER BY ("ownerId", "factoryId", "sensorId");"""

client.command(create_measurements_aggregates_table_query)
print("Aggregates table created successfully.")

# first drop the MV if it exists
drop_measurements_aggregates_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.measurements_aggregates_per_device_mv"""
client.command(drop_measurements_aggregates_mv_query)

create_measurements_aggregates_mv_query = f"""CREATE MATERIALIZED VIEW iot_analytics.measurements_aggregates_per_device_mv
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
GROUP BY ownerId, factoryId, sensorId"""

client.command(create_measurements_aggregates_mv_query)
print("Aggregates materialized view created successfully.")


# create the number of readings per sensor MV
# create table first
create_measurements_count_per_sensor_table_query = f"""CREATE OR REPLACE TABLE iot_analytics.num_measurements_per_sensor
(
    ownerId LowCardinality(String),
    factoryId LowCardinality(String),
    sensorId String,
    sensorType Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6),
    numMeasurements AggregateFunction(count, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY ("ownerId", "factoryId", "sensorId");"""

client.command(create_measurements_count_per_sensor_table_query)
print("Number of measurements per sensor table created successfully.")

# first drop the MV if it exists
drop_measurements_count_per_sensor_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.num_measurements_per_sensor_mv"""
client.command(drop_measurements_count_per_sensor_mv_query)

create_measurements_count_per_sensor_mv_query = f"""CREATE MATERIALIZED VIEW iot_analytics.num_measurements_per_sensor_mv
TO iot_analytics.num_measurements_per_sensor
AS
SELECT 
    ownerId,
    factoryId,
    sensorId,
    countState() as numMeasurements
FROM iot_analytics.iot_measurements_raw
GROUP BY ownerId, factoryId, sensorId"""

client.command(create_measurements_count_per_sensor_mv_query)
print("Number of measurements per sensor materialized view created successfully.")


# Create the high values alert mv
#   first create the table
create_high_value_alerts_table_query = f"""CREATE OR REPLACE TABLE iot_analytics.high_value_alerts
(
    ownerId LowCardinality(String),
    factoryId LowCardinality(String),
    sensorId String,
    sensorType Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6),
    highValue Float64,
    quant90Value Float64,
    timestamp DateTime64
) ENGINE = MergeTree()
ORDER BY ("ownerId", "factoryId", "sensorId");"""

client.command(create_high_value_alerts_table_query)
print("High value alerts table created successfully.")

# Now create the dictionary to hold the values
create_high_value_alerts_dict_query = f"""CREATE OR REPLACE DICTIONARY iot_analytics.quantile_dict
(
    ownerId String,
    factoryId String,
    sensorId String,
    avgValue Float64,
    quant90Value Float64
) PRIMARY KEY ownerId, factoryId, sensorId
SOURCE(CLICKHOUSE(QUERY 'SELECT ownerId, factoryId, sensorId, avgMerge(avgValue) as avgValue, quantileMerge(quant90) as quant90Value FROM iot_analytics.measurements_aggregates_per_device GROUP BY ownerId, factoryId, sensorId'))
LAYOUT(HASHED())
LIFETIME(60)"""

client.command(create_high_value_alerts_dict_query)
print("High value alerts dictionary created successfully.")


# first drop the MV if it exists
drop_high_value_alerts_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.high_value_alerts_mv"""
client.command(drop_high_value_alerts_mv_query)

create_high_value_alerts_mv_query = f"""CREATE MATERIALIZED VIEW iot_analytics.high_value_alerts_mv
TO iot_analytics.high_value_alerts
AS
SELECT
    m.ownerId,
    m.factoryId,
    m.sensorId,
    m.value as highValue,
    q.quant90Value,
    m.timestamp
FROM iot_analytics.iot_measurements_raw AS m
ANY LEFT JOIN iot_analytics.quantile_dict AS q
ON m.ownerId = q.ownerId AND m.factoryId = q.factoryId AND m.sensorId = q.sensorId
WHERE highValue > q.quant90Value"""

client.command(create_high_value_alerts_mv_query)
print("High value alerts materialized view created successfully.")



# send high value alerts to kafka 
# first drop MV if it exists

drop_high_value_alerts_kafka_table_mv = f"""DROP VIEW IF EXISTS iot_analytics.high_value_alerts_kafka_table_mv"""
client.command(drop_high_value_alerts_kafka_table_mv)

create_measurements_aggregates_per_device_kafka_table_mv = f"""CREATE MATERIALIZED VIEW iot_analytics.high_value_alerts_kafka_table_mv
TO `service_kafka-for-clickhouse-bench`.iot_high_values_kafka_table
AS
SELECT * FROM iot_analytics.high_value_alerts"""

client.command(create_measurements_aggregates_per_device_kafka_table_mv)
print("High value alerts kafka table materialized view created successfully.")

# send aggregated data to kafka
# first drop MV if it exists

drop_measurements_aggregates_per_device_kafka_table_mv = f"""DROP VIEW IF EXISTS iot_analytics.measurements_aggregates_per_device_kafka_table_mv"""
client.command(drop_measurements_aggregates_per_device_kafka_table_mv)

create_measurements_aggregates_per_device_kafka_table_mv = f"""CREATE MATERIALIZED VIEW iot_analytics.measurements_aggregates_per_device_kafka_table_mv
TO `service_kafka-for-clickhouse-bench`.iot_aggregates_kafka_table
AS
SELECT
    ownerId,
    factoryId,
    sensorId,
    avgMerge(avgValue) AS avgValue,
    quantileMerge(quant90) AS quant90,
    quantileMerge(quant99) AS quant99,
    now() AS timestamp
FROM iot_analytics.measurements_aggregates_per_device
GROUP BY
    ownerId,
    factoryId,
    sensorId"""

client.command(create_measurements_aggregates_per_device_kafka_table_mv)
print("Aggregates kafka table materialized view created successfully.")