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
drop_iot_sensor_data_kafka_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.iot_measurements_raw_script_mv"""
client.command(drop_iot_sensor_data_kafka_mv_query)

create_iot_sensor_data_kafka_mv_query = f"""CREATE MATERIALIZED VIEW iot_analytics.iot_measurements_raw_script_mv
TO iot_analytics.iot_measurements_raw_temp_script
AS
SELECT * FROM `service_{kafka_cluster_name}`.iot_measurements_kafka_table"""

client.command(create_iot_sensor_data_kafka_mv_query)
print("Materialized view created successfully.")

# create an MV to calculate the avg, quant90 and quant99 values

create_measurements_aggregates_table_query = f"""CREATE OR REPLACE TABLE iot_analytics.measurements_aggregates_per_device_script
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
drop_measurements_aggregates_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.measurements_aggregates_per_device_mv_script"""
client.command(drop_measurements_aggregates_mv_query)

create_measurements_aggregates_mv_query = f"""CREATE MATERIALIZED VIEW iot_analytics.measurements_aggregates_per_device_mv_script
TO iot_analytics.measurements_aggregates_per_device_script
AS
SELECT
    ownerId,
    factoryId,
    sensorId,
    avgState(value) as avgValue,
    quantileState(0.9)(value) as quant90,
    quantileState(0.99)(value) as quant99
FROM iot_analytics.iot_measurements_raw_temp_script
GROUP BY ownerId, factoryId, sensorId"""

client.command(create_measurements_aggregates_mv_query)
print("Aggregates materialized view created successfully.")


# create the number of readings per sensor MV
# create table first
create_measurements_count_per_sensor_table_query = f"""CREATE OR REPLACE TABLE iot_analytics.num_measurements_per_sensor_script
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
drop_measurements_count_per_sensor_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.num_measurements_per_sensor_mv_script"""
client.command(drop_measurements_count_per_sensor_mv_query)

create_measurements_count_per_sensor_mv_query = f"""CREATE MATERIALIZED VIEW iot_analytics.num_measurements_per_sensor_mv_script
TO iot_analytics.num_measurements_per_sensor_script
AS
SELECT 
    ownerId,
    factoryId,
    sensorId,
    countState() as numMeasurements
FROM iot_analytics.iot_measurements_raw_temp_script
GROUP BY ownerId, factoryId, sensorId"""

client.command(create_measurements_count_per_sensor_mv_query)
print("Number of measurements per sensor materialized view created successfully.")


