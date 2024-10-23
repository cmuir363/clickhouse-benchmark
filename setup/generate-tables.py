import clickhouse_connect
from dotenv import load_dotenv
import os


load_dotenv()

db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')

client = clickhouse_connect.get_client(host=db_host, port=db_port, username=db_username, password=db_password, interface="https")

# create sensor-metadata table
sensor_metadata_table_query = """CREATE OR REPLACE TABLE iot_analytics.iot_metadata_script
(
    "rowNumber" UInt64 CODEC(Delta, ZSTD(1)),
    "ownerId" LowCardinality(String) CODEC(ZSTD(1)),
    "factoryId" LowCardinality(String) CODEC(ZSTD(1)),
    "sensorId" String CODEC(ZSTD(1)),
    "sensorType" Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6) CODEC(ZSTD(1))
)
ENGINE = MergeTree()
ORDER BY ("rowNumber", "ownerId", "factoryId", "sensorId")"""

client.command(sensor_metadata_table_query)

# create raw sensor table
raw_sensor_table_query = """CREATE OR REPLACE TABLE iot_analytics.iot_measurements_raw_temp_script
(
    "ownerId" LowCardinality(String) CODEC(ZSTD(1)),
    "factoryId" LowCardinality(String) CODEC(ZSTD(1)),
    "sensorId" String CODEC(ZSTD(1)),
    "timestamp" DATETIME64 NOT NULL CODEC(ZSTD(1)),
    "sensorType" Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6) CODEC(ZSTD(1)),
    "value" Float64 CODEC(ZSTD(1))
)
ENGINE = MergeTree()
ORDER BY ("ownerId", "factoryId", "sensorId", "timestamp");"""

client.command(raw_sensor_table_query)

# create timestamps table
timestamps_table_query = """CREATE OR REPLACE TABLE iot_analytics.generated_timestamps_script
(
    `timestamp` DateTime64,
    `rowNumber` UInt64
)
ENGINE = MergeTree
ORDER BY rowNumber
PARTITION BY toYYYYMM(timestamp)"""

resp = client.command(timestamps_table_query)
print(resp)