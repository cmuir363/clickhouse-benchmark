import logging

from clickhouse_benchmark.client import ClickHouseClient

LOG = logging.getLogger(__name__)


def create_tables(client: ClickHouseClient) -> None:
    LOG.info("Creating tables for %s", client.host)
    # create sensor-metadata table
    sensor_metadata_table_query = """CREATE OR REPLACE TABLE default.iot_metadata
    (
        "rowNumber" UInt64 CODEC(Delta, ZSTD(1)),
        "ownerId" LowCardinality(String) CODEC(ZSTD(1)),
        "factoryId" LowCardinality(String) CODEC(ZSTD(1)),
        "sensorId" String CODEC(ZSTD(1)),
        "sensorType" Enum ('Temperature' = 1, 'Humidity' = 2, 'Pressure' = 3, 'Vibration' = 4, 'Current' = 5, 'Rotation' = 6) CODEC(ZSTD(1))
    )
    ENGINE = MergeTree()
    ORDER BY ("rowNumber", "ownerId", "factoryId", "sensorId")"""

    client.execute(sensor_metadata_table_query)

    # create raw sensor table
    raw_sensor_table_query = """CREATE OR REPLACE TABLE default.iot_measurements_raw
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

    client.execute(raw_sensor_table_query)

    # create timestamps table
    timestamps_table_query = """CREATE OR REPLACE TABLE default.generated_timestamps
    (
        `timestamp` DateTime64,
        `rowNumber` UInt64
    )
    ENGINE = MergeTree
    ORDER BY rowNumber
    PARTITION BY toYYYYMM(timestamp)"""

    resp = client.execute(timestamps_table_query)
    print(resp)
