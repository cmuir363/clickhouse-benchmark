import logging
from datetime import datetime

from clickhouse_benchmark.client import ClickHouseClient

LOG = logging.getLogger(__name__)


def generate_data(client: ClickHouseClient, num_rows_in_dataset: int) -> None:
    LOG.info("Generating historical data for %s", client.host)
    # get the max number of threads
    max_threads_query = "SELECT value FROM system.settings WHERE name = 'max_threads'"
    max_threads: int = client.execute_json(max_threads_query).results[0]["value"]

    num_timestamps = int(num_rows_in_dataset)
    interval_step = 10000000
    # insert the timestamps

    truncate_timestamps_query = "TRUNCATE TABLE default.generated_timestamps"
    client.execute(truncate_timestamps_query)

    insert_timestamps(client, num_timestamps, interval_step, max_threads)

    # deleting existing timestamps
    truncate_sensor_data_query = "TRUNCATE TABLE default.iot_measurements_raw"
    client.execute(truncate_sensor_data_query)

    print("Generating the raw sensor data")

    sensor_data_count = get_sensor_data_count(client)

    insert_sensor_data(client, sensor_data_count + 1, 100000, max_threads)


def insert_timestamps_subset_query(
    client: ClickHouseClient, min_number, max_number, max_threads
) -> None:
    insert_timestamps_query = f"""INSERT INTO default.generated_timestamps
    WITH total_count AS (SELECT count(*) AS total_metadata_count FROM default.iot_metadata)
    SELECT
        now() - INTERVAL number + {min_number} MILLISECOND AS timestamp,
        rand() % (SELECT total_metadata_count FROM total_count)
    FROM
        numbers({max_number}-{min_number})
    SETTINGS
        min_insert_block_size_rows = 322122533,
        min_insert_block_size_bytes = 2048576000,
        max_threads = {max_threads},
        max_insert_threads = {max_threads};"""
    generate_timestamps_result = client.execute(insert_timestamps_query)
    generate_timestamps_query_id = generate_timestamps_result.query_id
    print(f"Generate timestamps query id: {generate_timestamps_query_id}")


def insert_timestamps(
    client: ClickHouseClient, num_timestamps, interval_step, max_threads
) -> None:
    for i in range(0, num_timestamps, interval_step):
        print(f"Inserting timestamps from {i} to {i + interval_step}")
        timestamp_before = datetime.now()
        insert_timestamps_subset_query(client, i, i + interval_step, max_threads)
        timestamp_after = datetime.now()
        print(
            f"Inserted timestamps from {i} to {i + interval_step} in {(timestamp_after - timestamp_before).total_seconds()} "
        )
        count_query = "SELECT count(*) FROM default.generated_timestamps"
        count = client.execute_json(count_query).results[0]["count()"]
        print(f"Total number of timestamps inserted: {count}")


# Generate the raw sensor data
def insert_subset_sensor_data(
    client: ClickHouseClient, min_number, max_number, max_threads
) -> None:
    insert_sensor_data_query = f"""INSERT INTO default.iot_measurements_raw
    WITH rows_to_insert AS (SELECT * FROM default.generated_timestamps WHERE rowNumber BETWEEN {min_number} AND {max_number - 1})
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
    FROM rows_to_insert
    INNER JOIN default.iot_metadata
    ON rows_to_insert.rowNumber = iot_metadata.rowNumber
    SETTINGS
        min_insert_block_size_rows = 322122533,
        min_insert_block_size_bytes = 2048576000,
        max_threads = {max_threads},
        max_insert_threads = {max_threads};"""

    generate_sensor_data_result = client.execute(insert_sensor_data_query)
    generate_sensor_data_query_id = generate_sensor_data_result.query_id
    print(f"Generate sensor data query id: {generate_sensor_data_query_id}")


def insert_sensor_data(
    client: ClickHouseClient, sensor_data_count, interval_step, max_threads
) -> None:
    for i in range(0, sensor_data_count, interval_step):
        print(f"Inserting sensor data from {i} to {i + interval_step}")
        timestamp_before = datetime.now()
        insert_subset_sensor_data(client, i, i + interval_step, max_threads)
        timestamp_after = datetime.now()
        print(
            f"Inserted sensor data from {i} to {i + interval_step} in {(timestamp_after - timestamp_before).total_seconds()} "
        )
        count_query = "SELECT count(*) FROM default.iot_measurements_raw"
        count = client.execute_json(count_query).results[0]["count()"]
        print(f"Total number of sensor data inserted: {count}")


def get_sensor_data_count(client: ClickHouseClient) -> int:
    count_query = "SELECT count(*) FROM default.iot_metadata"
    count = client.execute_json(count_query).results[0]["count()"]
    print(f"Total number of sensor data to be inserted: {count}")
    return int(count)
