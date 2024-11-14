import csv
import logging
import os
import threading

from clickhouse_connect.driver.client import Client
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

LOG = logging.getLogger(__name__)


def generate_metadata(client: Client) -> None:
    LOG.info("Generating metadata for %s", client.uri)

    def map_sensor_type(sensor_type):
        sensor_type_mapping = {
            "Temperature": 1,
            "Humidity": 2,
            "Pressure": 3,
            "Vibration": 4,
            "Current": 5,
            "Rotation": 6,
        }
        return sensor_type_mapping.get(sensor_type, None)

    def check_if_sensors_metadata_file_exists():
        return os.path.isfile("sensors.csv")

    def retrieve_sensors_metadata_file():
        # need to set a dumm project and use anon creds to donwload a public file
        storage_client = storage.Client(
            credentials=AnonymousCredentials(), project="dummy-project"
        )
        bucket = storage_client.bucket("cmuir-clickhouse-demo")
        sensors_file = bucket.blob("sensors.csv")
        sensors_file.download_to_filename("sensors.csv")

    truncate_query = "TRUNCATE TABLE default.iot_metadata"
    client.command(truncate_query)

    insert_query = """
    INSERT INTO default.iot_metadata (rowNumber, ownerId, factoryId, sensorId, sensorType)
    VALUES
    """
    with threading.Lock():
        if not check_if_sensors_metadata_file_exists():
            print(
                "Sensors metadata file does not exist. Retrieving sensors metadata file from GCS."
            )
            retrieve_sensors_metadata_file()
        else:
            print("Sensors metadata file already exists.")

    with open("sensors.csv", mode="r") as file:
        reader = csv.DictReader(file)

        rows = []
        i = 0
        for row in reader:
            # Map the sensorType to the ENUM values
            sensor_type_enum = map_sensor_type(row["sensorType"])
            if sensor_type_enum is None:
                continue

            rows.append(
                f"('{i}', '{row['ownerId']}', '{row['factoryId']}', '{row['sensorId']}', {sensor_type_enum})"
            )
            i += 1

        if rows:
            final_query = insert_query + ",".join(rows)

    print("Inserting sensors metadata into ClickHouse.")
    client.command(final_query)
    print("CSV data loaded into ClickHouse successfully.")
