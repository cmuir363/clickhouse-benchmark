import csv
import logging
import os
import threading
from pathlib import Path

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage  # type: ignore

from clickhouse_benchmark.client import ClickHouseClient

LOG = logging.getLogger(__name__)

LOCK_DOWNLOAD_FILE = threading.Lock()

# We can saturate bandwidth more with multiple threads here
LOCK_UPLOAD_FILE = threading.Semaphore(3)


def generate_metadata(client: ClickHouseClient) -> None:
    LOG.info("Generating metadata for %s", client.host)

    def map_sensor_type(sensor_type: str) -> int | None:
        sensor_type_mapping = {
            "Temperature": 1,
            "Humidity": 2,
            "Pressure": 3,
            "Vibration": 4,
            "Current": 5,
            "Rotation": 6,
        }
        return sensor_type_mapping.get(sensor_type, None)

    def retrieve_sensors_metadata_file() -> None:
        if Path("sensors.csv").exists():
            return
        if not Path("sensors_prev.csv").exists():
            creds = AnonymousCredentials()  # type: ignore
            # need to set a dumm project and use anon creds to donwload a public file
            storage_client = storage.Client(credentials=creds, project="dummy-project")
            bucket = storage_client.bucket("cmuir-clickhouse-demo")
            sensors_file = bucket.blob("sensors.csv")
            sensors_file.download_to_filename("sensors_prev.csv")
        with open("sensors_prev.csv", "r") as in_, open(
            "sensors.csv", "w", newline=""
        ) as out:
            reader = csv.DictReader(in_)
            writer = csv.DictWriter(
                out,
                fieldnames=[
                    "rowNumber",
                    "ownerId",
                    "factoryId",
                    "sensorId",
                    "sensorType",
                ],
            )
            writer.writeheader()
            try:
                for i, row in enumerate(reader):
                    sensor_type_enum = map_sensor_type(row["sensorType"])
                    if sensor_type_enum is not None:
                        writer.writerow(
                            {
                                "rowNumber": i,
                                "ownerId": row["ownerId"],
                                "factoryId": row["factoryId"],
                                "sensorId": row["sensorId"],
                                "sensorType": sensor_type_enum,
                            }
                        )
            except RuntimeError:
                os.remove("sensors.csv")

    truncate_query = "TRUNCATE TABLE default.iot_metadata"
    client.execute_no_result(truncate_query)

    with LOCK_DOWNLOAD_FILE:
        retrieve_sensors_metadata_file()

    with LOCK_UPLOAD_FILE:
        insert_query = """
        INSERT INTO default.iot_metadata (rowNumber, ownerId, factoryId, sensorId, sensorType)
        FORMAT CSVWithNames
        """

        print("Inserting sensors metadata into ClickHouse.")
        client.execute_no_result(insert_query, input=Path("sensors.csv"))
        print("CSV data loaded into ClickHouse successfully.")
