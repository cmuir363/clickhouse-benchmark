import csv
import clickhouse_connect
from dotenv import load_dotenv
import os
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials


load_dotenv()

db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')

client = clickhouse_connect.get_client(host=db_host, port=db_port, username=db_username, password=db_password, interface="https", connect_timeout=10000, send_receive_timeout=10000)

def map_sensor_type(sensor_type):
    sensor_type_mapping = {
        'Temperature': 1,
        'Humidity': 2,
        'Pressure': 3,
        'Vibration': 4,
        'Current': 5,
        'Rotation': 6
    }
    return sensor_type_mapping.get(sensor_type, None)

def check_if_sensors_metadata_file_exists():
    return os.path.isfile('../iot-simulator/src/main/resources/sensors.csv')

def retrieve_sensors_metadata_file():
    # need to set a dumm project and use anon creds to donwload a public file
    storage_client = storage.Client(credentials=AnonymousCredentials(), project="dummy-project")
    bucket = storage_client.bucket("cmuir-clickhouse-demo")
    sensors_file = bucket.blob("sensors.csv")
    sensors_file.download_to_filename("../iot-simulator/src/main/resources/sensors.csv")


truncate_query = "TRUNCATE TABLE iot_analytics.iot_metadata"
client.command(truncate_query)

insert_query = """
INSERT INTO iot_analytics.iot_metadata (rowNumber, ownerId, factoryId, sensorId, sensorType) 
VALUES
"""

if not check_if_sensors_metadata_file_exists():
    print("Sensors metadata file does not exist. Retrieving sensors metadata file from GCS.")
    retrieve_sensors_metadata_file()
else:
    print("Sensors metadata file already exists.")
    
with open('../iot-simulator/src/main/resources/sensors.csv', mode='r') as file:
    reader = csv.DictReader(file)
    
    rows = []
    i = 0
    for row in reader:
        # Map the sensorType to the ENUM values
        sensor_type_enum = map_sensor_type(row['sensorType'])
        if sensor_type_enum is None:
            continue 
        
        rows.append(f"('{i}', '{row['ownerId']}', '{row['factoryId']}', '{row['sensorId']}', {sensor_type_enum})")
        i += 1
    
    if rows:
        final_query = insert_query + ",".join(rows)

print("Inserting sensors metadata into ClickHouse.")
client.command(final_query)
print("CSV data loaded into ClickHouse successfully.")

