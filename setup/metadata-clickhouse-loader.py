import csv
import clickhouse_connect
from dotenv import load_dotenv
import os


load_dotenv()

db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')

client = clickhouse_connect.get_client(host=db_host, port=db_port, username=db_username, password=db_password, interface="https")

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

insert_query = """
INSERT INTO iot_analytics.iot_metadata (rowNumber, ownerId, factoryId, sensorId, sensorType) 
VALUES
"""

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
        print(rows[i])
        i += 1
    
    if rows:
        final_query = insert_query + ",".join(rows)
        
client.command(final_query)
print("CSV data loaded into ClickHouse successfully.")