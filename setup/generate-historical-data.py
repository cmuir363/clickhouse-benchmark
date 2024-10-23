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

#get the max number of threads
max_threads_query = "SELECT value FROM system.settings WHERE name = 'max_threads'"
max_threads = client.query(max_threads_query).first_item["value"]

num_timstamps = 1000 #30000000000
# insert the timestamps
insert_timestamps_query = f"""INSERT INTO iot_analytics.generated_timestamps_script
SELECT 
    now() - INTERVAL number MILLISECOND AS timestamp,
    rand() % (SELECT count(*) FROM iot_analytics.iot_metadata)
FROM 
    numbers(0, {num_timstamps})
SETTINGS
    min_insert_block_size_rows = 322122533,
    min_insert_block_size_bytes = 2048576000,
    max_threads = {max_threads},
    max_insert_threads = {max_threads};"""

generate_timestamps_result = client.command(insert_timestamps_query)
generate_timestamps_query_id = generate_timestamps_result.query_id()
print(f"Generate timestamps query id: {generate_timestamps_query_id}")

#print("Gathering details on the generated timestamps query")
#generate_timestamps_query_details = client.query(f"SELECT query_duration_ms, memory_usage, read_rows, read_bytes FROM system.query_log WHERE query_id = '68c208f8-cd38-45da-ae6c-00773fcea8be' LIMIT 1")

#SELECT query_duration_ms, memory_usage, read_rows, read_bytes FROM system.query_log WHERE query_id = '68c208f8-cd38-45da-ae6c-00773fcea8be'

print(f"Generate timestamps query details: {generate_timestamps_query_details.first_item}")

# Generate the raw sensor data
insert_sensor_data_query = f"""INSERT INTO iot_analytics.iot_measurements_raw_temp_script 
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
FROM iot_analytics.generated_timestamps_script
JOIN iot_analytics.iot_metadata
ON generated_timestamps_script.rowNumber = iot_metadata.rowNumber
SETTINGS
    min_insert_block_size_rows = 322122533,
    min_insert_block_size_bytes = 2048576000,
    max_threads = {max_threads},
    max_insert_threads = {max_threads};"""

client.command(insert_sensor_data_query)
