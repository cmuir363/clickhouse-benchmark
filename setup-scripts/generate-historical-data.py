import clickhouse_connect
from dotenv import load_dotenv
import os
from datetime import datetime as dt


load_dotenv()

db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
num_rows_in_dataset = os.getenv('NUM_ROWS_IN_DATASET') #31400000000 corresponds to 1 year of data

client = clickhouse_connect.get_client(host=db_host, port=db_port, username=db_username, password=db_password, interface="https")

#get the max number of threads
max_threads_query = "SELECT value FROM system.settings WHERE name = 'max_threads'"
max_threads = client.query(max_threads_query).first_item["value"]

num_timestamps = int(num_rows_in_dataset)
interval_step = 10000000
# insert the timestamps

truncate_timestamps_query = "TRUNCATE TABLE iot_analytics.generated_timestamps"
client.command(truncate_timestamps_query)

def insert_timestamps_subset_query(min_number, max_number, max_threads):
    insert_timestamps_query = f"""INSERT INTO iot_analytics.generated_timestamps
    WITH total_count AS (SELECT count(*) AS total_metadata_count FROM iot_analytics.iot_metadata)
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
    generate_timestamps_result = client.command(insert_timestamps_query)
    generate_timestamps_query_id = generate_timestamps_result.query_id()
    print(f"Generate timestamps query id: {generate_timestamps_query_id}")

def insert_timestamps(num_timestamps, interval_step, max_threads):
    for i in range(0, num_timestamps, interval_step):
        print(f"Inserting timestamps from {i} to {i + interval_step}")
        timestamp_before = dt.now()
        insert_timestamps_subset_query(i, i + interval_step, max_threads)
        timestamp_after = dt.now()
        print(f"Inserted timestamps from {i} to {i + interval_step} in {(timestamp_after - timestamp_before).total_seconds()} ")
        count_query = "SELECT count(*) FROM iot_analytics.generated_timestamps"
        count = client.query(count_query).first_item
        print(f"Total number of timestamps inserted: {count}")

insert_timestamps(num_timestamps, interval_step, max_threads)



#print("Gathering details on the generated timestamps query")
#generate_timestamps_query_details = client.query(f"SELECT query_duration_ms, memory_usage, read_rows, read_bytes FROM system.query_log WHERE query_id = '68c208f8-cd38-45da-ae6c-00773fcea8be' LIMIT 1")

#SELECT query_duration_ms, memory_usage, read_rows, read_bytes FROM system.query_log WHERE query_id = '68c208f8-cd38-45da-ae6c-00773fcea8be'

#print(f"Generate timestamps query details: {generate_timestamps_query_details.first_item}")

#deleting existing timestamps
truncate_sensor_data_query = "TRUNCATE TABLE iot_analytics.iot_measurements_raw"
client.command(truncate_sensor_data_query)

print("Generating the raw sensor data")
# Generate the raw sensor data
def insert_subset_sensor_data(min_number, max_number, max_threads):
    insert_sensor_data_query = f"""INSERT INTO iot_analytics.iot_measurements_raw
    WITH rows_to_insert AS (SELECT * FROM iot_analytics.generated_timestamps WHERE rowNumber BETWEEN {min_number} AND {max_number - 1})
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
    INNER JOIN iot_analytics.iot_metadata
    ON rows_to_insert.rowNumber = iot_metadata.rowNumber
    SETTINGS
        min_insert_block_size_rows = 322122533,
        min_insert_block_size_bytes = 2048576000,
        max_threads = {max_threads},
        max_insert_threads = {max_threads};"""
    
    generate_sensor_data_result = client.command(insert_sensor_data_query)
    generate_sensor_data_query_id = generate_sensor_data_result.query_id()
    print(f"Generate sensor data query id: {generate_sensor_data_query_id}")

def insert_sensor_data(sensor_data_count, interval_step, max_threads):
    for i in range(0, sensor_data_count, interval_step):
        print(f"Inserting sensor data from {i} to {i + interval_step}")
        timestamp_before = dt.now()
        insert_subset_sensor_data(i, i + interval_step, max_threads)
        timestamp_after = dt.now()
        print(f"Inserted sensor data from {i} to {i + interval_step} in {(timestamp_after - timestamp_before).total_seconds()} ")
        count_query = "SELECT count(*) FROM iot_analytics.iot_measurements_raw"
        count = client.query(count_query).first_item
        print(f"Total number of sensor data inserted: {count}")

def get_sensor_data_count():
    count_query = "SELECT count(*) FROM iot_analytics.iot_metadata"
    count = client.query(count_query).first_item["count()"]
    print(f"Total number of sensor data to be inserted: {count}")
    return count

sensor_data_count = get_sensor_data_count()

insert_sensor_data(sensor_data_count + 1, 100000, max_threads)