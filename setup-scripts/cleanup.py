import clickhouse_connect
from dotenv import load_dotenv
import os


load_dotenv()

db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')

client = clickhouse_connect.get_client(host=db_host, port=db_port, username=db_username, password=db_password, interface="https")


drop_iot_measurements_raw_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.iot_measurements_raw_mv"""
client.command(drop_iot_measurements_raw_mv_query)

drop_iot_measurements_raw_query = f"""DROP TABLE IF EXISTS iot_analytics.iot_measurements_raw"""
client.command(drop_iot_measurements_raw_query)

drop_iot_measurements_raw_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.iot_measurements_raw_mv"""

drop_high_value_alerts_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.high_value_alerts_mv"""
client.command(drop_high_value_alerts_mv_query)

drop_high_value_alerts_query = f"""DROP TABLE IF EXISTS iot_analytics.high_value_alerts"""
client.command(drop_high_value_alerts_query)

drop_measurements_aggregates_per_device_query = f"""DROP TABLE IF EXISTS iot_analytics.measurements_aggregates_per_device"""
client.command(drop_measurements_aggregates_per_device_query)

drop_measurements_aggregates_per_device_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.measurements_aggregates_per_device_mv"""
client.command(drop_measurements_aggregates_per_device_mv_query)

drop_quantile_dict_query = f"""DROP DICTIONARY IF EXISTS iot_analytics.quantile_dict"""
client.command(drop_quantile_dict_query)

drop_num_measurements_per_sensor_query = f"""DROP TABLE IF EXISTS iot_analytics.num_measurements_per_sensor"""
client.command(drop_num_measurements_per_sensor_query)

drop_num_measurements_per_sensor_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.num_measurements_per_sensor_mv"""
client.command(drop_num_measurements_per_sensor_mv_query)

drop_measurements_aggregates_per_device_kafka_table_mv = f"""DROP VIEW IF EXISTS iot_analytics.measurements_aggregates_per_device_kafka_table_mv"""
client.command(drop_measurements_aggregates_per_device_kafka_table_mv)

drop_iot_metadata_query = f"""DROP TABLE IF EXISTS iot_analytics.iot_metadata"""
client.command(drop_iot_metadata_query)

drop_measurements_aggregates_per_device_updated_query = f"""DROP TABLE IF EXISTS iot_analytics.measurements_aggregates_per_device_updated"""
client.command(drop_measurements_aggregates_per_device_updated_query)

drop_generated_timestamps_query = f"""DROP TABLE IF EXISTS iot_analytics.generated_timestamps"""
client.command(drop_generated_timestamps_query)

drop_high_value_alerts_mv_query = f"""DROP VIEW IF EXISTS iot_analytics.high_value_alerts_mv"""
client.command(drop_high_value_alerts_mv_query)