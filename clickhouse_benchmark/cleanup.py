from clickhouse_connect.driver.client import Client


def cleanup(client: Client) -> None:
    drop_iot_measurements_raw_mv_query = (
        """DROP VIEW IF EXISTS default.iot_measurements_raw_mv"""
    )
    client.command(drop_iot_measurements_raw_mv_query)

    drop_iot_measurements_raw_query = (
        """DROP TABLE IF EXISTS default.iot_measurements_raw"""
    )
    client.command(drop_iot_measurements_raw_query)

    drop_high_value_alerts_query = """DROP TABLE IF EXISTS default.high_value_alerts"""
    client.command(drop_high_value_alerts_query)

    drop_measurements_aggregates_per_device_query = (
        """DROP TABLE IF EXISTS default.measurements_aggregates_per_device"""
    )
    client.command(drop_measurements_aggregates_per_device_query)

    drop_quantile_dict_query = """DROP DICTIONARY IF EXISTS default.quantile_dict"""
    client.command(drop_quantile_dict_query)

    drop_num_measurements_per_sensor_query = (
        """DROP TABLE IF EXISTS default.num_measurements_per_sensor"""
    )
    client.command(drop_num_measurements_per_sensor_query)

    drop_iot_metadata_query = """DROP TABLE IF EXISTS default.iot_metadata"""
    client.command(drop_iot_metadata_query)

    drop_measurements_aggregates_per_device_updated_query = (
        """DROP TABLE IF EXISTS default.measurements_aggregates_per_device_updated"""
    )
    client.command(drop_measurements_aggregates_per_device_updated_query)

    drop_generated_timestamps_query = (
        """DROP TABLE IF EXISTS default.generated_timestamps"""
    )
    client.command(drop_generated_timestamps_query)
