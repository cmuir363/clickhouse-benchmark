Project to benchmark Clickhouse. Unlike exisiting Clickhouse benchmarks the project has realistic scale (> 30 Billion Rows), contains several realistic Materialized Views to be computed and is ingesting data in realtime using the Kafka Table engine. 

The project is based on an IOT scenario with telemetry data being fed from millions of sensors across thousands of factories. The benchmark starts with 31.54 Billion data points spread out over 365 days.

This forms the basis of the historical data. The real time data is being ingested via the Kafka table engine. The rate of incoming data is controlled via a iot-simulator java app which is available on Docker Hub. 


### Structure
The main database in use is ```iot_analytics``` which will have the following tables within it:

```
    ┌─name──────────────────────────────────────────────┐
 1. │ generated_timestamps                              │
 2. │ high_value_alerts                                 │
 3. │ high_value_alerts_kafka_table_mv                  │
 4. │ high_value_alerts_mv                              │
 5. │ iot_flow_gcs                                      │
 6. │ iot_measurements_raw                              │
 7. │ iot_measurements_raw_mv                           │
 8. │ iot_metadata                                      │
 9. │ measurements_aggregates_per_device                │
10. │ measurements_aggregates_per_device_kafka_table_mv │
11. │ measurements_aggregates_per_device_mv             │
12. │ num_measurements_per_sensor                       │
13. │ num_measurements_per_sensor_mv                    │
14. │ quantile_dict                                     │
    └────────────────────────────────
```


### Generating the Data
There is a startup script which will create the tables, Materialized Views and required dictionaries. It will then pull a sensor metadata file from GCS which will be loaded into clickhouse and used to generate the sensor data.



To use the startup script ensure that the .env file in the setup-scripts directory is created according to the example.env file in the setup-scripts directory.

Make the setup script executable:
```
chmod +x check_env.sh
```
Start the script:
```
./startup.sh
```


### Starting the Stream
Dockerfile needs to be fed with a .env file following format of the example.env file to function. Example command is 

```
docker run --env-file .env cmuir363/iot-simulator:latest
```