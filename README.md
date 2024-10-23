Project to benchmark Clickhouse. Unlike exisiting Clickhouse benchmarks the project has realistic scale (> 30 Billion Rows), contains several realistic Materialized Views to be computed and is ingesting data in realtime using the Kafka Table engine. 

The project is based on an IOT scenario with telemetry data being fed from millions of sensors across thousands of factories. The benchmark starts with 31.54 Billion data points spread out over 365 days.

This forms the basis of the historical data. The real time data is being ingested via the Kafka table engine. The rate of incoming data is controlled via a iot-simulator java app which is available on Docker Hub. 


Dockerfile needs to be fed with a .env file following format of the example.env file to function. Example command is 

```
docker run --env-file .env iot-simulator:latest
```