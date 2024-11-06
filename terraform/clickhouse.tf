// ClickHouse service in the same region
resource "aiven_clickhouse" "clickhouse" {
  project                 = var.aiven_project #aiven_project = "cmuir-demo"
  cloud_name              = var.cloud_region #cloud_region = "aws-eu-central-1"
  plan                    = "startup-16"
  service_name            = "clickhouse-bench"
  maintenance_window_dow  = "sunday"
  maintenance_window_time = "10:00:00"
}


// This will create a `service_kafka-gcp-eu` database with a
// `edge_measurements_raw` using the Kafka ClickHouse Engine.
resource "aiven_service_integration" "clickhouse_kafka_source" {
  project                  = var.aiven_project
  integration_type         = "clickhouse_kafka"
  source_service_name      = aiven_kafka.kafka.service_name
  destination_service_name = aiven_clickhouse.clickhouse.service_name
  clickhouse_kafka_user_config {
    tables {
      name        = "iot_measurements_kafka_table"
      group_name  = "clickhouse-ingestion"
      data_format = "JSONEachRow"
      columns {
        name = "ownerId"
        type = "LowCardinality(String)"
      }
      columns {
        name = "factoryId"
        type = "LowCardinality(String)"
      }
      columns {
        name = "sensorId"
        type = "String"
      }
      columns {
        name = "timestamp"
        type = "DateTime64"
      }
      columns {
        name = "sensorType"
        type = "Enum('TEMPERATURE' = 1, 'HUMIDITY' = 2, 'PRESSURE' = 3, 'VIBRATION' = 4, 'CURRENT' = 5, 'ROTATION' = 6)"
      }
      columns {
        name = "value"
        type = "Float64"
      }
      topics {
        name = aiven_kafka_topic.iot_measurements.topic_name
      }
    }

    tables {
      name        = "iot_high_values_kafka_table"
      group_name  = "clickhouse-high-values"
      data_format = "JSONEachRow"
      columns {
        name = "ownerId"
        type = "LowCardinality(String)"
      }
      columns {
        name = "factoryId"
        type = "LowCardinality(String)"
      }
      columns {
        name = "sensorId"
        type = "String"
      }
      columns {
        name = "timestamp"
        type = "DateTime64"
      }
      columns {
        name = "highValue"
        type = "Float64"
      }
      columns {
        name = "quant90Value"
        type = "Float64"
      }
      topics {
        name = aiven_kafka_topic.iot_measurements_high_values.topic_name
      }
    }

    tables {
      name        = "iot_aggregates_kafka_table"
      group_name  = "clickhouse-aggregates"
      data_format = "JSONEachRow"
      columns {
        name = "ownerId"
        type = "LowCardinality(String)"
      }
      columns {
        name = "factoryId"
        type = "LowCardinality(String)"
      }
      columns {
        name = "sensorId"
        type = "String"
      }
      columns {
        name = "avgValue"
        type = "Float64"
      }
      columns {
        name = "quant99"
        type = "Float64"
      }
      columns {
        name = "quant90"
        type = "Float64"
      }
      topics {
        name = aiven_kafka_topic.iot_aggregates_values.topic_name
      }
    }
  }
      
}



# I can create this table manually in clickhouse but the TF throws an error on the Enum type
#  {
#   "tables": [
#     {
#       "name": "iot_measurements_raw",
#       "columns": [
#           {"name": "ownerId", "type": "LowCardinality(String)"},
#           {"name": "factoryId", "type": "LowCardinality(String)"},
#           {"name": "sensorId", "type": "String"},
#           {"name": "timestamp", "type": "DateTime64(3)"},
#           {"name": "sensorType", "type": "Enum ('TEMPERATURE' = 1, 'HUMIDITY' = 2, 'PRESSURE' = 3, 'VIBRATION' = 4, 'CURRENT' = 5, 'ROTATION' = 6)"},
#           {"name": "value", "type": "Float64"}
#       ],
#       "topics": [{"name": "iot_measurements"}],
#       "data_format": "JSONEachRow",
#       "group_name": "clickhouse-ingestion"
#     }
#   ]
# }

// ClickHouse database that can be used to run analytics over the ingested data
resource "aiven_clickhouse_database" "iot_analytics" {
  project      = var.aiven_project
  service_name = aiven_clickhouse.clickhouse.service_name
  name         = "iot_analytics"
}

