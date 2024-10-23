// Kafka service on GCP eu-west
resource "aiven_kafka" "kafka" {
  project                 = var.aiven_project
  cloud_name              = var.cloud_region
  plan                    = "business-4"
  service_name            = "kafka-for-clickhouse-bench"
  maintenance_window_dow  = "sunday"
  maintenance_window_time = "10:00:00"

  // Enable kafka REST to view and send messages from the Console
  kafka_user_config {
    kafka_rest = true

    public_access {
      kafka_rest = true
    }
  }
}

// Kafka topic used to ingest edge measurements from the IoT devices fleet
resource "aiven_kafka_topic" "iot_measurements" {
  project                = var.aiven_project
  service_name           = aiven_kafka.kafka.service_name
  topic_name             = "iot_measurements"
  partitions             = 50
  replication            = 3
  termination_protection = false

  config {
    cleanup_policy                 = "delete"
    retention_ms                   = "4800000"
  }
}
