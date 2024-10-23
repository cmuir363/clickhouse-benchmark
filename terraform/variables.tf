variable "aiven_api_token" {
  description = "Aiven console API token"
  type        = string
}

variable "aiven_project" {
  description = "Aiven project name"
  type        = string
}

variable "cloud_region" {
  description = "Cloud region to deploy the services"
  type        = string
}

variable "kafka_plan_size" {
  description = "Kafka plan size"
  type        = string
}

variable clickhouse_plan_size {
  description = "ClickHouse plan size"
  type        = string
}